#!/usr/bin/env python3
# pylint: disable=missing-docstring,not-an-iterable,too-many-locals,too-many-arguments,invalid-name,too-many-return-statements,too-many-branches,len-as-condition,too-many-nested-blocks,wrong-import-order,duplicate-code, anomalous-backslash-in-string, too-many-statements, singleton-comparison, consider-using-in

import singer
import datetime
import time
import decimal
from singer import utils, get_bookmark
import singer.metadata as metadata
import tap_postgres.db as post_db
import tap_postgres.sync_strategies.common as sync_common
from dateutil.parser import parse
import psycopg2
import copy
from select import select
from functools import reduce
import json
import re

LOGGER = singer.get_logger()

UPDATE_BOOKMARK_PERIOD = 10000

def get_pg_version(cur):
    cur.execute("SELECT setting::int AS version FROM pg_settings WHERE name='server_version_num'")
    version = cur.fetchone()[0]
    LOGGER.debug("Detected PostgreSQL version: %s", version)
    return version

def lsn_to_int(f_lsn):
    """Convert pg_lsn to int"""
    file, index = f_lsn.split('/')
    return (int(file, 16)  << 32) + int(index, 16)

def fetch_current_lsn(conn_config):
    with post_db.open_connection(conn_config, False) as conn:
        with conn.cursor() as cur:
            # Make sure PostgreSQL version is 9.4 or higher
            version = get_pg_version(cur)

            # Do not allow minor versions with PostgreSQL BUG #15114
            if (version >= 110000) and (version < 110002):
                raise Exception('PostgreSQL upgrade required to minor version 11.2')
            elif (version >= 100000) and (version < 100007):
                raise Exception('PostgreSQL upgrade required to minor version 10.7')
            elif (version >= 90600) and (version < 90612):
                raise Exception('PostgreSQL upgrade required to minor version 9.6.12')
            elif (version >= 90500) and (version < 90516):
                raise Exception('PostgreSQL upgrade required to minor version 9.5.16')
            elif (version >= 90400) and (version < 90421):
                raise Exception('PostgreSQL upgrade required to minor version 9.4.21')
            elif (version < 90400):
                raise Exception('Logical replication not supported before PostgreSQL 9.4')

            # Use version specific lsn command
            if version >= 100000:
                cur.execute("SELECT pg_current_wal_lsn() AS current_lsn")
            elif version >= 90400:
                cur.execute("SELECT pg_current_xlog_location() AS current_lsn")
            else:
                raise Exception('Logical replication not supported before PostgreSQL 9.4')

            current_lsn = cur.fetchone()[0]
            return lsn_to_int(current_lsn)

def add_automatic_properties(stream, conn_config):
    stream['schema']['properties']['_sdc_deleted_at'] = {'type' : ['null', 'string'], 'format' :'date-time'}
    if conn_config.get('debug_lsn'):
        LOGGER.debug('debug_lsn is ON')
        stream['schema']['properties']['_sdc_lsn'] = {'type' : ['null', 'string']}
    else:
        LOGGER.debug('debug_lsn is OFF')

    return stream

def get_stream_version(tap_stream_id, state):
    stream_version = singer.get_bookmark(state, tap_stream_id, 'version')

    if stream_version is None:
        raise Exception("version not found for log miner {}".format(tap_stream_id))

    return stream_version

def tuples_to_map(accum, t):
    accum[t[0]] = t[1]
    return accum

def create_hstore_elem(conn_info, elem):
    with post_db.open_connection(conn_info) as conn:
        with conn.cursor() as cur:
            sql = """SELECT hstore_to_array('{}')""".format(elem)
            cur.execute(sql)
            res = cur.fetchone()[0]
            hstore_elem = reduce(tuples_to_map, [res[i:i + 2] for i in range(0, len(res), 2)], {})
            return hstore_elem

def create_array_elem(elem, sql_datatype, conn_info):
    if elem is None:
        return None

    with post_db.open_connection(conn_info) as conn:
        with conn.cursor() as cur:
            if sql_datatype == 'bit[]':
                cast_datatype = 'boolean[]'
            elif sql_datatype == 'boolean[]':
                cast_datatype = 'boolean[]'
            elif sql_datatype == 'character varying[]':
                cast_datatype = 'character varying[]'
            elif sql_datatype == 'cidr[]':
                cast_datatype = 'cidr[]'
            elif sql_datatype == 'citext[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'date[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'double precision[]':
                cast_datatype = 'double precision[]'
            elif sql_datatype == 'hstore[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'integer[]':
                cast_datatype = 'integer[]'
            elif sql_datatype == 'inet[]':
                cast_datatype = 'inet[]'
            elif sql_datatype == 'json[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'jsonb[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'macaddr[]':
                cast_datatype = 'macaddr[]'
            elif sql_datatype == 'money[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'numeric[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'real[]':
                cast_datatype = 'real[]'
            elif sql_datatype == 'smallint[]':
                cast_datatype = 'smallint[]'
            elif sql_datatype == 'text[]':
                cast_datatype = 'text[]'
            elif sql_datatype in ('time without time zone[]', 'time with time zone[]'):
                cast_datatype = 'text[]'
            elif sql_datatype in ('timestamp with time zone[]', 'timestamp without time zone[]'):
                cast_datatype = 'text[]'
            elif sql_datatype == 'uuid[]':
                cast_datatype = 'text[]'

            else:
                #custom datatypes like enums
                cast_datatype = 'text[]'

            sql = """SELECT $stitch_quote${}$stitch_quote$::{}""".format(elem, cast_datatype)
            cur.execute(sql)
            res = cur.fetchone()[0]
            return res

#pylint: disable=too-many-branches,too-many-nested-blocks
def selected_value_to_singer_value_impl(elem, og_sql_datatype, conn_info):
    sql_datatype = og_sql_datatype.replace('[]', '')

    if elem is None:
        return elem
    if sql_datatype == 'timestamp without time zone':
        return parse(elem).isoformat() + '+00:00'
    if sql_datatype == 'timestamp with time zone':
        if isinstance(elem, datetime.datetime):
            return elem.isoformat()
        else:
            return parse(elem).isoformat()
    if sql_datatype == 'date':
        if  isinstance(elem, datetime.date):
            #logical replication gives us dates as strings UNLESS they from an array
            return elem.isoformat() + 'T00:00:00+00:00'
        return parse(elem).isoformat() + "+00:00"
    if sql_datatype == 'time with time zone':
        return parse(elem).isoformat().split('T')[1]
    if sql_datatype == 'bit':
        #for arrays, elem will == True
        #for ordinary bits, elem will == '1'
        return elem == '1' or elem == True
    if sql_datatype == 'boolean':
        return elem
    if sql_datatype == 'hstore':
        return create_hstore_elem(conn_info, elem)
    if 'numeric' in sql_datatype:
        return decimal.Decimal(elem)
    if isinstance(elem, int):
        return elem
    if isinstance(elem, float):
        return elem
    if isinstance(elem, str):
        return elem

    raise Exception("do not know how to marshall value of type {}".format(elem.__class__))

def selected_array_to_singer_value(elem, sql_datatype, conn_info):
    if isinstance(elem, list):
        return list(map(lambda elem: selected_array_to_singer_value(elem, sql_datatype, conn_info), elem))
    else:
        return selected_value_to_singer_value_impl(elem, sql_datatype, conn_info)

def selected_value_to_singer_value(elem, sql_datatype, conn_info):
    #are we dealing with an array?
    if sql_datatype.find('[]') > 0:
        cleaned_elem = create_array_elem(elem, sql_datatype, conn_info)
        return list(map(lambda elem: selected_array_to_singer_value(elem, sql_datatype, conn_info), (cleaned_elem or [])))
    else:
        return selected_value_to_singer_value_impl(elem, sql_datatype, conn_info)

def row_to_singer_message(stream, row, version, columns, time_extracted, md_map, conn_info):
    row_to_persist = ()
    md_map[('properties', '_sdc_deleted_at')] = {'sql-datatype' : 'timestamp with time zone'}
    md_map[('properties', '_sdc_lsn')] = {'sql-datatype' : "character varying"}

    for idx, elem in enumerate(row):
        sql_datatype = md_map.get(('properties', columns[idx])).get('sql-datatype')

        if not sql_datatype:
            LOGGER.info("No sql-datatype found for stream %s: %s", stream, columns[idx])
            raise Exception("Unable to find sql-datatype for stream {}".format(stream))

        cleaned_elem = selected_value_to_singer_value(elem, sql_datatype, conn_info)
        row_to_persist += (cleaned_elem,)

    rec = dict(zip(columns, row_to_persist))

    return singer.RecordMessage(
        stream=post_db.calculate_destination_stream_name(stream, md_map),
        record=rec,
        version=version,
        time_extracted=time_extracted)

def consume_message(streams, state, msg, time_extracted, conn_info, end_lsn):
    # Strip leading comma generated by write-in-chunks and parse valid JSON
    try:
        payload = json.loads(msg.payload.lstrip(','))
    except:
        return state

    lsn = msg.data_start

    streams_lookup = {}
    for s in streams:
        streams_lookup[s['tap_stream_id']] = s

    tap_stream_id = post_db.compute_tap_stream_id(conn_info['dbname'], payload['schema'], payload['table'])
    if streams_lookup.get(tap_stream_id) is None:
        return state

    target_stream = streams_lookup[tap_stream_id]
    stream_version = get_stream_version(target_stream['tap_stream_id'], state)
    stream_md_map = metadata.to_map(target_stream['metadata'])

    desired_columns = [c for c in target_stream['schema']['properties'].keys() if sync_common.should_sync_column(stream_md_map, c)]

    if payload['kind'] == 'insert':
        col_names = []
        col_vals = []
        for idx, col in enumerate(payload['columnnames']):
            if col in set(desired_columns):
                col_names.append(col)
                col_vals.append(payload['columnvalues'][idx])

        col_names = col_names + ['_sdc_deleted_at']
        col_vals = col_vals + [None]
        if conn_info.get('debug_lsn'):
            col_names = col_names + ['_sdc_lsn']
            col_vals = col_vals + [str(lsn)]
        record_message = row_to_singer_message(target_stream, col_vals, stream_version, col_names, time_extracted, stream_md_map, conn_info)

    elif payload['kind'] == 'update':
        col_names = []
        col_vals = []
        for idx, col in enumerate(payload['columnnames']):
            if col in set(desired_columns):
                col_names.append(col)
                col_vals.append(payload['columnvalues'][idx])

        col_names = col_names + ['_sdc_deleted_at']
        col_vals = col_vals + [None]

        if conn_info.get('debug_lsn'):
            col_vals = col_vals + [str(lsn)]
            col_names = col_names + ['_sdc_lsn']
        record_message = row_to_singer_message(target_stream, col_vals, stream_version, col_names, time_extracted, stream_md_map, conn_info)

    elif payload['kind'] == 'delete':
        col_names = []
        col_vals = []
        for idx, col in enumerate(payload['oldkeys']['keynames']):
            if col in set(desired_columns):
                col_names.append(col)
                col_vals.append(payload['oldkeys']['keyvalues'][idx])

        col_names = col_names + ['_sdc_deleted_at']
        col_vals = col_vals  + [singer.utils.strftime(time_extracted)]
        if conn_info.get('debug_lsn'):
            col_vals = col_vals + [str(lsn)]
            col_names = col_names + ['_sdc_lsn']
        record_message = row_to_singer_message(target_stream, col_vals, stream_version, col_names, time_extracted, stream_md_map, conn_info)

    else:
        raise Exception("unrecognized replication operation: {}".format(payload['kind']))

    singer.write_message(record_message)
    state = singer.write_bookmark(state, target_stream['tap_stream_id'], 'lsn', lsn)

    if msg.data_start > end_lsn:
        raise Exception("incorrectly attempting to flush an lsn({}) > end_lsn({})".format(msg.data_start, end_lsn))

    # Flush Postgres log up to lsn received in current run
    # This is the behaviour of the original tap-progres
    # The Pipelinewise version flushes only at the start of the next run
    # This is to ensure the data has been comitted on the destination
    # LOGGER.info("Sending flush_lsn = {} to source server".format(msg.data_start))
    # msg.cursor.send_feedback(flush_lsn=msg.data_start)

    return state

def locate_replication_slot(conn_info):
    with post_db.open_connection(conn_info, False) as conn:
        with conn.cursor() as cur:
            db_specific_slot = "stitch_{}".format(conn_info['dbname'])
            cur.execute("SELECT * FROM pg_replication_slots WHERE slot_name = %s AND plugin = %s", (db_specific_slot, 'wal2json'))
            if len(cur.fetchall()) == 1:
                LOGGER.info("Using pg_replication_slot %s", db_specific_slot)
                return db_specific_slot
            else:
                raise Exception("Unable to find replication slot {} with wal2json".format(db_specific_slot))


def sync_tables(conn_info, logical_streams, state, end_lsn):
    comitted_lsn = min([get_bookmark(state, s['tap_stream_id'], 'lsn') for s in logical_streams])
    start_lsn = comitted_lsn
    time_extracted = utils.now()
    slot = locate_replication_slot(conn_info)
    last_lsn_processed = None
    logical_poll_total_seconds = conn_info['logical_poll_total_seconds'] or 600
    poll_interval = 10
    poll_timestamp = datetime.datetime.now()
    wal_received_timestamp = datetime.datetime.now()
    wal_entries_processed = 0

    for s in logical_streams:
        sync_common.send_schema_message(s, ['lsn'])

    with post_db.open_connection(conn_info, True) as conn:
        with conn.cursor() as cur:
            LOGGER.info("Starting Logical Replication for %s(%s): %s -> %s. logical_poll_total_seconds: %s", list(map(lambda s: s['tap_stream_id'], logical_streams)), slot, start_lsn, end_lsn, logical_poll_total_seconds)
            try:
                cur.start_replication(slot_name=slot, decode=True, start_lsn=start_lsn, options={'write-in-chunks': 1})
            except psycopg2.ProgrammingError:
                raise Exception("unable to start replication with logical replication slot {}".format(slot))

            # Flush Postgres log up to lsn saved in state file from previous run
            LOGGER.info("Sending flush_lsn = {} to source server".format(comitted_lsn))
            cur.send_feedback(flush_lsn=comitted_lsn, reply=True)
            # Give source server a short rest
            time.sleep(1)


            while True:
                # Disconnect when no data received for logical_poll_total_seconds
                # needs to be long enough to wait for the largest single wal payload to avoid unplanned timeouts
                poll_duration = (datetime.datetime.now() - wal_received_timestamp).total_seconds()
                if poll_duration > logical_poll_total_seconds:
                    LOGGER.info("Breaking after %s seconds of polling with no data", poll_duration)
                    break

                msg = cur.read_message()
                if msg:
                    wal_received_timestamp = datetime.datetime.now()
                    if msg.data_start > end_lsn:
                        LOGGER.info("Gone past end_lsn %s for run. breaking", end_lsn)
                        break

                    state = consume_message(logical_streams, state, msg, time_extracted, conn_info, end_lsn)
                    last_lsn_processed = msg.data_start
                    wal_entries_processed = wal_entries_processed + 1
                    if wal_entries_processed >= UPDATE_BOOKMARK_PERIOD:
                        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
                        wal_entries_processed = 0

                # When data is received, and when data is not received, a keep-alive poll needs to be returned to PostgreSQL
                if datetime.datetime.now() >= (poll_timestamp + datetime.timedelta(seconds=poll_interval)):
                    LOGGER.info("{} : Sending keep-alive to source server - waiting for wal entries".format(datetime.datetime.utcnow()))
                    cur.send_feedback()
                    poll_timestamp = datetime.datetime.now()

    if last_lsn_processed:
        for s in logical_streams:
            LOGGER.info("updating bookmark for stream %s to last_lsn_processed %s", s['tap_stream_id'], last_lsn_processed)
            state = singer.write_bookmark(state, s['tap_stream_id'], 'lsn', last_lsn_processed)

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
    return state