import logging
#import psycopg2
import vertica_python #importing verticas connector

import psycopg2.extras
import json
import gzip

from typing import List

from . import utils
from .transform_utils import SQLFlavor, TransformationHelper

LOGGER = logging.getLogger(__name__)


# pylint: disable=missing-function-docstring,no-self-use,too-many-arguments
class FastSyncTargetVertica:
    """
    Common functions for fastsync to Vertica
    """

    EXTRACTED_AT_COLUMN = '_SDC_EXTRACTED_AT'
    BATCHED_AT_COLUMN = '_SDC_BATCHED_AT'
    DELETED_AT_COLUMN = '_SDC_DELETED_AT'

    def __init__(self, connection_config, transformation_config=None):
        self.connection_config = connection_config
        self.transformation_config = transformation_config

    def open_connection(self):
        #only the dbname is changed for vertica 
        conn_string = "host='{}' database='{}' user='{}' password='{}' port='{}'".format(
            self.connection_config['host'],
            self.connection_config['database'], #dbname to database
            self.connection_config['user'],
            self.connection_config['password'],
            self.connection_config['port']
        )

        #for ssl make it false by default
        if 'ssl' in self.connection_config:
            conn_string += " sslmode='false'"

        return vertica_python.connect(dict(conn_string))

    def query(self, query, params=None):
        LOGGER.info('Running query: %s', query)
        with self.open_connection() as connection:
            with connection.cursor() as cur:
                cur.execute(query, params)

                if cur.rowcount > 0 and cur.description:
                    return cur.fetchall()

                return []

    def create_schema(self, schema):
        sql = 'CREATE SCHEMA IF NOT EXISTS {}'.format(schema)       #this remains the same
        self.query(sql)

    def create_schemas(self, tables):
        schemas = utils.get_target_schemas(self.connection_config, tables) #also this remains the same 
        for schema in schemas:
            self.create_schema(schema)

    def drop_table(self, target_schema, table_name, is_temporary=False):
        table_dict = utils.tablename_to_dict_vertica(table_name)    #using vertica tailored utils
        target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')

        sql = 'DROP TABLE IF EXISTS {}'.format(target_schema, target_table)
        self.query(sql)

    def create_table(self, target_schema: str, table_name: str, columns: List[str], primary_key: List[str],
                     is_temporary: bool = False, sort_columns=False):

        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')

        # skip the EXTRACTED, BATCHED and DELETED columns in case they exist because they gonna be added later
        columns = [c for c in columns if not (c.startswith(self.EXTRACTED_AT_COLUMN) or
                                              c.startswith(self.BATCHED_AT_COLUMN) or
                                              c.startswith(self.DELETED_AT_COLUMN))]

        columns += [f'{self.EXTRACTED_AT_COLUMN} TIMESTAMP',    #here changing the types
                    f'{self.BATCHED_AT_COLUMN} TIMESTAMP',      #these thre columns are coming created in taps.
                    f'{self.DELETED_AT_COLUMN} VARCHAR']

        # We need the sort the columns for some taps (for now tap-s3-csv)

        if sort_columns:
            columns = columns.sort()

        sql_columns = ','.join(columns)

        # when primary keys exists what to do.
        if primary_key:
            sql_primary_keys = ','.join(primary_key)
        else:
            sql_primary_keys = None

        if primary_key:
            # @TODO - Confirm Create Table if not exists and other formatting here. 
            sql = f'CREATE TABLE IF NOT EXISTS {target_schema}."{target_table}" (' \
                f'{sql_columns}' \
                f'{f", PRIMARY KEY ({sql_primary_keys}))" if primary_key else ")"}'
        else:
            sql = f'CREATE TABLE IF NOT EXISTS {target_schema}."{target_table}" (' \
                f'{sql_columns}'

        self.query(sql)

    def copy_to_table(self, filepath, target_schema: str, table_name: str, size_bytes: int,
                      is_temporary: bool = False, skip_csv_header: bool = False):
        LOGGER.info('Loading %s into Vertica...', filepath)
        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')

        with self.open_connection() as connection:
            with connection.cursor() as cur:
                inserts = 0

                # @TODO - Confirm Copy statement format
                copy_sql = f"""COPY {target_schema}."{target_table}"
                FROM STDIN WITH (FORMAT CSV, HEADER {'TRUE' if skip_csv_header else 'FALSE'}, ESCAPE '"')
                """

                with gzip.open(filepath, 'rb') as file:
                    cur.copy_expert(copy_sql, file)

                inserts = cur.rowcount
                LOGGER.info('Loading into %s."%s": %s',
                            target_schema,
                            target_table,
                            json.dumps({'inserts': inserts, 'updates': 0, 'size_bytes': size_bytes}))


    def grant_select_on_table(self, target_schema, table_name, role, is_temporary, to_group=False):
        # Grant role is not mandatory parameter, do nothing if not specified
        if role:
            table_dict = utils.tablename_to_dict(table_name)
            target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')
            # @TODO - Confirm the Vertica Grant statement formats
            sql = 'GRANT SELECT ON {}."{}" TO GROUP {}'.format(target_schema, target_table, role)
            self.query(sql)

    # pylint: disable=unused-argument
    def grant_usage_on_schema(self, target_schema, role, to_group=False):
        # Grant role is not mandatory parameter, do nothing if not specified
        if role:
            sql = 'GRANT USAGE ON SCHEMA {} TO GROUP {}'.format(target_schema, role)
            self.query(sql)

    # pylint: disable=unused-argument
    def grant_select_on_schema(self, target_schema, role, to_group=False):
        # Grant role is not mandatory parameter, do nothing if not specified
        if role:
            sql = 'GRANT SELECT ON ALL TABLES IN SCHEMA {} TO GROUP {}'.format(target_schema, role)
            self.query(sql)

    def obfuscate_columns(self, target_schema: str, table_name: str, is_temporary: bool = False):
        """
        Apply any configured transformations to the given table
        Args:
            target_schema: target schema name
            table_name: table name
        """
        LOGGER.info('Starting obfuscation rules...')

        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')
        transformations = self.transformation_config.get('transformations', [])

        # Input table_name is formatted as {{schema}}.{{table}}
        # Stream name in taps transformation.json is formatted as {{schema}}-{{table}}
        #
        # We need to convert to the same format to find the transformation
        # has that has to be applied
        tap_stream_name_by_table_name = '{}-{}'.format(table_dict.get('schema_name'), table_dict.get('table_name'))

        trans_cols = TransformationHelper.get_trans_in_sql_flavor(
            tap_stream_name_by_table_name,
            transformations,
            SQLFlavor('vertica'))

        self.__apply_transformations(trans_cols, target_schema, target_table)

        LOGGER.info('Obfuscation rules applied.')

    def swap_tables(self, schema, table_name):
        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name')
        temp_table = table_dict.get('temp_table_name')

        # Swap tables and drop the temp tamp
        # @TODO - Confirm these queries are the order of operations for Vertica
        self.query('DROP TABLE IF EXISTS {}."{}"'.format(schema, target_table))
        self.query('ALTER TABLE {}."{}" RENAME TO "{}"'.format(schema, temp_table, target_table))

    def __apply_transformations(self, transformations, target_schema, table_name):
        """
        Generate and execute the SQL queries based on the given transformations.
        Args:
            transformations: List of dictionaries in the form {"trans": "", conditions: "... AND ..."}
            target_schema: name of the target schema where the table lives
            table_name: the table name on which we want to apply the transformations
        """
        full_qual_table_name = f'"{target_schema}"."{table_name}"'

        if transformations:
            all_cols_update_sql = ''

            # Conditional transformations will have to be executed one at time separately

            for trans_item in transformations:

                # If we have conditions, then we need to construct the query and execute it to transform the
                # single column conditionally
                if trans_item['conditions']:
                    sql = f'UPDATE {full_qual_table_name} ' \
                          f'SET {trans_item["trans"]} WHERE {trans_item["conditions"]};'

                    self.query(sql)

                # Otherwise, we can add this column to a general UPDATE query with no predicates
                else:

                    # if the variable is empty, then initialize it otherwise append the
                    # current transformation to it
                    if not all_cols_update_sql:
                        all_cols_update_sql = trans_item['trans']
                    else:
                        all_cols_update_sql = f'{all_cols_update_sql}, {trans_item["trans"]}'

            # If we have some non-conditional transformations then construct and execute a query
            if all_cols_update_sql:
                all_cols_update_sql = f'UPDATE {full_qual_table_name} SET {all_cols_update_sql};'

                self.query(all_cols_update_sql)
