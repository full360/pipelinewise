import os
import pytest

from .helpers import tasks
from .helpers import assertions
from .helpers.env import E2EEnv

DIR = os.path.dirname(__file__)
TAP_S3_CSV_ID = 's3_csv_to_v'
TARGET_ID = 'vertica'


# pylint: disable=attribute-defined-outside-init
class TestTargetVertica:
    """
    End to end tests for Target Vertica
    """

    def setup_method(self):
        """Initialise test project by generating YAML files from
        templates for all the configured connectors"""
        self.project_dir = os.path.join(DIR, 'test-project')

        # Init query runner methods
        self.e2e = E2EEnv(self.project_dir)
        self.run_query_target_vertica = self.e2e.run_query_target_vertica

    def teardown_method(self):
        """Delete test directories and database objects"""

    @pytest.mark.dependency(name='import_config')
    def test_import_project(self):
        """Import the YAML project with taps and target and do discovery mode
        to write the JSON files for singer connectors """

        # Skip every target_vertica related test if required env vars not provided
        if not self.e2e.env['TARGET_VERTICA']['is_configured']:
            pytest.skip(
                'Target Vertica environment variables are not provided')

        # Setup and clean source and target databases
        if self.e2e.env['TAP_S3_CSV']['is_configured']:
            self.e2e.setup_tap_s3_csv()
        self.e2e.setup_target_vertica()   # target vertica working

        # Import project
        [return_code, stdout, stderr] = tasks.run_command(
            f'pipelinewise import_config --dir {self.project_dir} --profiler')

        assertions.assert_command_success(return_code, stdout, stderr)
        assertions.assert_profiling_stats_files_created(
            stdout, 'import_project')

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_s3_to_v(self):
        """Replicate csv files from s3 to Vertica"""
        # Skip tap_s3_csv related test if required env vars not provided
        if not self.e2e.env['TAP_S3_CSV']['is_configured']:
            pytest.skip('Tap S3 CSV environment variables are not provided')

        def assert_columns_exist():
            """Helper inner function to test if every table and column exists in target snowflake"""
            assertions.assert_cols_in_table(self.run_query_target_vertica, 'ppw_e2e_tap_s3_csv', 'countries',
                                            ['city', 'country', 'currency', 'id', 'language'])
            assertions.assert_cols_in_table(self.run_query_target_vertica, 'ppw_e2e_tap_s3_csv', 'people',
                                            ['birth_date', 'email', 'first_name', 'gender', 'group', 'id',
                                             'ip_address', 'is_pensioneer', 'last_name'])

        # 1. Run tap first time - both fastsync and a singer should be triggered
        assertions.assert_run_tap_success(
            TAP_S3_CSV_ID, TARGET_ID, ['fastsync', 'singer'])
        assert_columns_exist()

        # 2. Run tap second time - both fastsync and a singer should be triggered
        assertions.assert_run_tap_success(
            TAP_S3_CSV_ID, TARGET_ID, ['fastsync', 'singer'])
        assert_columns_exist()
