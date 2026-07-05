"""Generic Soda runner."""

import logging

DEFAULT_SODA_DIR = '/opt/airflow/soda'


def run_soda_scan(data_source, checks_file, variables=None, soda_dir=DEFAULT_SODA_DIR):
    from soda.scan import Scan

    scan = Scan()
    scan.set_data_source_name(data_source)
    scan.add_configuration_yaml_file(f'{soda_dir}/configuration.yml')
    scan.add_sodacl_yaml_file(f'{soda_dir}/{checks_file}')

    if variables:
        scan.add_variables(variables)

    scan.execute()
    logging.info(scan.get_logs_text())
    scan.assert_no_checks_fail()
