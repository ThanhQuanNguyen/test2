from ETL import ETL_process
data_source = ETL_process()
data_source.extract()
data_source.transform()
data_source.load_data_to_sqlite()