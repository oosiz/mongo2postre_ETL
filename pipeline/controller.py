from ast import literal_eval

from settings import DB_SETTINGS

from db.connector import DBConnector
from db.queries_rdb import queries_rdb
from db.queries_ddb import queries_ddb

from pipeline import extract, transform, load


def etl_job_1(_batch_month):
    print('<< Start etl_job_1 >>')

    _source_db_conn = DBConnector(**DB_SETTINGS['source_db_localhost_rdb'])
    _target_db_conn = DBConnector(**DB_SETTINGS['target_db_localhost_rdb'])

    _source_table_list = ['actor', 'film']
    _target_table_list = ['actor_back_v1', 'film_back_v1']
    
    for _idx, _tb in enumerate(_source_table_list):
    
        print(f"######------ Start Extract : table_name == '{_tb}' ------######")
        _read_query = queries_rdb['read'][_tb]
        data = extract.rdb_cursor_extractor(_source_db_conn, _read_query)
        print(f"######------ End Extract : row_cnt == '{len(data)}' ------######", '\n')
        
        _tb_back = _target_table_list[_idx]
        print(f"######------ Start Load : table_name == '{_tb_back}' ------######")
        _create_query = queries_rdb['create'][_tb_back]
        load.rdb_cursor_loader(_target_db_conn, _create_query, data)
        print(f'######------ End Load ------######', '\n')

    print('<< End etl_job_1 >>')


def etl_job_2(_batch_month):
    print('<< Start etl_job_2 >>')

    _source_db_conn = DBConnector(**DB_SETTINGS['source_db_localhost_rdb'])
    _target_db_conn = DBConnector(**DB_SETTINGS['target_db_localhost_rdb'])

    _source_table_list = ['actor', 'film', 'film_actor']
    _target_table_list = ['actor_back_v1', 'film_back_v1', 'film_actor_back_v1']

    for _idx, _tb in enumerate(_source_table_list):
        
        print(f"######------ Start Extract : table_name == '{_tb}' ------######")
        _read_query = queries_rdb['read'][_tb]
        pdf = extract.rdb_pandas_extractor(_source_db_conn, _read_query)
        print(f"######------ End Extract : row_cnt == '{len(pdf)}' ------######", '\n')
        

        # transform 들어갈 영역 (모델 학습)


        _tb_back = _target_table_list[_idx]
        print(f"######------ Start Load : table_name == '{_tb_back}' ------######")
        load.rdb_pandas_loader(_target_db_conn, _tb_back, pdf)
        print(f'######------ End Load ------######', '\n')

    print('<< End etl_job_2 >>')


def etl_job_3(_batch_month):
    print('<< Start etl_job_3 >>')

    _source_db_conn = DBConnector(**DB_SETTINGS['source_db_localhost_rdb'])
    _target_db_conn = DBConnector(**DB_SETTINGS['target_db_localhost_rdb'])

    _source_table_list = ['actor_yyyymm', 'film_yyyymm']
    _target_table_list = ['actor_back_v2', 'film_back_v2']
    
    for _idx, _tb in enumerate(_source_table_list):
    
        print(f"######------ Start Extract : table_name == '{_tb}' ------######")
        _read_query = queries_rdb['read'][_tb].format(*[_batch_month])
        data = extract.rdb_cursor_extractor(_source_db_conn, _read_query)
        print(f"######------ End Extract : row_cnt == '{len(data)}' ------######", '\n')
        
        _tb_back = _target_table_list[_idx]
        print(f"######------ Start Load : table_name == '{_tb_back}' ------######")
        _create_query = queries_rdb['create'][_tb_back]
        load.rdb_cursor_loader(_target_db_conn, _create_query, data)
        print(f'######------ End Load ------######', '\n')

    print('<< End etl_job_3 >>')


def etl_job_4(_batch_month):
    print('<< Start etl_job_4 >>')

    _source_db_conn = DBConnector(**DB_SETTINGS['source_db_localhost_rdb'])
    _target_db_conn = DBConnector(**DB_SETTINGS['target_db_localhost_rdb'])

    _source_table_list = ['actor', 'film', 'film_actor']
    _target_table_list = []

    _pdfs = []
    for _idx, _tb in enumerate(_source_table_list):
        
        print(f"######------ Start Extract : table_name == '{_tb}' ------######")
        _read_query = queries_rdb['read'][_tb]
        pdf = extract.rdb_pandas_extractor(_source_db_conn, _read_query)
        _pdfs.append(pdf)
        print(f"######------ End Extract : row_cnt == '{len(pdf)}' ------######", '\n')

    print(f"######------ Start Transform ------######")
    _transform_df = transform.transform_etl_job_4(_pdfs)
    print(f"######------ END Transform ------######")

    _tb_name = 'join_table_actor_and_film'
    print(f"######------ Start Load : table_name == '{_tb_name}' ------######")
    load.rdb_pandas_loader(_target_db_conn, _tb_name, _transform_df)
    print(f'######------ End Load ------######', '\n')

    print('<< End etl_job_4 >>')


def etl_job_5(_batch_month):
    print("<< Start etl_job_5 >> ")

    _source_db_conn = DBConnector(**DB_SETTINGS['source_db_localhost_ddb'])
    _target_db_conn = DBConnector(**DB_SETTINGS['target_db_localhost_ddb'])

    _source_collection_list = ['book_code']
    _target_collection_list = ['book_code_back_v2']

    for _idx, _coll in enumerate(_source_collection_list):

        print(f"######------ Start Extract : collection_name == '{_coll}' ------######")
        _read_query = literal_eval(queries_ddb['read'][_coll].strip())
        data = extract.ddb_cursor_extractor(_source_db_conn, _coll, _read_query)
        print(f"######------ End Extract : row_cnt == '{len(data)}' ------######", '\n')

        _coll_back = _target_collection_list[_idx]
        print(f"######------ Start Load : collection_name == '{_coll_back}' ------######")
        load.ddb_cursor_loader(_target_db_conn, _coll_back, data)
        print(f'######------ End Load ------######', '\n')

    print(" << End etl_job_5 >> ")


def etl_job_6(_batch_month):
    print("<< Start etl_job_6 >> ")

    _source_db_conn = DBConnector(**DB_SETTINGS['source_db_localhost_ddb'])
    _target_db_conn = DBConnector(**DB_SETTINGS['target_db_localhost_rdb'])

    _source_collection_list = ['book_code']
    _target_table_list = ['book_code_basic_back']

    for _idx, _coll in enumerate(_source_collection_list):
        print(f"######------ Start Extract : collection_name == '{_coll}' ------######")
        _read_query = literal_eval(queries_ddb['read'][_coll].strip())
        data = extract.ddb_cursor_extractor(_source_db_conn, _coll, _read_query)
        print(f"######------ End Extract : row_cnt == '{len(data)}' ------######", '\n')

        _transform_df = transform.transform_etl_job_6(data)

        _tb_name = _target_table_list[_idx]
        print(f"######------ Start Load : table_name == '{_tb_name}' ------######")
        load.rdb_pandas_loader(_target_db_conn, _tb_name, _transform_df)
        print(f'######------ End Load ------######', '\n')
    
    print(" << End etl_job_6 >> ")


def jisukim(_batch_month):
    print('<< Start jisukim >>')

    _source_db_conn = DBConnector(**DB_SETTINGS['source_db_localhost_ddb'])
    _target_db_conn = DBConnector(**DB_SETTINGS['sample_db_localhost_rdb'])

    _source_collection_list = ['math_book']
    _target_table_list = []
    
    for _idx, _coll in enumerate(_source_collection_list):
        print(f"######------ Start Extract : collection_name == '{_coll}' ------######")
        _read_query = literal_eval(queries_ddb['read'][_coll].strip())
        data = extract.ddb_cursor_extractor(_source_db_conn, _coll, _read_query)
        print(f"######------ End Extract : row_cnt == '{len(data)}' ------######", '\n')

        _transform_df, _online_data_df = transform.transform_jisukim(data, _batch_month)

        _tb_name = f'{_batch_month}_math_book_analytics_jisukim'
        print(f"######------ Start Load : table_name == '{_tb_name}' ------######")
        load.rdb_pandas_loader(_target_db_conn, _tb_name, _transform_df)
        print(f'######------ End Load ------######', '\n')

        _tb_name = 'online_data'
        print(f"######------ Start Load : table_name == '{_tb_name}' ------######")
        load.rdb_pandas_loader(_target_db_conn, _tb_name, _online_data_df)
        print(f'######------ End Load ------######', '\n')

    print('<< End jisukim >>')