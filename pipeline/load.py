from sqlalchemy import create_engine
import pandas as pd


def rdb_cursor_loader(_db_connector, _query: str, _result: list):
    """
    Input :
        1) _db_connector (Class)
            : 데이터 적재를 실행하기 위해 연결이 필요한 데이터베이스 정보
        2) _query (str)
            : INSERT 쿼리
        3) _result (list(tuple))
            : 적재할 데이터
    
    Output :
        1) None (데이터베이스에서 확인하세요~!)

    """
    print('<< rdb_cursor_loader >>')

    with _db_connector as connected:
        cur = connected.conn.cursor()
        cur.executemany(_query, _result)
        connected.conn.commit()

    return print("데이터 이행이 완료 되었습니다!!!")


def rdb_pandas_loader(_db_connector, _table_name: str, _result, _engine: str = 'postgre'):
    """
    Input 
        1) _db_connector (Class)
            : 데이터 적재를 실행하기 위해 연결이 필요한 데이터베이스 정보
        2) _table_name (str)
            : 데이터 적재 테이블 명
        3) _result (pandas.core.frame.DataFrame)
            : 적재 할 데이터
    Output
        1) None (데이터베이스에서 확인 하세요~!)
    """
    with _db_connector as connected:
        con = \
            create_engine('{engine}://{user}:{password}@{host}:{port}/{database}'\
                .format(\
                    engine = 'postgresql' if _engine == 'postgre' else None
                    , user = connected._user
                    , password = connected._pw
                    , host = connected._host
                    , port = connected._port
                    , database = connected._db)
                    )
        _result.to_sql(f"{_table_name}", con, if_exists='append', index=False)

    return print("데이터 이행이 완료 되었습니다!!! (그런데, pandas 로...)")
    

def ddb_cursor_loader(_db_connector, _collection: str, _result):
    """
    Input :
        1) _db_connector (Class)
            : 데이터 적재를 실행하기 위해 연결이 필요한 데이터베이스 정보
        2) _collection (str)
            : Collection 이름
        3) _result (list(dict))
            : 적재할 데이터
    
    Output :
        1) None (데이터베이스에서 확인하세요~!)

    """

    with _db_connector as connected:
        _coll = connected.conn.get_collection(_collection)
        _coll.insert_many(_result)

    return print("데이터 이행이 완료 되었습니다!!!")