from sqlalchemy import create_engine

import pandas as pd


def rdb_cursor_extractor(_db_connector, _query: str):
    """
    Input :
        1) _db_connector (Class)
            : 데이터 추출을 실행하기 위해 연결이 필요한 데이터베이스 정보
        2) _query (str)
            : 데이터 추출 쿼리
    
    Output :
        1) result (list(tuple))
            : cursor.execute(_query) 후 cursor.fetchall() 결과

    """

    print('<< rdb_cursor_extractor >>')

    with _db_connector as connected:
        cur = connected.conn.cursor()
        cur.execute(_query)
        result = cur.fetchall()

    return result


def rdb_pandas_extractor(_db_connector, _query: str,  _engine: str = 'postgre'): # overwrite 위험
    """
    Input :
        1) _db_connector (Class)
            : 데이터 추출을 실행하기 위해 연결이 필요한 데이터베이스 정보
        2) _query (str)
            : 데이터 추출 쿼리
    
    Output
        1) result (pandas.core.frame.DataFrame)
            : pandas.read_sql_query(_query, 'DB커넥션') 결과

    """
    print('<< rdb_pandas_extractor >>')
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
        result = pd.read_sql_query(_query, con)

    return result

    # with _db_connector as connected:
    #     con = connected.conn
    #     result = pd.read_sql_query(_query, con)

    # return result


def ddb_cursor_extractor(_db_connector, _collection: str, _query: dict):
    """
    Input :
        1) _db_connector (Class)
            : 데이터 추출을 실행하기 위해 연결이 필요한 데이터베이스 정보
        2) _collection (str)
            : 콜렉션 이름 ex) book_code
        3) _query (dict)
            : 데이터 추출 쿼리 ex) {'영역': { $eq:'기본서' }}
    
    Output :
        1) result (list(dict))
            : collection.find({ _query }) 후 pandas.read_sql_query(_query, 'DB커넥션') 결과

    """
    print('<< ddb_cursor_extractor >> ')
    with _db_connector as connected:
        _coll = connected.conn.get_collection(_collection)
        _docs = _coll.find(_query)

        result = []

        for _row in _docs:
            _row.pop("_id")
            result.append(_row)
    
    return result