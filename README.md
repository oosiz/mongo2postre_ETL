# mongo2postre_ETL
데이터 ETL (mongoDB → postgreSQL)


### 📌 프로젝트 목적

온라인 채널 yes24 에서 판매된 천재교육 도서의 분석을 위한 데이터 생성 프로젝트

### 📌 프로젝트 설명

'yes24' database 의 수학 과목의 도서 중(collection == 'math_book') "영역" field 의 값이 "연산" 인 도서를 대상으로

1_1) 매 일자 별(field == "크롤링 날짜") "판매지수" field 의 값을  확인함.

1_2) 해당 데이터는 각 월 별로 테이블을 분리하여 적재함.

1_3) 월별 "판매지수" 총계만 적재하는 별도의 테이블 생성하여 2022년 11월의 도서 별 판매지수를 insert 함.

1_1) 127.0.0.1:5432 에 'product_analytics' database를 생성함.

2_1) 수집된 데이터는 '{yyyymm}_math_book_analytics_{본인영문이름}' 형태의 테이블 명으로 데이터를 적재함.

2_2) 상데 테이블은 매 월 독립된 테이블이 생성되는 형태이므로 {yyyymm} 의 값은 가변적으로 적용 될 수 있어야 합니다.

3_1) 판매지수 총계 데이터는 'online_data' 테이블에 ('yyyymm', 'code', 'name', 'author', 'book_group', 'level', 'subject', 'sales_qty_avg') 형태로 데이터를 적재함.

3_2) 의미는 각각, ('년월', '상품번호', '상품명', '저자', '브랜드', '학제', '과목', '판매지수평균')

4_1) 데이터가 정상적으로 추출 되었고, 정상적으로 적재 되었는지를 확인 할 수 있는 검증 로직을 transform 함수에 적용함.

✔Extract.py

mongoDB에서 데이터 추출

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

✔Transform.py

추출한 데이터를 원하는 형태로 변환함.

데이터프레임으로 변환 후 크롤링 날짜에서 yyyymm형태로 변환

yyyymm과 batch_month가 같은 데이터만 추출

판매지수 평균 컬럼 생성

데이터 검증로직

판매지수 평균 개수와 생성한 테이블의 상품번호의 중복제거 개수가 같으면 병합

✔Load.py

변환한 데이터를 postgresSQL DB에 적재함

Input
1) _db_connector (Class)
: 데이터 적재를 실행하기 위해 연결이 필요한 데이터베이스 정보
2) _table_name (str)
: 데이터 적재 테이블 명
3) _result (pandas.core.frame.DataFrame)
: 적재 할 데이터
Output
1) None (데이터베이스에서 확인 하세요~!)
