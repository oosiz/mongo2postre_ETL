import pandas as pd

def transform_jisukim(_result, _batch_month):
    ## _batch_month_math_book_analytics_jisukim
    # 데이터 프레임으로 변환
    _transform_result = pd.DataFrame(_result)
    # 크롤링 날짜에서 yyyymm 형태로 변환한 컬럼 생성
    _transform_result['yyyymm'] = _transform_result['크롤링 날짜'].apply(lambda x: x[:4]+x[5:7])
    # yyyymm과 batch_month가 같은 데이터만 추출
    month_transform_result = _transform_result[_transform_result['yyyymm'] == _batch_month]


    ## online_data (판매지수 평균 데이터)
    online_data = month_transform_result[['yyyymm', '상품번호', '상품명', '저자', '브랜드', '학제', '과목', '판매지수']]
    # 컬럼명 변경
    online_data.columns = ['yyyymm', 'code', 'name', 'author', 'book_group', 'level', 'subject', 'sales_qty']
    # sales_qty 평균 컬럼 생성
    sales_qty_mean = online_data.groupby(['code']).mean('sales_qty').reset_index()
    sales_qty_mean.rename(columns = {'sales_qty': 'sales_qty_avg'}, inplace = True)
    # join
    sales_qty_mean = sales_qty_mean.merge(online_data, on = 'code', how = 'inner').drop(['sales_qty'], axis=1).drop_duplicates('code')
    # 컬럼 순서 변경
    sales_qty_mean = sales_qty_mean[['yyyymm', 'code', 'name', 'author', 'book_group', 'level', 'subject', 'sales_qty_avg']]

    # _batch_month_math_book_analytics_jisukim 테이블 yyyymm 컬럼 삭제
    month_transform_result = month_transform_result.drop(['yyyymm'], axis=1)

    # 데이터 검증
    if len(sales_qty_mean) == len(month_transform_result['상품번호'].drop_duplicates()):
        return month_transform_result, sales_qty_mean
    else:
        raise Exception('데이터프레임이 잘못 병합된 것 같아요!!!')