import pandas as pd
import json
import requests
import xmltodict
import time
import dataportal_utils
from module import mysql_utils
from module import date_utils

# db insert 실행시 최대 갯수
MAX_INSERT_BULK_SIZE = 1000

db_info = {
  'host' : 'my5701.gabiadb.com',
  'port' : 3306,
  'user' : 'nextu',
  'passwd' : 'sprtmcb!@#123',
  'db' : 'nextu'
}

# 실거래 데이터 조회
def get_api_data(lawd_cd, deal_ymd):

    res = dataportal_utils.req_api('아파트매매', lawd_cd, deal_ymd)
    data = json.loads(json.dumps(xmltodict.parse(res.text)))
    # print(f"RES = {data}" )

    if data == None:
        print(f"[SKIP] no data!!! {data}")
        return pd.DataFrame()


    if ('response' in data) and ('body' in data['response']) and ('items' in data['response']['body']):

        # 실제 거래가 없을때의 예외처리
        if data['response']['body']['items'] == None:
            print(f"[SKIP] no data!!! {data}")
            return pd.DataFrame()

        elif ( 'item' in data['response']['body']['items']):
            res = data['response']['body']['items']['item']
            if type( res ) == list:
                return pd.DataFrame( res )
            # 데이터가 한건일때 배열이 아닌 Object 형태로 들어옴
            elif type( res ) == dict:
                return pd.DataFrame( [res] )


    print(f"no data!!! {data}")
    exit(0)

def process( start_month, end_month, loc_cd, sido_cd ):

    loc_tuple_list = dataportal_utils.get_location_dataframe(loc_cd)
    month_list = date_utils.get_month_list(start_month, end_month, in_fmt='%Y%m', out_fmt='%Y%m')

    print("\n")
    print("==============================")
    print( "1) LOCATION_LIST")
    print("==============================")
    print( loc_tuple_list )

    print("\n")
    print("==============================")
    print("2) MONTH")
    print("==============================")
    print( month_list )

    # info 확인용
    time.sleep( 5 )

    print("\n")
    print("==============================")
    print("3) SAVE DB")
    print("==============================")
    output( loc_tuple_list, month_list, sido_cd )

# db 저장
def output( loc_tuple_list, month_list, sido_cd_filter ):

    db_conn = mysql_utils.get_db_conn()

    for loc_idx, loc in enumerate(loc_tuple_list ):
        for month_idx, month in enumerate(month_list):

            # loc[0] : 시도코드, [1] : 법정동코드, [2] : 시군구명, [3] : 시도명 (서울특별시)
            sido_cd = loc[0]
            loc_cd = loc[1]

            # 서울(11), 부산(26), 대전(30), 인천(28), 경기(41), 충남(44) 필터링
            if sido_cd != sido_cd_filter:
                continue

            df_month = get_api_data(loc_cd, month)
            if df_month.empty:
                continue

            print(f"[DB > SAVE] loc_idx = {loc_idx+1}/{len(loc_tuple_list)}, month_idx = {month_idx+1}/{len(month_list)}, loc_cd = {loc_cd}, current_month = {month}, total_data_cnt = {len(df_month)} ")

            # 전체 데이터를 리스트로 변환
            his_list = list(df_month.T.to_dict().values())
            # 부분 리스트로 변환
            his_sub_list = list(dataportal_utils.divide_list(his_list, MAX_INSERT_BULK_SIZE))

            for bulk_idx, sub_list in enumerate(his_sub_list):
                print(f"\t bulk idx = {bulk_idx+1} / {len(his_sub_list)}, count = {len(sub_list)}")
                sql = make_insert_template(sub_list)
                # print( sql )
                try:
                    rows = mysql_utils.write_db(db_conn, sql)
                    print(f"\t\t{rows} insert rows")

                    # insert 부분 성공
                    # if len(sub_list) != rows:
                        # print("[Exception] sub-sql = ", sql)
                        # break

                except Exception as e:
                    print("[Exception] sql = ", sql)
                    print("[Exception] message = ", e)
                    break


## value list 생성
def make_value_template(  his_list ):
    value_template = ''

    for idx, his in enumerate(his_list):
        value_template += "(" + f"""
            {his['거래금액'].replace(',', '')}, CAST(NULLIF('{his['거래유형']}', 'None') AS CHAR ), {his['건축년도']}, '{his['법정동']}', '{his['아파트'].replace("'", '')}',
            {his['년']}, {his['월']}, {his['일']}, {his['전용면적']}, '{his['중개사소재지']}',
            '{his['지번']}', '{his['지역코드']}', {his['층']}, CAST(NULLIF('{his['해제사유발생일']}', 'None') AS CHAR ), CAST(NULLIF('{his['해제여부']}', 'None') AS CHAR )
        """.format( his=his ) + ")"

        if idx < len(his_list) - 1 :
            value_template += ', '

    return value_template

## sql 생성
def make_insert_template( his_list ):

    value_template = make_value_template( his_list )
    sql_template = """
    insert into apartment_trade
    (
      `price`, `tr_type`, `birth_year`, `bubjungdong`, `name`,
      `tr_year`, `tr_month`, `tr_day`, `size`, `trader_loc`,
      `jibun`, `loc_cd`, `layer`, `cancel_date`, `cancel_yn`
    )
    VALUES
      {value_template}
    ON DUPLICATE KEY UPDATE
      reg_date = now()
    """.format( value_template = value_template)

    return sql_template


if __name__ == "__main__":
    args = dataportal_utils.arg_parse()
    process( args.start_month, args.end_month, args.loc_cd, args.sido_cd )


"""
200601 ~ 202301

CREATE TABLE `apartment_trade` (

    `price` int NOT NULL COMMENT '거래금액',
    `tr_type` varchar(16) NULL COMMENT '거래유형(중개거래,직거래)',
    `birth_year` int NULL COMMENT '건축년도',
    `bubjungdong` varchar(32) NULL COMMENT '법정동',
    `name` varchar(64) NOT NULL COMMENT '아파트',

    `tr_year` int NOT NULL COMMENT '거래년',
    `tr_month` tinyint not NULL COMMENT '거래월',
    `tr_day` tinyint not NULL COMMENT '거래일',

    `size` float NOT NULL COMMENT '전용면적',
    `trader_loc` varchar(32) NULL COMMENT '중개사소재지',
    `jibun` varchar(32) NULL COMMENT '지번',
    `loc_cd` char(5) NOT NULL COMMENT '지역코드',
    `layer` int not NULL COMMENT '층',

    `cancel_date` datetime NULL COMMENT '해제사유발생일',
    `cancel_yn` char(1) NULL COMMENT '해제여부',
    `reg_date` datetime default now() COMMENT '등록시간',

    PRIMARY KEY ( tr_year, tr_month, tr_day, loc_cd, name, size, layer, price )
)
COMMENT '아파트 매매(실거래) 정보';
"""