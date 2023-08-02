import pprint
import pandas as pd
import numpy as np
from module import xml_utils
from apps.dataportal.dataportal_base import DataPortal

class DataPortalStoreTrade( DataPortal ):

    # 실거래 데이터 조회
    def get_api_data(self, lawd_cd, deal_ymd):
        response = self.req_api('상업용부동산', lawd_cd, deal_ymd )

        # 깔끔한 출력 위한 코드
        # pp = pprint.PrettyPrinter(indent=4)
        # print(pp.pprint( res.text ))

        data = xml_utils.xml_to_json( response.text )
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
                    # print(pd.DataFrame(res).columns)
                    # print(pd.DataFrame(res).head())
                    return pd.DataFrame( res )
                # 데이터가 한건일때 배열이 아닌 Object 형태로 들어옴
                elif type( res ) == dict:
                    return pd.DataFrame( [res] )

        print(f"no data!!! {data}")
        exit(0)

    ## value list 생성
    def make_value_template( self, his_list ):
        value_template = ''

        # ['거래금액', '거래유형', '건물면적', '건물주용도', '건축년도', '년', '대지면적', '법정동', '시군구', '용도지역',
        # '월', '유형', '일', '중개사소재지', '지번', '지역코드', '해제사유발생일', '해제여부', '층'],
        for idx, his in enumerate(his_list):

            try:
                if ( '층' not in his ) or ( pd.isna( his['층'] )):
                    his['층'] = None

                if pd.isna( his['건축년도'] ):
                    his['건축년도'] = None
            except Exception as e:
                print(f"[Exception] message = {e}, data = {his}")
                exit(0)

            # if pd.isna( his['대지면적'] ):
            #     his['대지면적'] = 0

            value_template += "(" + f"""
                {his['거래금액'].replace(',', '')}, CAST(NULLIF('{his['거래유형']}', 'None') AS CHAR ), CAST(NULLIF('{his['유형']}', 'None') AS CHAR ), CAST(NULLIF('{his['건축년도']}', 'None') AS CHAR ), '{his['법정동']}',                
                '{his['시군구']}', {his['년']}, {his['월']}, {his['일']}, CAST(NULLIF( '{his['층']}', 'None') AS CHAR ),
                {his['건물면적']}, CAST(NULLIF('{his['대지면적']}', 'None') AS CHAR ), '{his['건물주용도']}', '{his['용도지역']}',                  
                '{his['지번']}', '{his['지역코드']}', CAST(NULLIF('{his['중개사소재지']}', 'None') AS CHAR ), CAST(NULLIF('{his['해제사유발생일']}', 'None') AS CHAR ), CAST(NULLIF('{his['해제여부']}', 'None') AS CHAR )
            """.format( his=his ) + ")"

            if idx < len(his_list) - 1 :
                value_template += ', '

        return value_template

    ## sql 생성
    def make_insert_template( self, his_list ):

        value_template = self.make_value_template( his_list )
        sql_template = """
        insert into store_trade
        (
          `price`, `ct_type`, `ct_type2`, `birth_year`, `bubjungdong`, 
          `sido`, `year`, `month`, `day`, `layer`, 
          `building_size`, `land_size`, `building_purpose`, `land_purpose`,
          `jibun`, `loc_cd`, `trader_loc`, `cancel_date`, `cancel_yn`
        )        
        VALUES
          {value_template}
        ON DUPLICATE KEY UPDATE
          reg_date = now()
        """.format( value_template = value_template)

        # print( "sql = ", sql_template )
        return sql_template


if __name__ == "__main__":

    dataportal = DataPortalStoreTrade()
    args = dataportal.arg_parse()
    dataportal.process( args.start_month, args.end_month, args.loc_cd, args.sido_cd )
