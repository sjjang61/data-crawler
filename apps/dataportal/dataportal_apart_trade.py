import pandas as pd
import json
import xmltodict

from apps.dataportal.dataportal_base import DataPortal

class DataPortalAptTrade( DataPortal ):

    # 실거래 데이터 조회
    def get_api_data(self, lawd_cd, deal_ymd):

        res = self.req_api('아파트매매', lawd_cd, deal_ymd )
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

    ## value list 생성
    def make_value_template( self, his_list ):
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
    def make_insert_template( self, his_list ):

        value_template = self.make_value_template( his_list )
        sql_template = """
        insert into apartment_trade
        (
          `price`, `tr_type`, `birth_year`, `bubjungdong`, `name`,
          `year`, `month`, `day`, `size`, `trader_loc`,
          `jibun`, `loc_cd`, `layer`, `cancel_date`, `cancel_yn`
        )
        VALUES
          {value_template}
        ON DUPLICATE KEY UPDATE
          reg_date = now()
        """.format( value_template = value_template)

        return sql_template


if __name__ == "__main__":

    dataportal = DataPortalAptTrade()
    args = dataportal.arg_parse()
    dataportal.process( args.start_month, args.end_month, args.loc_cd, args.sido_cd )