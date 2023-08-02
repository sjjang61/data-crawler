from module import xml_utils
from apps.dataportal.dataportal_base import DataPortal

class DataPortalAptRent( DataPortal ):

    # 아파트 전월세 데이터 조회
    def get_api_data( self, lawd_cd, deal_ymd):

        res = self.req_api('아파트전월세', lawd_cd, deal_ymd )
        df = xml_utils.xml_to_dataframe( res.text, 'item' )
        if df.empty:
            print(f"[SKIP] no data!!! {res.text}")

        # print( df.head())
        return df

    ## value list 생성
    def make_value_template( self,  his_list ):
        value_template = ''

        for idx, his in enumerate(his_list):

            his['아파트'] = his['아파트'].replace("'", '')
            his['계약구분'] = his['계약구분'].strip()
            his['계약기간'] = his['계약기간'].strip()
            his['갱신요구권사용'] = his['갱신요구권사용'].strip()
            his['보증금액'] = his['보증금액'].replace(',', '')
            his['월세금액'] = his['월세금액'].replace(',', '')
            his['종전계약보증금'] = his['종전계약보증금'].replace(',', '').strip()
            if his['종전계약보증금'] == '':
                his['종전계약보증금'] = 0

            his['종전계약월세'] = his['종전계약월세'].replace(',', '').strip()
            if his['종전계약월세'] == '':
                his['종전계약월세'] = 0

            value_template += "(" + f"""          
                '{his['갱신요구권사용']}', {his['건축년도']}, CAST(NULLIF('{his['계약구분']}', 'None') AS CHAR ), CAST(NULLIF('{his['계약기간']}', 'None') AS CHAR ), '{his['법정동']}',             
                {his['보증금액']}, {his['월세금액']}, {his['종전계약보증금']}, {his['종전계약월세']}, '{his['아파트']}',                        
                {his['년']}, {his['월']}, {his['일']}, {his['전용면적']}, '{his['지번']}',
                '{his['지역코드']}', CAST(NULLIF('{his['층']}', 'None') AS CHAR )
            """.format( his=his ) + ")"

            if idx < len(his_list) - 1 :
                value_template += ', '

        return value_template

    ## sql 생성
    def make_insert_template( self, his_list ):

        value_template = self.make_value_template( his_list )
        sql_template = """
        insert into apartment_rent
        (
          `req_yn`, `birth_year`, `rent_type`, `duration`, `bubjungdong`,   
          `deposit_price`, `rental_price`, `prev_deposit_price`, `prev_rental_price`, `name`,
          `year`, `month`, `day`, `size`, `jibun`, 
          `loc_cd`, `layer`
        )
        VALUES
          {value_template}
        ON DUPLICATE KEY UPDATE
          reg_date = now()
        """.format( value_template = value_template)

        return sql_template


## 갱신요구권사용	건축년도	계약구분	계약기간	년	법정동	보증금액	아파트	월	월세금액	일	전용면적	종전계약보증금	종전계약월세	지번	지역코드	층
if __name__ == "__main__":

    dataportal = DataPortalAptRent()
    args = dataportal.arg_parse()
    dataportal.process( args.start_month, args.end_month, args.loc_cd, args.sido_cd )
