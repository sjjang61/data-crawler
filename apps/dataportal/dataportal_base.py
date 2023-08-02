import time
import requests
import argparse
from apps.dataportal import dataportal_utils
from module import date_utils
from module import mysql_utils
from abc import *

MAX_INSERT_BULK_SIZE = 1000

class DataPortal(object):
    __metaclass__ = ABCMeta

    def __init(self):
        print("super init")

    @abstractmethod
    def make_value_template(self, node):
        pass

    @abstractmethod
    def make_insert_template(self, node):
        pass

    @abstractmethod
    def get_api_data(self, loc_cd, month):
        pass

    def arg_parse(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("-st", "--start_month", help="start_month", required=True)
        parser.add_argument("-end", "--end_month", help="end_month", required=True)
        parser.add_argument("-loc", "--loc_cd", required=False)
        parser.add_argument("-sido", "--sido_cd", required=False)

        args = parser.parse_args()
        print("[Configure]")
        print("\t- Month : %s ~ %s" % (args.start_month, args.end_month))
        print("\t- Location Cd : %s" % (args.loc_cd))
        print("\t- Sido Cd : %s" % (args.sido_cd))
        return args

    def req_api( self, data_type, lawd_cd, deal_ymd):
        api_info = dataportal_utils.get_api_info(data_type)
        if api_info == None:
            exit(0)

        api_url = f"{api_info['API_URL']}?serviceKey={api_info['API_KEY'][0]}&LAWD_CD={lawd_cd}&DEAL_YMD={deal_ymd}"
        # print( f"API_URL = {api_url}")
        return requests.get(api_url)

    def process(self, start_month, end_month, loc_cd, sido_cd):

        loc_tuple_list = dataportal_utils.get_location_dataframe(sido_cd, loc_cd)
        month_list = date_utils.get_month_list(start_month, end_month, in_fmt='%Y%m', out_fmt='%Y%m')

        print("\n")
        print("==============================")
        print("1) LOCATION_LIST")
        print("==============================")
        print(loc_tuple_list)

        print("\n")
        print("==============================")
        print("2) MONTH")
        print("==============================")
        print(month_list)

        # info 확인용
        time.sleep(5)

        print("\n")
        print("==============================")
        print("3) SAVE DB")
        print("==============================")
        self.output(loc_tuple_list, month_list, sido_cd)


    def output(self, loc_tuple_list, month_list, sido_cd_filter):

        db_conn = mysql_utils.get_db_conn()

        for loc_idx, loc in enumerate(loc_tuple_list):
            for month_idx, month in enumerate(month_list):

                # loc[0] : 시도코드, [1] : 법정동코드, [2] : 시군구명, [3] : 시도명 (서울특별시)
                sido_cd = loc[0]
                loc_cd = loc[1]

                # 서울(11), 부산(26), 대전(30), 인천(28), 경기(41), 충남(44) 필터링
                if sido_cd != sido_cd_filter:
                    continue

                df_month = self.get_api_data(loc_cd, month)
                if df_month.empty:
                    continue

                print(
                    f"[DB > SAVE] loc_idx = {loc_idx + 1}/{len(loc_tuple_list)}, month_idx = {month_idx + 1}/{len(month_list)}, loc_cd = {loc_cd}, current_month = {month}, total_data_cnt = {len(df_month)} ")

                # 전체 데이터를 리스트로 변환
                his_list = list(df_month.T.to_dict().values())
                # 부분 리스트로 변환
                his_sub_list = list(dataportal_utils.divide_list(his_list, MAX_INSERT_BULK_SIZE))

                for bulk_idx, sub_list in enumerate(his_sub_list):
                    print(f"\t bulk idx = {bulk_idx + 1} / {len(his_sub_list)}, count = {len(sub_list)}")
                    sql = self.make_insert_template(sub_list)
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
