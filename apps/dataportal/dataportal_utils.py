import PublicDataReader as pdr
import numpy as np
import argparse
import requests

API_INFO = {
    "아파트매매" : {
        "API_URL" : "http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade",
        "API_KEY" : [ "38p2hc6rQYjRNIJHT7ECWCIZGml4FWabPmEht1aDQPugEJq7LUbWwo4run61Dg9XSNwdYhpX8BQQGj20Ay0r%2BA%3D%3D",
                     "4m0yoSHquV4ELZ0YOawJZJpbKJ%2FZ5GT777yyfdPfo4zFbzN389sS2GzlZxyM9EyTy3waAbL5TNYt4xKu400P4g%3D%3D",
                      "wn1UqSMyNRDd9T0Oedf6Tusk57Qk0zI9jNkoZOqvGNGgsAaGZc3t8sR6UY64%2FOEu9grWOVPEKffSayZnk%2BBYmw%3D%3D"
                      ],
        "FORMAT" : "json"
    },
    "아파트전월세" : {
        "API_URL" : "http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptRent",
        "API_KEY" : [ "38p2hc6rQYjRNIJHT7ECWCIZGml4FWabPmEht1aDQPugEJq7LUbWwo4run61Dg9XSNwdYhpX8BQQGj20Ay0r%2BA%3D%3D",
                     "4m0yoSHquV4ELZ0YOawJZJpbKJ%2FZ5GT777yyfdPfo4zFbzN389sS2GzlZxyM9EyTy3waAbL5TNYt4xKu400P4g%3D%3D",
                     "wn1UqSMyNRDd9T0Oedf6Tusk57Qk0zI9jNkoZOqvGNGgsAaGZc3t8sR6UY64%2FOEu9grWOVPEKffSayZnk%2BBYmw%3D%3D"
        ],
        "FORMAT" : "xml"
    },
    "상업용부동산" : {
        "API_URL" : "http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcNrgTrade",
        "API_KEY" : [ "38p2hc6rQYjRNIJHT7ECWCIZGml4FWabPmEht1aDQPugEJq7LUbWwo4run61Dg9XSNwdYhpX8BQQGj20Ay0r%2BA%3D%3D",
                      "4m0yoSHquV4ELZ0YOawJZJpbKJ%2FZ5GT777yyfdPfo4zFbzN389sS2GzlZxyM9EyTy3waAbL5TNYt4xKu400P4g%3D%3D"

                     ],
        "FORMAT" : "xml"
    },
    "토지매매" : {
        "API_URL" : "http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcLandTrade",
        "API_KEY" : ["38p2hc6rQYjRNIJHT7ECWCIZGml4FWabPmEht1aDQPugEJq7LUbWwo4run61Dg9XSNwdYhpX8BQQGj20Ay0r%2BA%3D%3D",
                     "4m0yoSHquV4ELZ0YOawJZJpbKJ%2FZ5GT777yyfdPfo4zFbzN389sS2GzlZxyM9EyTy3waAbL5TNYt4xKu400P4g%3D%3D"
                     ],
        "FORMAT" : "xml"
    }
}

def get_api_info( data_type ):
    if data_type in API_INFO:
        return API_INFO[data_type]

    print(f"Unknown data_type = {data_type}")
    return None


def get_location_dataframe( sido_cd = None, loc_cd = None ):
    dong_info = pdr.code_hdong_bdong()
    dong_info.head()

    dong_info['법정동코드_5'] = dong_info['법정동코드'].str[:5]
    dong_info_new = dong_info[(dong_info['말소일자'] == '') & (dong_info['시군구명'] != '')]

    if sido_cd != None and len(sido_cd) > 0:
        dong_info_new = dong_info_new[dong_info_new['시도코드'] == sido_cd]

    if loc_cd != None and len(loc_cd) > 0:
        dong_info_new = dong_info_new[dong_info_new['법정동코드_5'] == loc_cd]

    group_key_list = ['시도코드', '시도명', '시군구명', '법정동코드_5']
    loc_tuple_list = dong_info_new.groupby(group_key_list)[group_key_list].apply(lambda x: list(np.unique(x)))

    # for loc in loc_tuple_list[:5]:
    #     print(loc)
    # print(f"location count : {len(loc_tuple_list)}")
    return loc_tuple_list


## 배열을 n개 단위로 분할 (sub 배열)
def divide_list(l, n):
    # 리스트 l의 길이가 n이면 계속 반복
    for i in range(0, len(l), n):
        yield l[i:i + n]
