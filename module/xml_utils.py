import json
import xmltodict
from urllib.parse import urlencode, quote_plus
from bs4 import BeautifulSoup
import bs4
import pandas as pd

def xml_to_json_from_file( filename ):
    with open( filename, 'r') as f:
        content = f.read()
    return xml_to_json( content )


def xml_to_json( content ):
    jsonString = json.dumps(xmltodict.parse( content ), indent=4, ensure_ascii=False)
    return json.loads(jsonString)


def xml_to_dataframe( xml_content, data_nodename ):
    rowList = []
    nameList = []
    columnList = []

    xmlobj = bs4.BeautifulSoup( xml_content, 'lxml-xml')
    rows = xmlobj.findAll( data_nodename )
    # print( rows )
    rowsLen = len(rows)

    for i in range(0, rowsLen):
        columns = rows[i].find_all()
        columnsLen = len(columns)
        for j in range(0, columnsLen):
            if i == 0:
                nameList.append(columns[j].name)
            eachColumn = columns[j].text
            columnList.append(eachColumn)
        rowList.append(columnList)
        columnList = []

    return pd.DataFrame(rowList, columns=nameList)
