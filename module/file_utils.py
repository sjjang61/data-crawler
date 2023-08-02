# -*- coding: utf-8 -*-
import codecs
import json
import os

# data 를 통째로 저장
def write_file(filename, data):
    # file_path = os.path.dirname(filename)
    # if exist(file_path) == False:
    #     # print("mkdir_dir1 = %s" % (file_path))
    #     make_dir(file_path)
    #
    # write_file_ver2(filename, data)
    fp = open(filename, 'w', encoding='UTF-8')
    fp.write(data)
    fp.close()


# open, write, close 3가지가 쌍으로 이루어져야함
def open_file(filename, mode):
    fp = open(filename, mode, encoding='UTF-8')
    return fp


def close_file(fp):
    fp.close()


def write_file_line(fp, line):
    if fp != None:
        # fp.write(unicode(line + '\n', 'euc-kr').encode('utf-8'))
        fp.write(line + '\n')


def read_file(filename):
    return open(filename, 'r',  encoding='UTF8' ).read()



# 파일을 읽어 json object 형태로 리턴 : For Python 3.x
def read_json_file(filename):
    json_data = open(filename, 'r',  encoding='UTF8' ).read()
    return json.loads(json_data)


def exist(path):
    return os.path.exists(path)


def make_dir(path):
    if exist(path) == False:
        try:
            os.makedirs(path)
        except OSError as e:
            print("error = %s " % (e.strerror.decode('cp949').encode('utf-8')))
            raise


def delete_file(filename):
    if exist(filename):
        os.remove(filename)
        return True

    return False
