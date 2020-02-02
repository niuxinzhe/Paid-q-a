import requests
import pandas as pd
import pickle
import random
import re
import time
import logging
import sys
import conj
from urllib import parse
import os
from lxml import html


# logging.basicConfig(level=logging.WARNING, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S', filename=r'../wiki_txt/'+__name__+'.log', filemode='w')
# logger = logging.getLogger(__name__)
data = {
    'name':'15708426257',
    'password':'a12345123 '
       }

headers = {
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Connection': 'keep-alive',
    'Cookie': 'SINAGLOBAL=6602033343892.077.1570419994353; SCF=AggyYma_GtwsdhDBuBkInChFzyrf4h-c6xIyt9HoFg9MxQLgRh8LSOCt3MqJrcT-LH9eeLX5n2fi4d0SwN2M4GA.; SUHB=0A0J9t8F9-KoqQ; SUB=_2A25wsu3HDeRhGeVJ41YQ9S_MzjmIHXVQXPOPrDV8PUJbkNBeLXPxkW1NT8Wm8BUecohZtXYin8k469UUZvH0Ae2V; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WFVwy_5W1_NYGmdgVwWBcfN5JpX5oz75NHD95Q0S0nXeK-peh-fWs4DqcjZUcL09sv_qJHyU5tt; Ugrow-G0=140ad66ad7317901fc818d7fd7743564; wvr=6; YF-V5-G0=125128c5d7f9f51f96971f11468b5a3f; YF-Page-G0=112e41ab9e0875e1b6850404cae8fa0e|1577000839|1577000839; wb_view_log_3784151055=1920*10801; _s_tentry=www.baidu.com; UOR=,,www.baidu.com; Apache=5693699685045.213.1577000839989; ULV=1577000840099:4:1:1:5693699685045.213.1577000839989:1571642766359; webim_unReadCount=%7B%22time%22%3A1577000845682%2C%22dm_pub_total%22%3A0%2C%22chat_group_client%22%3A0%2C%22allcountNum%22%3A2%2C%22msgbox%22%3A0%7D',
    'Host': 'weibo.com',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'cross-site',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.9 Safari/537.36',
}

crawlers = conj.Crawler(headers, data)
filesolver = conj.FileSolve()
fp = '{"os":"2","browser":"Chrome62,0,3202,9","fonts":"undefined","screenInfo":"1920*1080*24","plugins":"Portable Document Format::internal-pdf-viewer::Chrome PDF Plugin|::mhjfbmdgcfjbbpaeojofohoefgiehjai::Chrome PDF Viewer|::internal-nacl-plugin::Native Client"}'
getcookie = conj.GetSinaCookie(fp)

dir_list = os.listdir('../qa_txt/')
result = 'question_all_1222.pkl'
input_file = 'question191028.pkl'
cookies = getcookie.get_cookie()
isValid = True
if result in dir_list:
    prepare_data = filesolver.read_pkl('../qa_txt/'+result)
else:
    prepare_data = filesolver.read_pkl('../qa_txt/'+input_file)
    if 'question_all' not in prepare_data.columns:
        prepare_data['question_all'] = None
prepare_data = prepare_data.reset_index(drop = True)
print('－－－－－－－－－－－－－－－－－－－－－－－－－－开始－－－－－－－－－－－－－－－－－－－－－－－')
start_time = time.time()
count = 0
for idx in prepare_data.index:
    if idx % 1000 == 0:
        filesolver.write_pkl('../qa_txt/' + result, prepare_data)
        record_time = time.time()
        print('There has been ' + str(idx) + ' pieces in pkl which we used ' + str(
            record_time - start_time) + 's to get.')
        if count % 1000 == 0:
            cookies = getcookie.get_cookie()
            print('cookie is :' + str(cookies))
    if prepare_data.at[idx, 'question_all'] is None:
        count += 1
        question_uid = prepare_data.question_uid[idx]
        question_all, isVaild = crawlers.get_question(uid = question_uid, cookies=cookies, num_retries=3)
        if isValid:
            prepare_data.loc[idx, 'question_all'] = question_all
        else:
            cookies = getcookie.get_cookie()
            question_all, isVaild = crawlers.get_question(uid=question_uid, cookies=cookies, num_retries=3)
            if not isValid:
                print('Error uid is', question_uid)
                cookies = getcookie.get_cookie()
            prepare_data.loc[idx, 'question_all'] = question_all
end_time = time.time()
print("Totally use" + str(end_time - start_time) + 's')
filesolver.write_pkl('../wiki_txt/' + result, prepare_data)