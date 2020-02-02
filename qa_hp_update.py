# -*- coding: utf-8 -*-
"""
Created on Mon May 29 14:00:29 2017

@author: solaris
"""


import pandas as pd
import conj
import os
import datetime


data = {
    'name':'15708426257',
    'password':'a12345123 '
       }

print("——————开始抓取主页信息——————")
headers = {
    'Accept': 'application/json, text/plain, */*',
             'Accept-Encoding': 'gzip, deflate, br',
             'Accept-Language': 'zh-CN,zh;q=0.9',
             'Connection': 'keep-alive',
           'Cookie':'SINAGLOBAL=6602033343892.077.1570419994353; SCF=AggyYma_GtwsdhDBuBkInChFzyrf4h-c6xIyt9HoFg9MxQLgRh8LSOCt3MqJrcT-LH9eeLX5n2fi4d0SwN2M4GA.; SUHB=0A0J9t8F9-KoqQ; SUB=_2A25wsu3HDeRhGeVJ41YQ9S_MzjmIHXVQXPOPrDV8PUJbkNBeLXPxkW1NT8Wm8BUecohZtXYin8k469UUZvH0Ae2V; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WFVwy_5W1_NYGmdgVwWBcfN5JpX5oz75NHD95Q0S0nXeK-peh-fWs4DqcjZUcL09sv_qJHyU5tt; wvr=6; _s_tentry=www.baidu.com; UOR=,,www.baidu.com; Apache=5693699685045.213.1577000839989; ULV=1577000840099:4:1:1:5693699685045.213.1577000839989:1571642766359; NEWEIBO-G0=0cc08d524c8e09533962296b43dff606; webim_unReadCount=%7B%22time%22%3A1577428517367%2C%22dm_pub_total%22%3A0%2C%22chat_group_client%22%3A0%2C%22allcountNum%22%3A4%2C%22msgbox%22%3A0%7D',
             'Sec-Fetch-Mode': 'cors',
             'Sec-Fetch-Site': 'same-origin',
             'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0_1 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A402 Safari/604.1'
           }

crawlers = conj.Crawler(headers, data)
filesolver = conj.FileSolve()

ticks = datetime.date.today().strftime('%y%m%d')
filesolver.dir_jdg('../qa_txt/qa_homepage/update')
ticks_dir = '../qa_txt/qa_homepage/update' + '/' + ticks + '.pkl'
author_dta = filesolver.file_jdg(ticks_dir, pd.DataFrame(columns=['avat_url', 'identity', 'profile_url', 'nickname', 'intro', 'ask_url', 'content_url', 'look_num', 'question_num', 'uid']))
hlr_flie_name = '../qa_txt/qa_homepage/'+ ticks +'_record.pkl'
homepage_file_record = filesolver.file_jdg(hlr_flie_name, [])
page_url = 'https://e.weibo.com/v1/public/h5/aj/qa/getfamousanswer?fieldtype=all&page=1&pagesize=1000'
page_num = crawlers.get_ajsn(url=page_url, num_retries=2)['data']['total_page']
for i in range(page_num):
    url_num = str(i+1)
    file_name = '../qa_txt/qa_homepage/update/'+url_num+'.pkl'
    if not os.path.exists(file_name):
        print('——————正在抓取主页第%s页信息——————' % url_num)
        homepage_record = crawlers.download_homepage(url_num=url_num, num_retries=2, filename=file_name)
        author_dta = author_dta.append(homepage_record)
        homepage_file_record.append(file_name)
        filesolver.write_pkl(hlr_flie_name, homepage_file_record)
print('主页信息抓取完毕， 共抓取%s条信息' % len(author_dta))
author_dta = author_dta.drop_duplicates()
filesolver.write_pkl(ticks_dir, author_dta)
for file in homepage_file_record:
    os.remove(file)
os.remove(hlr_flie_name)
print('主页信息存储完毕')





