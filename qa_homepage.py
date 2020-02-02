# -*- coding: utf-8 -*-
"""
Created on Mon May 29 14:00:29 2017

@author: solaris
"""


import pandas as pd
import conj
import os


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
           'Cookie':'_s_tentry=passport.weibo.com; Apache=6703165809074.767.1566712929067; SINAGLOBAL=6703165809074.767.1566712929067; ULV=1566712929881:1:1:1:6703165809074.767.1566712929067:; login_sid_t=e891aed0bf935a34540cca504d0e57bb; cross_origin_proto=SSL; UOR=,,login.sina.com.cn; NEWEIBO-G0=99a7817ee60927ac360996a486571011; SSOLoginState=1569137399; un=15708426257; wvr=6; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WFVwy_5W1_NYGmdgVwWBcfN5JpX5KMhUgL.FoeN1hBpSK27SK-2dJLoI7DNIPS.dcfb9g8X; ALF=1601004992; SCF=AhaIT1M0ewUcM8Y2GYsJdvwtYt-ES1QhTCwgBGq_1KrM_EpsnBwwMiYRAumrfdoUfc9vmnl1timo8gvtr0B8aDY.; SUB=_2A25wiEIVDeRhGeVJ41YQ9S_MzjmIHXVT_DTdrDV8PUNbmtBeLWTxkW9NT8Wm8FhHS5PnP73C3pIQ4bzMiNq-Bt_I; SUHB=0rr7i_JPZeW0H-; WBStorage=384d9091c43a87a5|undefined',
             'Sec-Fetch-Mode': 'cors',
             'Sec-Fetch-Site': 'same-origin',
             'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0_1 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A402 Safari/604.1'
           }

crawlers = conj.crawler(headers, data)
filesolver = conj.filesolve()

author_dta = filesolver.file_jdg('../qa_txt/author_info.pkl', pd.DataFrame(columns=['avat_url', 'identity', 'profile_url', 'nickname', 'intro', 'ask_url', 'content_url', 'look_num', 'question_num', 'uid']))
hlr_flie_name = '../qa_txt/qa_homepage/record.pkl'
homepage_len_record = filesolver.file_jdg(hlr_flie_name, {})
page_url = 'https://e.weibo.com/v1/public/h5/aj/qa/getfamousanswer?fieldtype=all&page=1&pagesize=1000'
page_num = crawlers.get_ajsn(url=page_url, num_retries=2)['data']['total_page']
for i in range(page_num):
    url_num = str(i+1)
    file_name = '../qa_txt/qa_homepage/'+url_num+'.pkl'
    try:
        len_record = homepage_len_record[url_num]
    except KeyError:
        len_record = 0
    if not os.path.exists(file_name):
        print('——————正在抓取主页第%s页信息——————' % url_num)
        homepage_record = crawlers.download_homepage(url_num=url_num, num_retries=2, filename=file_name)
        homepage_len_record[url_num] = len(homepage_record)
        filesolver.write_pkl(hlr_flie_name, homepage_len_record)
        author_dta = author_dta.append(homepage_record)
    elif url_num == str(page_num) and len_record < 900:
        print('——————正在抓取主页第%s页信息——————' % url_num)
        print('——————新增数据, 当前数据数： %s——————' % len_record)
        homepage_record = crawlers.download_homepage(url_num=url_num, num_retries=2,
                                            filename=file_name)
        if len(homepage_record) > len_record:
            author_dta = author_dta.append(homepage_record[len_record:])
            homepage_len_record[url_num] = len(homepage_record)
            print('——————新增数据, new数据数:%s——————' % str(len(homepage_record) - len_record))
            filesolver.write_pkl(hlr_flie_name, homepage_len_record)
print('主页信息抓取完毕， 共抓取%s条信息' % len(author_dta))
author_dta = author_dta.drop_duplicates()
filesolver.write_pkl('../qa_txt/author_info.pkl', author_dta)
print('主页信息存储完毕')





