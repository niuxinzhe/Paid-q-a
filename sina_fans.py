import conj
import pandas as pd
import os
import datetime


headers = {'accept': 'application/json, text/plain, */*',
 'accept-encoding': 'gzip, deflate, br',
 'accept-language': 'zh-CN,zh;q=0.9',
 'cookie': '_T_WM=51449543537; WEIBOCN_FROM=1110003030; ALF=1574402457; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WFVwy_5W1_NYGmdgVwWBcfN5JpX5K-hUgL.FoeN1hBpSK27SK-2dJLoI7DNIPS.dcfb9g8X; MLOGIN=1; SCF=AggyYma_GtwsdhDBuBkInChFzyrf4h-c6xIyt9HoFg9MZfRq8WQgL1pXQxVYwwayYLv_vu6gicNVt2KDE6ofZcM.; SUB=_2A25wq547DeRhGeVJ41YQ9S_MzjmIHXVQVyJzrDV6PUJbktANLVn4kW1NT8Wm8Dq_jO_PosHk-r88d5dazQKGDJ1n; SUHB=0IIVtU8bMn5Rc2; SSOLoginState=1571810923; XSRF-TOKEN=204527; M_WEIBOCN_PARAMS=luicode%3D10000011%26lfid%3D1076032393886580%26fid%3D1005052393886580%26uicode%3D10000011',
 'MWeibo-Pwa': '1',
 'sec-fetch-mode': 'cors',
 'sec-fetch-site': 'same-origin',
 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.9 Safari/537.36',
 'x-requested-with': 'XMLHttpRequest',
 'x-xsrf-token': '1751a7'}

data = {
    'name':'username',
    'password':'password'
       }

filesolver = conj.FileSolve()
crawler = conj.Crawler(headers, data)
ticks = datetime.date.today().strftime('%y%m%d')
filesolver.dir_jdg('../qa_txt/qa_users/update')
user_filename = '../qa_txt/qa_users/update/' + ticks + '.pkl'
user_dta = filesolver.file_jdg(user_filename, pd.DataFrame(columns= ['uid', 'name', 'follow_count', 'followers_count', 'urank', 'gender', 'verified_type']))
users = user_dta['uid']
user_info = filesolver.read_pkl('../qa_txt/user_info.pkl')
user_uid = user_info.keys()
print('___Begin___')
for uid in user_uid:
    if uid not in users:
        record_dta = crawler.get_sina_dta(uid=uid, num_retries=3)
        if record_dta != {}:
            user_dta = user_dta.append(record_dta, ignore_index=True)
    if len(user_dta) % 50 == 0:
        print('已抓取', len(user_dta))
        user_dta = user_dta.reset_index(drop=True)
        filesolver.write_pkl(user_filename, user_dta)
user_dta = user_dta.reset_index(drop=True)
filesolver.write_pkl(user_filename, user_dta)

