import pandas as pd
import conj
import os
import math
import datetime


data = {
    'name':'15708426257',
    'password':'a12345123 '
       }

headers = {'Accept': 'application/json, text/plain, */*',
 'Accept-Encoding': 'gzip, deflate, br',
 'Accept-Language': 'zh-CN,zh;q=0.9',
 'Connection': 'keep-alive',
 'Cookie': 'SINAGLOBAL=6602033343892.077.1570419994353; SCF=AggyYma_GtwsdhDBuBkInChFzyrf4h-c6xIyt9HoFg9MxQLgRh8LSOCt3MqJrcT-LH9eeLX5n2fi4d0SwN2M4GA.; SUHB=0A0J9t8F9-KoqQ; SUB=_2A25wsu3HDeRhGeVJ41YQ9S_MzjmIHXVQXPOPrDV8PUJbkNBeLXPxkW1NT8Wm8BUecohZtXYin8k469UUZvH0Ae2V; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WFVwy_5W1_NYGmdgVwWBcfN5JpX5oz75NHD95Q0S0nXeK-peh-fWs4DqcjZUcL09sv_qJHyU5tt; wvr=6; _s_tentry=www.baidu.com; UOR=,,www.baidu.com; Apache=5693699685045.213.1577000839989; ULV=1577000840099:4:1:1:5693699685045.213.1577000839989:1571642766359; NEWEIBO-G0=0cc08d524c8e09533962296b43dff606; webim_unReadCount=%7B%22time%22%3A1577428517367%2C%22dm_pub_total%22%3A0%2C%22chat_group_client%22%3A0%2C%22allcountNum%22%3A4%2C%22msgbox%22%3A0%7D',
 'Host': 'e.weibo.com',
 'Referer': 'https://e.weibo.com/v1/public/center/qauthor?uid=1039916297',
 'Sec-Fetch-Mode': 'cors',
 'Sec-Fetch-Site': 'same-origin',
 'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0_1 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A402 Safari/604.1'}


crawlers = conj.Crawler(headers, data)
filesolver = conj.FileSolve()

print("——————开始抓取问题信息——————")
ticks = datetime.date.today().strftime('%y%m%d')
question_dta = filesolver.file_jdg('../qa_txt/question'+ticks+'.pkl', pd.DataFrame(columns = ['time','oid','vtype','avatar','profile_url', 'ask_url','content_url','intro','nickname','onlooker_count','look_status','ask_price','look_price']))
ticks_dir = '../qa_txt/qa_homepage/update' + '/' + ticks + '.pkl'
author_dta = filesolver.read_pkl(ticks_dir)
author_dta = author_dta.reset_index(drop=True)
mark = False
for idx in author_dta.index:
    if idx % 100 == 0:
        print('正在抓取第%s位' % str(idx))
    author_name = author_dta['nickname'].iloc[idx]
    print('——————正在抓取%s回答的问题——————' % author_name)
    author_dir_name = '../qa_txt/qa_question'+ticks+'/'+ author_name
    filesolver.dir_jdg(author_dir_name)
    record_filename = author_dir_name + '/' + author_name + '.pkl'
    record_dta = filesolver.file_jdg(record_filename, pd.DataFrame(
        columns=['time', 'oid', 'vtype', 'avatar', 'profile_url', 'ask_url', 'content_url', 'intro', 'nickname',
                 'onlooker_count', 'look_status', 'ask_price', 'look_price']))
    author_record_file_name = author_dir_name + '/author_record.pkl'
    author_record = filesolver.file_jdg(author_record_file_name, {})
    uid = str(author_dta['uid'].iloc[idx])
    if len(record_dta) == 0:
        mark = True
    pages = math.ceil(author_dta['question_num'].iloc[idx]/5)
    for i in range(pages):
        page = str(i+1)
        try:
            question_len_record = author_record[page]
        except KeyError:
            question_len_record = 0
        page_file_name = author_dir_name + '/' + page + '.pkl'
        if not os.path.exists(page_file_name):
            if int(page) % 10 == 0:
                print('——————正在抓取%s回答的问题列表第%s页——————' % (author_name, page))
            question_record = crawlers.download_question(uid=uid, page=page, num_retries=2, filename=page_file_name)
            record_dta = record_dta.append(question_record)
            author_record[page] = len(question_record)
            filesolver.write_pkl(author_record_file_name, author_record)
        elif os.path.exists(page_file_name) and mark:
            question_record = filesolver.read_pkl(page_file_name)
            record_dta = record_dta.append(question_record)
        elif page ==  str(pages) and question_len_record < 5:
            print('——————新增数据, 当前数据数： %s——————' % question_len_record)
            question_record = crawlers.download_question(uid=uid, page=page, num_retries=2,
                                                filename=page_file_name)
            if len(question_record) > question_len_record:
                record_dta = record_dta.append(question_record[question_len_record:])
                author_record[page] = len(question_record)
                filesolver.write_pkl(author_record_file_name, author_record)
    record_dta = record_dta.drop_duplicates()
    print('%s的问题已抓取完毕， 共抓取%d条数据' % (author_name, len(record_dta)))
    filesolver.write_pkl(record_filename, record_dta)
    question_dta = question_dta.append(record_dta)
question_dta = question_dta.drop_duplicates()
question_dta = question_dta.reset_index(drop=True)
print('问题信息抓取完毕， 共抓取%s条信息' % len(question_dta))
filesolver.write_pkl('../qa_txt/question'+ticks+'.pkl', question_dta)
print('问题信息存储完毕')