import requests
import pandas as pd
import pickle
import random
import re
import time
import logging
import sys
import get_cookie
from urllib import parse
import os


# def download(idx, url, cookies=None, question='', n=1, num_retries=2):
#     repost = -1
#     time.sleep(10)
#     try:
#         res = requests.get(url, cookies=cookies)
#         if res.cookies:
#             if '<h4 class=\\"WB_feed_spec_tit W_autocut\\">'+question in res.text:
#                 repost = 1
#             elif '你搜的太频繁了，休息一会再搜吧' in res.text:
#                 time.sleep(300*n)
#                 n += 1
#                 repost = download(idx, url, cookies, question, n, num_retries)
#             else:
#                 repost = 0
#         else:
#             logging.warning('WeiboSearch failed because the cookies has lost. Now we stop in %s', idx)
#             logging.warning('The wrong url is: %s', url)
#             cookies = get_cookie.get_cookie(fp)
#             return download(idx, url, cookies, question, n, num_retries)
#     except requests.RequestException as e:
#         logging.warning("Cannot get page beacause:", e)
#         logging.warning('Error idx is %s', idx)
#         logging.warning('The wrong url is: %s', url)
#         if hasattr(e, 'code'):
#             code = e.code
#             if num_retries > 0 and 500 <= code < 600:
#                 # retry 5XX HTTP errors
#                 return download(idx, url, cookies, question, n, num_retries - 1)
#             else:
#                 code = None
#     except requests.exceptions.ConnectTimeout:
#         NETWORK_STATUS = False
#     except requests.exceptions.Timeout:
#         REQUEST_TIMEOUT = True
#     return repost

def sleep_method(url, cookies, num_retries=2):
    sleep_time = 1
    while True:
        try:
            res = requests.get(url, cookies=cookies)
            if '<h4 class=\\"WB_feed_spec_tit W_autocut\\">' in res.text:
                return 1
            elif '你搜的太频繁了，休息一会再搜吧' in res.text:
                logging.warning('current sleep time is' + str(sleep_time) + 's')
                time.sleep(sleep_time)
            else:
                return 0
            sleep_time = sleep_time << 1
        except requests.RequestException as e:
            logging.warning("Cannot get page beacause:", e)
            logging.warning('Error idx is %s', idx)
            logging.warning('The wrong url is: %s', url)
            if hasattr(e, 'code'):
                code = e.code
                if num_retries > 0 and 500 <= code < 600:
                    # retry 5XX HTTP errors
                    return sleep_method(url, cookies, num_retries - 1)
                else:
                    code = None
        except requests.exceptions.ConnectTimeout:
            NETWORK_STATUS = False
        except requests.exceptions.Timeout:
            REQUEST_TIMEOUT = True


def read_pkl(filename):
    with open(filename, 'rb') as input_file:
        data = pickle.load(input_file)
    return data


def write_pkl(filename, data):
    with open(filename, 'wb') as output_file:
        pickle.dump(data, output_file)

if __name__ == '__main__':
    dir_list = os.listdir('../wiki_txt/')
    result = 'sina_data_in_3_month_search_result_1.pkl'
    input_file = 'sina_in_3_month_search_1.pkl'
    if result in dir_list:
        prepare_data = read_pkl('../wiki_txt/'+result)
    else:
        prepare_data = read_pkl('../wiki_txt/'+input_file)
        prepare_data['repost'] = None
    prepare_data = prepare_data.reset_index(drop = True)
    logging.warning(len(prepare_data))
# prepare_data = prepare_data[5083:]
    cookies = None
    fp = '{"os":"2","browser":"Chrome62,0,3202,9","fonts":"undefined","screenInfo":"1920*1080*24","plugins":"Portable Document Format::internal-pdf-viewer::Chrome PDF Plugin|::mhjfbmdgcfjbbpaeojofohoefgiehjai::Chrome PDF Viewer|::internal-nacl-plugin::Native Client"}'
    for idx in prepare_data.index:
        try:
            id = prepare_data.url[idx][0]
        except IndexError as ie:
            id = ''
        if id and (prepare_data.at[idx, 'repost'] is None):
            if idx % 50 == 0:
                write_pkl('../wiki_txt/'+result, prepare_data)
                logging.warning('cache is stored in pkl.')
                cookies = get_cookie.get_cookie(fp)
                logging.warning('cookie is :' + str(cookies))
            question = prepare_data.question_without_punc[idx]
            logging.warning('Question is :'+ question)
            url = dict(profile_ftype='1', is_all='1', is_search='1', key_word=question)
            search_url = 'https://weibo.com/u/' + id + '?' + parse.urlencode(url)
            logging.warning(search_url)
            repost = sleep_method(search_url, cookies)
            logging.warning(repost)
            prepare_data.loc[idx, 'repost'] = repost
        else:
            prepare_data.loc[idx, 'repost'] = -1
        write_pkl('../wiki_txt/' + result, prepare_data)

# question = prepare_data.question_without_punc[34]
# print(question)
# cookies = get_cookie.get_cookie(fp)
# res = requests.get('https://weibo.com/u/'+prepare_data.url[34][0]+'?profile_ftype=1&is_all=1&is_search=1&key_word='+question, cookies)
# print(res.cookies)
# print(res.text)
# print(question in res.text)

# inputfile = open('../wiki_txt/sina_fans.pkl', 'rb')
# sina_fans = pickle.load(inputfile)
# inputfile.close()
# del sina_fans['followers_count']
# dict_fans = sina_fans.set_index('author').T.to_dict('list')
# dict_fans['马栏坡的新鲜事'] = ''
# dict_fans['房地产门外汉'] = ''
# dict_fans['鲍秀兰'] = '1893410897'
# dict_fans['苏醒叨逼叨'] = ''
# dict_fans['电锯甜心小雨'] = ''
# dict_fans['不似旧日'] = ''
# dict_fans['放克灵魂'] = 'braass'
# dict_fans['地产课Chaige'] = '5827568512'
# dict_fans['江南愤青心'] = '2189910831'
# dict_fans['安易递'] = '575377776'
# dict_fans['JusFly-3'] = '2602793613'
# dict_fans['女权主义贴吧'] = ''
# dict_fans['肺癌专科医生王昆'] = '5706642300'
# dict_fans['协和胖大夫'] = '1766780711'
# dict_fans['生殖科老钱'] = '3913908806'
# dict_fans['陌川--女性互帮'] = ''
# dict_fans['陈左小'] = '6069074107'
# dict_fans['京城楼少'] = ''
# dict_fans['A股伏击者'] = '219911209'
# dict_fans['牙尖的南瓜'] = '1861868194'
# dict_fans['AAA心理医生'] = 'ww1045006'
# outputfile = open('../wiki_txt/sina_fans_dict.pkl', 'wb')
# pickle.dump(dict_fans, outputfile)
# outputfile.close()
# inputfile_2 = open('../wiki_txt/sina_in_3_month_new_data.pkl', 'rb')
# prepare_data = pickle.load(inputfile_2)
# inputfile_2.close()
# url = []
# no_url = []
# for author in prepare_data['author']:
#     try:
#         url.append(dict_fans[author])
#     except:
#         url.append('')
#         if author not in no_url:
#             no_url.append(author)
# prepare_data['url'] = url
#
# pattern_cut_punctuation = re.compile((u'[\，\。\？\!\～\（\）\〈\《\》\——\“\”\<\>\［\］\、\：\「\」\【\】\ ]'))
# prepare_data['question_without_punc'] = prepare_data['question'].apply(lambda x: pattern_cut_punctuation.sub('', x) )
# outputfile_2 = open('../wiki_txt/sina_in_3_month_search.pkl', 'wb')
# pickle.dump(prepare_data, outputfile_2)
# outputfile_2.close()

# inputfile3 = open('../wiki_txt/sina_in_3_month_search.pkl', 'rb')
# prepare_data = pickle.load(inputfile3)
# inputfile3.close()
# outputfile_2_1 = open('../wiki_txt/sina_in_3_month_search_1.pkl', 'wb')
# outputfile_2_2 = open('../wiki_txt/sina_in_3_month_search_2.pkl', 'wb')
# outputfile_2_3 = open('../wiki_txt/sina_in_3_month_search_3.pkl', 'wb')
# p1 = prepare_data[:int(len(prepare_data)/3)]
# p2 = prepare_data[int(len(prepare_data)/3):int(len(prepare_data)/3)*2]
# print(len(p2))
# p3 = prepare_data[int(len(prepare_data)/3)*2:]
# pickle.dump(p1, outputfile_2_1)
# outputfile_2_1.close()
# pickle.dump(p2, outputfile_2_2)
# outputfile_2_2.close()
# pickle.dump(p3, outputfile_2_3)
# outputfile_2_3.close()
