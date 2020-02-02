import requests, re, json, time
import pandas as pd
import pickle
import os
import random
from urllib import parse
from lxml import html, etree


class GetSinaCookie:
    def __init__(self, fp):
        self.fp = fp

    def get_cookie(self):
        res1 = requests.get('https://weibo.com')
        cookie = requests.utils.dict_from_cookiejar(res1.history[0].cookies)
        data2 = {'cb': (None, 'gen_callback'),
                 'fp': (None, self.fp)}
        res2 = requests.post('https://passport.weibo.com/visitor/genvisitor', files=data2)
        res2_json = GetSinaCookie.get_data_from_json(self, res2.text)
        tid = res2_json['data']['tid']
        para = dict(a='incarnate', t=tid, w=3 if (res2_json['data']['new_tid']) else 2, c='095', gc='',
                    cb='cross_domain', _rand=random.random())
        para['from'] = 'weibo'

        res3 = requests.get('https://passport.weibo.com/visitor/visitor?' + parse.urlencode(para))
        res3_json = GetSinaCookie.get_data_from_json(self, res3.text)
        cookie['SUB'] = res3_json['data']['sub']
        cookie['SUBP'] = res3_json['data']['subp']
        return cookie

    def get_data_from_json(self, text):
        json_text = text[text.index('(') + 1: text.index(')')]
        return json.loads(json_text)


class FileSolve:

    def read_pkl(self, filename):
        with open(filename, 'rb') as input_file:
            data = pickle.load(input_file)
        return data

    def write_pkl(self, filename, data):
        with open(filename, 'wb') as output_file:
            pickle.dump(data, output_file)

    def dir_jdg(self, dir_name):
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)

    def file_jdg(self, filename, dta):
        if not os.path.exists(filename):
            FileSolve.write_pkl(self, filename, dta)
        new_dta = FileSolve.read_pkl(self, filename)
        return new_dta


class Crawler:
    def __init__(self, headers, data):
        self.headers = headers
        self.data = data

    def get_ajsn(self, url='', num_retries=2):
        time.sleep(0.5)
        ajsn = {}
        n = 1
        try:
            res = requests.get(url=url, headers=self.headers, timeout=20)
            while res.status_code != 200:
                n = n * 2
                time.sleep(0.5 * n)
                res = requests.get(url=url, headers=self.headers, data = self.data, timeout=20)
                print(url)
            if res.text != '':
                ajsn = json.loads(res.text)
        except requests.RequestException as requests_error:
            print('Error 1')
            print("网页抓取异常：", requests_error)
            print('Error Url: ', url)
            if hasattr(requests_error, 'code'):
                code = requests_error.code
                if num_retries > 0 and 500 <= code < 600:
                    # retry 5XX HTTP errors
                    return Crawler.get_ajsn(self, url, num_retries - 1)
        except requests.exceptions.ConnectTimeout as ct_error:
            print('Error 2')
            print("网页抓取异常：", ct_error)
            print('Error Url: ', url)
            time.sleep(180)
            return Crawler.get_ajsn(self, url, num_retries)
        except requests.exceptions.SSLError as ssl_error:
            print('Error 3')
            print("网页抓取异常：", ssl_error)
            print('Error Url: ', url)
            time.sleep(180)
            return Crawler.get_ajsn(self, url, num_retries)
        except json.JSONDecodeError as json_error:
            print('Error 4')
            print("网页抓取异常：", json_error)
            print('Error Url: ', url)
            if '微博-出错了' in res.text:
                return ajsn
            else:
                time.sleep(180)
                return Crawler.get_ajsn(self, url, num_retries)

        return ajsn

    def download_homepage(self, url_num='1', num_retries=2, filename=''):
        record_dta = pd.DataFrame(
            columns=['avat_url', 'identity', 'profile_url', 'nickname', 'intro', 'ask_url', 'content_url', 'look_num',
                     'question_num'])
        url = 'https://e.weibo.com/v1/public/h5/aj/qa/getfamousanswer?fieldtype=all&page=' + url_num + '&pagesize=1000'
        ajsn = Crawler.get_ajsn(self, url=url, num_retries=num_retries)
        if ajsn != {}:
            record_dta = pd.DataFrame(ajsn['data']['list'])  # 直接将获取的存储信息的dict转为Dataframe
            p = re.compile(r'\d+')
            for idx in record_dta.index:
                uid = p.search(record_dta.at[idx, 'ask_url']).group()
                record_dta.at[idx, 'uid'] = uid
            fs = FileSolve()
            fs.write_pkl(filename, record_dta)

        return record_dta

    def download_question(self, uid='', page='', num_retries=2, filename=''):
        record_dta = pd.DataFrame(
            columns=['time', 'oid', 'vtype', 'avatar', 'profile_url', 'ask_url', 'content_url', 'intro', 'asker_name',
                     'onlooker_count', 'look_status', 'ask_price', 'look_price'])
        url = 'https://e.weibo.com/v1/public/h5/aj/qa/getauthor?uid=' + uid + '&page=' + page
        referer_url = 'https://e.weibo.com/v1/public/center/qauthor?uid=' + uid + '&page=' + page
        self.headers['Referer'] = referer_url
        ajsn = Crawler.get_ajsn(self, url=url, num_retries=num_retries)
        if ajsn != {}:
            record_dta = pd.DataFrame(ajsn['data']['list'])  # 直接将获取的存储信息的dict转为Dataframe
            p = re.compile(r'(?<=userinfo\?uid\=)\d+')
            for idx in record_dta.index:
                try:
                    profile_url = record_dta.at[idx, 'profile_url']
                    uid = p.search(profile_url).group()
                    record_dta['asker_uid'] = uid
                except TypeError:
                    print(url)
                    print(profile_url)
                except KeyError:
                    print(url)
            record_dta['ask_enable'] = ajsn['data']['ask_enable']
            record_dta['author_name'] = ajsn['data']['author_info']['nickname']
            record_dta['label'] = ajsn['data']['author_info']['label']
            record_dta.rename(columns={'nickname': 'asker_name'})
            fs = FileSolve()
            fs.write_pkl(filename, record_dta)
        return record_dta

    def get_question(self, uid = '', cookies = '', num_retries=3):
        time.sleep(0.8)
        url = 'https://weibo.com/ttwenda/p/show?id=' + uid
        question_result = None
        isValid = False
        if num_retries == 0:
            return question_result, isValid
        try:
            res = requests.get(url, cookies)
            tree = html.fromstring(res.text)
            try:
                question_sel = tree.cssselect('div.ask_con')[0].text_content()
                question_result = question_sel.replace('\n', '').replace(' ', '')
                isValid = True
            except IndexError as e:
                print('Error Url: ', url)
                try:
                    print('尝试直接获取问题')
                    res = requests.get(url = url, headers=self.headers, data=self.data)
                    tree = html.fromstring(res.text)
                    question_sel = tree.cssselect('div.ask_con')[0].text_content()
                    question_result = question_sel.replace('\n', '').replace(' ', '')
                    isValid = True
                except IndexError as e:
                    try:
                        print('判断问题是否已不存在')
                        question_sel = tree.cssselect('p.text')[0].text_content().replace('\n', '').replace(' ', '')
                        isValid = True
                        print(question_sel)
                    except IndexError:
                        print('无法获取问题，需尝试更换cookie')
                except etree.XMLSyntaxError:
                    print('网页解析异常， url为', url)
                    return Crawler.get_question(self, uid, cookies, num_retries - 1)
        except etree.XMLSyntaxError:
            print('网页解析异常，Error url为', url)
            return Crawler.get_question(self, uid, cookies, num_retries - 1)
        except requests.RequestException as requests_error:
            print('Error 1')
            print("网页抓取异常：", requests_error)
            print('Error Url: ', url)
            if hasattr(requests_error, 'code'):
                code = requests_error.code
                if num_retries > 0 and 500 <= code < 600:
                    # retry 5XX HTTP errors
                    return Crawler.get_question(self, uid, cookies, num_retries - 1)
        except requests.exceptions.ConnectTimeout as ct_error:
            print('Error 2')
            print("网页抓取异常：", ct_error)
            print('Error Url: ', url)
            time.sleep(180)
            return Crawler.get_question(self, uid, cookies, num_retries)
        except requests.exceptions.SSLError as ssl_error:
            print('Error 3')
            print("网页抓取异常：", ssl_error)
            print('Error Url: ', url)
            time.sleep(180)
            return Crawler.get_question(self, uid, cookies, num_retries)
        return question_result, isValid

    def get_sina_dta(self, uid='', num_retries=2):
        record_dta = {}
        url ='https://m.weibo.cn/api/container/getIndex?type=uid&value=' + uid
        ajsn = Crawler.get_ajsn(self, url=url, num_retries=num_retries)
        if ajsn != {}:
            try:
                record_dta['uid'] = uid
                user_info = ajsn['data']['userInfo']
                record_dta['name'] = user_info['screen_name']
                record_dta['follow_count'] = user_info['follow_count']
                record_dta['followers_count'] = user_info['followers_count']
                record_dta['urank'] = user_info['urank']
                record_dta['gender'] = user_info['gender']
                record_dta['verified_type'] = user_info['verified_type']
            except KeyError as e:
                print(e)
                print(url)
        else:
            print('Error url:', url)
        return record_dta








