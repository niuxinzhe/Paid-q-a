import pandas as pd
import conj
import os
import time


headers = {}
data = {
    'name':'username',
    'password':'password'
       }

crawlers = conj.Crawler(headers, data)
filesolver = conj.FileSolve()
print('————————开始抓取用户微博数据————————')
user_filename = ''
user_uid = filesolver.read_pkl(user_filename)['uid']
ticks = str(time.time())
filesolver.dir_jdg('../qa_txt/qa_question/update')
ticks_dir = '../qa_txt/qa_question/update' + '/' + ticks + '.pkl'
result_dta = filesolver.file_jdg(ticks_dir, pd.DataFrame(columns=['uid', 'avatar_hd', 'close_blue_v', 'cover_image_phone', 'description', 'follow_count', 'follow_me', 'followers_count', 'following', 'gender', 'id', 'like', 'like_me', 'mbrank', 'mbtype',
 'profile_image_url', 'profile_url', 'screen_name', 'statuses_count', 'urank', 'verified', 'verified_reason', 'verified_type', 'verified_type_ext']))
uid_record = list(result_dta['uid'])
for uid in user_uid:
    if uid not in uid_record:
        url = 'https://m.weibo.cn/profile/info?uid=' + uid
        ajsn = crawlers.get_ajsn(url=url, num_retries=2)
        if ajsn != {}:
            record_dta = pd.DataFrame(ajsn['data']['user'], index=[0])
            result_dta = result_dta.append(record_dta)
    if len(result_dta) % 100 == 0:
        filesolver.write_pkl(ticks_dir, result_dta)
filesolver.write_pkl(ticks_dir, result_dta)
