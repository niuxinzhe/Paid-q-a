"""
Prepare_data.py uses 8 steps to do data processing.
"""

import pandas as pd
import jieba
import re
import pickle
import numpy as np
import conj
from os import path
import io
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler


def norm_log(dta, attributs, norm_method):
    for at in attributes_lst:
        try:
            norm = norm_method.fit_transform(dta[at].reshape(-1,1))
            dta[at+'_log'] = np.log(norm+1)
        except ValueError:
            print(at)

def fk(question, bihua, sen_count):
    question_str = ''
    for word in question:
        question_str += word
    total_bihua = 0
    count = 0
    for word in question_str:
        try:
            total_bihua += bihua[word]
            count += 1
        except KeyError:
            print(word)
    fk = (11.8*total_bihua/count) + (0.39*len(question_str)/sen_count) - 15.59
    return fk


def load_vectors(fname):
    fin = io.open(fname, 'r', encoding='utf-8', newline='\n', errors='ignore')
    n, d = map(int, fin.readline().split())
    data = {}
    for line in fin:
        tokens = line.rstrip().split(' ')
        data[tokens[0]] = map(float, tokens[1:])
    return data

def text_cut(questions):
    """
    This def will cut the text into words by jieba and del the stopwords then return the words,else return a string when
    fail to find the stop_list.
    :param questions: A list of text.
    :return: A list of cut-words
    Raises: FileNotFoundError: An error occurred searching stop-list and return a string when occurred.
    """

    pattern = re.compile(u'[\u4e00-\u9fa5]+')
    questions_chinese = questions.apply(lambda x: ' '.join(re.findall(pattern, x)))
    # del the punctuation
    pattern_cut_punctuation = re.compile((u'[\，\。\？\!\～\（\）\〈\《\》\——\“\”\<\>\［\］\、\：\「\」\【\】]'))
    questions_chinese = questions_chinese.apply(lambda x: pattern_cut_punctuation.sub('', x))
    cut = lambda s: jieba.lcut(s) # use it to cut question
    question_cut = questions_chinese.apply(cut)  # use broadcast to improve the speed of cut words
    return question_cut


def del_stopwords(questions, stop_list):
    """
    This def will del the stopwords then return the words
    :param questions: A list of text.
    :param stop_list: A txt of stop words.
    :return: A list of cut-words which have deleted stop-words.
    Raises: FileNotFoundError: An error occurred searching stop-list and return a string when occurred.
    """
    stop = pd.read_csv(stop_list, encoding='utf-8', header=None, sep='tipdm')
    stop = [' ', ''] + list(stop[0])
    # question_split = questions.apply(lambda s: s.split(' '))  # split questions into words
    question_del_stopwords = questions.apply(lambda x: [i for i in x if i not in stop]) #  del the stopwords
    return question_del_stopwords


def data_group(data, group_list, beta):
    """
    groupby the data and do the select
    :param data: the data need to be group which should contain the feature 'the number of questions answered'. DataFrame
    :param group_list: select which to groupby. List
    :param beta: select the number bigger than beta. Int
    :return: group_selected: DataFramequeques
    """
    group = data.groupby(group_list).sum()
    group_selected = group[group['the number of questions answered'] > beta]
    return group_selected


filesovler = conj.FileSolve()

dta = filesovler.read_pkl('../qa_txt/question_all.pkl')
user_dta = filesovler.read_pkl('../qa_txt/qa_users/update/191024.pkl')
user_dta = user_dta.dropna().drop_duplicates(subset='uid')
dta1_name = '../qa_txt/prepare_dta1.pkl'
dta2_name = '../qa_txt/prepare_dta2.pkl'
dta3_name = '../qa_txt/prepare_dta3.pkl'
dta4_name = '../qa_txt/prepare_dta4.pkl'
dta5_name = '../qa_txt/prepare_dta5.pkl'
dta6_name = '../qa_txt/prepare_dta6.pkl'
dta7_name = '../qa_txt/prepare_dta7.pkl'
dta8_name = '../qa_txt/prepare_dta8.pkl'

if not path.exists(dta1_name):
    answer_dta = filesovler.read_pkl('../qa_txt/author_info.pkl')
    question_dta = dta[
        ['asker_uid', 'question_all', 'time', 'author_name', 'look_price', 'onlooker_count', 'ask_price', 'label']]
    question_dta = question_dta.dropna(subset=['question_all'])
    question_dta = question_dta.reset_index(drop=True)
    answer_uid = answer_dta[['uid', 'nickname']].set_index('nickname').to_dict()
    user_uid = user_dta[['uid', 'name']].set_index('name').to_dict()
    question_dta['answer_uid'] = ''
    for idx in question_dta.index:
        try:
            question_dta['answer_uid'].iloc[idx] = user_uid['uid'][question_dta['author_name'].iloc[idx]]
        except KeyError:
            try:
                question_dta['answer_uid'].iloc[idx] = answer_uid['uid'][question_dta['author_name'].iloc[idx]]
            except KeyError as e:
                print(e)
    print(question_dta.info())
    filesovler.write_pkl('../qa_txt/prepare_dta1.pkl', question_dta)

if not path.exists(dta2_name):
    question_dta = filesovler.read_pkl(dta1_name)
    del user_dta['name']
    del user_dta['follow_count']
    answer = user_dta.rename(
        columns={'uid': 'answer_uid', 'followers_count': 'answer_followers_count', 'urank': 'answer_urank',
                 'gender': 'answer_gender', 'verified_type': 'answer_verified_type'})
    asker = user_dta.rename(columns={'uid': 'asker_uid', 'followers_count': 'asker_followers_count', 'urank': 'asker_urank',
                                     'gender': 'asker_gender', 'verified_type': 'asker_verified_type'})
    question_dta = pd.merge(question_dta, answer, how='left', on=['answer_uid'])
    question_dta = pd.merge(question_dta, asker, how='left', on=['asker_uid'])
    print(question_dta.info())
    question_dta = question_dta[question_dta['look_price'] == 1]
    print(len(question_dta))
    filesovler.write_pkl('../qa_txt/prepare_dta2.pkl', question_dta)

if not path.exists(dta3_name):
    question_dta = filesovler.read_pkl(dta2_name)
    # print('***Question length***')
    # question_len = []
    # for question in question_dta['question_all']:
    #     question_len.append(len(question))
    # question_dta['question_len'] = question_len

    print('***cut word now***')
    stop = '/Users/wisdombeat/PycharmProjects/wiki_txt/baidu_stopwords.txt'
    question_dta['Sina_cut'] = text_cut(question_dta['question_all'])  # cut word and del stopword
    question_dta['question_context'] = del_stopwords(question_dta['Sina_cut'], stop)
    del question_dta['Sina_cut']
    # stopword_count = []
    # for i in question_dta.index:
    #     try:
    #         sc = len(question_dta['Sina_cut'][i]) - len(question_dta['question_context'][i])  # count the stop word
    #         stopword_count.append(sc)
    #     except:
    #         print(i)
    # question_dta['stopword_count'] = stopword_count
    filesovler.write_pkl('../qa_txt/prepare_dta3.pkl', question_dta)

if not path.exists(dta4_name):
    question_dta = filesovler.read_pkl(dta3_name)
    print(question_dta.info())
    question_dta = question_dta.dropna()
    question_dta = question_dta[question_dta['label'] != '']
    question_dta = question_dta[question_dta['label'] != '无分类']
    question_dta = question_dta.reset_index(drop=True)
    for idx in question_dta.index:
        if question_dta['label'].iloc[idx] == '时事':
            question_dta['label'].iloc[idx] = ['社会', '民生']
        elif question_dta['label'].iloc[idx] == '军事':
            question_dta['label'].iloc[idx] = ['社会', '民生']
        elif question_dta['label'].iloc[idx] == '职场':
            question_dta['label'].iloc[idx] = ['社会', '民生']
        elif question_dta['label'].iloc[idx] == '政府官员':
            question_dta['label'].iloc[idx] = ['社会', '民生']
        elif question_dta['label'].iloc[idx] == '政府':
            question_dta['label'].iloc[idx] = ['社会', '民生']
        elif question_dta['label'].iloc[idx] == '宗教':
            question_dta['label'].iloc[idx] = ['社会', '民生']
        elif question_dta['label'].iloc[idx] == '段子手':
            question_dta['label'].iloc[idx] = ['娱乐', '休闲']
        elif question_dta['label'].iloc[idx] == '媒体人':
            question_dta['label'].iloc[idx] = ['娱乐', '休闲']
        elif question_dta['label'].iloc[idx] == '媒体':
            question_dta['label'].iloc[idx] = ['娱乐', '休闲']
        elif question_dta['label'].iloc[idx] == '娱乐明星':
            question_dta['label'].iloc[idx] = ['娱乐', '休闲']
        elif question_dta['label'].iloc[idx] == '音乐':
            question_dta['label'].iloc[idx] = ['娱乐', '休闲']
        elif question_dta['label'].iloc[idx] == '游戏':
            question_dta['label'].iloc[idx] = ['娱乐', '休闲']
        elif question_dta['label'].iloc[idx] == '搞笑幽默':
            question_dta['label'].iloc[idx] = ['娱乐', '休闲']
        elif question_dta['label'].iloc[idx] == '星座命理':
            question_dta['label'].iloc[idx] = ['娱乐', '休闲']
        elif question_dta['label'].iloc[idx] == '动漫':
            question_dta['label'].iloc[idx] = ['娱乐', '休闲']
        elif question_dta['label'].iloc[idx] == '综艺节目':
            question_dta['label'].iloc[idx] = ['娱乐', '休闲']
        elif question_dta['label'].iloc[idx] == '美女帅哥':
            question_dta['label'].iloc[idx] = ['娱乐', '休闲']
        elif question_dta['label'].iloc[idx] == '电视剧':
            question_dta['label'].iloc[idx] = ['娱乐', '休闲']
        elif question_dta['label'].iloc[idx] == '电影':
            question_dta['label'].iloc[idx] = ['娱乐', '休闲']
        elif question_dta['label'].iloc[idx] == '收藏':
            question_dta['label'].iloc[idx] = ['娱乐', '休闲']
        elif question_dta['label'].iloc[idx] == '旅游出行':
            question_dta['label'].iloc[idx] = ['娱乐', '休闲']
        elif question_dta['label'].iloc[idx] == '韩星':
            question_dta['label'].iloc[idx] = ['娱乐', '休闲']
        elif question_dta['label'].iloc[idx] == '财经':
            question_dta['label'].iloc[idx] = ['经济', '金融']
        elif question_dta['label'].iloc[idx] == '房地产':
            question_dta['label'].iloc[idx] = ['经济', '金融']
        elif question_dta['label'].iloc[idx] == '电商':
            question_dta['label'].iloc[idx] = ['经济', '金融']
        elif question_dta['label'].iloc[idx] == '区域号':
            question_dta['label'].iloc[idx] = ['区域']
        elif question_dta['label'].iloc[idx] == '海外':
            question_dta['label'].iloc[idx] = ['区域']
        elif question_dta['label'].iloc[idx] == '读书作家':
            question_dta['label'].iloc[idx] = ['文化', '艺术']
        elif question_dta['label'].iloc[idx] == '历史':
            question_dta['label'].iloc[idx] = ['文化', '艺术']
        elif question_dta['label'].iloc[idx] == '设计美学':
            question_dta['label'].iloc[idx] = ['文化', '艺术']
        elif question_dta['label'].iloc[idx] == '美妆':
            question_dta['label'].iloc[idx] = ['文化', '艺术']
        elif question_dta['label'].iloc[idx] == '摄影拍照':
            question_dta['label'].iloc[idx] = ['文化', '艺术']
        elif question_dta['label'].iloc[idx] == '人文艺术':
            question_dta['label'].iloc[idx] = ['文化', '艺术']
        elif question_dta['label'].iloc[idx] == '时尚':
            question_dta['label'].iloc[idx] = ['文化', '艺术']
        elif question_dta['label'].iloc[idx] == '健康医疗':
            question_dta['label'].iloc[idx] = ['医疗']
        elif question_dta['label'].iloc[idx] == '体育':
            question_dta['label'].iloc[idx] = ['体育', '运动']
        elif question_dta['label'].iloc[idx] == '科学科普':
            question_dta['label'].iloc[idx] = ['科学']
        elif question_dta['label'].iloc[idx] == '母婴育儿':
            question_dta['label'].iloc[idx] = ['健康', '生活']
        elif question_dta['label'].iloc[idx] == '汽车':
            question_dta['label'].iloc[idx] = ['健康', '生活']
        elif question_dta['label'].iloc[idx] == '运动健身':
            question_dta['label'].iloc[idx] = ['健康', '生活']
        elif question_dta['label'].iloc[idx] == '动物宠物':
            question_dta['label'].iloc[idx] = ['健康', '生活']
        elif question_dta['label'].iloc[idx] == '美食':
            question_dta['label'].iloc[idx] = ['健康', '生活']
        elif question_dta['label'].iloc[idx] == '健康养生':
            question_dta['label'].iloc[idx] = ['健康', '生活']
        elif question_dta['label'].iloc[idx] == '婚庆服务':
            question_dta['label'].iloc[idx] = ['健康', '生活']
        elif question_dta['label'].iloc[idx] == '情感两性':
            question_dta['label'].iloc[idx] = ['心理']
        elif question_dta['label'].iloc[idx] == '互联网':
            question_dta['label'].iloc[idx] = ['互联网']
        elif question_dta['label'].iloc[idx] == '教育':
            question_dta['label'].iloc[idx] = ['教育']
        elif question_dta['label'].iloc[idx] == '数码':
            question_dta['label'].iloc[idx] = ['数码']
        elif question_dta['label'].iloc[idx] == '法律':
            question_dta['label'].iloc[idx] = ['法律']
        elif question_dta['label'].iloc[idx] == '公益':
            question_dta['label'].iloc[idx] = ['公益']
    print(question_dta['label'].unique())
    print(question_dta.info())
    filesovler.write_pkl(dta4_name, question_dta)

if not path.exists(dta5_name):
    question_dta = filesovler.read_pkl(dta4_name)
    question_dta['distance'] = float('inf')
    print(question_dta.info())
    word_vector = filesovler.read_pkl('../qa_txt/word_vector.pkl')
    word_norm = filesovler.read_pkl('../qa_txt/word_norm.pkl')
    print('模型加载完毕')

    for idx in question_dta.index:
        if question_dta['distance'].iloc[idx] == float('inf'):
            count = 0
            total_distance = 0
            for label in question_dta['label'].iloc[idx]:
                vector1 = word_vector[label]
                for word in question_dta['question_context'].iloc[idx]:
                    try:
                        vector2 = word_vector[word]
                        cos_dis = np.dot(vector1,vector2)/(word_norm[label]*word_norm[word])
                        count += 1
                        total_distance += cos_dis
                    except KeyError:
                        print(idx, word)
                    except ValueError as e:
                        print(e)
                        print(idx, len(vector1), len(vector2))
            if count != 0:
                question_dta['distance'].iloc[idx] = total_distance/count

            filesovler.write_pkl(dta5_name, question_dta)
            print('%s data has solved.' % str(idx))
    question_dta = question_dta[question_dta['distance'] != float('inf')]
    print(question_dta.info())
    filesovler.write_pkl(dta5_name, question_dta)

if not path.exists(dta6_name):
    question_dta = filesovler.read_pkl(dta5_name)
    question_dta.dropna(inplace=True)
    question_dta = question_dta.reset_index(drop=True)
    bihua = {}
    with open('../bihua.txt') as f:
        for line in  f.readlines():
            l = line.strip().split()
            bihua[l[0]] = int(l[1])
    bihua['一'] = 1
    stop_word = ['?','.','。','？','！']
    sen_count = []
    for question in question_dta['question_all']:
        s_c = 0
        for sw in stop_word:
            s_c += question.count(sw)
        if s_c == 0:
            s_c = 1
        sen_count.append(s_c)
    question_dta['sen_count'] = sen_count
    question_dta['readability'] = 0
    for idx in question_dta.index:\
        question_dta['readability'].iloc[idx] = fk(question_dta['question_context'].iloc[idx], bihua, question_dta['sen_count'].iloc[idx])
    filesovler.write_pkl(dta6_name, question_dta)

if not path.exists(dta7_name):
    question_dta = filesovler.read_pkl(dta6_name)
    attributes_lst = ['ask_price', 'answer_followers_count', 'answer_urank', 'asker_followers_count', 'asker_urank', 'onlooker_count', 'distance','readability']
    minmax_scaler = MinMaxScaler()
    norm_log(question_dta, attributes_lst, minmax_scaler)
    print(question_dta.info())
    filesovler.write_pkl(dta7_name, question_dta)

if not path.exists(dta8_name):
    question_dta = filesovler.read_pkl(dta7_name)
    length = []
    word_num = []
    for idx in question_dta.index:
        length.append(len(question_dta['question_all'].iloc[idx]))
        word_num.append(len(question_dta['question_context'].iloc[idx]))
    question_dta['question_len'] = length
    question_dta['question_len_squared'] = question_dta['question_len']**2
    question_dta['word_num'] = word_num
    question_dta['word_num_squared'] = question_dta['word_num']**2
    question_dta['gen_dis'] = 1 - question_dta['distance']
    attributes_lst = ['question_len', 'word_num', 'gen_dis', 'question_len_squared', 'word_num_squared']
    minmax_scaler = MinMaxScaler()
    norm_log(question_dta, attributes_lst, minmax_scaler)
    question_dta['gen_dis_log'] = np.log(question_dta['gen_dis']+1)
    print(question_dta.info())
    filesovler.write_pkl(dta8_name, question_dta)











