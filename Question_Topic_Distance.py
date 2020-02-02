"""
Question_Topic_Distance is for calculate the distance between answers' default tag and their each question' topic

def get_word2Vec_model gets the word2Vec model trained before
"""
# encoding: utf-8

import pandas as pd
import gensim
from gensim import corpora, models
from gensim.models import FastText
import pickle
import numpy as np
import io

def load_vectors(fname):
    fin = io.open(fname, 'r', encoding='utf-8', newline='\n', errors='ignore')
    n, d = map(int, fin.readline().split())
    data = {}
    for line in fin:
        tokens = line.rstrip().split(' ')
        data[tokens[0]] = map(float, tokens[1:])
    return data

def get_word2vec_model(model_path):
    """
    This def can get word2vec model.Use it can get whether the model load success.When load success return the model, else return a string.
    :param model_path:where the word2vec model saved.
    :return: the model we want
    Raises: FileNotFoundError: An error occurred searching model and return a string when occurred.
    """
    try:
        # model = FastText.load_fasttext_format(model_path)
        model = gensim.models.Word2Vec.load(model_path)
        print('model load successfully')
        return model
    except FileNotFoundError:
        return 'model load fail'


# This part is to count the average distance between questions' topic and label and save into average_distance.pkl.
#
file = open('/home/nxz2/nxz/result_0120_pd.pkl', 'rb')
Sina_data = pickle.load(file)
print(len(Sina_data.question_context[0]))
model = get_word2vec_model('/home/nxz2/nxz/models/model_d500_w8_12_08.w2vec')
# mdel = gensim.models.NormModel.normalize(model, '/home/nxz2/nxz/models/model_d500_w8_12_08.w2vec')

print('Training distance begin ...................................')
print(Sina_data['label'][:10])
average_distance = []
label_unavailable = []
i = 0
count = 0
distance_count = []
for index,label in enumerate(Sina_data['label']):
    try:

        for word in Sina_data.question_context[index]:
            try:
                # count the average distance between each label word and topic kewywords.
                distance = model.wv.distance(label, word)
                # if distance < 0:
                #     print(distance)
                distance_count.append(distance)
            except:
                print('For %s, there is a zero.' % index)
                print(word)
                # distance_count.append(0)
        # count count the average distance between label and topic kewywords.
        # if sum(distance_count) != 0:
        #     average_distance.append(sum(distance_count) / len(Sina_data.question_context[index]))
        # else:
        #     average_distance.append(0)
        # count = 0
    except KeyError as keyerror:
        label_unavailable.append(index)  # save the label which cannot train by model
        print(index, '\n', keyerror)

# label_unavailable_unique = []
# for label in label_unavailable:
#     if label not in label_unavailable_unique:
#         label_unavailable_unique.append(label)
# print('lu', label_unavailable_unique)
# print('len lu', len(label_unavailable_unique))
# print('len ad', len(average_distance))
# print(average_distance)
# output_file = '/home/nxz2/nxz/average_distance.pkl'
# output = open(output_file, 'wb')
# pickle.dump(average_distance, output)
# output.close()

print(np.mean(distance_count))
print(np.median(distance_count))
print(np.std(distance_count))
print(np.percentile(distance_count, 25))
print(np.percentile(distance_count, 75))
print(max(distance_count))
print(min(distance_count))
