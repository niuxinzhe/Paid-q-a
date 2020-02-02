import pickle
import pandas as pd
from gensim import corpora, models, similarities
from collections import defaultdict

# file = open('/Users/wisdombeat/PycharmProjects/wiki_txt/sina_in_3_month_original_data.pkl', 'rb')
# prepare_data = pickle.load(file)
# file.close()
#
# # del the words which only appear only one time
# documents = prepare_data['Sina_cut']
# frequency = defaultdict(int)
# for text in documents:
#     for token in text:
#         frequency[token] += 1
# texts = [[token for token in text if frequency[token] > 1] for text in documents]
# print(len(texts))
#
# dictionary = corpora.Dictionary(texts)
# dictionary.save('/Users/wisdombeat/PycharmProjects/wiki_txt/sina_in_3_month_original_data.dict')
#
# corpus = [dictionary.doc2bow(text) for text in texts]
# corpora.MmCorpus.serialize('/Users/wisdombeat/PycharmProjects/wiki_txt/sina_in_3_month_original_data.mm', corpus)
#
# tf_idf = models.TfidfModel(corpus)
#
# index = similarities.SparseMatrixSimilarity(tf_idf[corpus], num_features= len(dictionary.token2id.keys()))
# sim = index[tf_idf[corpus]]
# outputfile = open('/Users/wisdombeat/PycharmProjects/wiki_txt/sina_in_3_month_original_sim.pkl', 'wb')
# pickle.dump(sim, outputfile)
# outputfile.close()

file = open('/Users/wisdombeat/PycharmProjects/wiki_txt/sina_in_3_month_original_sim.pkl', 'rb')
sim = pickle.load(file)
file.close()
count = 0
for i in sim:
    for j in i:
        if 0.8 < j < 1:
            count += 1
print(count/2)
# print(text_sim[:10])
# print(len(text_sim))
# outputfile = open('/Users/wisdombeat/PycharmProjects/wiki_txt/sina_in_3_month_original_text_sim.pkl', 'wb')
# pickle.dump(text_sim, outputfile)
# outputfile.close()

# file = open('/Users/wisdombeat/PycharmProjects/wiki_txt/sina_in_3_month_original_text_sim.pkl', 'rb')
# sim = pickle.load(file)
# print(sim[0])
# file.close()
# sina_file = open('/Users/wisdombeat/PycharmProjects/wiki_txt/sina_in_3_month_original_data.pkl', 'rb')
# data = pickle.load(sina_file)
# sina_file.close()
# # data = data.reset_index(drop=True)
# label_d = []
# topic_d = []
# price_d = []
# t = []
# # print(data.iloc[[7545]]['label'])
# for i in sim:
#     df = pd.DataFrame(columns=data.columns)
#     df = df.append(data.iloc[i[0]])
#     for j in i:
#     #     if str(data.iloc[[i[0]]]['label']) != str(data.iloc[[j]]['label']):
#     #         df = df.append(data.iloc[[j]])
#     # label_d.append(df)
#     #     if str(data.iloc[[i[0]]]['question_topics']) != str(data.iloc[[j]]['question_topics']):
#     #         df = df.append(data.iloc[[j]])
#     # topic_d.append(df)
#         if str(data.iloc[[i[0]]]['price']) != str(data.iloc[[j]]['price']):
#             df = df.append(data.iloc[[j]])
#     price_d.append(df)
# # print(len(label_d), len(topic_d), len(price_d))
# outputfile1 = open('/Users/wisdombeat/PycharmProjects/wiki_txt/label_difference.pkl', 'wb')
# pickle.dump(label_d, outputfile1)
# outputfile1.close()
# outputfile2 = open('/Users/wisdombeat/PycharmProjects/wiki_txt/topic_difference.pkl', 'wb')
# pickle.dump(topic_d, outputfile2)
# outputfile2.close()
# outputfile3 = open('/Users/wisdombeat/PycharmProjects/wiki_txt/price_difference.pkl', 'wb')
# pickle.dump(price_d, outputfile3)
# outputfile3.close()