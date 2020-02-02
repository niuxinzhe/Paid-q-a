#word_embedding code is for training Word2Vec
#Word2Vec expects single sentences,each one as a list of words.In other words,the input format is a list of lists
#So The first step is to split a article into sentences.Then train the model
import pickle,pprint
import numpy
import pandas as pd
import re,os
import logging
from gensim.models import word2vec
from chardet import detect

#Define a function to get the root of articles that have been cut
def get_file_list(rootdir):
    #creat a list to save the roots
    #
    lists = []
    dirs = os.listdir(rootdir)
    for dir in dirs:
        #combine the file and its path
        path = os.path.join(rootdir,dir)
        lists.append(path)
    #
    #Return the list of path
    return lists


#Define a function to split a article into parsed sentences
def get_sentences(input):
    # Function to split a article into parsed sentences.Return a list of sntences, where each sentence is a list of
    # words
    #
    #1.Use re to split the paragraph into sentences
    #2.Loop over each sentence
    sentences = []
    for file in input:
        pkl_file_for_split = pickle.load(open(file, 'rb')).decode('utf-8')
        #if a paragraph is empty, skip it
        if pkl_file_for_split:
            #Otherwise, call re.split to get a list of words
            sep_line = re.split('(.*?[\。\？\，\！])', pkl_file_for_split)
            #del punctuation
            punction = re.compile(u'[’!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~]+')
            sep_line = punction.sub('', str(sep_line))
            sentences.append(sep_line.split())
            if len(sentences)% 10000 == 0:
                #print record
                print(sentences[-1])
    #
    #Return the list of sentences(each sentence is a list of words,so this returns a list of lists
    return sentences

#Define a function to train Word2Vec model
def Word2Vec_train(sentences, model_name,num_features=100, min_word_count=5, num_workers=3):
    #This function gets the sentences list to train the model,then save the result
    #Give it a meaningful model name
    #num_features is the dimensionality of the feature vectors.Defaults:100
    #min_word_count= ignore all words with total frequency lower than this.Defaults:5
    #num_workers= use this many worker threads to train the model (=faster training with multicore machines).Defaults:3
    #log the output messages
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    # Initialize and train the model
    print('Training model...')
    print(model_name, num_workers, num_features, min_word_count)
    model = word2vec.Word2Vec(sentences, workers=num_workers,
                              size=num_features, min_count=min_word_count)
    #Save the model
    model.init_sims(replace=True)
    model.save(model_name)
    print('Make a test')
    print(len(model.wv.vocab))
    print(model.wv['自然'])

    #
    #Return the model for test if you want
    return model

rootdir_for_split = '/home/nxz/wiki_cut'
print('get the path of articles that cut')
list_name_for_split = get_file_list(rootdir_for_split)
print('Parsing sentences from articles cut')
sentences = get_sentences(list_name_for_split)
print(len(sentences))
print(sentences[0])
#Set values for various parameters
num_features = 500   # Word vector dimensionality
min_word_count = 0   # This helps limit the size of the vocabulary to meaningful words.
num_workers = 12      # Number of threads to run in parallel
#context = 10        # Context window size
#downsampling = 1e-3 # Downsample setting for frequent words
model_name = '500features_0min_word_count_12num_workers'
Word2Vec_train(sentences, model_name, num_features, min_word_count, num_workers)
