import pprint, pickle, re, os, codecs,logging,jieba
from multiprocessing import Pool
from tqdm import tqdm

#打logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level = logging.INFO)


def filter_(s):
    #the first step is to del the other language
    pattern_cut_language = re.compile(u'[^\u3400-\u4DB5\u4E00-\u9FA5\u9FA6-\u9FBB\uF900-\uFA2D\uFA30-\uFA6A\uFA70-\uFAD9\uFF00-\uFFEF\u2E80-\u2EFF\u3000-\u303F'
        '\u31C0-\u31EF\u2F00-\u2FDF\u2FF0-\u2FFF\u3100-\u312F\u31A0-\u31BF\u3040-\u309F\u30A0-\u30FF\u31F0-\u31FF\uAC00-\uD7AF'
        '\u1100-\u11FF\u3130-\u318F\u4DC0-\u4DFF\uA000-\uA48F\uA490-\uA4CF\u2800-\u28FF\u3200-\u32FF\u3300-\u33FF\u2700-\u27BF'
        '\u2600-\u26FF\uFE10-\uFE1F\uFE30-\uFE4F]')
    first_filter = pattern_cut_language.sub('', s)
    #then del the punctuation(for don't know which punctuantion in the line, this step repeats several time )
    #save . , ? !
    pattern_cut_punctuation = re.compile((u'[\（\）\〈\《\》\——\“\”\<\>\［\］\、\：\「\」\【\】]'))
    return pattern_cut_punctuation.sub('', first_filter)

def get_file_list(rootdir):
    #the function is to find files' path
    lists = []
    dirs = os.listdir(rootdir)
    for dir in dirs:
        path = os.path.join(rootdir,dir)
        lists.append(path)
    del lists[0]
    return lists


def save_chinese(filelists):
    #the function transform the wikis into wiki_chineses which only own chinese
    i = 0
    w = tqdm(filelists, desc=u'已解析0篇文章')
    outputfile_all = open('/Users/wisdombeat/PycharmProjects/wiki_txt/wiki_chinese.pkl', 'wb+')
    for path in filelists:
        outputfile = codecs.open('/Users/wisdombeat/PycharmProjects/wiki_txt/wiki_chinese/line' + str(i) + '.pkl', 'wb+')
        pkl_file = pickle.load(open(path, 'rb')) #get the pickle
        result = filter_(pkl_file)
        pickle.dump(result, outputfile) #save the pickle
        pickle.dump(result, outputfile_all) # save all artical into wiki_chinese.pickle
        i += 1
        if i % 100 == 0:
            #print record
            w.set_description(u'已解析%s篇文章' % i)
        outputfile.close()
    outputfile_all.close()
    return

def cut_words(sentence):
    # use jieba to cut word
     return " ".join(jieba.cut(sentence)).encode('utf-8')

def split_words(input):
    # the function split the wiki_chineses into wiki_cuts
    i = 0
    w = tqdm(input, desc=u'已对0篇文章进行切词')
    outputfile_all = open('/Users/wisdombeat/PycharmProjects/wiki_txt/wiki_cut.pkl',
                                 'wb+')
    for files in input:
        outputfile = codecs.open('/Users/wisdombeat/PycharmProjects/wiki_txt/wiki_cut/line' + str(i) + '.pkl',
                                 'wb+')
        pkl_file_for_wordcut = pickle.load(open(files, 'rb'))
        result = cut_words(pkl_file_for_wordcut)
        pickle.dump(result, outputfile)
        i += 1
        if i % 100 == 0:
            w.set_description(u'已对%s篇文章进行切词' % i)
    #     outputfile.close()
    outputfile_all.close()


if __name__ == "__main__":
    #save each to pickle
    # rootdir = '../wiki_txt/wikis'
    # list_name = get_file_list(rootdir)
    # save_chinese(list_name)
    rootdir_for_split = '../wiki_txt/wikis'
    list_name_for_split = get_file_list(rootdir_for_split)
    split_words(list_name_for_split)
