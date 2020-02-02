# 繁转简
from gensim.corpora.wikicorpus import extract_pages,filter_wiki
import bz2file
import re
import opencc
from tqdm import tqdm
import codecs

def wiki_replace(d):
#wikipedia Extractor will del the word with mark like {{}}; wikicorpus will del all punctuation,so I write a fuction to save the punctuation and the word in {{}}
    s = d[1]
    s = re.sub(':*{\|[\s\S]*?\|}', '', s)
    s = re.sub('[\s\S]*?', '', s)
    s = re.sub('(.){{([^{}\n]*?\|[^{}\n]*?)}}', '\\1[[\\2]]', s)
    s = filter_wiki(s)
    s = re.sub('\* *\n|\'{2,}', '', s)
    s = re.sub('\n+', '\n', s)
    s = re.sub('\n[:;]|\n +', '\n', s)
    s = re.sub('\n==', '\n\n==', s)
    s = u'【' + d[0] + u'】\n' + s
    return opencc.convert(s).strip()

# def fantojan_savetxt(input,output):
#     #this function will save all data into a txt
#     i = 0
#     w = tqdm(input, desc = u'已获取0篇文章') #tqdm is for progressbar prompt
#     f = codecs.open(output, 'w', encoding='utf-8')
#     for d in w:
#         if not re.findall('^[a-zA-Z]+:', d[0]) and d[0] and not re.findall(u'^#', d[1]):
#             s = wiki_replace(d)
#             f.write(s + '\n\n\n')
#             i += 1
#             if i % 100000 == 0:
#                 w.set_description(u'已获取%s篇文章' % i)
#     f.close()
#     w.set_description(u'All have done, there are %s papers' % i)
#     return

import pickle
def fantojan_savepickle(input):
    #this function will save every line into a pickle
    i = 0
    w = tqdm(input, desc = u'已获取0篇文章') #tqdm is for progressbar prompt
    for d in w:
        if not re.findall('^[a-zA-Z]+:', d[0]) and d[0] and not re.findall(u'^#', d[1]):
            outputfile = codecs.open('/Users/wisdombeat/PycharmProjects/wiki_txt/wikis/line' + str(i) + '.pkl', 'wb+')
            s = wiki_replace(d)
            pickle.dump(s, outputfile)
            outputfile.close()
            i += 1
            if i % 100 == 0:
                w.set_description(u'已获取%s篇文章' % i)
    w.set_description(u'All have done, there are %s papers' % i)
    return

if __name__ == "__main__":
    openbz2 = extract_pages(bz2file.open('/Users/wisdombeat/Desktop/zhwiki-latest-pages-articles.xml.bz2'))
    #resultname = '/Users/wisdombeat/PycharmProjects/wiki_txt/wiki.txt'
    #fantojan_savetxt(input=openbz2, output=resultname)
    fantojan_savepickle(openbz2)