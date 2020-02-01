# Paid-q-a
The coding of research about questions' profit in paid question &amp; answer platform.  Project includes crawling data from sina weibo and weibo qa platform, text analysis and economic models.

All classes are in conj.py.
To combat Sina Weibo's powerful anti-crawling mechanism, class GetSinaCookie try to get cookie without logging in. You can get more details in https://blog.thinker.ink/passage/28/.

For trainning wiki chinese word vectors requires too much computer power and time, you can visit https://fasttext.cc/docs/en/pretrained-vectors.html. Then text_similarity.py has some tricks to further speed up the entire calculation process.
