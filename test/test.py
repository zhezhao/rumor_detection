#! /usr/bin/python

import rumor_detect
import re
from nltk.stem import PorterStemmer

stemmer = PorterStemmer()

text1 = 'I don\'t have to race to pause the video on @hoopshype\'s rumors page.  Thanks!'
text12 = 'I don\'t have to race to pause the video on @hoopshype\'s rumors page.  Thanks!, what??'
text2 = '@Tayy_ShakinLife for what?'
text3 = 'RT @UberFaactz: iPhone 6 rumors:   # Wireless Charging # iOS 8 # New Look see more: http://t.co/RWuLwZdjGE http://t.co/FwbvhtHqyLFri Apr 04 18:41:31 +0000'
text32 = 'is it true???? RT @UberFaactz: iPhone 6 rumors:   # Wireless Charging # iOS 8 # New Look see more: http://t.co/RWuLwZdjGE http://t.co/FwbvhtHqyLFri Apr 04 18:41:31 +0000'
text4 = '@hello bethers what\'s your favorite cereal?'

tweet1 = ( '1', text1, '4:01pm', rumor_detect.shingle_minhash( rumor_detect.shingle( rumor_detect.zhe_pipeline(text1, stemmer), 3), 50) )
tweet2 = ( '1', text1, '4:02pm', rumor_detect.shingle_minhash( rumor_detect.shingle( rumor_detect.zhe_pipeline(text1, stemmer), 3), 50) )
tweet3 = ( '2', text3, '4:03pm', rumor_detect.shingle_minhash( rumor_detect.shingle( rumor_detect.zhe_pipeline(text3, stemmer), 3), 50) )
tweet4 = ( '3', text32, '4:03pm', rumor_detect.shingle_minhash( rumor_detect.shingle( rumor_detect.zhe_pipeline(text32, stemmer), 3), 50) )
tweet5 = ( '4', text12, '4:03pm', rumor_detect.shingle_minhash( rumor_detect.shingle( rumor_detect.zhe_pipeline(text12, stemmer), 3), 50) )
tweet6 = ( '5', text2, '4:03pm', rumor_detect.shingle_minhash( rumor_detect.shingle( rumor_detect.zhe_pipeline(text2, stemmer), 3), 50) )
tweet7 = ( '6', text2, '5:01pm', rumor_detect.shingle_minhash( rumor_detect.shingle( rumor_detect.zhe_pipeline(text2, stemmer), 3), 50) )
tweet8 = ( '8', text4, '11:01pm', rumor_detect.shingle_minhash( rumor_detect.shingle( rumor_detect.zhe_pipeline(text4, stemmer), 3), 50) )

rp = rumor_detect.rumorpool()

rp.insert(tweet1)
rp.insert(tweet2)
rp.insert(tweet3)
rp.insert(tweet4)
rp.insert(tweet5)
rp.insert(tweet6)
rp.insert(tweet7)

rp.update_statement()

print rp.rumors[1].output_summary()

print rp.rumors[1].output_tweets(1)

f = open('test_sum','w')
f2 = open('test_twe','w')

rp.output(f,f2)

test = open('test', 'r')
tweets = {}
tweets2 = []

for line in test:
	line_s = re.sub('\n','',line)
	texts = line_s.split('\t')
	if texts.__len__() >= 4:
		text = texts[1]
		tid = texts[0]
		signaltext = texts[3]
		ctime = texts[2]
		minhash = rumor_detect.shingle_minhash( rumor_detect.shingle( rumor_detect.zhe_pipeline(signaltext, stemmer), 3 ) , 50 )
		if minhash is not None:
			tweets[tid] = ( tid, text, ctime, minhash )
			tweets2.append( tweets[tid] )


# test rumor retrieval part

rp = rumor_detect.rumorpool_center()

f = open('../../output_summary')

line = f.readline()

line_s = re.sub('\n','',line)
texts = line_s.split('\t')

minhash = rumor_detect.shingle_minhash( rumor_detect.shingle( rumor_detect.zhe_pipeline(texts[2], stemmer), 3 ) , 50 )


rp.addrumor(texts[0],texts[1],texts[2],texts[3],minhash)
