#! /usr/bin/python

import fileinput
import re
import rumor_detect
import time
from nltk.stem import PorterStemmer

stemmer = PorterStemmer()
rp = rumor_detect.rumorpool()

sum_f = open('output_summary','w')
twe_f = open('output_tweets', 'w')

def stream_clustering():
	log = open('log','w')
	count = 0
	allcount = 0
	global rp
	for line in fileinput.input():
		allcount = allcount + 1
		line_s = re.sub('\n','',line)
		texts = line_s.split('\t')
		if texts.__len__() >= 4:
			text = texts[1]
			tid = texts[0]
			signaltext = texts[3]
			ctime = texts[2]
			minhash = rumor_detect.shingle_minhash( rumor_detect.shingle( rumor_detect.zhe_pipeline(signaltext, stemmer), 3 ) , 50 )
			if minhash is not None:
				tweet = ( tid, text, ctime, minhash )
				count = count + 1
				log.write(str(count) + 'th tweets out of ' + str(allcount) + ' inserted into cluster' + str(rp.insert(tweet)) +'\n')
		if count % 50 == 1:
			rp.once_statement()
			sum_f = open('output_summary','w')
			twe_f = open('output_tweets', 'w')
			rp.output_select(sum_f,twe_f)
			sum_f.close()
			twe_f.close()
	

stream_clustering()
