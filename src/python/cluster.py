#! /usr/bin/python

import fileinput
import re
import rumor_detect
import time
from nltk.stem import PorterStemmer

stemmer = PorterStemmer()
rp = rumor_detect.rumorpool()

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
				print str(count) + 'th tweets out of ' + str(allcount) + ' inserted into cluster' + str(rp.insert(tweet))
		if count % 50 == 1:
			rp.once_statement()
			sum_f = open('output_summary','w')
			twe_f = open('output_tweets', 'w')
			clu_f = open('cluster_history','w')
			rp.output_select(sum_f,twe_f,1)
			rp.output_mergelog(clu_f)
			sum_f.close()
			twe_f.close()
			clu_f.close()
	
		if count % 1000 == 1:
			his_sum_f = open('summary_history','a')
			his_twe_f = open('tweets_history','a')
			rp.output_select(his_sum_f,his_twe_f, 1, str(count)+':')
			his_sum_f.close()
			his_twe_f.close()
			rp.delete_old_rumor()
			
	

stream_clustering()
