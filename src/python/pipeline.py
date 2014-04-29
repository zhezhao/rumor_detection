#! /usr/bin/python

import fileinput
import re
import rumor_detect
import time
from nltk.stem import PorterStemmer
import sys
import threading
import os.path
import datetime



def stream_clustering():
	log = open('log','w')
	count = 0
	allcount = 0
	stemmer = PorterStemmer()
	rp = rumor_detect.rumorpool()
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
				print '[CLU] ' + str(count) + 'th tweets out of ' + str(allcount) + ' inserted into cluster' + str(rp.insert(tweet))
		if count % 50 == 1:
			rp.once_statement()
			sum_f = open('output_summary','w',0)
			twe_f = open('output_tweets', 'w',0)
			clu_f = open('cluster_history','w',0)
			rp.output_select(sum_f,twe_f)
			rp.output_mergelog(clu_f)
			sum_f.close()
			twe_f.close()
			clu_f.close()
	
		if count % 1000 == 1:
			his_sum_f = open('summary_history','a')
			his_twe_f = open('tweets_history','a')
			rp.output_select(his_sum_f,his_twe_f, str(count)+':')
			his_sum_f.close()
			his_twe_f.close()
			rp.delete_old_rumor()
			
	

def line_to_tweet(line, stemmer):
	line_s = re.sub('\n','',line)
	texts = line_s.split('\t')
	if texts.__len__() >= 3:
		text = texts[1]
		tid = texts[0]
		ctime = texts[2]
		minhash = rumor_detect.shingle_minhash( rumor_detect.shingle( rumor_detect.zhe_pipeline(text, stemmer), 3 ) , 50 )
		if minhash is not None:
			return ( tid, text, ctime, minhash )
	return None
	

#support for multi threading
def read_follow(filehandle, nump = 1, num = 0):
	stemmer = PorterStemmer()
#	filehandle.seek(0,2)
	for line in filehandle:
		tweet = line_to_tweet(line, stemmer)
		if tweet is not None:
			if long(tweet[0]) % nump == num:
				yield tweet
	while 1:
		line = filehandle.readline()
		if not line:
			time.sleep(1.0)
			continue
		tweet = line_to_tweet(line,stemmer)
		if tweet is not None:
			if long(tweet[0]) % nump == num:
				yield tweet			

def retrieve(output_summary, nsignal, matched, nump = 1, num = 0):
	while os.path.isfile(output_summary) is not True:
		time.sleep(1)
	while os.path.isfile(nsignal) is not True:
		time.sleep(1)
	count = 0
	f = open(nsignal,'r')
	mat = open(matched,'w',0)
	
	repo = rumor_detect.retrieve_pool()
	nk = repo.copy_rumor(output_summary)
	
	for tweet in read_follow(f, nump, num):
		if int(time.time())%60 == 1:
			nk = repo.copy_rumor(output_summary)
			rtcount = repo.retrieve_back( nk, mat )
			print '[RET]: ' + datetime.datetime.now().__str__()  + ':' + str(rtcount) + ' tweets retrieved back in ' + str(nk.__len__()) + ' new rumor clusters'
			# get rid of old tweets
			repo.update_tweets()
	
		count = count + 1
		rid = repo.add_tweets(tweet)
		if rid == 0:
			if count % 1000 == 1:
				print '[RET] ' + str(count) + 'th tweet inserted into retrieve pool. timestamp: ' + tweet[2]
		else:
			print '[RET] ' + str(count) + 'th tweet matched to cluster' + str(rid) 
			mat.write(str(rid) + '\t' + tweet[0] +'\t' + tweet[1] + '\t' + tweet[2] + '\n')




clustering =  threading.Thread(target=stream_clustering, args = ())

print '[INFO] Starting Clustering'

clustering.start()

#nump = 5
#threads = []
#for num in range(0,nump):
#	print '[INFO] Retrieve: starting thread number ' + str(num) + ' of ' + str(nump) + ' threads!'
#	t = threading.Thread(target=retrieve, args = ('output_summary', 'nsignal', 'matched' , nump, num))
#	threads.append(t)
#	t.start()
#
#
#for thread in threads:
#	thread.join()

clustering.join()
