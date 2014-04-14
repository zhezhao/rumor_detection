#! /usr/bin/python

import fileinput
import re
import rumor_detect
import time
from nltk.stem import PorterStemmer
import sys
import threading


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
	filehandle.seek(0,2)
#	for line in filehandle:
#		tweet = line_to_tweet(line, stemmer)
#		if tweet is not None:
#			if long(tweet[0]) % nump == num:
#				yield tweet
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
	count = 0
	f = open(nsignal,'r')
	mat = open(matched,'w',0)
	
	repo = rumor_detect.retrieve_pool()
	nk = repo.copy_rumor(output_summary)
	
	for tweet in read_follow(f, nump, num):
		if int(time.time())%60 == 1:
			nk = repo.copy_rumor(output_summary)
			rtcount = repo.retrieve_back( nk, mat )
			print str(time.time()) + '\t' + str(rtcount) + ' tweets retrieved back in ' + str(nk.__len__()) + ' new rumor clusters'
			# get rid of old tweets
			repo.update_tweets()
	
		count = count + 1
		rid = repo.add_tweets(tweet)
		if rid == 0:
			print str(count) + 'th tweet inserted into retrieve pool'
		else:
			print str(count) + 'th tweet matched to cluster' + str(rid) 
			mat.write(str(rid) + '\t' + tweet[0] +'\t' + tweet[1] + '\t' + tweet[2] + '\n')


if sys.argv.__len__() < 5:
	nump = 1
else:
	nump = int(sys.argv[4])

threads = []


for num in range(0,nump):
	print 'starting thread number ' + str(num) + ' of ' + str(nump) + ' threads!'
	t = threading.Thread(target=retrieve, args = (sys.argv[1], sys.argv[2], sys.argv[3], nump, num))
	threads.append(t)
	t.start()


for thread in threads:
	thread.join()

