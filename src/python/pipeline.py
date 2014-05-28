#! /usr/bin/python

import fileinput
import re
import rumor_detect
import time
from nltk.stem import PorterStemmer
import sys
import threading
import datetime
import MySQLdb
import os

def insert_ranklist( t, cur ):
	count = 0
	ranks = t.split('\n')
	for rank in ranks:
		score = rank.split('\t')
		if score.__len__() < 2:
			continue
		rid = score[0]
		rscore = score[1]
		cur.execute('''REPLACE INTO ranklist VALUES(''' + rid + ''', ''' + rscore + ''', NOW() );''' )
		count = count + 1
	return count

def rank_cluster(ranklist, useDB = 0):
	if useDB == 1:
		try:
			db =MySQLdb.connect(host="173.194.241.163", user="root", passwd="rumorlens", db="rumor_detection")
			cur = db.cursor()
		except:
			print 'DB error!'
			useDB = 0
	while 1:
		t = os.popen("./rumor_detection/src/shell/extract_feature_f13.sh | ./rumor_detection/src/shell/decision_tree_f13_7.sh" ).read()
		f = open(ranklist,'w',0)
		f.write(t)
		f.close()
		print '[RANK] caculating ranking scores and store in file'
		if useDB ==  1:
			try:
				numr = insert_ranklist(t,cur)
				db.commit()
				print '[RANK] ' + str(numr) + ' ranking scores insert into Database'
			except:
				print 'DB error! try reopen!'
				try:
					db.close()
				except:
					print 'DB closed! reopen!'
				try:
					db =MySQLdb.connect(host="173.194.241.163", user="root", passwd="rumorlens", db="rumor_detection")
					cur = db.cursor()
					numr = insert_ranklist(t,cur)
					db.commit()
					print '[RANK] ' + str(numr) + ' ranking scores insert into Database'
				except:
					print '[RANK - ERR] database update failure, will try next time'
		sys.stdout.flush()
		time.sleep(200)
		
	

def stream_clustering( useDB = 0):

	if useDB == 1:
		try:
			db =MySQLdb.connect(host="173.194.241.163", user="root", passwd="rumorlens", db="rumor_detection")
			cur = db.cursor()
		except:
			print 'DB error!'
			useDB = 0

	log = open('log','w',0)
	count = 0
	allcount = 0
	stemmer = PorterStemmer()
	if os.path.isfile('output_tweets'):
		print 'history file located, read history!'
		history = open( 'output_tweets','r')
		rp = rumor_detect.read_rp_from_file(history)
		history.close()
	else:
		rp = rumor_detect.rumorpool()
	for line in sys.stdin:
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
				insert_res = rp.insert(tweet)
				print '[CLU] ' + str(count) + 'th tweets out of ' + str(allcount) + ' inserted into cluster' + str(insert_res)
				log.write( '[CLU] ' + str(count) + 'th tweets out of ' + str(allcount) + ' inserted into cluster' + str(insert_res) + '\tDetailed:\t' + line_s + '\n' )
		if count % 50 == 1:
			rp.once_statement()
			sum_f = open('output_summary','w',0)
			twe_f = open('output_tweets', 'w',0)
			clu_f = open('cluster_history','w',0)
			rp.output_select(sum_f,twe_f,1)
			rp.output_mergelog(clu_f)
			sum_f.close()
			twe_f.close()
			clu_f.close()
			
			if useDB == 1:
				print 'Updating Database!'
				try:
					dbupdat = rp.update_database(cur,3)
					db.commit()
					print str(dbupdat) + ' rumors inserted!'
				except:
					print 'DB error! try reopen!'
					try:
						db.close()
					except:
						print 'DB closed! reopen!'
					try:
						db =MySQLdb.connect(host="173.194.241.163", user="root", passwd="rumorlens", db="rumor_detection")
						cur = db.cursor()
						dbupdat = rp.update_database(cur,3)
						db.commit()
						print str(dbupdat) + ' rumors inserted!'
					except:
						print 'DB reopen failed, will try again later'
					
						
	
		if count % 1000 == 1:
			his_sum_f = open('summary_history','a')
			his_twe_f = open('tweets_history','a')
			rp.output_select(his_sum_f,his_twe_f,1, str(count)+':')
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
	mat = open(matched,'a+',0)
	
	repo = rumor_detect.retrieve_pool()
	nk = repo.copy_rumor(output_summary)
	
	for tweet in read_follow(f, nump, num):
		if int(time.time())%60 == 1:
			nk = repo.copy_rumor(output_summary)
			rtcount = repo.retrieve_back( nk, mat )
			print '[RET Worker' + str(num+1) +' ]: ' + datetime.datetime.now().__str__()  + ':' + str(rtcount) + ' tweets retrieved back in ' + str(nk.__len__()) + ' new rumor clusters'
			# get rid of old tweets
			repo.update_tweets()
	
		count = count + 1
		rid = repo.add_tweets(tweet)
		if rid == 0:
			if count % 1000 == 1:
				print '[RET Worker' + str(num+1) +' ]: ' + str(count) + 'th tweet inserted into retrieve pool. timestamp: ' + tweet[2]
		else:
			print '[RET Worker' + str(num+1) +' ]: ' + str(count) + 'th tweet matched to cluster' + str(rid) 
			mat.write(str(rid) + '\t' + tweet[0] +'\t' + tweet[1] + '\t' + tweet[2] + '\n')
		sys.stdout.flush()



if len(sys.argv) > 1:
	useDB = int(sys.argv[1])
	if len(sys.argv) > 2:
		useRank = 1
	else:
		useRank = 0
else:
	useDB = 0


clustering =  threading.Thread(target=stream_clustering, args = ([useDB]))

print '[INFO] Starting Clustering'

clustering.start()

nump = 10
threads = []
for num in range(0,nump):
	print '[INFO] Retrieve: starting thread number ' + str(num+1) + ' of ' + str(nump) + ' threads!'
	t = threading.Thread(target=retrieve, args = ('output_summary', 'nsignal', 'matched-'+str(num) , nump, num))
	threads.append(t)
	t.start()

if useRank == 1:
	ranking = threading.Thread( target=rank_cluster, args = ('ranklist', useDB ) )
	ranking.start()

for thread in threads:
	thread.join()

clustering.join()
ranking.join()
