#! /usr/bin/python

import fileinput
import re
import rumor_detect
import time
from nltk.stem import PorterStemmer
import sys

stemmer = PorterStemmer()


def read_follow(filehandle):
	for line in filehandle:
		yield line
	while 1:
		line = filehandle.readline()
		if not line:
			time.sleep(1.0)
			continue
		yield line			

def match(output_summary, nsignal, matched, unmatched):
	count = 0
	allcount = 0
	f = open(nsignal,'r')
	mat = open(matched,'w')

	frp = open(output_summary,'r')
	rp_mat = rumor_detect.update_rumorpool_from_file(frp)
	frp .close()
	while rp_mat.rumors.__len__() < 1:
		frp = open(output_summary,'r')
		rp_mat = rumor_detect.update_rumorpool_from_file(frp)
		frp .close()

	# no buffer for this! output immediate
	umat = open(unmatched,'w',0 )
	for line in read_follow(f):
		if int(time.time())%60 == 1:
			frp = open(output_summary,'r')
			rp_mat = rumor_detect.update_rumorpool_from_file(frp)
			frp.close()
			while rp_mat.rumors.__len__() < 1:
				time.sleep(0.5)
				frp = open(output_summary,'r')
				rp_mat = rumor_detect.update_rumorpool_from_file(output_summary)
				frp.close()
		allcount = allcount + 1
		line_s = re.sub('\n','',line)
		texts = line_s.split('\t')
		if texts.__len__() >= 3:
			text = texts[1]
			tid = texts[0]
			ctime = texts[2]
			minhash = rumor_detect.shingle_minhash( rumor_detect.shingle( rumor_detect.zhe_pipeline(text, stemmer), 3 ) , 50 )
			if minhash is not None:
				tweet = ( tid, text, ctime, minhash )
				count = count + 1
				rid = rp_mat.match(tweet)
			#to here
				if rid > 0:				
					print str(count) + 'th tweets out of ' + str(allcount) + ' matched to cluster' + str(rid) 
					mat.write(str(rid) + '\t' + line_s + '\n')
				else:
					umat.write( line_s + '\n' )

match( sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4] )


