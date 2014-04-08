#! /usr/bin/python

import sys
import codecs
import time
import re

f = codecs.open(sys.argv[1], encoding='utf-8')

UTF8Writer = codecs.getwriter('utf8')
sys.stdout = UTF8Writer(sys.stdout)

for line in f:
	line_s = re.sub('\n','',line)
	texts = line_s.split('\t')
	if texts.__len__() >= 4:
		tid = texts[0]
		text = texts[2]
		uid = texts[1]
		ctime = texts[3]
		print(tid + '\t' + 'user\t' + uid + '\t' + text + '\t' + ctime + '\ten')
		time.sleep(0.0001)
	
