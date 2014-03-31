#! /usr/bin/python

import fileinput
import re
import rumor
from nltk.stem import PorterStemmer

stemmer = PorterStemmer()

for line in fileinput.input():
	line_s = re.sub('\n','',line)
	texts = line_s.split('\t')
	if texts.__len__() >= 5:
		if texts[5] == 'en':
			text = texts[3]
			tid = texts[0]
			ctime = texts[4]
			minhash = rumor.minhash_str( rumor.shingle( rumor.zhe_pipeline(text, stemmer), 5 ), 50 );
			if minhash is not None:
				print tid + '\t' + text + '\t' + minhash + '\t' + ctime
