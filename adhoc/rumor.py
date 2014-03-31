#! /usr/bin/python

import fileinput
import re
import time
import hashlib
from nltk.stem import PorterStemmer


def zhe_pipeline(text, stemmer):
	pre_text = re.sub('[^A-Za-z0-9#\?\!\'\"\@\(\)\:\$\%]+',' ',text)
	pre_text = re.sub('\?',' ? ',pre_text)
	pre_text = re.sub('\@[A-Za-z0-9_]*',' @user ',pre_text)
	pre_text = re.sub('\!',' ! ',pre_text)
	pre_text = re.sub('\(',' ( ',pre_text)
	pre_text = re.sub('\)',' ) ',pre_text)
	pre_text = re.sub('\"',' \" ',pre_text)
	pre_text = re.sub('\:',' : ',pre_text)
	pre_text = re.sub('\$',' $ ',pre_text)
	pre_text = re.sub('\%',' % ',pre_text)
	pre_text = re.sub(' [ ]*', ' ', pre_text)
	pre_text = pre_text.lower()
	out_text = stemmer.stem(pre_text)
	return out_text

def shingle(text, size):
	if text is None:
		return None
	pre_text = re.sub('[^A-Za-z0-9_#\@]+',' ',text)
	pre_text = re.sub(' [ ]*', ' ', pre_text)
	pre_text = pre_text.lower()
	tokens = pre_text.split(' ')
	while tokens.__contains__(''):
		tokens.remove('')
	if size < 1:
		size = 1
	if tokens.__len__() < size:
		#size = tokens.__len__()
		return None
	shingles=[]
	for i in range(0,tokens.__len__()-size+1):
		shingles.append('+'.join(tokens[i:i+size]))
	return shingles

def shingle_minhash(shingles, numhash):
	base = pow(2,32)-1
	if shingles is None:
		return None
	minH = [base] * numhash
	for i in shingles:
		temp = hashlib.md5(i[0]).hexdigest()
		hseed = long(temp,16)
		for j in range(0,numhash):
			hvalue = hseed * (j + 1) % base
			if hvalue < minH[j]:
				minH[j] = hvalue
	minhash = []
	for i in range(0,numhash):
		minhash.append((i+1,minH[i]))
	return minhash


def minhash_str(shingles, numhash):
	base = pow(2,32)-1
	if shingles is None:
		return None
	minH = [base] * numhash
	for i in shingles:
		temp = hashlib.md5(i[0]).hexdigest()
		hseed = long(temp,16)
		for j in range(0,numhash):
			hvalue = hseed * (j + 1) % base
			if hvalue < minH[j]:
				minH[j] = hvalue
	minhash = ''
	for i in range(0,numhash):
		minhash = minhash + ' ' + str(minH[i])
	return minhash



