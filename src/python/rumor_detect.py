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

def zhe_preprocess(text):
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
	return pre_text


def shingle(text, size, minlen = 7):
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
	if tokens.__len__() < size or tokens.__len__() < minlen:
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
		temp = hashlib.md5(i).hexdigest()
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
		temp = hashlib.md5(i).hexdigest()
		hseed = long(temp,16)
		for j in range(0,numhash):
			hvalue = hseed * (j + 1) % base
			if hvalue < minH[j]:
				minH[j] = hvalue
	minhash = ''
	for i in range(0,numhash):
		minhash = minhash + ' ' + str(minH[i])
	return minhash


def in_match(text):
	m1 = re.search('is (this|it|that) true', text)
	m2 = re.search('wha[a]*t[\?!][\?!]*', text)
	m3 = re.search('(rumor|debunk|unconfirmed)', text)
	m4 = re.search('(that|this|it) is not true', text)
	m5 = re.search('(real|really)\?', text)
	m6 = re.search('guess what', text)
	if m1 is None and m2 is None and m3 is None and m4 is None and m5 is None:
		return None
	if m6 is not None:
		return None
	text = re.sub('is (this|it|that) true', '9494632128',text)
	text = re.sub('wha[a]*t[\?!][\?!]*', '9494632128',text)
	text = re.sub('(rumor|debunk|unconfirmed)', '9494632128',text)
	text = re.sub('(that|this|it) is not true', '9494632128',text)
	text = re.sub('really\?', '9494632128', text)
	text = re.sub('real\?', '9494632128', text)
	text = re.sub('RT ','9494632128',text)
	text = re.sub('9494632128','',text)
	return text


def minhash_similarity(minhash1, minhash2, numhash):
	if minhash1 is None or minhash2 is None:
		return 0
	sims = [ -1 for i in range(numhash) ]
	for i in minhash1:
		sims[i[0]-1] = i[1]
	for i in minhash2:
		if sims[i[0]-1] == i[1]:
			sims[i[0]-1]=1.0
		else:
			sims[i[0]-1]=0.0
	sim = sum(sims)/numhash;
	return sim


def twitter_date_to_sec(text):
	if text is None:
		return time.time()
	try:
		secs = time.mktime(time.strptime(text,'%a %b %d %H:%M:%S +0000 %Y'))
	except:
		secs = time.time()
	return secs


class rumor:
	tweets = {}
	statement = ''
	words = []
	wfre = {}
	wcount = 0
	last_tweet = '0'
	last_sec = 0
	def __init__( self, tweets = {}):
		self.tweets = tweets
		if tweets != {}:
			self.once_statement()
	
	def connect( self, tweet, thres ,numhash):
		for tid in self.tweets:
			if tid == tweet[0]:
				return 2
			sim = minhash_similarity(self.tweets[tid][3], tweet[3] ,numhash )
			if sim >= thres:
				return 1
			else:
				if sim <= thres/2:
					return -1
		return -1

	def addtweet( self, tweet ):
		if self.tweets.has_key(tweet[0]):
			return 0
		self.tweets[tweet[0]]=tweet
		self.last_tweet = tweet[2]
		self.last_sec = twitter_date_to_sec(tweet[2])
		return self.sec
	
	def calculate_words( self ):
		self.words = []
		self.wfre = {}
		self.wcount = 0
		for tid in self.tweets:
			self.update_words(tid)
		return self.wcount

	def calculate_statement( self, thres = 0.8 ):
		stat = ''
		for i in range(0,self.wcount):
			if self.wfre[self.words[i]] >= thres* self.tweets.__len__():
				stat = stat + ' ' + self.words[i][0]
		self.statement = stat[1:]
		return self.statement
	
	def once_statement( self ):
		if self.calculate_words() >= 1:
			return self.calculate_statement()
		else:
			return self.statement	

	def update_words( self, tid ):
		if not self.tweets.has_key(tid):
			return -1
		wordsint = {}
		tweet = zhe_preprocess(self.tweets[tid][1])
		tokens = tweet.split(' ')
		while tokens.__contains__(''):
			tokens.remove('')
		for word in tokens:
			if wordsint.has_key(word):
				wordsint[word] = wordsint[word]+1
				if self.wfre.has_key((word,wordsint[word])):
					self.wfre[(word,wordsint[word])] = self.wfre[(word,wordsint[word])] + 1
				else:
					self.wfre[(word,wordsint[word])] = 1
					self.words.append((word,wordsint[word]))
					self.wcount = self.wcount + 1
			else:
				wordsint[word] = 1
				if self.wfre.has_key((word, wordsint[word])):
					self.wfre[(word,wordsint[word])] = self.wfre[(word,wordsint[word])] + 1
				else:
					self.wfre[(word,wordsint[word])] = 1
					self.words.append((word,wordsint[word]))
					self.wcount = self.wcount + 1
		return self.wcount
	
	def update_statement( self, tid ):
		if self.update_words(tid) >= 1:
			return self.calculate_statement()
		else:
			return self.statement
		
			
	def output_summary( self ):
		return 'last_update: ' + self.last_tweet + '\t' + self.statement + '\t' + str( self.tweets.__len__() )
	
	def output_tweets( self, prefix ):
		outputstr = '\n'
		for tid in self.tweets:
			outputstr = outputstr + str(prefix) + '\t' + str(tid) + '\t' + self.tweets[tid][1] + '\t' + self.tweets[tid][2] + '\n'
		return outputstr[1:]


class rumorpool:
	rumors = {}
	curid = 0
	connectthres = 0.70
	numhash = 50
	mergelog = []
	last_sec = 0
	hour_thres = 24

	def __init__( self, thres = 0.70, numhash = 50 , hour_thres = 24):
		self.rumors = {}
		self.curid = 0
		self.connectthres = thres
		self.numhash = numhash
		self.mergelog.append((0,time.time()))
		self.last_sec = 0
		self.hour_thres = hour_thres

	def mergerumor( self, keys ):
		if keys == []:
			return None
		if keys.__len__() == 1:
			return keys[0]
		new_rumor = rumor({})
		self.curid = self.curid + 1
		self.mergelog.append((0, time.time()))
		for i in keys:
			old_rumor = self.rumors.pop(i)
			for tid in old_rumor.tweets:
				new_rumor.addtweet( old_rumor.tweets[tid] )
			self.mergelog[i] = ( self.curid, time.time() )
		self.rumors[self.curid] = new_rumor
		return self.curid
	
	def insert( self, tweet ):
		matched = []
		for key in self.rumors:
			ismatch = self.rumors[key].connect(tweet, self.connectthres, self.numhash )
			if ismatch == 1:
				matched.append(key)
			if ismatch == 2:
				return -1
		rid = self.mergerumor(matched)
		if rid is None:
			self.curid = self.curid + 1
			self.mergelog.append((0,time.time()))
			self.rumors[self.curid] = rumor({})
			rid = self.curid
		self.last_sec = self.rumors[rid].addtweet(tweet)
		return rid

	def once_statement(self):
		for key in self.rumors:
			self.rumors[key].once_statement()
		return self.curid



	def output(self, summary_file, tweets_file ):
		for key in self.rumors:
			summary_file.write( str(key) + '\t' + self.rumors[key].output_summary() + '\n')
			tweets_file.write( self.rumors[key].output_tweets(key) )

	def output_select(self, summary_file, tweets_file , prefix=''):
		for i in range(0,self.curid):
			if self.rumors.has_key(self.curid-i):
				if self.rumors[self.curid-i].tweets.__len__() >= 3:
					key = self.curid-i
					summary_file.write( prefix + str(key) + '\t' + self.rumors[key].output_summary() + '\n')
					tweets_file.write( self.rumors[key].output_tweets( prefix + str(key) ) )

	def output_mergelog(self,outfile):
		for i in range(1, self.curid):
			outfile.write( str(i) + '\t' + str(self.mergelog[i][0]) + '\t' + str(self.mergelog[i][1]) + '\n' )
		return self.curid

	def delete_old_rumor(self):
		for rid in self.rumors:
			if self.rumors[rid].last_sec + self.hour_thres*3600 < self.last_sec:
				self.rumors.pop(rid)
				self.mergelog[rid] = ( -1, time.time() )





