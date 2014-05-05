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
	first_sec = 0
	first_tweet = '0'
	def __init__( self, tweets = {}):
		self.tweets = tweets
		self.statement = ''
		self.words = []
		self.wfre = {}
		self.wcount = 0
		self.last_tweet = '0'
		self.last_sec = 0
		self.first_sec = time.time()
		self.first_tweet = '0'
		if tweets != {}:
			self.once_statement()
	
	def connect( self, tweet, thres ,numhash):
		if self.tweets.has_key(tweet[0]):
			return 2
		for tid in self.tweets:
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
		thissec = twitter_date_to_sec(tweet[2])
		if thissec > self.last_sec:
			self.last_tweet = tweet[2]
			self.last_sec = thissec
		if thissec < self.first_sec:
			self.first_sec = thissec
			self.first_tweet = tweet[2]
		return self.last_sec
	
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
		return 'last_update: ' + self.last_tweet + '\t' + self.statement + '\t' + str( self.tweets.__len__() ) + '\t' + self.first_tweet
	
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
	hour_thres = 36

	def __init__( self, thres = 0.70, numhash = 50 , hour_thres = 36):
		self.rumors = {}
		self.mergelog = []
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

	def output_select(self, summary_file, tweets_file , thres = 3, prefix=''):
		for i in range(0,self.curid):
			if self.rumors.has_key(self.curid-i):
				if self.rumors[self.curid-i].tweets.__len__() >= thres:
					key = self.curid-i
					summary_file.write( prefix + str(key) + '\t' + self.rumors[key].output_summary() + '\n')
					tweets_file.write( self.rumors[key].output_tweets( prefix + str(key) ) )

	def output_mergelog(self,outfile):
		for i in range(1, self.curid):
			outfile.write( str(i) + '\t' + str(self.mergelog[i][0]) + '\t' + str(self.mergelog[i][1]) + '\n' )
		return self.curid

	def delete_old_rumor(self):
		del_ids = []
		for rid in self.rumors:
			if self.rumors[rid].last_sec + self.hour_thres*3600 < self.last_sec:
				del_ids.append(rid)
		for rid in del_ids:
			self.rumors.pop(rid)
			self.mergelog[rid] = ( -1, time.time() )
	

def read_rp_from_file( output_tweets, numhash=50):
	rp = rumorpool()
	stemmer = PorterStemmer()
	for line in output_tweets:
		line_n = re.sub('\n','',line)
		temp = line_n.split('\t')
		if len(temp) < 4:
			continue
		rid = int(temp[0])
		if len(rp.mergelog) < rid + 1:
			rp.mergelog[len(rp.mergelog):rid+1] = [( -1, time.time()) for i in range(len(rp.mergelog), rid+1)]
			rp.mergelog[rid] = ( 0, time.time() )
			rp.curid = rid+1
		if rp.rumors.has_key(rid):
			minhash = shingle_minhash( shingle( zhe_pipeline(temp[2], stemmer), 3 ) , numhash )
			tweet = ( temp[1], temp[2], temp[3], minhash )
			rp.rumors[rid].addtweet(tweet)
		else:
			rp.rumors[rid] = rumor({})
			rp.mergelog[rid] = ( 0, time.time() )
			minhash = shingle_minhash( shingle( zhe_pipeline(temp[2], stemmer), 3 ), numhash )
			tweet  = ( temp[1], temp[2], temp[3], minhash )
			rp.rumors[rid].addtweet(tweet)
	for rid in rp.rumors:
		if rp.last_sec < rp.rumors[rid].last_sec:
			rp.last_sec = rp.rumors[rid].last_sec
	rp.once_statement()
	return rp



# for match only, will be replaced by retrieve back
def update_rumorpool_from_file( output_summary ):
	stemmer = PorterStemmer()
	rp = rumorpool_center()
	for line in output_summary:
		line_s = re.sub('\n','',line)
		texts = line_s.split('\t')
		if texts.__len__() < 4:
			return rumorpool_center()
		minhash = shingle_minhash( shingle( zhe_pipeline(texts[2], stemmer), 3 ) , 50 )
		rp.addrumor(texts[0],texts[1],texts[2],texts[3],minhash)

	return rp

# for match only, will be replaced by retrieva back
class rumorpool_center:
	rumors = {}
	curid = 0
	numhash = 50
	connectthres = 0.60
	last_sec = 0
	def __init__(self, curid = 0, numhash = 50, connectthres = 0.60 ):
		self.rumors = {}
		self.curid = curid
		self.numhash = numhash
		self.connectthres = connectthres
	
	
	def addrumor( self, rid, last_update, text, scount, minhash ):
		self.rumors[rid] = ( rid, last_update, text, scount, minhash )
		if self.curid < rid:
			self.curid = rid
			self.last_sec = twitter_date_to_sec(last_update[13:])

	def match( self, tweet ):
		maxsim = 0
		maxrid = 0
		for rid in self.rumors:
			sim = minhash_similarity( self.rumors[rid][4], tweet[3], self.numhash)
			if maxsim < sim:
				maxsim = sim
				maxrid = rid
		if maxsim > self.connectthres:
			return maxrid

		return 0


def update_rumors_from_file( output_summary ):
	rumors = {}
	stemmer = PorterStemmer()
	for line in output_summary:
		line_s = re.sub('\n','',line)
		texts = line_s.split('\t')
		if texts.__len__() < 4:
			return {}
		minhash = shingle_minhash( shingle( zhe_pipeline(texts[2], stemmer), 3 ) , 50 )
		rumors[texts[0]] = (texts[0],texts[1],texts[2],texts[3],minhash)
	return rumors


# for match and retrieval back
# it can aslo be used to do retrieval only, in this case, a buffer layer will be created
class retrieve_pool:
	rumors = {}
	timestamp = {}
	tweets = []
	cur_time = 0
	curid = 0
	numhash = 50
	connectthres = 0.60
	update_thres = 5
	count_thres = 5000000
	def __init__( self, numhash = 50, connectthres = 0.60, update_thres = 5, count_thres = 5000000 ):
		self.numhash = numhash
		self.rumors = {}
		self.timestamp = {}
		self.tweets = []
		self.cur_time = 0
		self.curid = 0
		self.numhash = numhash
		self.connectthres = connectthres
		self.update_thres = update_thres
		self.count_thres = count_thres
	
	# for retrieve back new only, for match and retrieve , use copy rumor
	def update_rumor( self, rp_center ):
		new_keys = []
		for rid in self.rumors:
			if self.timestamp[rid] >= self.update_thres:
				self.rumors.pop(rid)
				self.timestamp.pop(rid)
			self.timestamp[rid] = self.timestamp[rid] + 1
		for rid in rp_center.rumors:
			if rid > self.curid:
				self.curid = rid
				self.rumors[rid] = rp_center.rumors[rid]
				self.timestamp[rid] = 0
				new_keys.append(rid)

		return new_keys.__len__()

	def copy_rumor( self, filename ):
		maxrid = 0
		output_summary = open(filename, 'r')
		rumors = update_rumors_from_file(output_summary)
		output_summary.close()
		while rumors.__len__() < 1:
			time.sleep(1)
			output_summary = open(filename, 'r')
			rumors = update_rumors_from_file(output_summary)
			output_summary.close()
		self.rumors = {}
		new_keys = []
		for rid in rumors:
			self.rumors[rid] = rumors[rid]
			if rid > self.curid:
				new_keys.append(rid)
				if rid > maxrid:
					maxrid = rid
		
		self.curid = maxrid
		return new_keys

	def update_tweets(self):
		self.tweets = self.tweets[-self.count_thres:]

	def add_tweets(self, tweet ):
		maxsim = 0
		maxrid = 0
		for rid in self.rumors:
			sim = minhash_similarity( self.rumors[rid][4], tweet[3], self.numhash)
			if maxsim < sim:
				maxsim = sim
				maxrid = rid
		if maxsim > self.connectthres:
			return maxrid
		else:
			self.tweets.append(tweet)
			return 0

	def retrieve_back(self, new_keys, output_file):
		count = 0
		for i in range(0,self.tweets.__len__()):
			tweet = self.tweets[i]
			maxsim = 0
			maxrid = 0
			for rid in new_keys:
				sim = minhash_similarity( self.rumors[rid][4], tweet[3], self.numhash)
				if maxsim < sim:
					maxsim = sim
					maxrid = rid
			if maxsim > self.connectthres:
				output_file.write( str(maxrid) + '\t' + tweet[0] +'\t' + tweet[1] + '\t' + tweet[2] + '\n')
				del self.tweets[i]
				count = count + 1
		return count













