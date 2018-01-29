#!/usr/bin/env python2
# -*- coding: utf-8 -*-
from __future__ import division
import redis
import pandas as pd
from scipy import stats,integrate
import numpy as np
import re
import os
from  itertools import combinations
from operator import itemgetter
from itertools import combinations,groupby
from RedisProto import RedisProto 

# the csv file must have: id,type,lng,lat etc. fields
CSV_FILE_PATH = "test.csv"
#neighbor distance in km
DISTANCE_THREOD_KM = 3  
PARTICIPATIONTHREOD  = 0.4
PATTERN = re.compile(".*?__.*")
# start from size-2 pattern
CURRENT_SIZE = 2  
DATANAME= "whatever"
globalInstanceCount= dict()

redis_pool = redis.ConnectionPool(host='localhost', port=6379, db=0)

def loadCSV2Redis():
	commands_args = list()
	df = pd.read_csv(CSV_FILE_PATH,encoding="utf-8",dtype={"id":str,"key":str,"lng": str,"lat": str})
	rc = redis.Redis(connection_pool=redis_pool)
	for index, row in df.iterrows():
		commands_args.append(("GEOADD",DATANAME,row["lng"],row["lat"],row["id"]+"__"+row["key"]))
	commands = ''.join([RedisProto().pack_command(*args)[0] for args in commands_args])
	pf = open("tempProteo.txt","w")
	pf.write(commands)
	pf.close()

	print "inserting data..."
	print os.system("cat  tempProteo.txt | redis-cli --pipe")
	
def buildNeighbourList():
	print "building NeighbourList ..."
	global globalInstanceCount
	rc = redis.Redis(connection_pool=redis_pool)
	for item in rc.zscan_iter(DATANAME):
		id = item[0]
		key = id.split("__")[1]
		nbs = filter(lambda nb:nb.split("__")[1]> key,rc.georadiusbymember(DATANAME,id,DISTANCE_THREOD_KM, unit="km"))
		nbs.sort()
		nbs.insert(0,id)
		rc.set(item[0],",".join(nbs))
		if globalInstanceCount.has_key(key):
			globalInstanceCount[key]=globalInstanceCount[key]+1
		else:
			globalInstanceCount[key]=1
	print "building NeighbourList done,start Colocation ..."
def isPrevalentType(id):
	if CURRENT_SIZE<3:
		return True
	key = id.split("__")[1]
	rc = redis.Redis(connection_pool=redis_pool)
	isPrevalentType = False
	for item in rc.zscan_iter("patternValid"+str(CURRENT_SIZE-1)):

		if key in item[0]:
			isPrevalentType = True
			break
	return isPrevalentType

def generateCandidate():
	rc = redis.Redis(connection_pool=redis_pool)
	for id_key in rc.scan_iter():
		if PATTERN.match(id_key):
			seq = rc.get(id_key)
			nbs =  seq.split(",")[1:]
			nbs=filter(isPrevalentType,nbs)
			nbs.sort()
			for candy in  combinations(nbs,CURRENT_SIZE-1):
				cliqueness = True
				for pair in combinations(candy,2):
					if pair[0].split("__")[1] == pair[1].split("__")[1] or rc.geodist(DATANAME,pair[0],pair[1],unit="km") > DISTANCE_THREOD_KM:
						cliqueness = False 
						break;
				if cliqueness:
					candyList = list(candy)
					candyList.insert(0,id_key)
					pattern = ",".join(map(lambda ID_KEY:ID_KEY.split("__")[1],candyList))
					# recode as { pattern1,pattern1 }
					rc.sadd("patternTemp"+str(CURRENT_SIZE),pattern)
					rc.sadd(pattern , ",".join(candyList))
					# record as {pattern1:[instance ...]}
			nbs.insert(0,id_key)
			rc.set(id_key,",".join(nbs))

def compParticipation():
	rc = redis.Redis(connection_pool=redis_pool)
	for pattern in rc.sscan_iter("patternTemp"+ str(CURRENT_SIZE)):
		localInstanceCount = dict()
		id_keySet = set()
		for instance in rc.sscan_iter(pattern):
			id_keySet=id_keySet.union(set(instance.split(",")))
		for id_key in id_keySet:
			key = id_key.split("__")[1]
			if localInstanceCount.has_key(key):
				localInstanceCount[key] = localInstanceCount[key]+1
			else:
				 localInstanceCount[key] = 1
		participation = min(map(lambda key :localInstanceCount[key]/globalInstanceCount[key],localInstanceCount))
		if participation>=PARTICIPATIONTHREOD:
			rc.zadd("patternValid"+ str(CURRENT_SIZE),pattern,participation)
	rc.delete("patternTemp"+ str(CURRENT_SIZE))

if __name__ == '__main__':
	
	loadCSV2Redis()
	buildNeighbourList()
	for i in range(2,4):
		generateCandidate()
		compParticipation()
		CURRENT_SIZE += 1
		rc = redis.Redis(connection_pool=redis_pool)
		for pattern in rc.zscan_iter("patternValid"+ str(i)):
			print str(i)+"-size:\t",pattern