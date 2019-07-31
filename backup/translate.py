# coding:utf-8
# 
# @author: xsh

from googletrans import Translator

import time
import pymongo
import queue
import traceback
import time
import random

def trans_info():
	trans = Translator(service_urls=[
      # 'translate.google.com.hk',
      # 'translate.google.co.kr',
      'translate.google.cn'
    ])
	entq = queue.Queue(maxsize=0)
	db = pymongo.MongoClient('mongodb://localhost:27017')['icd11']
	cl = db['info']
	cnt = 0
	for curs in cl.find({'icd_title_zh':{'$exists':False}}):
		entq.put(curs)
	print('[INFO] start trans total %s entities' % entq.qsize())
	while entq.qsize() > 0:
		curs = entq.get()
		time.sleep(random.randint(0,5))
		cnt += 1
		icd_title_en = curs['icd_title']
		try:
			icd_title_zh = trans.translate(icd_title_en, dest='zh-CN').text
			print('%sth %s ---> %s, qsize %s remains' % (cnt, icd_title_en, icd_title_zh, entq.qsize()))
			cl.update_one({'foundation_id':curs['foundation_id']}, {
				'$set':{
					'icd_title_zh' : icd_title_zh
				}})
		except Exception as e:
			print('[ERROR] %s -> %s error with code %s' % (curs['foundation_id'], curs['icd_title'], str(e)))
			time.sleep(30)
			entq.put(curs)

if __name__ == '__main__':
	trans_info()