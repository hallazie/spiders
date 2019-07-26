# coding:utf-8
# 
# @author:xsh
# thanks http://www.a-hospital.com/ for the open source contribution

import urllib2
import re
import pymongo
import random
import time
import queue
import logging 

from bs4 import BeautifulSoup as bsoup

class Spider():
	def __init__(self):
		self.root_path = 'http://www.a-hospital.com/w/'
		self.root_list = ['全身疾病查询', '皮肤病', '头部疾病查询', '眼科疾病', '耳朵疾病查询', '鼻部疾病查询', '口腔科疾病', '颈部疾病查询', '胸部疾病查询', '背部疾病查询', '腹部疾病查询', '腰部疾病查询', '臀部疾病查询', '盆腔疾病查询', '妇科疾病', '男科疾病', '上肢疾病查询', '下肢疾病查询']
		self.path_patt = re.compile(r'<li><a href="(/w/.+)" title=".+">.+</a></li>')
		self.db = pymongo.MongoClient('mongodb://localhost:27017')['medical']
		self.cl = self.db['pedia']
		self.page_queu = queue.Queue(maxsize=0)

	def crawl_page(self):
		for root in self.root_list:
			curr_root_path = self.root_path + root
			page = urllib2.urlopen(curr_root_path, timeout=120).read()
			curr_path_list = self.path_patt.find_all(page)
			for curr_path in curr_path_list:
				self.page_queu.put(curr_path)

		while self.page_queu.qsize() > 0:
			url = self.page_queu.get()
			time.sleep(random.randint(1,5))
			try:
				self.crawl_content(curr_path)
				return
			except Exception as e:
				self.page_queu.put(url)

	def get_raw_text(self, para):
		bolds = para.findAll('b')
		strgs = para.findAll('strong')
		for b in bolds:
			print('[SYNO-B]: %s' % b)
		for s in strgs:
			print('[SYNO-S]: %s' % s)
		for c in para.contents:
			print(c)

	def crawl_content(self, url):
		page = urllib2.urlopen(url, timeout=120).read()
		block_sup = bsoup(page, features='html.parser')
		body = block_sup.findAll('div', {'id':'bodyContent'})[0]
		para_list = body.findAll('p')
		for para in para_list:
			self.get_raw_text(para)

if __name__ == '__main__':
	spider = Spider()
	spider.crawl_page()