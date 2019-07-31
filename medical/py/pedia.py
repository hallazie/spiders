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
import bs4.element as element

from bs4 import BeautifulSoup as bsoup
from urllib import unquote

class Spider():
	def __init__(self):
		self.root_path = 'http://www.a-hospital.com'
		# self.root_list = ['全身疾病查询']
		self.root_list = ['全身疾病查询', '皮肤病', '头部疾病查询', '眼科疾病', '耳朵疾病查询', '鼻部疾病查询', '口腔科疾病', '颈部疾病查询', '胸部疾病查询', '背部疾病查询', '腹部疾病查询', '腰部疾病查询', '臀部疾病查询', '盆腔疾病查询', '妇科疾病', '男科疾病', '上肢疾病查询', '下肢疾病查询']
		self.path_patt = re.compile(r'<li><a href="(/w/.+)" title=".+">.+</a></li>')
		self.tabl_list = ['msg-table', 'wikitable']
		self.db = pymongo.MongoClient('mongodb://localhost:27017')['medical']
		self.cl = self.db['pedia']
		self.page_queu = queue.Queue(maxsize=0)
		self.curr_syno = []
		self.curr_text = []

	def crawl_page(self):
		for root in self.root_list:
			curr_root_path = self.root_path + '/w/' + root
			page = urllib2.urlopen(curr_root_path, timeout=120).read()
			curr_path_list = self.path_patt.findall(page)
			for curr_path in curr_path_list:
				self.page_queu.put(curr_path)
				print('[INFO] put %s into queue, size: %s' % (curr_path, self.page_queu.qsize()))

		while self.page_queu.qsize() > 0:
			url = self.page_queu.get()
			self.curr_syno = []
			self.curr_text = []
			try:
				self.crawl_content(url)
			except Exception as e:
				print('[ERROR] %s with error code : %s' % (url, str(e)))
				self.page_queu.put(url)
			finally:
				time.sleep(random.randint(1,5))

	def get_raw_text(self, para, idx):
		if idx < 5:
			bolds = para.findAll('b')
			for b in bolds:
				self.curr_syno.append(b.text)
		for c in para.contents:
			if isinstance(c, element.NavigableString):
				self.curr_text.append(c.replace('\n', '').replace('\t', '').replace('  ', ' '))
			else:
				self.curr_text.append(c.text.replace('\n', '').replace('\t', '').replace('  ', ' '))

	def crawl_content(self, url):
		page = urllib2.urlopen(self.root_path + url, timeout=120).read()
		block_sup = bsoup(page, features='html.parser')
		for tabl in block_sup.findAll('table'):
			tabl.decompose()
		body = block_sup.findAll('div', {'id':'bodyContent'})[0]
		para_list = body.findAll('p')
		for idx, para in enumerate(para_list):
			self.get_raw_text(para, idx)
		curr_title = unquote(url.replace('/w/', ''))
		curr_item = {
			'title':curr_title.decode('utf-8'),
			'content':' '.join(self.curr_text),
			'syno':self.curr_syno
		}
		self.cl.insert_one(curr_item)
		print('[INFO] %s finished with %s remains' % (curr_title, self.page_queu.qsize()))

if __name__ == '__main__':
	spider = Spider()
	spider.crawl_page()