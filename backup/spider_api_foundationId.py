# coding:utf-8
# @author:xsh

import Queue as queue
import urllib2
import json
import socket
import time
import random

class Spider():
	def __init__(self):
		self.basic_format = 'https://icd.who.int/browse11/l-m/en/JsonGetChildrenConcepts?ConceptId=http://id.who.int/icd/entity/%s&useHtml=false'
		self.basic_head = 'https://icd.who.int/browse11/l-m/en/JsonGetChildrenConcepts?ConceptId=http://id.who.int/icd/entity/'
		self.basic_tail = '&useHtml=false'
		self.idx_set = set()
		self.url_queue = queue.Queue(maxsize=0)
		self.root_id_list = [
			'1435254666',
			'1630407678',
			'1766440644',
			'1954798891',
			'21500692',
			'334423054',
			'274880002',
			'1296093776',
			'868865918',
			'1218729044',
			'426429380',
			'197934298',
			'1256772020',
			'1639304259',
			'1473673350',
			'30659757',
			'577470983',
			'714000734',
			'1306203631',
			'223744320',
			'1843895818',
			'435227771',
			'850137482',
			'1249056269',
			'1596590595',
			'718687701',
			'231358748',
			'979408586'
		]
		for e in self.root_id_list:
			self.url_queue.put(self.basic_format % e)
		self.sleep_count = 0
		self.sleep_thresh = random.randint(15, 20)
		self.sleep_time = random.randint(2,4)
		self.total_cnt = 0
		self.leaf_dict  = {}

	def run(self):
		self.crawl()

	def crawl_recursive(self, url):
		content_list = json.loads(urllib2.urlopen(url, timeout=30).read())
		if len(content_list) == 0:
			return True
		for content in content_list:
			try:
				idx = content['ID'].split('/')[-1]
				self.idx_set.add(idx)
				try:
					t = int(idx)
					self.save_idx(idx)
				except Exception as e:
					print('[INFO] encountered with false idx')
				print('[INFO] add %s into id set...' % idx)
				self.crawl_recursive(self.basic_format % idx)
			except Exception as e:
				print('[ERROR] ' % e)

	def save_idx(self, content):
		with open('idx_total.json', 'a') as f:
			json.dump(content, f)
			f.write(',\n')

	def crawl(self):
		while(self.url_queue.qsize() != 0):
			url_curr = self.url_queue.get()
			try:
				url_page = urllib2.urlopen(url_curr, timeout=15)
				self.sleep_count += 1
				if self.sleep_count == self.sleep_thresh:
					time.sleep(self.sleep_time)
					self.sleep_count = 0
					self.sleep_thresh = random.randint(15, 20)
					self.sleep_time = random.randint(2, 4)
			except urllib2.URLError as err_to:
				if isinstance(err_to.reason, socket.timeout):
					print('[ERROR] time out, add to queue end and try again later...')
					self.url_queue.put(url_curr)
				else:
					print('[ERROR] %s with url: %s' % (str(err_to), url_curr))
					self.url_queue.put(url_curr)
					continue
			except Exception as err_dft:
				print('[ERROR] unknown type error occurs: %s' % err_dft)
				continue
			finally:
				pass
			try:
				content_list = json.loads(url_page.read())
			except Exception as e:
				print('[ERROR] json convert error, invalid page: %s' % e)
				continue
			if len(content_list) == 0:
				print('[INFO] reach to leaf node %s, queue size remains %s' % (url_curr.replace(self.basic_head, '').replace(self.basic_tail, ''), self.url_queue.qsize()))
				self.leaf_dict[url_curr] = True
				leaf_flag = True
				continue
			else:
				self.leaf_dict[url_curr] = False
				leaf_flag = False
			for content in content_list:
				try:
					url_idx = content['ID']
					self.save_idx(content)
					url_postfix = url_idx.split('/')[-1]
					try:
						t = int(url_postfix)
						self.url_queue.put(self.basic_format % url_postfix)
					except Exception as err_cvt:
						print('[ERROR] other or unspecified page, save to file only')
						continue
					finally:
						self.total_cnt += 1
					print('[INFO] %sth\tnew url found: %s, queue size remains: %s' % (self.total_cnt,  url_idx, self.url_queue.qsize()))
				except KeyError as err_key:
					print('[ERROR] key ID not exist, jump over')
				except Exception as err:
					print('[ERROR] inner unknown type error occurs... %s' % err)

if __name__ == '__main__':
	s = Spider()
	s.run()