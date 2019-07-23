# coding:utf-8
# @author:xsh

import Queue as queue
import urllib2
import json
import socket
import time
import random
import requests
import pymongo
import traceback
import re

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup as bsoup
from collections import defaultdict

class Spider():
	def __init__(self):
		# self.url_tree_format = 'https://icd.who.int/browse11/l-m/en/JsonGetChildrenConcepts?ConceptId=http://id.who.int/icd/entity/%s&useHtml=false'
		# json格式，包括了title，id等基本信息，parent，children层级信息，以及inclusion，exclusion信息。但是没有coded elsewhere信息。
		self.url_entity_format = 'https://id.who.int/icd/entity/%s'

		# html格式，单一用于提取coded elsewhere信息。后期可增量进来。
		# self.url_page_format = 'https://icd.who.int/browse11/l-m/en/GetConcept?ConceptId=http://id.who.int/icd/entity/%s'
		self.url_page_format = 'https://icd.who.int/browse11/l-m/en#/http%3a%2f%2fid.who.int%2ficd%2fentity%2f'

		# json格式，传入对应的id及extension名，返回当前extension的根结点（s）。页面中非弹出框中的extension可同样获得。
		self.url_popup_format_0 = 'https://icd.who.int/browse11/l-m/en/JsonGetPCAxisRoot?ConceptId=http%3A%2F%2Fid.who.int%2Ficd%2Fentity%2F'
		self.url_popup_format_1 = '&useHtml=false&axisName=http%3A%2F%2Fwho.int%2Ficd%23'
		self.url_popup_format_3 = '&useHtml=true&axisName=http%3A%2F%2Fwho.int%2Ficd%23'
		self.url_popup_format_2 = '&fullAxis=false&itemViewName=TreeViewItem'
		
		# json格式，传入extension id，返回当前id的子节点。可形成树状结构。
		self.url_extension_format = 'https://icd.who.int/browse11/l-m/en/JsonGetChildrenConcepts?ConceptId=http://id.who.int/icd/entity/%s&useHtml=false&showAdoptedChildren=true&itemViewName=TreeViewItem&isAdoptedChild=false'
		self.url_extension_format_html = 'https://icd.who.int/browse11/l-m/en/JsonGetChildrenConcepts?ConceptId=http://id.who.int/icd/entity/%s&useHtml=true&showAdoptedChildren=true&itemViewName=TreeViewItem&isAdoptedChild=false'
		
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
		self.multi_extension_list = [
			'associatedWith',
			'medication',
			'hasManifestation',
			'specificAnatomy',
			'hasCausingCondition',
			'chemicalAgent',
			'infectiousAgent',
			]
		self.multi_extension_filter = [e.lower() for e in self.multi_extension_list]
		self.uniq_extension_list = [
			'laterality',
			'histopathology',
			'hasSeverity'
		]
		self.uniq_extension_filter = [e.lower() for e in self.uniq_extension_list]
		self.extension_dict = {
			"Laterality":"laterality",
			"Time in life":"timeInLife",
			"Associated with":"associatedWith",
			"Has pupil reaction score":"hasPupilReactionScore",
			"Has gcs eye score":"hasGCSEyeScore",
			"Has gcs motor score":"hasGCSMotorScore",
			"Has gcs verbal score":"hasGCSVerbalScore",
			"Specific anatomy":"specificAnatomy",
			"Has manifestation":"hasManifestation",
			"Infectious agent":"infectiousAgent",
			"Relational":"relational",
			"Histopathology":"histopathology",
			"Has severity":"hasSeverity",
			"Course":"course",
			"Fracture subtype":"fractureSubtype",
			"Fracture open or closed":"fractureOpenOrClosed",
			"Has causing condition":"hasCausingCondition",
			"Type of injury":"typeOfInjury",
			"Joint involvement in fracture":"jointInvolvementInFracture",
			"Chemical agent":"chemicalAgent",
			"Object or substance producing injury":"objectOrSubstanceProducingInjury",
			"Place of occurrence":"placeOfOccurrence",
			"Place of occurrence descriptor":"placeOfOccurrenceDescriptor",
			"Activity when injured":"activityWhenInjured",
			"Violence descriptor":"violenceDescriptor",
			"Medication":"medication",
			"Duration of coma":"durationOfComa",
			"Transport event descriptor":"transportEventDescriptor",
			"Intent":"intent",
			"Extent of burn by body surface":"extentOfBurnByBodySurface",
			"Extent of full thickness burn by body surface":"extentOfFullThicknessBurnByBodySurface",
			"Outcome of full thickness burn":"outcomeOfFullThicknessBurn",
			"Distribution":"distribution",
			"Temporal pattern and onset":"temporalPatternAndOnset"
		}

		self.url_queue = queue.Queue(maxsize=0)
		self.ext_queue = queue.Queue(maxsize=0)

		self.sleep_count = 0
		self.sleep_thresh = random.randint(15, 20)
		self.sleep_time = random.randint(2,4)
		self.total_cnt = 0

		self.token_endpoint = 'https://icdaccessmanagement.who.int/connect/token'
		self.client_id = '9a4cb39a-5fba-468d-962a-6b0e98aa401b_8846a032-67c7-46f5-bf40-eb52f34541e5'
		self.client_secret = 'E7aENZUc6dFjoATx6x6vr0Nv2Pgw61s6fmb4spxE/Z4='
		self.scope = 'icdapi_access'
		self.grant_type = 'client_credentials'
		self.payload = {
			'client_id': self.client_id, 
			'client_secret': self.client_secret, 
			'scope': self.scope, 
			'grant_type': self.grant_type
			}
		self.page_request = requests.post(self.token_endpoint, data=self.payload, verify=False).json()
		self.token = self.page_request['access_token']
		self.headers = {
			'Authorization': 'Bearer '+self.token, 
			'Accept': 'application/json', 
			'API-Version': 'v2',
			'Accept-Language': 'en'}

		self.client = pymongo.MongoClient('mongodb://localhost:27017')
		self.db = self.client['icd11']
		self.col = self.db['html']
		self.crawled_idx = set()
		self.crawled_idx_pattern = re.compile(r'Foundation Id : (http://id.who.int/icd/entity/[0-9]+)')

	def save_mongo_basic(self, page, title):
		ele = {
			'title' : title,
			'content' : page,
			'label' : self.j_label,
			'breath' : self.j_breath_value,
			'depth' : self.j_depth,
			'leaf' : self.j_leaf}
		if self.col.find({'title' : title}).count() == 0:
			self.col.insert_one(ele)	
			with open('crawled.txt', 'a') as crawled:
				crawled.write(title + '\n')
			print('[INFO] %sth\t%s saved in mongodb' % (self.total_cnt, title))
		else:
			print('[INFO] %s already exists in mongodb' % title)
		self.total_cnt += 1

	def save_mongo_rawpage(self, idx, page, breath, leaf):
		if self.col.find({'idx' : idx}).count() == 0:
			self.col.insert_one({
				'idx' : idx,
				'html' : page,
				'breath' : breath,
				'leaf' : leaf})
			print('[INFO] %sth\t%s saved in mongodb' % (self.total_cnt, idx))
		else:
			print('[INFO] %sth\t%s already exists in mongodb' % (self.total_cnt, idx))

	def update_mongo_rawpage(self, idx, page, breath, leaf):
		if self.col.find({'idx' : idx}).count() != 0:
			self.col.update_one({
				'idx' : idx
			},{
				'$set':{'html' : page}
			})
			print('[INFO] %sth\t%s updated in mongodb' % (self.total_cnt, idx))
		else:
			print('[INFO] %sth\t%s not exists in mongodb yet!' % (self.total_cnt, idx))


	def save_mongo_extension(self):
		pass

	def init_html_url_queue(self):
		with open('idx_total_0714.json', 'r') as f:
			for line in f.readlines():
				try:
					line_j = json.loads(line.strip()[:-1])
					self.url_queue.put({
						'idx' : line_j['ID'].replace('http://id.who.int/icd/entity/', ''),
						'breath' : line_j['breadthValue'],
						'leaf' : line_j['isLeaf']
						})
				except Exception as e:
					print('[ERROR] error parsing [%s] with error code [%s]' % (line_j, str(e)))

	def init_extension_url_queue(self):
 		self.col = self.db['info']
		self.cursor = self.col.find()
		for c in self.cursor:
			self.url_queue.put(c)

	def init_entity_queue(self):
		self.col = self.db['info']
		self.cursor = self.col.find()
		for c in self.cursor:
			self.url_queue.put(c)		

	def recrawl_duplicate_page(self):
		# 对因为超时重复的page，重新爬取。

		asc_index = 0
		dup_key = 'icd_code'
		db = pymongo.MongoClient('mongodb://localhost:27017')['icd11']
		info = db['info']
		html = db['html']
		total_num = info.find().count()
		icd_code_dict = {}
		dup_idx_queue = queue.Queue(maxsize=0)
		for cursor in info.find({}, {dup_key:1, 'foundation_id':1}):
			if cursor[dup_key] == 'None':
				cursor[dup_key] += str(asc_index)
				asc_index += 1
			if cursor[dup_key] not in icd_code_dict.keys():
				icd_code_dict[cursor[dup_key]] = [cursor['foundation_id']]
			else:
				icd_code_dict[cursor[dup_key]].append(cursor['foundation_id'])
		for k in icd_code_dict:
			if len(icd_code_dict[k]) > 1:
				print('[INFO] %s with size %s, members: %s' % (k, len(icd_code_dict[k]), ', '.join(icd_code_dict[k])))
				for e in icd_code_dict[k]:
					dup_idx_queue.put(e)

		self.browser = webdriver.Safari()
		self.browser.implicitly_wait(5)
		self.browser.set_page_load_timeout(40)

		part_cnt = 1
		while dup_idx_queue.qsize() > 0:
			self.total_cnt += 1

			curr_url_idx = dup_idx_queue.get()
			try:
				part_cnt += 1
				curr_url = self.url_page_format + curr_url_idx

				self.browser.get(curr_url)
				time.sleep(5)
				page = self.browser.page_source

				page_sup = bsoup(page, features='html.parser')
				page_div = page_sup.find_all('div', {'id' : 'firstright'})
				if len(page_div) > 0:
					self.update_mongo_rawpage(curr_url_idx, str(page_div[0]), 0, 0)
				else:
					self.update_mongo_rawpage(curr_url_idx, page, 0, 0)
			except TimeoutException as time_exp:
				print('[ERROR] time out error with code : %s' % time_exp)
				dup_idx_queue.put(curr_url_idx)
			except Exception as e:
				dup_idx_queue.put(curr_url_idx)
				print('[ERROR] unexpected error occurs with code : %s' % e)
				traceback.print_exc()
		self.browser.close()




	def crawl_page(self):		
		self.browser = webdriver.Safari()
		self.browser.implicitly_wait(5)
		self.browser.set_page_load_timeout(40)

		self.init_html_url_queue()
		part_cnt = 1
		while self.url_queue.qsize() > 0:
			self.total_cnt += 1
			if part_cnt % 500 == 0:
				print('[INFO] refreshing browser to collapse tree...')
				self.browser.refresh()
				time.sleep(10)

			curr_url_ele = self.url_queue.get()
			curr_url_idx = curr_url_ele['idx']
			curr_url_bth = curr_url_ele['breath']
			curr_url_lef = curr_url_ele['leaf']
			if self.col.find({'idx' : curr_url_idx}).count() != 0:
				print('[INFO] %sth\t%s already exists in mongodb' % (self.total_cnt, curr_url_idx))
				continue
			try:
				part_cnt += 1
				curr_url = self.url_page_format + curr_url_idx
				# print('[INFO] --- start crawling with url %s ------->' % curr_url)
				self.browser.get(curr_url)
				time.sleep(3)
				page = self.browser.page_source

				# curr_idx = self.crawled_idx_pattern.findall(page)[0]
				# if curr_idx in self.crawled_idx:
				# 	print('[ERROR] %s occurs again, probably due to page frozen, sleep for a while' % curr_idx)
				# 	self.url_queue.put(curr_url_ele)
				# 	self.browser.refresh()
				# 	time.sleep(10)
				# 	continue
				# self.crawled_idx.add(curr_idx)

				page_sup = bsoup(page, features='html.parser')
				page_div = page_sup.find_all('div', {'id' : 'firstright'})
				if len(page_div) > 0:
					self.save_mongo_rawpage(curr_url_idx, str(page_div[0]), curr_url_bth, curr_url_lef)
				else:
					self.save_mongo_rawpage(curr_url_idx, page, curr_url_bth, curr_url_lef)
			except TimeoutException as time_exp:
				print('[ERROR] time out error with code : %s' % time_exp)
				self.url_queue.put(curr_url_ele)
			except Exception as e:
				self.url_queue.put(curr_url_ele)
				print('[ERROR] unexpected error occurs with code : %s' % e)
				traceback.print_exc()
				# break
		self.browser.close()

	def crawl_extension_root(self):
		'''
			@function: first use self.url_popup_format to crawl root extension for each node, then find if the parent extension exists in icd11/extension. if so, use the exists extension. if not, recursively crawl the extension tree use self.url_extension_format.
		'''
		self.init_extension_url_queue()
		cnt = 0
		while self.url_queue.qsize() > 0:
			curr_info = self.url_queue.get()
			if len(curr_info['extension']) <= 0:
				print('[INFO]%sth\t%s has no extension' % (cnt, curr_info['foundation_id']))
				cnt += 1
				continue
			if 'extension_tree' in curr_info.keys():
				print('[INFO]%sth\t%s already crawled, qsize remain %s' % (cnt, curr_info['foundation_id'], self.url_queue.qsize()))
				cnt +=1 
				continue
			try:
				ext_list = []
				for ext_k in curr_info['extension']:
					ext_v = self.extension_dict[ext_k]
					url_extension = self.url_popup_format_0 + curr_info['foundation_id'] + self.url_popup_format_3 + ext_v + self.url_popup_format_2
					content = json.loads(urllib2.urlopen(url_extension, timeout=120).read())
					children = [e['ID'].replace('http://id.who.int/icd/entity/', '') for e in content]
					ext_list.append({ ext_v : children})
				self.col.update_one(
					{'foundation_id' : curr_info['foundation_id']},
					{'$set' : {'extension_tree' : ext_list}})
				print('[INFO]%sth\t%s finished root extension with length %s, qsize remain %s' % (cnt, curr_info['foundation_id'], len(ext_list), self.url_queue.qsize()))
				cnt += 1
			except Exception as e:
				print('[ERROR] error at %s with error code %s' % (curr_info['foundation_id'], str(e)))
				# traceback.print_exc()
				self.url_queue.put(curr_info)

	# def crawl_extension(self):
	# 	self.init_extension_url_queue()
	# 	while self.url_queue.qsize() > 0:
	# 		curr_info = self.url_queue.get()
	# 		if len(curr_info['extension']) <= 0:
	# 			print('[INFO] %s has no extension' % curr_info['foundation_id'])
	# 			continue
	# 		try:
	# 			for ext_k in curr_info['extension']:
	# 				ext_v = self.extension_dict[ext_k]
	# 				for ext in curr_info['ext_v']:
	# 					if self.db['extension'].find({'foundation_id' : ext['foundation_id']}).count > 0:
	# 						continue
	# 					else:
	# 						# recursive find all the children til all leaves are reached.
	# 						with self.ext_queue.mutex:
	# 							self.ext_queue.clear()
	# 						self.ext_queue.put(ext['foundation_id'])
	# 		except Exception as e:
	# 			print('[ERROR] extension root crawl error with code %s' % str(e))
	# 			# traceback.print_exc()
	# 			self.url_queue.put(curr_info)

	# def crawl_extension_recurs(self):
	# 	total_error_count = 0
	# 	while self.ext_queue.qsize() > 0:
	# 		try:
	# 			ext_idx = self.ext_queue.get()
	# 			ext_url = self.url_extension_format % ext_idx
	# 			content = json.loads(urllib2.urlopen(ext_url, timeout=15).read())

	# 			res = {}
	# 			res['foundation_id'] = ext_idx
	# 			res['children'] = [e['ID'].replace('http://id.who.int/icd/entity/', '') for e in content]

	# 			if len(content) == 0:
	# 				continue
	# 			for ele in content:
	# 				if ele['isLeaf'] == 'false':
	# 					self.ext_queue.put(ele['ID'].replace('http://id.who.int/icd/entity/', ''))
	# 		except Exception as e:
	# 			if total_error_count > 99:
	# 				break
	# 			print('[ERROR] extension recurs crawling error with code %s' % str(e))
	# 			self.ext_queue.put(ext_idx)
	# 			total_error_count += 1

	def crawl_extension(self):
		code_patt = re.compile(r'<span class="xicode ">(.+)</span>')
		xode_patt = re.compile(r'<span class="icode ">(.+)</span>')
		titl_patt = re.compile(r'</span>(.+)</a>&nbsp;')
		self.ext_collection = self.db['extension']
		self.ent_collection = self.db['info']
		self.total_cnt = 0
		self.ext_set = set()
		for curs in self.db['info'].find():
			if 'extension_tree' not in curs.keys():
				continue
			for ext_cat in curs['extension_tree']:
				for ext in ext_cat.values()[0]:
					self.ext_set.add(ext)
		print('[INFO] total ext root size: %s' % len(self.ext_set))
		for ext in self.ext_set:
			self.ext_queue.put(ext)
		while self.ext_queue.qsize() > 0:
			self.total_cnt += 1
			ext_idx = self.ext_queue.get()
			try:
				# ext_url = self.url_extension_format % ext_idx
				ext_url = self.url_extension_format_html % ext_idx
				content = json.loads(urllib2.urlopen(ext_url, timeout=60).read())
				for child in content:
					child['ID'] = child['ID'].replace('http://id.who.int/icd/entity/', '')

					child['html'] = child['html'].replace('\r', '').replace('\t', '').replace('\n', '').replace('  ', ' ')
					code_str = code_patt.findall(child['html'])
					xode_str = xode_patt.findall(child['html'])
					titl_str = titl_patt.findall(child['html'])
					if len(code_str) > 0:
						child['label'] = code_str[0] + ' ' + titl_str[0].strip()
					elif len(xode_str) > 0:
						child['label'] = xode_str[0] + ' ' + titl_str[0].strip()
					elif len(titl_str) > 0:
						child['label'] = 'None ' + titl_str[0].strip()
					else:
						child['label'] = 'None None'

					if not child['isLeaf'] and child['ID'] not in self.ext_set:
						self.ext_set.add(child['ID'])
						self.ext_queue.put(child['ID'])
					entity_node = self.ent_collection.find_one({'foundation_id' : child['ID']})
					if entity_node != None:
						child['icd_code'] = entity_node['icd_code']
						child['icd_title'] = entity_node['icd_title']
						if 'icd_title_zh' in entity_node.keys():
							child['icd_title_zh'] = entity_node['icd_title_zh']
					else:
						print('[WARN] %s | %s not exist in info...' % (child['ID'], child['label']))
					child_node = self.ext_collection.find_one({'ID' : child['ID']})
					if child_node == None:
						if child['isAdoptedChild'] == True:
							child['adopted_parent'] = [ext_idx]
							print('[INFO] %sth insert one %s with adopted_parent %s qsize remains %s' % (self.total_cnt, child['ID'], ext_idx, self.ext_queue.qsize()))
						else:
							child['only_parent'] = ext_idx
							print('[INFO] %sth insert one %s with only_parent %s qsize remains %s' % (self.total_cnt, child['ID'], ext_idx, self.ext_queue.qsize()))
						self.ext_collection.insert_one(child)
					else:
						flag = False
						if child['isAdoptedChild'] and 'adopted_parent' in child_node.keys() and ext_idx not in child_node['adopted_parent']:
							adopted_parent_list = child_node['adopted_parent']
							adopted_parent_list.append(ext_idx)
							self.ext_collection.update_one({
								'ID' : child_node['ID']
							},{
								'$set' : {
									'adopted_parent' : adopted_parent_list
								}
							})
							flag = True
						elif child['isAdoptedChild'] and 'adopted_parent' not in child_node.keys():
							self.ext_collection.update_one({
								'ID' : child_node['ID']
							},{
								'$set' : {
									'adopted_parent' : [ext_idx]
								}
							})
							flag = True
						elif not child['isAdoptedChild'] and 'only_parent' not in child_node.keys():
							self.ext_collection.update_one({
								'ID' : child_node['ID']
							},{
								'$set' : {
									'only_parent' : ext_idx
								}
							})
							flag = True
						if flag:
							print('[INFO] %sth update one %s with adopted_flag %s, qsize remains %s' % (self.total_cnt, child['ID'], child['isAdoptedChild'], self.ext_queue.qsize()))
						else:
							print('[INFO] %sth nothing todo with %s, jump over, qsize remains %s' % (self.total_cnt, child['ID'], self.ext_queue.qsize()))
			except Exception as e:
				print(('[ERROR] %sth %s extension recurs crawling error with code %s' % (self.total_cnt, ext_idx, str(e))))
				# traceback.print_exc()
				self.ext_queue.put(ext_idx)

if __name__ == '__main__':
	s = Spider()
	s.crawl_extension()









