# coding:utf-8
# @author:xsh

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait as wd_wait
from selenium.webdriver.support import expected_conditions as wd_ec
from selenium.webdriver.common.by import By as wb_by
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup as bsoup

import bs4.element as element
import traceback
import Queue as queue
import time
import re
import codecs
import json
import os
import random

class Spider():
	def __init__(self, need_driver):
		if need_driver:
			self.browser = webdriver.Safari()
			self.browser.set_page_load_timeout(60)
			self.queue_url = queue.Queue(maxsize=0)
			with open('idx_acute.txt', 'r') as f:
				line_list = f.readlines()
				for line in line_list:
					if len(line) > 5:
						self.queue_url.put(self.base + line.replace('http://id.who.int/icd/entity/', '').strip())
		self.pctitle_pttr = re.compile(r'>(.+)</div>')
		self.pcaxis_pttr = re.compile(r'<h4 class="pcaxis allowed">(.+)<span')
		self.base = 'https://icd.who.int/browse11/l-m/en#/http%3a%2f%2fid.who.int%2ficd%2fentity%2f'
		self.total_cnt = 0

	def save_json(self, dct, name):
		self.total_cnt += 1
		with open('data2/%s_%s.json' % (self.total_cnt, name), 'w') as f:
			json.dump(dct, f)
			print('[DEBUG] saving json file current finished with %s' % name)

	def save_pages(self):
		while self.queue_url.qsize() > 0:
			url = self.queue_url.get()
			try:
				print('[INFO] --- start crawling with url %s ------->' % url)
				self.browser.get(url)
				save_path = url.replace(self.base, '').replace('/', '_') + '.html'
				time.sleep(5)
				page = self.browser.page_source
				with codecs.open('data_pages_BA40/%s' % save_path, 'w', encoding='utf-8') as f:
					f.write(page)
					print('[INFO] page %s saved' % save_path)
			except TimeoutException as time_exp:
				print('[ERROR] shit time out...')
				self.queue_url.put(url)
			except Exception as e:
				print('[ERROR] unhandeld: %s' % e)
				traceback.print_exc()
		self.browser.close()

	def run_from_request(self):
		while self.queue_url.qsize() > 0:
			url = self.queue_url.get()
			elem_dict = {}
			str_pccode = []
			str_icdcode = ''
			str_title = ''
			str_parent = ''
			str_descrip = ''
			str_inclusion = []
			str_pcaxis = []
			try:
				print('[INFO] --- start crawling with url %s ------->' % url)
				self.browser.get(url)
				time.sleep(5)
				page_sup = bsoup(self.browser.page_source, features='html.parser')
				try:
					elem_pccode = []
					for i in range(10):
						elem_pccode.extend(page_sup.findAll('span', {'class':'pccode d%s' % i}))
					for e in elem_pccode:
						str_pccode.append(e.string)
				except Exception as e:
					print('[ERROR] parse extensions error')
				try:
					elem_pctitle_r = page_sup.findAll('div', {'class':'detailsTitle'})[0]
					str_title = str(self.pctitle_pttr.findall(str(elem_pctitle_r))).replace('\n', '').replace('\t', '').replace(' ', '')
				except:
					print('[ERROR] parse title error')
				try:
					str_icdcode = elem_pctitle_r.span.string
				except:
					print('[ERROR] parse icd code error')
				try:
					elem_parent = page_sup.findAll('li', {'class':'parent onlyparent'})[0].a['href']
					str_parent = elem_parent
				except:
					print('[ERROR] parse parent error')
				try:
					elem_descrip = page_sup.findAll('div', {'class':'definition'})[0].string.strip()
					str_descrip = elem_descrip
				except:
					print('[ERROR] parse definition error')
				try:
					elem_inclus = page_sup.findAll('li', {'class':'inclusion'})
					for e in elem_inclus:
						str_inclusion.append(e.string.strip())
				except:
					print('[ERROR] parse inclusions error')
				try:
					elem_pcaxis = page_sup.findAll('h4', {'class':'pcaxis allowed'})
					for e in elem_pcaxis:
						str_pcaxis.append(str(pcaxis_pttr.findall(str(e))).replace('\n', '').replace('\t', '').replace(' ', ''))
				except:
					print('[ERROR] parse axis error')
				elem_dict['extension_list'] = str_pccode
				elem_dict['icd11_title'] = str_title
				elem_dict['icd11_idx'] = str_icdcode
				elem_dict['only_parent'] = str_parent
				elem_dict['definition'] = str_descrip
				elem_dict['inclusion'] = str_inclusion
				elem_dict['pc_axis'] = str_pcaxis
				self.save_json(elem_dict, str_icdcode)
			except TimeoutException as time_exp:
				print('[ERROR] shit time out...')
				self.queue_url.put(url)
			except Exception as err:
				traceback.print_exc()				
				continue
		self.browser.close()

	def parse_foundation_id(self, page_sup, page_name):
		res = page_name.split('.')[0].replace('_', '/')
		print('[INFO] foundation id: %s' % res)
		return res

	def parse_icd_code(self, page_sup):
		# at div | class : detailsTitle --> span | class : icode
		try:
			res = page_sup.findAll('div', {'class':'detailsTitle'})[0].span.string.strip()
		except:
			res = 'None'
		print('[INFO] icd code: %s' % res)
		return res

	def parse_icd_title(self, page_sup):
		# at div | class : detailsTitle --> string
		try:
			res = page_sup.findAll('div', {'class':'detailsTitle'})[0].contents[-1].strip()
		except:
			res = 'None'
		print('[INFO] icd title: %s' % res)
		return res

	def parse_only_parent(self, page_sup):
		# at li | class : parent onlyparent --> a | href
		res = page_sup.findAll('li', {'class':'parent onlyparent'})[0].a['href']
		print('[INFO] onlyparent: %s' % res)
		return res

	def parse_definition(self, page_sup):
		# at div | class : definition --> string
		ele = page_sup.findAll('div', {'class':'definition'})
		try:
			res = ele[0].string.strip()
		except:
			res = 'None'
		print('[INFO] definition: %s' % res)
		return res

	def parse_inclusions(self, page_sup):
		# ul, root div: div | class : details, followed in h3 --> string : Inclusions
		root = page_sup.findAll('div', {'id':'details'})[0]
		idx0 = 0
		for idx, ele in enumerate(root.contents):
			if str(ele.name) == 'h3' and str(ele.string).strip() == 'Inclusions':
				idx0 = idx
				break
		incl_list = []
		try:
			ul = root.contents[idx0 + 2]
		except:
			ul = root.contents[idx0]
		if str(ul.name) == 'ul':
			for ele in ul.contents:
				if ele != None and str(ele.name) == 'li':
					for ele_sub in ele.contents:
						if ele_sub != None and isinstance(ele_sub, element.NavigableString):
							e_body = str(ele_sub).strip()
							try:
								e_tail = ele.a.string.strip()[1:-1]
							except:
								e_tail = 'None'
							if len(e_body) != 0:
								incl_list.append('%s_%s' % (e_body, e_tail))
		print('[INFO] inclusions: %s' % str(incl_list))
		return incl_list

	def parse_exclusions(self, page_sup):
		# ul, root div: div | class : details, followed in h3 --> string : Exclusions
		root = page_sup.findAll('div', {'id':'details'})[0]
		idx0 = 0
		for idx, ele in enumerate(root.contents):
			if str(ele.name) == 'h3' and str(ele.string).strip() == 'Exclusions':
				idx0 = idx
				break
		excl_list = []
		try:
			ul = root.contents[idx0 + 2]
		except:
			ul = root.contents[idx0]
		if str(ul.name) == 'ul':
			for ele in ul.contents:
				if ele != None and str(ele.name) == 'li':
					for ele_sub in ele.contents:
						if ele_sub != None and isinstance(ele_sub, element.NavigableString):
							e_body = str(ele_sub).strip()
							try:
								e_tail = ele.a.string.strip()[1:-1]
							except:
								e_tail = 'None'
							if len(e_body) != 0:
								excl_list.append('%s_%s' % (e_body, e_tail))
		print('[INFO] exclusions: %s' % str(excl_list))
		return excl_list

	def parse_coded_elsewhere(self, page_sup):
		root = page_sup.findAll('div', {'id':'details'})[0]
		idx0 = 0
		for idx, ele in enumerate(root.contents):
			if str(ele.name) == 'h3' and str(ele.string).strip() == 'Coded Elsewhere':
				idx0 = idx
				break
		codd_list = []
		try:
			ul = root.contents[idx0 + 2]
		except:
			ul = root.contents[idx0]
		if str(ul.name) == 'ul':
			for ele in ul.contents:
				if ele != None and str(ele.name) == 'li':
					for ele_sub in ele.contents:
						if ele_sub != None and isinstance(ele_sub, element.NavigableString):
							e_body = str(ele_sub).strip()
							try:
								e_tail = ele.a.text.string.strip()[1:-1]
							except:
								e_tail = 'None'
							if len(e_body) != 0:
								codd_list.append('%s_%s' % (e_body, e_tail))
		print('[INFO] coded elsewhere: %s' % str(codd_list))
		return codd_list

	def parse_pc_page(self, page_sup, keyword):
		try:
			root = page_sup.findAll('div', {'class':'pcdiv pcdivouter'})[0]
		except:
			return []
		idx0 = 0
		for idx, ele in enumerate(root.contents):
			if str(ele.name) == 'h4' and str(ele.contents[0]).strip() == keyword:
				idx0 = idx
				break
		pc_list = []
		try:
			ul = root.contents[idx0 + 2]
		except:
			ul = root.contents[idx0]
		if str(ul.name) == 'ul':
			for ele in ul.contents:
				if ele != None and str(ele.name) == 'li':
					e_total = ele.findAll('span')
					e_body = e_total[-1].string.strip()
					e_tail = e_total[0].string.strip()
					pc_list.append('%s_%s' % (e_body, e_tail))
		return pc_list

	def parse_pc_popup(self, page_sup, keyword):
		return []

	def parse_pc_associated_with(self, page_sup):
		res_1 = self.parse_pc_page(page_sup, 'Associated with')
		res_2 = self.parse_pc_popup(page_sup, 'Associated with')
		if len(res_1) > len(res_2):
			print('[INFO] associated with: %s' % str(res_1))
			return res_1
		else:
			print('[INFO] associated with: %s' % str(res_2))
			return res_2

	def parse_pc_causing_condition(self, page_sup):
		res_1 = self.parse_pc_page(page_sup, 'Causing conditions')
		res_2 = self.parse_pc_popup(page_sup, 'Causing conditions')
		if len(res_1) > len(res_2):
			print('[INFO] causing conditions: %s' % str(res_1))
			return res_1
		else:
			print('[INFO] causing conditions: %s' % str(res_2))
			return res_2

	def parse_pc_has_manifestation(self, page_sup):
		res_1 = self.parse_pc_page(page_sup, 'Has manifestation')
		res_2 = self.parse_pc_popup(page_sup, 'Has manifestation')
		if len(res_1) > len(res_2):
			print('[INFO] has manifestation: %s' % str(res_1))
			return res_1
		else:
			print('[INFO] has manifestation: %s' % str(res_2))
			return res_2

	def parse_pc_specific_anatony(self, page_sup):
		res_1 = self.parse_pc_page(page_sup, 'Specific anatomy')
		res_2 = self.parse_pc_popup(page_sup, 'Specific anatomy')
		if len(res_1) > len(res_2):
			print('[INFO] specific anatomy: %s' % str(res_1))
			return res_1
		else:
			print('[INFO] specific anatomy: %s' % str(res_2))
			return res_2

	def parse_pc_infections_agent(self, page_sup):
		res_1 = self.parse_pc_page(page_sup, 'Infections agent')
		res_2 = self.parse_pc_popup(page_sup, 'Infections agent')
		if len(res_1) > len(res_2):
			print('[INFO] infections agent: %s' % str(res_1))
			return res_1
		else:
			print('[INFO] infections agent: %s' % str(res_2))
			return res_2

	def parse_pc_chemical_agents(self, page_sup):
		res_1 = self.parse_pc_page(page_sup, 'Chemical agents')
		res_2 = self.parse_pc_popup(page_sup, 'Chemical agents')
		if len(res_1) > len(res_2):
			print('[INFO] chemical agents: %s' % str(res_1))
			return res_1
		else:
			print('[INFO] chemical agents: %s' % str(res_2))
			return res_2

	def parse_pc_medication(self, page_sup):
		res_1 = self.parse_pc_page(page_sup, ' Medication')
		res_2 = self.parse_pc_popup(page_sup, 'Medication')
		if len(res_1) > len(res_2):
			print('[INFO] medication: %s' % str(res_1))
			return res_1
		else:
			print('[INFO] medication: %s' % str(res_2))
			return res_2

	def parse_page(self, page_str, page_name):
		page_sup = bsoup(page_str, features='html.parser')
		res = {}
		res['res_foundation_id'] = self.parse_foundation_id(page_sup, page_name)
		res['res_icd_code'] = self.parse_icd_code(page_sup)
		res['res_icd_title'] = self.parse_icd_title(page_sup)
		res['res_only_parent'] = self.parse_only_parent(page_sup)
		res['definition'] = self.parse_definition(page_sup)
		res['res_inclusions'] = self.parse_inclusions(page_sup)
		res['res_exclusions'] = self.parse_exclusions(page_sup)
		res['res_coded_elsewhere'] = self.parse_coded_elsewhere(page_sup)
		res['res_pc_associated_with'] = self.parse_pc_associated_with(page_sup)
		res['res_pc_causing_condition'] = self.parse_pc_causing_condition(page_sup)
		res['res_pc_has_manifestation'] = self.parse_pc_has_manifestation(page_sup)
		res['res_pc_specific_anatomy'] = self.parse_pc_specific_anatony(page_sup)
		res['res_pc_infections_agent'] = self.parse_pc_infections_agent(page_sup)
		res['res_pc_chemical_agents'] = self.parse_pc_chemical_agents(page_sup)
		res['res_pc_medication'] = self.parse_pc_medication(page_sup)
		return res

	def run(self):
		root_path = 'data_pages_BA40'
		for _,_,fs in os.walk(root_path):
			for f in fs:
				curr_path = os.path.join(root_path, f)
				with codecs.open(curr_path, 'r', encoding='utf-8') as file:
					page_str = file.read()
					self.parse_page(page_str, f)
				print('--------------------')
				self.total_cnt += 1
				# if self.total_cnt > 10:
				# 	break

if __name__ == '__main__':
	spider = Spider(False)
	spider.run()






















