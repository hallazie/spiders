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
			self.browser.implicitly_wait(5)
			self.browser.set_page_load_timeout(60)
			self.queue_url = queue.Queue(maxsize=0)
			self.base = 'https://icd.who.int/browse11/l-m/en#/http%3a%2f%2fid.who.int%2ficd%2fentity%2f'
			with open('ext.txt', 'r') as f:
				line_list = f.readlines()
				for line in line_list:
					if len(line) > 5:
						self.queue_url.put(self.base + line.replace('http://id.who.int/icd/entity/', '').strip())

		self.pctitle_pttr = re.compile(r'>(.+)</div>')
		self.pcaxis_pttr = re.compile(r'<h4 class="pcaxis allowed">(.+)<span')
		self.total_cnt = 0
		self.current_parse_name = ''
		self.saved_root_path = 'data_total_BA40'
		self.extension_idx = set()

	def save_json(self, dct, name):
		self.total_cnt += 1
		with open('data_x_json/%s_%s.json' % (self.total_cnt, name), 'w') as f:
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
				with codecs.open('data_total_X/%s' % save_path, 'w', encoding='utf-8') as f:
					f.write(page)
					print('[INFO] page %s saved' % save_path)
			except TimeoutException as time_exp:
				print('[ERROR] shit time out...')
				self.queue_url.put(url)
			except Exception as e:
				print('[ERROR] unhandeld: %s' % e)
				traceback.print_exc()
		self.browser.close()

	def save_page_fully_rendered(self):
		while self.queue_url.qsize() > 0:
			url = self.queue_url.get()
			self.browser.implicitly_wait(5)
			try:
				print('[INFO] --- start crawling with url %s ------->' % url)
				self.browser.get(url)
				save_path = url.replace(self.base, '').replace('/', '_') + '.html'
				time.sleep(5)
				page = self.browser.page_source
				with codecs.open('data_total_BA40/%s' % save_path, 'w', encoding='utf-8') as f:
					f.write(page)
					print('[INFO] page %s saved' % save_path)
				popupnum = self.find_popup_extensions()
				if popupnum != 0:
					print('[INFO] current page with %s popup extensions' % popupnum)
					for popupidx in range(popupnum):
						print('start testing popup idx %s' % popupidx)
						self.browser.refresh()
						time.sleep(5)
						iconlist = self.browser.find_elements_by_class_name('pchiearchyicon')
						curricon = iconlist[popupidx]
						try:
							curricon.click()
						except Exception as e:
							print('[ERROR] click current icon error with code : %s' % str(e))
							continue
						ext_list = self.browser.find_elements_by_xpath('//div[contains(@id, "tree_popuphierarchy")]')
						print('[DEBUG] get refreshed popup')

						for ext in ext_list:
						# ext = ext_list[popupidx]
							block = ext.get_attribute('innerHTML')
							print('[DEBUG] current pop with html size: %s' % len(block))
							if len(block) == 0:
								continue

							# ------------------------ expanding popups ------------------------
							cnt = 0
							self.browser.implicitly_wait(20)
							for x in range(999):
								try:
									arrowlist = ext.find_elements_by_xpath('.//table[contains(@class, "ygtv-collapsed")]')
								except TimeoutException:
									print('[ERROR] shit inner loop timeout')
									break
								cnt += len(arrowlist)
								print('[DEBUG] find %s collapesd arrow' % (len(arrowlist)))
								if len(arrowlist) == 0:
									break
								for i in range(len(arrowlist)):
									try:
										t = arrowlist[i].find_elements_by_xpath('.//a')[0]
										t.click()
										time.sleep(1)
									except:
										continue
								time.sleep(1)
							print('[DEBUG] expand finished with %s collapesd item' % cnt)
							# ------------------------ expanding popups ------------------------
							prefix = self.get_ext_prefix(ext)
							block = ext.get_attribute('innerHTML')
							print('[DEBUG] current pop with html size: %s' % len(block))
							if len(block) == 0:
								continue
							save_path_ext = url.replace(self.base, '').replace('/', '_') + '_' + prefix + '.html'
							with codecs.open('data_total_BA40/%s' % save_path_ext, 'w', encoding='utf-8') as f:
								f.write(block)
								print('[INFO] page %s with block %s saved' % (save_path, prefix))

			except TimeoutException as time_exp:
				print('[ERROR] shit time out...')
				self.queue_url.put(url)
			except Exception as e:
				print('[ERROR] unhandeld: %s' % e)
				traceback.print_exc()
		self.browser.close()		

	def find_popup_extensions(self):
		popupnum = len(self.browser.find_elements_by_class_name('pchiearchyicon'))
		return popupnum

	def expand_popups(self, popup):
		pass

	def get_ext_prefix(self, popup):
		title = popup.find_elements_by_xpath('.//a[@data-id="root"]')[0].get_attribute('innerHTML')
		return title

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
		curr_pop_name = self.current_parse_name.split('.')[0] + '_' + keyword + '.html'
		curr_pop_path = os.path.join(self.saved_root_path, curr_pop_name)
		ext_list = []
		if os.path.exists(curr_pop_path):
			with open(curr_pop_path, 'r') as curr_pop_file:
				block = curr_pop_file.read()
				block_sup = bsoup(block, features='html.parser')
				ele_list = block_sup.findAll('a', {'class':re.compile(r'ygtvlabel.*'), 'data-id':re.compile(r'http://id\.who\.int/icd/entity/.+')})
				print('[INFO] found %s extensions in current block' % len(ele_list))
				for ele in ele_list:
					if ele.span == None or ele.span.string == None:
						continue
					ele_tail = ele.span.string.strip()
					if ele_tail.startswith('X'):
						self.extension_idx.add(ele.attrs['data-id'].replace('http://id.who.int/icd/entity/', ''))
					for ele_body_none in ele.contents:
						if isinstance(ele_body_none, element.NavigableString):
							ele_body = ele_body_none.strip()
							ele_body.replace('\n', '').replace('\t', ' ').replace('\\n', '')
							ele_body = ' '.join(ele_body.split())
							# if len(ele_body) > 100:
							# 	ele_body = 'None'
					ext_list.append('%s_%s' % (ele_body, ele_tail))
		return ext_list

	def parse_pc_associated_with(self, page_sup):
		res_1 = self.parse_pc_page(page_sup, 'Associated with')
		res_2 = self.parse_pc_popup(page_sup, 'Associated with')
		if len(res_1) > len(res_2):
			print('[INFO] page associated with: %s' % str(res_1))
			return res_1
		else:
			print('[INFO] pop associated with: %s' % str(res_2))
			return res_2

	def parse_pc_causing_condition(self, page_sup):
		res_1 = self.parse_pc_page(page_sup, 'Causing conditions')
		res_2 = self.parse_pc_popup(page_sup, 'Causing conditions')
		if len(res_1) > len(res_2):
			print('[INFO] page causing conditions: %s' % str(res_1))
			return res_1
		else:
			print('[INFO] pop causing conditions: %s' % str(res_2))
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
		self.current_parse_name = page_name
		res = {}
		res['foundation_id'] = self.parse_foundation_id(page_sup, page_name)
		res['icd_code'] = self.parse_icd_code(page_sup)
		res['icd_title'] = self.parse_icd_title(page_sup)
		res['only_parent'] = self.parse_only_parent(page_sup)
		res['definition'] = self.parse_definition(page_sup)
		res['inclusions'] = self.parse_inclusions(page_sup)
		res['exclusions'] = self.parse_exclusions(page_sup)
		res['coded_elsewhere'] = self.parse_coded_elsewhere(page_sup)
		res['pc_associated_with'] = self.parse_pc_associated_with(page_sup)
		res['pc_causing_condition'] = self.parse_pc_causing_condition(page_sup)
		res['pc_has_manifestation'] = self.parse_pc_has_manifestation(page_sup)
		res['pc_specific_anatomy'] = self.parse_pc_specific_anatony(page_sup)
		res['pc_infections_agent'] = self.parse_pc_infections_agent(page_sup)
		res['pc_chemical_agents'] = self.parse_pc_chemical_agents(page_sup)
		res['pc_medication'] = self.parse_pc_medication(page_sup)
		return res

	def parse(self):
		root_path = 'data_total_X'
		for _,_,fs in os.walk(root_path):
			for f in fs:
				postfix = f.split('.')[0].split('_')[-1]
				try:
					int(postfix)
					postfix = '0'
				except Exception as e:
					pass
				if postfix not in ['unspecified', 'other', '0']:
					continue
				curr_path = os.path.join(root_path, f)
				res = {}
				with codecs.open(curr_path, 'r', encoding='utf-8') as file:
					page_str = file.read()
					res = self.parse_page(page_str, f)
				self.save_json(res, f.split('.')[0])
				print('--------------------')
				self.total_cnt += 1
		with open('ext.txt', 'w') as f:
			for e in self.extension_idx:
				f.write(e.strip() + '\n')

	def run(self):
		self.save_page_fully_rendered()

if __name__ == '__main__':
	spider = Spider(False)
	spider.parse()






















