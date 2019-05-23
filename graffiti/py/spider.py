# coding:utf-8
# @author    :    hallazie
# @mail      :    hallazie@outlook.com

import urllib2
import urllib
import os
import re

class Spider():
    '''
        从 https://www.graffiti.org 爬取涂鸦
        其 .jpg resource的结构遵从以下结构
        ____https://www.graffiti.org/index/europe.html
            |____https://www.graffiti.org/prague/prague_3.html
            |    |____https://www.graffiti.org/prague/scoutscu.jpg
            |    |____https://www.graffiti.org/prague/dskbus.jpg
            ...
            |____https://www.graffiti.org/berlin/berlin_6.html
                 |____https://www.graffiti.org/berlin/gpl10.jpg
        ....
        ____https://www.graffiti.org/index/usa.html
    '''
    def __init__(self):
        self.root_paths = [
            'https://www.graffiti.org/index/world.html',
            'https://www.graffiti.org/index/usa.html',
            'https://www.graffiti.org/index/europe.html',
            'https://www.graffiti.org/index/artists.html',
            ]
        self.pattern_root = re.compile(r'<a href="(\.\./[a-z0-9]+/[a-z0-9]+\.html)"> [0-9]+</a>')
        self.pattern_jpg = re.compile(r'<a href="([a-z0-9]+\.jpg)">')
        self.file_path = '../data'

    def craw_jpg_path(self, url):
        jpg_page = urllib2.urlopen(url).read()
        jpg_list = self.pattern_jpg.findall(jpg_page)
        for jpg_path in jpg_list:
            self.craw_jpg_file(jpg_path, url)

    def craw_jpg_file(self, path, url):
        jpg_path = '/'.join(url.split('/')[:-1]) + '/' + path
        jpg_file = os.path.join(self.file_path, path)
        urllib.urlretrieve(jpg_path, jpg_file)
        print('[INFO] %s retrieved' % jpg_path)

    def crawl(self):
        for root_path in self.root_paths:
            root_page = urllib2.urlopen(root_path).read()
            root_list = self.pattern_root.findall(root_page)
            for midd_path_surfix in root_list:
                midd_path = 'https://www.graffiti.org' + midd_path_surfix[2:]
                self.craw_jpg_path(midd_path)
                print('[INFO] page %s finished' % midd_path)
            print('[INFO] root %s finished' % root_path)


if __name__ == '__main__':
    spider = Spider()
    spider.crawl()
