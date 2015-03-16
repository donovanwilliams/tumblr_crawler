#!/usr/bin/python
# -*- encoding:utf-8 -*-

import gevent
from gevent import monkey

monkey.patch_all()

import os
import sys
import urllib2
import json
import re
from bs4 import BeautifulSoup


MAP_FILEEXT = {'video/mp4': 'mp4'}
NUM_WORKER = 20


class TumblrCrawler(object):
    def __init__(self, url):
        self.url = url.strip()
        self.trunk_name = re.search("^http://(?P<name>.+)\.tumblr\.com$", url).group('name')

        if not os.path.exists(self.trunk_name):
            os.mkdir(self.trunk_name, 0755)

    def _load_page(self, url):
        retry = 0
        while retry < 3:
            try:
                page = urllib2.urlopen(url)
                #print page.headers
                #size = int(page.headers['Content-Length'])

                return BeautifulSoup(page.read())
            except Exception, e:
                print e, url
                retry += 1

        raise e

    def download(self, url, filename):
        retry = 0
        while retry < 3:
            try:
                data = urllib2.urlopen(url)
                size = int(data.headers['Content-Length'])

                if os.path.exists(filename) and os.path.getsize(filename) == size:
                    print "Already downloaded, skip - %s" % filename
                    data.close()
                    return

                print "Download - ", filename, url

                fp = open(filename, "wb")
                fp.write(data.read())
                fp.close()
                return
            except Exception, e:
                print e, url
                print "try again..."
                retry += 1

    def _get_file_from_img_tag(self, node):
        #print "!!@#@!#!@#!@", node
        for img in node.find_all('img'):
            #print img
            if img.has_attr('src'):
                file_url = img['src']
                filename = "%s/%s" % (self.trunk_name, file_url.rpartition('/')[-1])
                self.download(file_url, filename)

    def process_video_link(self, node):
        for data in node.find_all('iframe'):
            contents = self._load_page(data['src'])

            for obj in contents.find_all(['source']):
                meta = json.loads(obj.parent['data-crt-options'])
                file_type = obj['type']
                if meta['hdUrl'] != False and isinstance(meta['hdUrl'], (str, unicode)):
                    print meta['hdUrl']
                    file_url = meta['hdUrl']
                else:
                    file_url = obj['src']

                # Check one more time
                if str(file_url.rpartition('/')[-1]).isdigit():
                    file_url = file_url.rpartition('/')[0]

                filename = "%s/%s.%s" % (self.trunk_name, file_url.rpartition('/')[-1], MAP_FILEEXT.get(file_type, 'unknown'))
                #print file_url, file_type, filename
                try:
                    self.download(file_url, filename)
                    pass
                except Exception, e:
                    raise e
                    print contents
                    print file_url, file_type, filename, meta

    def process_photo_link(self, node):
        #print node
        links = node.find_all('a')
        if False and len(links) > 0:
            try:
                for data in links:
                    file_url = data['href']
                    contents = self._load_page(file_url)
                    for img in contents.find_all('img'):
                        if img.has_attr('data-src'):
                            file_url = img['data-src']
                            filename = "%s/%s" % (self.trunk_name, file_url.rpartition('/')[-1])
                            self.download(file_url, filename)
            except Exception, e:
                print e
                self._get_file_from_img_tag(node)
        else:
            self._get_file_from_img_tag(node)

    def process_photoset_link(self, node):
        for data in node.find_all('iframe'):
            contents = self._load_page(data['src'])
            for img in contents.find_all('a', class_='photoset_photo'):
                file_url = img['href']
                filename = "%s/%s" % (self.trunk_name, img['href'].rpartition('/')[-1])
                self.download(file_url, filename)

    def crawler_page(self, page):
        for article in page.find_all('article'):
            print article['class']
            for figure in article.find_all('figure'):
                for container in figure.find_all(class_=['tumblr_video_container', 'photo-wrapper', 'html_photoset']):
                    try:
                        if 'photo-wrapper' in container['class']:
                            self.process_photo_link(container)
                            pass
                        elif 'html_photoset' in container['class']:
                            self.process_photoset_link(container)
                            pass
                        else:
                            self.process_video_link(container)
                            pass
                    except Exception, e:
                        print e, container

    def do_crawling(self):
        page_link = '/page/1'

        worker_list = []

        while page_link:
            if len(worker_list) < NUM_WORKER:
                try:
                    soup = self._load_page(self.url + page_link)
                except Exception, e:
                    print e, self.url + page_link
                    gevent.sleep(1)
                    continue

                print "## Crawl...", self.url + page_link
                w = gevent.spawn(self.crawler_page, soup)
                #print w.ready(), w.successful(), w.started, w.dead
                worker_list.append(w)
                next_page_link = soup.find('a', class_='next')

                if next_page_link:
                    page_link = next_page_link.get('href')
                else:
                    page_link = None
            else:
                worker_list = filter(lambda x: x.successful() and x.dead, worker_list)
                if len(worker_list) >= NUM_WORKER:
                    gevent.sleep(1)

        gevent.joinall(worker_list)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "Usage : tumblr <url>"
        exit()
    else:
        url = sys.argv[1]

    c = TumblrCrawler(url)
    c.do_crawling()