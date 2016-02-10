#!/usr/bin/python
# -*- coding: utf-8 -*-

import gevent
from gevent import monkey
import time

monkey.patch_all()

import os
import sys
import urllib2
import json
import re
from bs4 import BeautifulSoup


MAP_FILEEXT = {'video/mp4': 'mp4'}
NUM_WORKER = 4
BURST_SIZE = 40960
PROGRESS_BAR_SIZE = 30


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
                return BeautifulSoup(page.read(), "html.parser")
            except Exception, e:
                print e, url
                retry += 1

        raise e

    def download(self, url, filename):
        retry = 0
        while retry < 3:
            try:
                data = urllib2.urlopen(url)
                file_size = int(data.headers['Content-Length'])
                if os.path.exists(filename) and os.path.getsize(filename) >= file_size:
                    #print "Already downloaded, skip - %s" % filename
                    data.close()
                    return

                print "Downloading - {0} ({1})".format(filename, file_size)
                fp = open(filename, "wb")


                complete = False
                dn_size = 0
                check_time = 0
                while not complete:
                    ret = data.read(BURST_SIZE)
                    fp.write(ret)
                    dn_size += len(ret)

                    if BURST_SIZE != len(ret):
                        fp.flush()
                        fp.seek(0, os.SEEK_END)
                        if fp.tell() != file_size:
                            raise Exception("Download Error")
                        complete = True
                        print "Complete - {0} ({1} / {2})".format(filename, dn_size , file_size)

                    # Progress Bar for single worker
                    # if complete or (time.time() - check_time > 1):
                    #     progress = dn_size * 100 / file_size
                    #     filled_len = PROGRESS_BAR_SIZE * progress / 100
                    #     progress_text = "\r{0:50}{1:>10}% [{2}{3}]  {4}/{5}".format(filename, progress, '#'*filled_len,
                    #                                                            ' '*(PROGRESS_BAR_SIZE-filled_len),
                    #                                                            dn_size, file_size)
                    #     sys.stdout.write(progress_text)
                    #     sys.stdout.flush()
                    #     check_time = time.time()

                fp.close()
                break
            except Exception, e:
                print e, url
                print "try again..."
                os.remove(filename)
                retry += 1

    def process_photo_link(self, node):
        def _get_file_from_img_tag(img):
            if img.has_attr('src'):
                file_url = img['src']
                filename = "%s/%s" % (self.trunk_name, file_url.rpartition('/')[-1])
                self.download(file_url, filename)

        if node.name == 'img':
            _get_file_from_img_tag(node)
        else:
            for img in node.find_all('img'):
                _get_file_from_img_tag(img)

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
                try:
                    self.download(file_url, filename)
                    pass
                except Exception, e:
                    raise e
                    print contents
                    print file_url, file_type, filename, meta

    def process_photoset_link(self, node):
        self.process_photo_link(node)

        for data in node.find_all('iframe'):
            contents = self._load_page(data['src'])
            for img in contents.find_all('a', class_='photoset_photo'):
                file_url = img['href']
                filename = "%s/%s" % (self.trunk_name, img['href'].rpartition('/')[-1])
                self.download(file_url, filename)

    def crawler_page(self, page):
        for container in page.find_all(class_=['photo', 'image', 'photoset', 'video']):
            try:
                if 'video' in container['class']:
                    #print "VIDEO"
                    self.process_video_link(container)
                elif 'photoset' in container['class']:
                    #print "PHOTOSET"
                    self.process_photoset_link(container)
                else:
                    #print "PHOTO"
                    self.process_photo_link(container)
            except Exception, e:
                print e, container

    def do_crawling(self):
        page_link = 1
        worker_list = []

        while page_link:
            if len(worker_list) < NUM_WORKER:
                try:
                    soup = self._load_page(self.url + '/page/%d' % page_link)
                except Exception, e:
                    print e, self.url + page_link
                    gevent.sleep(1)
                    continue

                print "## Crawl...", self.url + '/page/%d' % page_link
                w = gevent.spawn(self.crawler_page, soup)
                worker_list.append(w)
                links = soup.find(id='footer').find_all('a')
                prev_page = page_link
                try:
                    for next_page_link in links:
                        new_link = int(re.match('/page/(\d+)', next_page_link.get('href')).group(1))
                        if new_link > prev_page:
                            page_link = new_link
                            break
                except Exception, e:
                    print e
                    page_link = None

                if prev_page == page_link:
                    # Last page
                    break
            else:
                finished_list = filter(lambda x: x.successful() and x.dead, worker_list)
                if len(finished_list) > 0:
                    worker_list = list(set(worker_list) - set(finished_list))
                    gevent.joinall(finished_list)

                if len(worker_list) >= NUM_WORKER:
                    gevent.sleep(1)

        gevent.joinall(worker_list)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "Usage : tumblr <url>"
        exit()
    else:
        url = sys.argv[1].strip('/')

    c = TumblrCrawler(url)
    c.do_crawling()
