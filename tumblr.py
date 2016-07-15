#!/usr/bin/python
# -*- coding: utf-8 -*-

from multiprocessing import Process, Queue as MPQueue
import subprocess
import Queue

import os
import sys
import time
import urllib2
import signal
import json
import re
import argparse
from bs4 import BeautifulSoup


MAP_FILEEXT = {'video/mp4': 'mp4'}
BURST_SIZE = 40960
PROGRESS_BAR_SIZE = 30

worker_running = True


def sig_handler(signum, frame):
    global worker_running
    worker_running = False


def worker_main(queue):
    # return
    signal.signal(signal.SIGINT, sig_handler)

    while worker_running:
        try:
            (dn_url, filename) = queue.get(timeout=1)
            if dn_url.startswith('https://www.youtube.com'):
                try:
                    ret = subprocess.call('youtube-dl -o "' + filename + '" "%s"' % dn_url, shell=True)
                except Exception, e:
                    print e
                    print "Error - downloading from Youtube. (%s)" % dn_url
            else:
                download_file(dn_url, filename)
        except Queue.Empty:
            time.sleep(1)
        except Exception, e:
            print e


def download_file(url, filename):
    retry = 0
    while retry < 3:
        try:
            data = urllib2.urlopen(url)
            file_size = int(data.headers['Content-Length'])
            if os.path.exists(filename) and os.path.getsize(filename) >= file_size:
                data.close()
                return
            print "Downloading - {0} ({1:,} bytes)".format(filename, file_size)
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
                    print "Complete - {0} ({1:} / {2:,} bytes)".format(filename, dn_size , file_size)

            fp.close()
            break
        except Exception, e:
            print e, url
            print "try again..."
            os.remove(filename)
            retry += 1


class TumblrCrawler(object):
    def __init__(self, config):
        self.config = config
        try:
            self.dest_path = re.search("^http://(?P<name>.+)\.tumblr\.com.*", config.url.strip()).group('name')
            self.url = 'http://%s.tumblr.com' % self.dest_path
        except Exception, e:
            raise Exception("Invalid URL - %s" % self.url)

        self.queue = MPQueue()
        self.dup_cache = []

        if not os.path.exists(self.dest_path):
            os.mkdir(self.dest_path, 0755)

    def add_download_queue(self, url, filename=None):
        if url not in self.dup_cache:
            if not filename:
                filename = "%s/%s" % (self.dest_path, url.rpartition('/')[-1])
            self.dup_cache.append(url)
            self.queue.put((url, filename))

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

    def process_photo_link(self, node):
        def _get_file_from_img_tag(img):
            if img.has_attr('src'):
                return img['src']

        if node.name == 'img':
            self.add_download_queue(_get_file_from_img_tag(node))
        else:
            for img in node.find_all('img'):
                self.add_download_queue(_get_file_from_img_tag(img))

    def process_video_link(self, node):
        for data in node.find_all('iframe'):
            vid_src = data['src']
            if vid_src.startswith('https://www.youtube.com'):
                filename = self.dest_path + '/%(title)s.%(ext)s'
                self.add_download_queue(vid_src, filename)
            else:
                contents = self._load_page(vid_src)
                for obj in contents.find_all(['source']):
                    meta = json.loads(obj.parent['data-crt-options'])
                    file_type = obj['type']
                    if meta['hdUrl'] != False and isinstance(meta['hdUrl'], (str, unicode)):
                        #print meta['hdUrl']
                        file_url = meta['hdUrl']
                    else:
                        file_url = obj['src']

                    # Check one more time
                    if str(file_url.rpartition('/')[-1]).isdigit():
                        file_url = file_url.rpartition('/')[0]

                    filename = "%s/%s.%s" % (self.dest_path, file_url.rpartition('/')[-1], MAP_FILEEXT.get(file_type, 'unknown'))
                    self.add_download_queue(file_url, filename)

    def process_photoset_link(self, node):
        self.process_photo_link(node)

        for data in node.find_all('iframe'):
            contents = self._load_page(data['src'])
            for img in contents.find_all('a', class_='photoset_photo'):
                self.add_download_queue(img['href'])

    def crawler_page(self, page):
        for container in page.find_all(class_=['photo', 'image', 'photoset', 'video']):
            try:
                if 'video' in container['class']:
                    self.process_video_link(container)
                elif 'photoset' in container['class']:
                    self.process_photoset_link(container)
                else:
                    self.process_photo_link(container)
            except Exception, e:
                print e, container

    def do_crawling(self):
        page_link = 1
        worker_list = []

        for idx in range(self.config.worker):
            w = Process(target=worker_main, args=(self.queue, ))
            worker_list.append(w)

        map(lambda x: x.start(), worker_list)

        try:
            while True:
                print "## Crawling : ", self.url + '/page/%d' % page_link
                try:
                    self.dup_cache = []
                    soup = self._load_page(self.url + '/page/%d' % page_link)
                except Exception, e:
                    print e, self.url + page_link
                    time.sleep(1)
                    continue

                container = soup.find('body').find_all(class_=['photo', 'image', 'photoset', 'video'])
                for content in container:
                    # print content
                    # raw_input()
                    try:
                        if 'video' in content['class']:
                            self.process_video_link(content)
                        elif 'photoset' in content['class']:
                            self.process_photoset_link(content)
                        else:
                            self.process_photo_link(content)
                    except Exception, e:
                        print e, content

                if len(container) == 0:
                    # No more data.
                    break

                page_link += 1

            while not self.queue.empty():
                time.sleep(1)

            map(lambda x: os.kill(x.pid, signal.SIGINT), worker_list)
            map(lambda x: x.join(), worker_list)
        except KeyboardInterrupt:
            map(lambda x: x.terminate(), worker_list)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Crawling Tumblr images and videos')
    parser.add_argument('-d', '--debug', action='store_true', help='debug mode')
    parser.add_argument('-w', '--worker', metavar='number of worker', default=4, type=int, help='use multiple downloads')
    parser.add_argument('url', help='tumblr url')

    config = parser.parse_args()

    TumblrCrawler(config).do_crawling()

