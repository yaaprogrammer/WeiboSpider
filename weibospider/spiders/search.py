#!/usr/bin/env python
# encoding: utf-8
"""
Author: rightyonghu
Created Time: 2022/10/22
"""
import json
import re
from scrapy import Spider, Request
from spiders.common import parse_tweet_info, parse_long_tweet


class SearchSpider(Spider):
    """
    关键词搜索采集
    """
    name = "search_spider"
    base_url = "https://s.weibo.com/"

    def start_requests(self):
        """
        爬虫入口
        """
        # 这里keywords可替换成实际待采集的数据
        keywords = ['官老爷', '食税阶级', '利益集团', '垄断阶级', '砖家', '叫兽', '学阀', '公知', '底层屁民',
                    '特权阶级', '上升通道堵死']
        start_time = "2021-11-06-0"
        end_time = "2022-11-06-0"
        is_search_with_specific_time_scope = True  # 是否在指定的时间区间进行推文搜索
        for keyword in keywords:
            if is_search_with_specific_time_scope:
                url = f"https://s.weibo.com/weibo?q={keyword}&timescope=custom%3A{start_time}%3A{end_time}&page=1"
            else:
                url = f"https://s.weibo.com/weibo?q={keyword}&page=1"
            yield Request(url, callback=self.parse, meta={'keyword': keyword})

    def parse(self, response, **kwargs):
        """
        网页解析
        """
        html = response.text
        tweet_ids = re.findall(r'\d+/(.*?)\?refer_flag=1001030103_\'\)">复制微博地址</a>', html)
        for tweet_id in tweet_ids:
            url = f"https://weibo.com/ajax/statuses/show?id={tweet_id}"
            yield Request(url, callback=self.parse_tweet, meta=response.meta)
        next_page = re.search('<a href="(.*?)" class="next">下一页</a>', html)
        if next_page:
            url = "https://s.weibo.com" + next_page.group(1)
            yield Request(url, callback=self.parse, meta=response.meta)

    def parse_tweet(self, response):
        """
        解析推文
        """
        data = json.loads(response.text)
        item = parse_tweet_info(data)
        item['keyword'] = response.meta['keyword']
        url = f"https://weibo.com/ajax/profile/detail?uid={item['user']['_id']}"
        yield Request(url, callback=self.parse_detail, meta={'item': item})

    @staticmethod
    def parse_detail(response):
        """
        解析详细数据
        """
        item = response.meta['item']
        data = json.loads(response.text)['data']
        item['user']['birthday'] = data.get('birthday', '')
        if 'created_at' not in item:
            item['created_at'] = data.get('created_at', '')
        item['user']['desc_text'] = data.get('desc_text', '')
        item['user']['ip_location'] = data.get('ip_location', '')
        item['user']['sunshine_credit'] = data.get('sunshine_credit', {}).get('level', '')
        item['user']['label_desc'] = [label['name'] for label in data.get('label_desc', [])]
        if 'company' in data:
            item['user']['company'] = data['company']
        if 'education' in data:
            item['user']['education'] = data['education']

        if item['isLongText']:
            url = "https://weibo.com/ajax/statuses/longtext?id=" + item['mblogid']
            yield Request(url, callback=parse_long_tweet, meta={'item': item})
        else:
            yield item
