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
        # keywords = ['官老爷', '食税阶级', '利益集团', '垄断阶级', '砖家', '叫兽', '学阀', '公知', '底层屁民',
        #            '特权阶级', '上升通道堵死']
        # keywords = ['殖人', '润人', '美狗', '二鬼子', '带路党', '黄鹅', '假洋鬼子', '慕洋犬', '高华', '洋奴', '恨国党',
        #             '洋大人', '白皮猪', '昂撒', '洋鬼子', '白楠',
        #             '小西巴', '小西八', '偷国人', '小日本', '日本鬼子',
        #             '阿三', '恒河水', '越南猴子',
        #             '鹅国', '鞑子']
        # keywords = ['爽飞', '黑飞', '提控回家', '一键放生', '炸机', '丢星', '射浆', '警用无人机', '无人机',
        #             '无人机偷窥', '无人机闯入禁区', '无人机坠毁', '无人机坠落', '无人机事故', '无人机案件',
        #             '无人机罚款']
        # keywords = ['滴滴赴美上市', '富士康', 'vy偷税', '薇娅偷税', '劣迹艺人', '郑爽张恒', '爽子', '疯爽',
        #             '芈十四考编', '老板娘遛狗威胁市民', '阿里巴巴性侵员工', '吴签', '李某迪', '李易峰嫖娼', '八孩',
        #             '丰县八', '储户红码', '村镇银行储户', '12岁残疾女孩自述被三人强奸',
        #             '陕西孕妇在医院门口等2小时后流产', '中国侨联一副处长被免职', '别墅侵蚀济南泉域保护区', '仝卓作弊',
        #             '你玩得过中建二局吗', '恶意返乡', '实名举报中交一公局高管女子疑轻生', '待死证明',
        #             '吉林货车司机被困高速20多天后死亡', '易烊千玺考编', '易烊千玺小镇做题家', '易烊千玺中考',
        #             '持证人员大增', '敲盆被拘', '张庭夫妇', '福利院老人未死亡被拉走', '人教版插图', '120延误救治大学生',
        #             '深圳国企书记夫人', '陈霄华猥亵', '超生孩子社会调剂', '网红王澄澄炫富', '丁香医生被禁言', '毁麦',
        #             '唐山打人']
        # keywords = ['孟晚舟获释', '农夫山泉气泡水原料', '福岛白桃', '桥本大辉出界', '“加拿大鹅”被处罚',
        #             '“加拿大鹅”双标', '美国联邦通信委员会', '李赛高', '铁笼女',
        #             '境外网络攻击', '美进行冠状病毒实验', '丁丁保卫战', '南京夏日祭', '南京玄奘寺日本',
        #             '佩洛西窜访台湾']
        keywords = ['#滴滴出行被实施网络安全审查#', '#滴滴回应被网络安全审查#', '#滴滴辟谣将国内用户数据交给美国#',
                    '#滴滴出行App下架#',
                    '#河南多地致信本地在郑州富士康工作人员#', '#当地政府已向郑州富士康派驻工作组#',
                    '#全力以赴确保富士康员工顺利安全返乡#', '#富士康称绝对尊重员工个人意愿#',
                    '#富士康厂区未发生重症感染现象#', '#富士康最新声明#',
                    '#薇娅全网被封#', '#薇娅被罚前已与丈夫注销合伙企业#', '#薇娅道歉#', '#薇娅淘宝直播间被封#',
                    '#薇娅的违法事实有哪些#',
                    '#张恒帮郑爽偷逃税被罚3227万元#', '#郑爽道歉#', '#郑爽偷逃税被追缴并处罚款共2.99亿元#',
                    '#郑爽超话被封#', '#郑爽张恒孩子出生证明#', '#郑爽张恒父母录音#', '#中央政法委评郑爽代孕弃养#',
                    '#芈十四 考编#',
                    '#徽州宴回应老板娘遛狗威胁市民#', '#徽州宴老板娘发视频道歉#',
                    '#杭州妇联回应阿里巴巴女员工遭侵害#',
                    '#吴亦凡事件通报详情#', '#北京警方通报吴亦凡事件#', '#吴亦凡涉嫌强奸罪#', '#吴亦凡被刑拘#',
                    '#吴亦凡微博被封#', '#吴亦凡被批捕#',
                    '#朝阳警方通报李云迪嫖娼#', '#李云迪嫖娼被拘#',
                    '#北京警方通报李易峰多次嫖娼#', '#李易峰多次嫖娼被行政拘留#',
                    '#丰县生育八孩女子事件调查处理情况#', '#丰县生育八孩女子事件调查组成立#',
                    '#丰县生育八孩女子事件调查和处理情况#',
                    '#郑州回应多名储户被赋红码#', '#河南纪委监委接到大量赋红码问题举报#',
                    '#郑州通报村镇银行储户被赋红码#',
                    '#西安孕妇流产事件相关责任人被处理#',
                    '#中国侨联一副处长被免职#',
                    '#数千栋违建别墅遍布济南泉域保护区#',
                    '#教育部调查仝卓改往届生身份#', '#仝卓道歉#', '#为仝卓办理虚假转学手续6人被处理#',
                    '#你玩得过中建二局吗#', '#中建二局项目经理就不当言论道歉#',
                    '#恶意返乡是对法律法规的恶意曲解#', '#人民日报评恶意返乡太伤人#',
                    '#实名举报中交一公局高管女子疑轻生#',
                    '#山东老人癌症晚期就医被小区要待死证明#',
                    '#易烊千玺考编#',
                    '#易烊千玺发声#', '#易烊千玺考编三大争议#', '#什么是小镇做题家#',
                    '#警方回应长春一居民煽动敲盆行动被拘#',
                    '#郑州就120延误救治事件成立调查组#', '#郑州120延误救治事件调度员被开除#',
                    '#陈霄华#', '#德云社辞退陈霄华#', '#陈霄华已被刑事拘留#',
                    '#全州超生孩子统一抱走被社会调剂#', '#全州超生孩子被调剂事件已立案#',
                    '#王澄澄#',
                    '#唐山 打人#', '#公安部门正抓捕唐山打人事件嫌疑人#', '#起底唐山打人者累累案底#',
                    '#唐山打人事件由廊坊警方侦办#', '#唐山受伤女子目前伤情稳定#',
                    '#陕西针对铁笼女事件成立调查组#', '#佳县小雨事件处分13名相关责任人#',
                    '#陕西榆林通报佳县小雨事件调查情况#',
                    ]
        for keyword in keywords:
            if len(keyword) > 2 and keyword[0] == '#' and keyword[-1] == '#':
                keywords[keywords.index(keyword)] = '%23' + keyword[1:-1] + '%23'
        start_time = "2021-11-16-0"
        end_time = "2022-11-16-0"
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
