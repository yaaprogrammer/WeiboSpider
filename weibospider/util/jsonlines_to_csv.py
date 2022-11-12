import codecs
from csv import DictWriter
import json
from pathlib import Path

jsonpath = Path('D://miniProjects/WeiboSpider/output/right.jsonl')
csvfile = Path('D://miniProjects/WeiboSpider/output/right.csv')
with open(jsonpath, 'r', encoding='utf-8') as inp, open(csvfile, 'w', newline='', encoding='utf-8-sig') as outp:
    # writer = DictWriter(outp, fieldnames=['Id', 'Filename', 'TimeStamp',
    #                                       'FrameNumber', 'DateTime', 'Confidence',
    #                                       'XMax', 'YMax', 'XMin', 'YMin', 'PeopleCount'])
    writer = DictWriter(outp, fieldnames=['content', 'ip_location', 'birthday', 'keyword'])
    writer.writeheader()
    for line in inp:
        row = json.loads(line)
        content_formatted:str = row['content'].strip()
        content_formatted = content_formatted.replace("\n", "")
        nested_row = {
            'content': content_formatted,
            'ip_location': row['user']['ip_location'][5:],
            'birthday': row['user']['birthday'],
            'keyword': row['keyword']
        }
        writer.writerow(nested_row)
