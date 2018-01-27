# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import scrapy
import sqlite3
import logging
from scrapy.exceptions import DropItem 
from datetime import datetime 

class FilmCommentsPipeline(object):
    def open_spider(self, spider):
        try:
            self.conn = None 
            self.conn = sqlite3.connect('/home/luiz/Database/scrapy.db')
            self.cur = self.conn.cursor()

            self.cur.execute('''CREATE TABLE film_comments 
                         (CUS_NAME text NOT NULL,
                         FILM_NAME text NOT NULL,
                         COMMENT text NOT NULL,
                         GRADE real,
                         TIME datetime,
                         SOURCE text,
                         PRIMARY KEY (CUS_NAME, FILM_NAME, COMMENT))''')
        except sqlite3.Error as e:
            logging.error('An error occurred:{}'.format(e.args[0]))

    def process_item(self, item, spider):
        try:
            cus_name = ','.join(item['name'])
            film_name = ','.join(item['film_name']) 
            comment = ','.join(item['comment']).strip()
            grade = ','.join(item['grade'])
            time = datetime.strptime(','.join(item['time']), '%Y-%m-%d %H:%M:%S')
            source = ','.join(spider.allowed_domains)
            
            if grade == '':
                data = (cus_name, film_name, comment, 'NULL', time, source,)
            else:
                try:
                    data = (cus_name, film_name, comment, float(grade), time, source,)
                except:
                    data = (cus_name, film_name, comment, grade, time, source,)

            logging.info('Start insert into database table film_comments with data:{}'.format(data))
            with self.conn:
                self.conn.execute('INSERT INTO film_comments VALUES (?,?,?,?,?,?)', data)
        except sqlite3.Error as e:
            logging.error("An sqlite3 Error Occured:{}".format(e.args[0]))
            raise DropItem('Data Error')
        except Exception as e:
            logging.error("An Error Occured:{}".format(e.args[0]))
            raise DropItem('Data Error')
        else:    
            return item

    def close_spider(self, spider):
        if(self.conn):
            self.conn.close()
