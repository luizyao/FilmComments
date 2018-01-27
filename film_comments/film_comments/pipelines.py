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
            
            sqlite_list =[
            '''CREATE TABLE film_comments 
                         (CUS_NAME text NOT NULL,
                         FILM_NAME text NOT NULL,
                         COMMENT text NOT NULL,
                         GRADE real,
                         TIME datetime,
                         SOURCE text,
                         PRIMARY KEY (CUS_NAME, FILM_NAME, COMMENT))''',
            '''CREATE TABLE grade_summary 
                         (FILM_NAME TEXT, 
                         HIGH_RECOMMEND INTEGER NOT NULL DEFAULT 0, 
                         RECOMMEND INTEGER NOT NULL DEFAULT 0, 
                         GENERAL INTEGER NOT NULL DEFAULT 0, 
                         POOR INTEGER NOT NULL DEFAULT 0, 
                         VERY_POOR INTEGER NOT NULL DEFAULT 0, 
                         SUM INTEGER NOT NULL DEFAULT 0, 
                         PRIMARY KEY (FILM_NAME))''',
            '''CREATE TABLE person_summary 
                         (CUS_NAME TEXT, 
                         HIGH_RECOMMEND INTEGER NOT NULL DEFAULT 0, 
                         RECOMMEND INTEGER NOT NULL DEFAULT 0, 
                         GENERAL INTEGER NOT NULL DEFAULT 0, 
                         POOR INTEGER NOT NULL DEFAULT 0, 
                         VERY_POOR INTEGER NOT NULL DEFAULT 0, 
                         SUM INTEGER NOT NULL DEFAULT 0, 
                         PRIMARY KEY (CUS_NAME))''',
            '''CREATE TRIGGER record_grades 
                    AFTER INSERT ON film_comments 
                    BEGIN 
                    INSERT INTO grade_summary (FILM_NAME) SELECT NEW.FILM_NAME 
                        WHERE NOT EXISTS (SELECT * FROM grade_summary WHERE FILM_NAME == NEW.FILM_NAME); 
                    UPDATE grade_summary SET HIGH_RECOMMEND = HIGH_RECOMMEND + 1, SUM = SUM + 1 WHERE FILM_NAME == NEW.FILM_NAME AND NEW.GRADE == '力荐'; 
                    UPDATE grade_summary SET RECOMMEND = RECOMMEND + 1, SUM = SUM + 1 WHERE FILM_NAME == NEW.FILM_NAME AND NEW.GRADE == '推荐'; 
                    UPDATE grade_summary SET GENERAL = GENERAL + 1, SUM = SUM + 1 WHERE FILM_NAME == NEW.FILM_NAME AND NEW.GRADE == '还行'; 
                    UPDATE grade_summary SET POOR = POOR + 1, SUM = SUM + 1 WHERE FILM_NAME == NEW.FILM_NAME AND NEW.GRADE == '较差'; 
                    UPDATE grade_summary SET VERY_POOR = VERY_POOR + 1, SUM = SUM + 1 WHERE FILM_NAME == NEW.FILM_NAME AND NEW.GRADE == '很差'; 
                    END''',
            '''CREATE TRIGGER record_person 
                    AFTER INSERT ON film_comments 
                    BEGIN 
                    INSERT INTO person_summary (CUS_NAME) SELECT NEW.CUS_NAME 
                        WHERE NOT EXISTS (SELECT * FROM person_summary WHERE CUS_NAME == NEW.CUS_NAME); 
                    UPDATE person_summary SET HIGH_RECOMMEND = HIGH_RECOMMEND + 1, SUM = SUM + 1 WHERE CUS_NAME == NEW.CUS_NAME AND NEW.GRADE == '力荐'; 
                    UPDATE person_summary SET RECOMMEND = RECOMMEND + 1, SUM = SUM + 1 WHERE CUS_NAME == NEW.CUS_NAME AND NEW.GRADE == '推荐'; 
                    UPDATE person_summary SET GENERAL = GENERAL + 1, SUM = SUM + 1 WHERE CUS_NAME == NEW.CUS_NAME AND NEW.GRADE == '还行'; 
                    UPDATE person_summary SET POOR = POOR + 1, SUM = SUM + 1 WHERE CUS_NAME == NEW.CUS_NAME AND NEW.GRADE == '较差'; 
                    UPDATE person_summary SET VERY_POOR = VERY_POOR + 1, SUM = SUM + 1 WHERE CUS_NAME == NEW.CUS_NAME AND NEW.GRADE == '很差'; 
                    END''']
            for sql in sqlite_list:
                try:
                    self.cur.execute(sql)
                except sqlite3.Error as ie:
                    logging.info(ie.args[0])
        except sqlite3.Error as e:
            logging.info(e.args[0])

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
