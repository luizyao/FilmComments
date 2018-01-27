# encoding=utf-8
from scrapy import Spider
from film_comments.items import FilmCommentsItem 
from scrapy.http import Request, FormRequest 
import requests 
import numpy
import cv2

class DouBanSpider(Spider):
    '''
    Login before crawl douban 
    '''
    name = 'douban'
    allowed_domains = ['douban.com']
    # the start page for all palying film now
    start_urls = ['https://movie.douban.com/cinema/nowplaying/nanjing/']

    headers = {
            "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Encoding":"gzip, deflate",
            "Accept-Language":"zh-CN,zh;q=0.8,en;q=0.6",
            "Connection":"keep-alive",
            "User-Agent":"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.87 Safari/537.36"
            }
   
    def __init__(self, FILM_NAME=None, USER=None, PASS=None, *args, **kwargs):
        super(type(self), self).__init__(*args, **kwargs)
        self.FILM_NAME = FILM_NAME 
        self.USER = USER
        self.PASS = PASS

    def start_requests(self):
        if(not (self.USER and self.PASS)):
            self.logger.error("请输入豆瓣的用户名和密码，因为有些数据只有登陆后才能看到；")
            return
        else:
            yield Request(url='https://accounts.douban.com/login', meta={'cookiejar':1}, callback=self.post_login)

    def post_login(self, response):
        self.logger.info("开始登录豆瓣...")
        if(response.xpath("//img[@id='captcha_image']").extract_first()):
            self.logger.info("登录需要输入图片中的验证码")
            img_url = response.xpath("//img[@id='captcha_image']/@src").extract_first()
            if(img_url): 
                self.logger.info("验证码图片的网络地址是：{}".format(img_url))
                res = requests.get(img_url, stream=True)
                if(res.status_code == 200):
                    with open('验证码.jpg', 'wb') as f:
                        for chunk in res:
                            f.write(chunk)
                    self.logger.info("保存验证码图片到本地目录：/验证码.jpg")
                else:
                    self.logger.error("下载验证码失败")
                    return 
                captcha_id = response.xpath("//input[@name='captcha-id']/@value").extract_first()
                if(captcha_id):
                    img = cv2.imread('验证码.jpg', 0)
                    if(img.any()):
                        cv2.imshow('image', img)
                        cv2.waitKey(0)
                        cv2.destroyAllWindows()
                    solution = input("'请输入图片上的验证码：'")
                    self.logger.info("captcha id是:{}".format(captcha_id))
                    yield FormRequest.from_response(response,
                        meta = {'cookiejar':response.meta['cookiejar']},
                        headers = self.headers,
                        formdata = {
                            'source':'None',
                            'redir':'https://movie.douban.com/',
                            'form_email':self.USER,
                            'form_password':self.PASS,
                            'captcha-solution':solution,
                            'captcha-id':captcha_id,
                            'login':'登录'
                            },
                        callback=self.after_login)
        else:
            yield FormRequest.from_response(response,
                        meta = {'cookiejar':response.meta['cookiejar']},
                        headers = self.headers,
                        formdata = {
                            'source':'None',
                            'redir':'https://movie.douban.com/',
                            'form_email':self.USER,
                            'form_password':self.PASS,
                            'login':'登录'
                            },
                        callback=self.after_login)

    def after_login(self, response):
        if '不匹配' in response.body.decode():
            self.logger.error("帐号和密码不匹配，登陆失败；")
            return
        elif '不正确' in response.body.decode():
            self.logger.error("验证码不正确，登录失败；")
            return
        elif '不存在' in response.body.decode():
            self.logger.error('帐号不存在，登录失败')
            return
        else:
            for start_url in self.start_urls:
                yield Request(url=start_url, 
                        meta={'cookiejar':response.meta['cookiejar']}, 
                        callback=self.parse_first_page)

    def parse_first_page(self, response):
        dic = {}
        for sel in response.xpath("//div[@id='nowplaying']//li[starts-with(@class, 'list-item')]"):
            infoLst = sel.xpath("@id|@data-title").extract()
            if(len(infoLst)>0):
                dic[infoLst[1]] = infoLst[0]
                self.logger.info("Name:{}\tId:{}".format(infoLst[1], infoLst[0]))
        if(len(dic) > 0):
            if(self.FILM_NAME):
                try:
                    yield Request(url="https://movie.douban.com/subject/" + dic[self.FILM_NAME] + "/comments", 
                            meta={'cookiejar':response.meta['cookiejar']}, 
                            callback=self.parse)
                except KeyError as ke:
                    self.logger.error("电影《{}》不在正在上映之列".format(self.FILM_NAME))
                    return
                except Exception as e:
                    self.logger.error("发生错误：{}".format(e.args[0]))
                    return
            else:
                for info in dic.values():
                    yield Request(url="https://movie.douban.com/subject/" + info + "/comments", 
                            meta={'cookiejar':response.meta['cookiejar']}, 
                            callback=self.parse)
        else:
            yield response.request 

    def parse(self, response):
        filmName = ''.join(response.xpath("//div[@id='content']/h1/text()").extract()).split(' ')[0:1]
        for sel in response.xpath("//div[@class='comment-item']/div[@class='comment']"):
            comment = FilmCommentsItem()
            comment['name'] = sel.xpath(".//span[@class='comment-info']/a/text()").extract()
            comment['comment'] = sel.xpath("p/text()").extract()
            comment['grade'] = sel.xpath(".//span[@class='comment-info']/span[2]/@title").extract()
            comment['time'] = sel.xpath(".//span[@class='comment-info']/span[3]/@title").extract()
            comment['film_name'] = filmName 
            yield comment
        nextPage = response.xpath("//div[@id='paginator']/a[@class='next']/@href").extract_first()
        if(nextPage):
            nextUrl = response.request.url.split('?')[0] + ''.join(nextPage)
            self.logger.info("Next search page url is {}".format(nextUrl))
            yield Request(url=nextUrl, 
                    meta = {'cookiejar':response.meta['cookiejar']},
                    callback=self.parse)
