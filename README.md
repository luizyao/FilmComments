# FilmComments
  Crawl short film comments in douban.com

# Usage
  > douban username and password are required
  ```bash
  scrapy crawl douban -a USER='123' -a PASS='123'
  ```
  or
  ```bash
  scrapy crawl douban -a USER='123' -a PASS='123' -a FILM_NAME='芳华'
  ```

# Comments
  1. Some dependencies are necessary, such as, scrapy, cv2 .etc
  2. For example, you can see the [result.txt](film_comments/result.txt)
