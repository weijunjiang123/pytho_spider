import json
import requests
from bs4 import BeautifulSoup
import mysql.connector
import os
import sys

class DoubanMovie:
    def __init__(self) -> None:
         self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36',
            'Referer': 'https://movie.douban.com/top250'
            }
         for i in range(11):
            self.url = "https://movie.douban.com/top250?start="+ str(i*25) +"&filter="
            html = self.get_html()
            data = self.get_data(html)
            self.connect_sql_server(data, i)
        
    def get_html(self) -> str:
        try:
            response = requests.get(self.url, headers=self.headers)
            if response.status_code == 200:
                return response.text
        except:
            return None
        
    def get_data(self, html: str) -> list:
        """将爬取的html数据处理

        Args:
            html (str): response的html

        Returns:
            list: 返回电影信息 [图片链接，标题，导演，类型，评分，信息]
        """
        soup = BeautifulSoup(html)
        article = soup.find(class_='grid_view')
        movies =  article.find_all(class_='item')
        movie_list = []
        for item in movies:
            picture = item.find(class_='pic').find('img').get('src')
            title = item.find(class_='pic').find('img').get('alt')
            author_and_type = item.find(class_='bd').find('p').text
            author_and_type = author_and_type.replace("\xa0", " ")
            author_and_type = author_and_type.split('\n')
            author = author_and_type[1]
            author = author.replace(' ','')
            type = author_and_type[2]
            type = type.replace(' ','')
            score = item.find(class_='rating_num').string
            try:
                info = item.find(class_='inq').string 
            except:
                info = "无内容"  
            one_movie = (str(picture), str(title), str(author), str(type), str(score), str(info))
            movie_list.append(one_movie)
        return movie_list
        
        
    def connect_sql_server(self, data, num):
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        sys.path.insert(0, BASE_DIR)
        with open('C:\\Users\\13662\\Desktop\\python项目\\python爬虫\\豆瓣电影\\豆瓣电影_数据库\\config.json', 'r') as f:
            config = json.load(f)
            mysql_data = config['mysql']
            self.host = mysql_data['host']
            self.user = mysql_data['user']
            self._password = mysql_data['password']
            self.port = mysql_data['port']
            try:
                conn = mysql.connector.connect(host=self.host, port=self.port, user=self.user, password=self._password, database = 'test')
                print("连接数据库成功")
            except :
                print("连接失败")
            cursor = conn.cursor() 
            # 创建表
            if num == 0:
                sql_create = '''CREATE TABLE douban (
                                    picture varchar(200) not null,
                                    title varchar(100) not null,
                                    author varchar(100) null,
                                    types varchar(100) null,
                                    score float(2,1) null,
                                    info varchar(200) null
                                    );'''
                cursor.execute(sql_create)
                print('创建表成功')
            sql_insert = '''INSERT INTO douban (picture, title, author, types, score, info) VALUES (%s, %s, %s, %s, %s, %s)'''
            cursor.executemany(sql_insert, data)
            conn.commit()
            print(f"已经保存第{num*25}部电影")

            
            
if __name__ == "__main__":
    db = DoubanMovie()          