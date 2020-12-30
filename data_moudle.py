# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 09:20:47 2020

@author: baixing

"""
import pandas as pd
import datetime
#from sqlalchemy import *
#from sqlalchemy.engine import create_engine
#from sqlalchemy.schema import *
import sys
#from pyhive import hive
from pyhive import presto
import importlib
importlib.reload(sys)

###定义函数
#读写SQL函数
def sqlexe(sql,type):
    try:
        sql_plus = sql.replace(')',') ').replace(') \'',')\'')
        print('*************执行sql**************')
#        print ('执行的sql语句为 : {}{}'.format('\n',sql_plus))#加括号
        if type =='presto':#封装成df
            cursor = presto.connect('localhost',port=9098,username='SecureUser').cursor()
            cursor.execute(sql_plus)
            data = cursor.fetchall()    
            columnDes = cursor.description #获取连接对象的描述信息    
            columnNames = [columnDes[i][0] for i in range(len(columnDes))]    
            df = pd.DataFrame([list(i) for i in data],columns=columnNames)
            return df
    except Exception as e:
        print (e)#加括号
        sys.exit()#改exit(1)为sys.exit()
#获取日期
def get_time_str(n=30): 
    #默认n=30
    today=datetime.date.today() 
    oneday=datetime.timedelta(days=n) 
    past=today-oneday  
    end_time = today.strftime("%Y-%m-%d")
    start_time = past.strftime("%Y-%m-%d")
    return start_time,end_time

class Data:
    def __init__(self, mode, amt, db):
          self.Mode = mode
          self.Amt = amt
          self.Db = db
    #获取类目
    #db='presto'
    def get_category(self):
        sqlquery = '''select category
                                from pg.ods.dbank_keyword_clean_20200512
                                where length(category) >= 2
                                group by category'''
        df = sqlexe(sqlquery, self.Db)
        return df
    #获取标王数据
    def get_bw_data(self, catelist):
        sql_plus = '''where second_category in {}'''.format(catelist) if self.Mode == 'test' else ''
        sql_query = f'''--筛选标王测试数据，生产一级、二级类目
                        select *
                        from
                        (
                        select T.word
                            ,T.cities
                            ,T.landing_page
                            ,T.creative_title
                            ,T.creative_content
                            ,T.cpc_price
                            ,T.budget
                            ,T.category
                            ,T.categoryset
                            ,Replace(T.f_category,'*','')  as top_category
                            ,Replace(T.s_category,'*','')  as second_category
                        from
                        (
                        select t.*
                            ,t1.category_names  as categoryset
                            ,substr(regexp_replace(t1.category_names,',','***************'), 1, 15) as f_category
                            ,substr(regexp_replace(t1.category_names,',','***************'), -15) as s_category
                        from pg.baselayer.s_phoenixs_tb_promote t
                        left join
                        (
                            select category_id
                                ,category_names
                            from
                            (
                            select a.*
                                ,row_number()over(partition by category_id
                                                    order by length(category_names) desc)   as rn
                            from hive.ods.babel_topic_all a --主站数据表
                            )
                            where rn = 1
                        )t1 -- category拼音对应类目
                        on t1.category_id = t.category
                        where length(t.category) >= 2
                        )T
                        ){sql_plus}
                        limit 100000'''
        df = sqlexe(sql_query, self.Db)
        if self.Mode == 'test':
            print('测试数据获取Done!')
        else:
            print('标王数据获取Done!')
        return df
    #获取训练数据
    # todo拉数据加随机数
    def get_train_data(self, category):
        amt = self.Amt
        sql_query = f'''
            select keyword
                ,refer
                ,city
                ,top_category
                ,category
                ,meta
                ,dt
            from pg.ods.dbank_keyword_clean_20200512
            where category = '{category}'
            limit {amt}
        '''
        df = sqlexe(sql_query, self.Db)
        print('{} 训练数据获取 {} Done!'.format(category, self.Amt))
        return df


if __name__ == '__main__':
    data = Data('full', '1', 10, db='presto')
    df_ = data.get_bw_data()