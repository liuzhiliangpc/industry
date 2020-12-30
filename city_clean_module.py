# -*- coding: utf-8 -*-
"""
Created on Thu Nov 12 16:55:26 2020

@author: baixing
"""

from tqdm import tqdm
import os

class CountryClean():
    def __init__(self):
        dir_path = os.path.dirname(os.path.abspath(__file__))
        self.country_file = os.path.join(dir_path, 'dicts\\country.txt')
    def city_collection(self):
        print(self.country_file)
        f = open(self.country_file, 'r', encoding='UTF-8')
        city_c = f.read()
        f.close()
        # 城市
    #    l = []
    #    for p in str(city_c[0]).split('\n'):
    #        l = l + p.split(' ')
    #        l = list(set(l))
    #        l = [c for c in l if len(c) >= 2]
        # 国家
        cl = str(city_c).split('\n')
        cl = [cn for cn in cl if len(cn) >= 2]
        # sum
    #    c_set = l + cl
    #    del(l, cl)
        return cl
    def country_clean(self, data):
        c_list = self.city_collection()
        for c in c_list:
            if str(c) in data:
                data = str(data).replace(str(c), '')
        return data
    
if __name__ == '__main__':
    c = CountryClean()
#    print(c.city_collection())
    print(c.country_clean('中国我爱你'))
    


