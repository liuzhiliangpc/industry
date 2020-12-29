 # -*- coding: utf-8 -*-
"""
Created on Thu Nov 26 14:51:34 2020

@author: baixing
"""

import pandas as pd
from tqdm import tqdm
import os
from utils import change_category

class GetCategoryDict():
    def __init__(self):
        dir_path = os.path.dirname(os.path.abspath(__file__))
        self.pkl = os.path.join(dir_path, 'dict_pkl')
        self.seconddicts = os.path.join(dir_path, 'dicts//B2B_sencond_unidicts')
        self.firstdicts = os.path.join(dir_path, 'dicts//B2B_first_unidicts')
        self.secondcrossdicts = os.path.join(dir_path, 'dicts//B2B_second_crossdicts')
        self.firstcrossdicts = os.path.join(dir_path, 'dicts//B2B_first_crossdicts')

    # 读取2级行业词典
    def get_2category_dict(self, dt):
        # 创建一个空的dataframe
        pathdir1 = os.listdir(self.seconddicts)
        pathdir2 = os.listdir(self.secondcrossdicts)
        unifile = [file for file in pathdir1]
        croflie = [file for file in pathdir2]
        D = {}
        print(self.seconddicts)
        for u in tqdm(unifile):
            dictr = {}
            # 读取行业唯一词典
            f = open(self.seconddicts + '\\' + u, 'r')
            try:
                while True:
                    line = f.readline().strip()
                    if line == '':
                        break
                    l = [n for n in line.split('\t')]
                    dictr[l[0]] = [int(l[1]), float(l[-1])]
            except:
                pass
            f.close()
            dictr = pd.DataFrame.from_dict(dictr, orient='index',
                                           columns=['freq', 'portion']).reset_index().rename(columns={'index': 'word'})
            D[str(u).replace(dt + 'update.txt', '')] = dictr
            print('********' + str(u).replace(dt + 'update.txt', '') + '唯一Done*******')
            del(dictr)
        df_dict = pd.DataFrame.from_dict(D, orient='index',
                                           columns=['cate_dict']).reset_index().rename(columns={'index': '2_category'})
        C = {}
        CC = {}
        print(self.secondcrossdicts)
        for c in tqdm(croflie):
            cdictr = {}
            # 读取行业交叉词典
            f = open(self.secondcrossdicts + '\\' + c, 'r')
            try:
                while True:
                    line = f.readline().strip()
                    if line == '':
                        break
                    l = [n for n in line.split('\t')]
                    cdictr[l[0]] = [int(l[1]), float(l[2])]
            except:
                pass
            f.close()
            cdictr = pd.DataFrame.from_dict(cdictr, orient='index',
                                           columns=['freq', 'portion']).reset_index().rename(columns={'index': 'word'})
            C[str(c).replace('交叉' + dt + 'update.txt', '')] = cdictr
            CC[str(c).replace('交叉' + dt + 'update.txt', '')] = l[-1]
            print('********' + str(c).replace('交叉' + dt + 'update.txt', '') + '交叉Done*******')
            del(cdictr)
        df_dict1 = pd.DataFrame.from_dict(C, orient='index',
                                           columns=['cross_dict']).reset_index().rename(columns={'index': '2_category'})
        df_dict2 = pd.DataFrame.from_dict(CC, orient='index',
                                           columns=['cross_category']).reset_index().rename(columns={'index': '2_category'})
        df_dict2['cross_category1'] = df_dict2.apply(lambda r: r.cross_category.split(','), axis=1)
        df_dict2['cross_category1'] = df_dict2.apply(lambda r: [s.replace("'", '') for s in r.cross_category1], axis=1)
        df_dict2['cross_category1'] = df_dict2.apply(lambda r: [s.replace("[", '') for s in r.cross_category1], axis=1)
        df_dict2['cross_category1'] = df_dict2.apply(lambda r: [s.replace("]", '') for s in r.cross_category1], axis=1)
    
        df_dict_sum = pd.merge(df_dict[["2_category", "cate_dict"]], df_dict1, on='2_category', how='left').reset_index(drop=True)
        df_dict_sum = pd.merge(df_dict_sum, df_dict2[['2_category', 'cross_category1']], on='2_category', how='left').rename(columns={'cross_category1': 'cross_category'}).reset_index(drop=True)
        del(df_dict1, df_dict2)
        df_dict = df_dict_sum
        df_dict['search_dict'] = 0
        for i in tqdm(range(df_dict.shape[0])):
            df_dict['search_dict'][i] = list(set(list(df_dict['cate_dict'][i]['word']) + [df_dict['2_category'][i]]))
        
        df_dict['2_category'] = df_dict.apply(lambda r: change_category(r['2_category']), axis=1)
        print('*************全部词典完毕****************')
        try:
            df_dict.to_pickle(self.pkl + '//' + 'df_dict.pkl')
        except:
            os.remove(self.pkl + '//' + 'df_dict.pkl')
            df_dict.to_pickle(self.pkl + '//' + 'df_dict.pkl')
        return df_dict
    # 读取1级行业词典
    def get_first_category_dict(self, dt):
        # 创建一个空的dataframe
        pathdir1 = os.listdir(self.firstdicts)
        pathdir2 = os.listdir(self.firstcrossdicts)
        unifile = [file for file in pathdir1]
        croflie = [file for file in pathdir2]
        D = {}
        print(self.firstdicts)
        for u in tqdm(unifile):
            dictr = {}
            # 读取行业唯一词典
            f = open(self.firstdicts + '\\' + u, 'r')
            try:
                while True:
                    line = f.readline().strip()
                    if line == '':
                        break
                    l = [n for n in line.split('\t')]
                    dictr[l[0]] = [int(l[1]), float(l[-1])]
            except:
                pass
            f.close()
            dictr = pd.DataFrame.from_dict(dictr, orient='index',
                                           columns=['freq', 'portion']).reset_index().rename(columns={'index': 'word'})
            D[str(u).replace(dt + 'update.txt', '')] = dictr
            print('********' + str(u).replace(dt + 'update.txt', '') + '唯一Done*******')
            del(dictr)
        df_dict = pd.DataFrame.from_dict(D, orient='index',
                                           columns=['cate_dict']).reset_index().rename(columns={'index': 'first_category'})
        C = {}
        CC = {}
        print(self.firstcrossdicts)
        for c in tqdm(croflie):
            cdictr = {}
            # 读取行业交叉词典
            f = open(self.firstcrossdicts + '\\' + c, 'r')
            try:
                while True:
                    line = f.readline().strip()
                    if line == '':
                        break
                    l = [n for n in line.split('\t')]
                    cdictr[l[0]] = [int(l[1]), float(l[2])]
            except:
                pass
            f.close()
            cdictr = pd.DataFrame.from_dict(cdictr, orient='index',
                                           columns=['freq', 'portion']).reset_index().rename(columns={'index': 'word'})
            C[str(c).replace('交叉' + dt + 'update.txt', '')] = cdictr
            CC[str(c).replace('交叉' + dt + 'update.txt', '')] = l[-1]
            print('********' + str(c).replace('交叉' + dt + 'update.txt', '') + '交叉Done*******')
            del(cdictr)
        df_dict1 = pd.DataFrame.from_dict(C, orient='index',
                                           columns=['cross_dict']).reset_index().rename(columns={'index': 'first_category'})
        df_dict2 = pd.DataFrame.from_dict(CC, orient='index',
                                           columns=['cross_category']).reset_index().rename(columns={'index': 'first_category'})
        df_dict2['cross_category1'] = df_dict2.apply(lambda r: r.cross_category.split(','), axis=1)
        df_dict2['cross_category1'] = df_dict2.apply(lambda r: [s.replace("'", '') for s in r.cross_category1], axis=1)
        df_dict2['cross_category1'] = df_dict2.apply(lambda r: [s.replace("[", '') for s in r.cross_category1], axis=1)
        df_dict2['cross_category1'] = df_dict2.apply(lambda r: [s.replace("]", '') for s in r.cross_category1], axis=1)
    
        df_dict_sum = pd.merge(df_dict[["first_category", "cate_dict"]], df_dict1, on='first_category', how='left').reset_index(drop=True)
        df_dict_sum = pd.merge(df_dict_sum, df_dict2[['first_category', 'cross_category1']], on='first_category', how='left').rename(columns={'cross_category1': 'cross_category'}).reset_index(drop=True)
        del(df_dict1, df_dict2)
        df_dict = df_dict_sum
        df_dict['search_dict'] = 0
        for i in tqdm(range(df_dict.shape[0])):
            df_dict['search_dict'][i] = list(set(list(df_dict['cate_dict'][i]['word']) + [df_dict['first_category'][i]]))
        
        df_dict['first_category'] = df_dict.apply(lambda r: change_category(r['first_category']), axis=1)
        print('*************全部词典完毕****************')
        try:
            df_dict.to_pickle(self.pkl + '//' + 'df_first_dict.pkl')
        except:
            os.remove(self.pkl + '//' + 'df_first_dict.pkl')
            df_dict.to_pickle(self.pkl + '//' + 'df_first_dict.pkl')
        return df_dict


if __name__ == '__main__':
    g = GetCategoryDict()
    df1 = g.get_2category_dict(dt)