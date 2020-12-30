# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 10:06:26 2020

@author: baixing
"""
import pandas as pd
import jieba
import time
import re
import os
# import threading
from tqdm import tqdm
from apply_muliti_process import multi_city_clean
from apply_muliti_process import multi_city_cleanwy
from apply_muliti_process import multi_find_cross
from utils import change_category
from cut_words_module import CutWords
from clean_wy_words import CleanWyWords
import warnings
import pickle


class DictBuildMethod:
    def __init__(self):
        dir_path = os.path.dirname(os.path.abspath(__file__))
        self.second_unidict_path = os.path.join(dir_path, 'dicts\\second_unidicts')
        self.second_crossdict_path = os.path.join(dir_path, 'dicts\\second_crossdicts')
        self.first_unidict_path = os.path.join(dir_path, 'dicts\\first_unidicts')
        self.first_crossdict_path = os.path.join(dir_path, 'dicts\\first_crossdicts')

    def apply_dictionary_build(self, dt, df_dict, mode):
        print('**************apply_dictionary_build start!**************')
        start = time.clock()
        print('***************地名过滤 可能会有报错****************')
        #地名识别并替换
#        df_dict['cate_dict'] = df_dict.apply(lambda r: multi_city_clean(r.cate_dict, 8), axis=1)
#        df_dict['cross_dict'] = df_dict.apply(lambda r: multi_city_clean(r.cross_dict, 8), axis=1)
        for i in tqdm(range(df_dict.shape[0])):
            # 唯一词典地名替换
            df_p = df_dict['cate_dict'][i]
            df_p = multi_city_clean(df_p, 8)
            #去空值
            df_p = df_p.dropna(subset=['word'])
            #去重
            df_p = df_p.drop_duplicates(['word']).reset_index(drop=True)
            df_p['word'] = df_p.apply(lambda r: 0 if len(r.word) < 2 else r.word, axis=1)
            df_p = df_p[df_p['word'] != 0].reset_index(drop=True)
            if mode == 2:
                print('{} 唯一词典地名过滤Done'.format(df_dict['2_category'][i]))
            else:
                print('{} 唯一词典地名过滤Done'.format(df_dict['first_category'][i]))
            df_p['portion'] = round(df_p['freq'] / df_p['freq'].sum(), 6)
            df_dict['cate_dict'][i] = df_p
        for i in tqdm(range(df_dict.shape[0])):
            # 交叉词典地名替换
            df_pc = df_dict['cross_dict'][i]
            df_pc = multi_city_clean(df_pc, 8)
            #去空值
            df_pc = df_pc.dropna(subset=['word'])
            #去重
            df_pc = df_pc.drop_duplicates(['word']).reset_index(drop=True)
            df_pc['word'] = df_pc.apply(lambda r: 0 if len(r.word) < 2 else r.word, axis=1)
            df_pc = df_pc[df_pc['word'] != 0].reset_index(drop=True)
            if mode == 2:
                print('{} 交叉地名过滤Done'.format(df_dict['2_category'][i]))
            else:
                print('{} 交叉地名过滤Done'.format(df_dict['first_category'][i]))
            df_pc['portion'] = round(df_pc['freq'] / df_pc['freq'].sum(), 6)
            df_dict['cross_dict'][i] = df_pc
        ends = time.clock()
        print('过滤耗时： {}'.format((ends - start)))
        
        # 生成行业唯一词典（txt）
        for i in range(df_dict.shape[0]):
            if mode == 2:
                name = df_dict['2_category'][i]
                f = open(self.second_unidict_path + '/' + str(name) + dt + 'update.txt', 'w')
            else:
                name = df_dict['first_category'][i]
                f = open(self.first_unidict_path + '/' + str(name) + dt + 'update.txt', 'w')
#            f = open(path + '/' + str(name) + dt + 'update.txt', 'w')
            for j in range(df_dict['cate_dict'][i].shape[0]):
                f.write(df_dict['cate_dict'][i]['word'][j] + '\t')
                f.write(str(df_dict['cate_dict'][i]['freq'][j]) + '\t')
                f.write(str(df_dict['cate_dict'][i]['portion'][j]) + '\n')
            print('********{} 唯一词典写入done！********'.format(name))
            f.close()
        # 生成行业交叉词典（txt）
        for i in range(df_dict.shape[0]):
            if mode == 2:
                name = df_dict['2_category'][i]
                f = open(self.second_crossdict_path + '/' + str(name) + '交叉' + dt + 'update.txt', 'w')
            else:
                name = df_dict['first_category'][i]
                f = open(self.first_crossdict_path + '/' + str(name) + '交叉' + dt + 'update.txt', 'w')
#            f = open(pathc + '/' + str(name) + '交叉' + dt + 'update.txt', 'w')
            for j in range(df_dict['cross_dict'][i].shape[0]):
                f.write(df_dict['cross_dict'][i]['word'][j] + '\t')
                f.write(str(df_dict['cross_dict'][i]['freq'][j]) + '\t')
                f.write(str(df_dict['cross_dict'][i]['portion'][j]) + '\t' + str(df_dict['cross_category'][i]) + '\n')
            print('********{} 交叉词典写入done！********'.format(name))
            f.close()
        
        return df_dict  
    
    @staticmethod
    def wyyb_portion_caculate(wyyblis, df_data):
        df_data['wyyb'] = df_data.apply(lambda r: 1 if r['2_category'] in wyyblis else 0, axis=1)
        df_data = df_data[df_data['wyyb'] == 1].reset_index(drop=True)
        # 统计各类目高频词
        # 唯一性高频词
        df_data['cate_dict'] = 0
        df_data['cross_dict'] = 0
        df_data['cate_dict'] = pd.DataFrame(columns=['keyword', 'f', 'catewords_num'])
        df_data['cross_dict'] = pd.DataFrame(columns=['keyword', 'f', 'catewords_num'])
        dfcatelis = []
        dfcrosslis = []
        for i in tqdm(range(len(df_data))):
            df = df_data['df_words'][i]
            df['ind'] = df.apply(lambda r: 1 if r.keyword in df_data['uniqewords'][i] else 0, axis=1)
            df['cind'] = df.apply(lambda r: 1 if r.keyword in df_data['crosswords'][i] else 0, axis=1)
            # 唯一性词
            dfcate = df[df['ind'] == 1].reset_index(drop=True)
            dfcatelis.append(dfcate)
            # 交叉性词
            dfcross = df[df['cind'] == 1].reset_index(drop=True)
            dfcrosslis.append(dfcross)
            del (df, dfcate, dfcross)
        for i in tqdm(range(len(df_data))):
            df1 = dfcatelis[i][['keyword', 'f', 'catewords_num']]
            df_data['cate_dict'][i] = df1
            df2 = dfcrosslis[i][['keyword', 'f', 'catewords_num']]
            df_data['cross_dict'][i] = df2
            del (df1, df2)
        # 排序(按portion排序)
        df_data['cate_dict'] = df_data.apply(lambda r: r.cate_dict.rename(columns={'keyword': 'word', 'f': 'portion', 'catewords_num': 'freq'}),
                                             axis=1)
        df_data['cate_dict'] = df_data.apply(lambda r: r.cate_dict.sort_values(by='portion', axis=0, ascending=False).
                                             reset_index(drop=True), axis=1)
        df_data['cross_dict'] = df_data.apply(
            lambda r: r.cross_dict.rename(columns={'keyword': 'word', 'f': 'portion', 'catewords_num': 'freq'}),
            axis=1)
        df_data['cross_dict'] = df_data.apply(lambda r: r.cross_dict.sort_values(by='portion', axis=0, ascending=False).
                                              reset_index(drop=True), axis=1)
        # 从未出现在词典中的词的默认频率
        new = pd.DataFrame({'word': 'default', 'freq': 0, 'portion': 0}, index=[1])
        for i in tqdm(range(df_data.shape[0])):
            df_data['cate_dict'][i]['portion'] = round(df_data['cate_dict'][i]['portion'], 6)
            df_data['cate_dict'][i] = df_data['cate_dict'][i].append(new, ignore_index=True)
            # 默认频率
            df_data['cate_dict'][i].loc[df_data['cate_dict'][i]['word'] == 'default',
                                        'portion'] = round(1 / (df_data['df_words'][i]['catewords_num'].sum() + 1), 6)
        # 计算每个高频词在其行业交叉词典中的比重
        for i in tqdm(range(df_data.shape[0])):
            df_data['cross_dict'][i]['portion'] = round(df_data['cross_dict'][i]['portion'], 6)
            df_data['cross_dict'][i] = df_data['cross_dict'][i].append(new, ignore_index=True)
            # 默认频率
            df_data['cross_dict'][i].loc[df_data['cross_dict'][i]['word'] == 'default',
                                         'portion'] = round(1 / (df_data['df_words'][i]['catewords_num'].sum() + 1), 6)
        return df_data

    @staticmethod
    def fm_portion_caculate(fmlis, df_data):
        df_data['fm'] = df_data.apply(lambda r: 1 if r['2_category'] in fmlis else 0, axis=1)
        df_data = df_data[df_data['fm'] == 1].reset_index(drop=True)
        # 统计各类目高频词
        # 唯一性高频词
        df_data['cate_dict'] = 0
        df_data['cross_dict'] = 0
        df_data['cate_dict'] = pd.DataFrame(columns=['keyword', 'f'])
        df_data['cross_dict'] = pd.DataFrame(columns=['keyword', 'f'])
        dfcatelis = []
        dfcrosslis = []
        for i in tqdm(range(len(df_data))):
            df = df_data['df_words'][i]
            df['ind'] = df.apply(lambda r: 1 if r.keyword in df_data['uniqewords'][i] else 0, axis=1)
            df['cind'] = df.apply(lambda r: 1 if r.keyword in df_data['crosswords'][i] else 0, axis=1)
            # 唯一性词
            dfcate = df[df['ind'] == 1].reset_index(drop=True)
            dfcatelis.append(dfcate)
            # 交叉性词
            dfcross = df[df['cind'] == 1].reset_index(drop=True)
            dfcrosslis.append(dfcross)
            del (df, dfcate, dfcross)
        for i in tqdm(range(len(df_data))):
            df1 = dfcatelis[i][['keyword', 'f']]
            df_data['cate_dict'][i] = df1
            df2 = dfcrosslis[i][['keyword', 'f']]
            df_data['cross_dict'][i] = df2
            del (df1, df2)
        # 排序(按频词排序)
        df_data['cate_dict'] = df_data.apply(lambda r: r.cate_dict.rename(columns={'keyword': 'word', 'f': 'freq'}),
                                             axis=1)
        df_data['cate_dict'] = df_data.apply(lambda r: r.cate_dict.sort_values(by='freq', axis=0, ascending=False).
                                             reset_index(drop=True), axis=1)
        df_data['cross_dict'] = df_data.apply(
            lambda r: r.cross_dict.rename(columns={'keyword': 'word', 'f': 'freq'}),
            axis=1)
        df_data['cross_dict'] = df_data.apply(lambda r: r.cross_dict.sort_values(by='freq', axis=0, ascending=False).
                                              reset_index(drop=True), axis=1)
        # 从未出现在词典中的词的默认频率
        new = pd.DataFrame({'word': 'default', 'freq': 0, 'portion': 0}, index=[1])
        # 计算每个高频词在其行业唯一词典中的频率
        for i in tqdm(range(df_data.shape[0])):
            df_data['cate_dict'][i]['portion'] = round(df_data['cate_dict'][i]['freq'] /
                                                       df_data['df_words'][i]['f'].sum(), 6)
            df_data['cate_dict'][i] = df_data['cate_dict'][i].append(new, ignore_index=True)
            # 默认频率
            df_data['cate_dict'][i].loc[df_data['cate_dict'][i]['word'] == 'default',
                                        'portion'] = round(1 / (df_data['df_words'][i]['f'].sum() + 1), 6)
        # 计算每个高频词在其行业交叉词典中的比重
        for i in tqdm(range(df_data.shape[0])):
            df_data['cross_dict'][i]['portion'] = round(df_data['cross_dict'][i]['freq'] /
                                                        df_data['df_words'][i]['f'].sum(), 6)
            df_data['cross_dict'][i] = df_data['cross_dict'][i].append(new, ignore_index=True)
            # 默认频率
            df_data['cross_dict'][i].loc[df_data['cross_dict'][i]['word'] == 'default',
                                         'portion'] = round(1 / (df_data['df_words'][i]['f'].sum() + 1), 6)
        return df_data

