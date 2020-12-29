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
#        self.path = r'C:\Users\baixing\Desktop\BX\dicts1'
#        self.pathc = r'C:\Users\baixing\Desktop\BX\cross_dicts'

    # @staticmethod
    # def find_cross_category(cross_ls, df):
    #     df['cross_ind'] = 0
    #     for word in cross_ls:
    #         df['cross_ind'] = df.apply(lambda r: 1 if word in r.wordslist else r.cross_ind, axis=1)
    #     return list(df.loc[df['cross_ind'] == 1, '2_category'])
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

#    def apply_dictionary_buildold(self,
#                               df_dict,
#                               dt,
#                               wyyblis,
#                               fmlis):
#        print('**************apply_dictionary_build start!**************')
#        start = time.clock()
#        # 行业词典
#        wordsdict2_search = {}
##        df_t = pd.read_pickle(r'C:\Users\baixing\Desktop\BX\re\dft.pkl')
#        print('***************地名过滤 可能会有报错****************')
#        #地名识别并替换
#        
#        for i in range(df_dict.shape[0]):
#            # 唯一词典地名替换
#            df_p = multi_city_clean(df_dict['cate_dict'][i], 8)
#            #去空值
#            df_p = df_p.dropna(subset=['word'])
#            #去重
#            df_p = df_p.drop_duplicates(['word']).reset_index(drop=True)
#            df_p['word'] = df_p.apply(lambda r: 0 if len(r.word) < 2 else r.word, axis=1)
#            df_p = df_p[df_p['word'] != 0].reset_index(drop=True)
#            print('{} 唯一词典地名过滤Done'.format(df_dict['2_category'][i]))
#            df_p['portion'] = round(df_p['freq'] / df_p['freq'].sum(), 6)
#            df_dict['cate_dict'][i] = df_p
#            # 唯一词典地名替换
#            df_pc = multi_city_clean(df_dict['cross_dict'][i], 8)
#            #去空值
#            df_pc = df_pc.dropna(subset=['word'])
#            #去重
#            df_pc = df_pc.drop_duplicates(['word']).reset_index(drop=True)
#            df_pc['word'] = df_pc.apply(lambda r: 0 if len(r.word) < 2 else r.word, axis=1)
#            df_pc = df_pc[df_pc['word'] != 0].reset_index(drop=True)
#            print('{} 交叉地名过滤Done'.format(df_dict['2_category'][i]))
#            df_pc['portion'] = round(df_pc['freq'] / df_pc['freq'].sum(), 6)
#            df_dict['cross_dict'][i] = df_pc
#        
#        
#        
#        
#        
#        df_data = df_t.copy()
#        df_data['okeyword'] = df_data['keyword']
#        #多进程过滤地名！
#        df_data = multi_city_clean(df_data, 8)
#        df_data = df_data.dropna(subset=['keyword']).reset_index(drop=True)
#        df_data['key_extr'] = df_data.apply(lambda r: CutWords().fs(r.keyword, '1'), axis=1)
#        print('****************地名过滤Done! ****************')
#        # df_data.to_pickle('df_t_clean.pkl')
#        # 建立词典0
#        start = time.clock()
#        for key, group in tqdm(df_data.groupby('category')):
#            group = group.reset_index(drop=True)
#            # 合并所有分词
#            l = []
#            for j in list(group['key_extr']):
#                l = l + j
#            ol = list(set(l))  # 去重
#            wdict2s = {}
#            # 统计词频
#            for s in ol:
#                # 单个字以及数字或字母一般无法判断行业
#                if len(s) >= 2 and s[0] not in [chr(i).upper() for i in range(97, 123)] \
#                        + [chr(i) for i in range(97, 123)] + ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']:
#                    wdict2s[s] = l.count(s)
#            wordsdict2_search[key] = [wdict2s, list(wdict2s.keys())]
#            print(key + 'Done!')
#        # dict转dataframe
#        df2_sm = pd.DataFrame.from_dict(wordsdict2_search, orient='index',
#                                        columns=['df_words', 'wordslist']).reset_index().\
#            rename(columns={'index': '2_category'})
#        df2_sm['df_words'] = df2_sm.apply(lambda r: pd.DataFrame.from_dict(r.df_words, orient='index', columns=['f']).
#                                          reset_index().
#                                          rename(columns={'index': 'keyword'}), axis=1)
#        print('****************凤鸣初步词典0 Done!****************')
#        # df2_sm.to_pickle('df2fm.pkl')
#        # 5118词库清洗
#        df_c = CleanWyWords().get_5118words_cleaned()
#        # df_c.to_pickle('dfc_5118.pkl')
#        for i in tqdm(range(len(df_c))):
#            dfss = df_c['df_words'][i]
#            if dfss.shape[0] != 0:
#                warnings.filterwarnings('ignore')
#                dfss = multi_city_cleanwy(dfss, 8)
#            #去空值
#            dfss = dfss.dropna(subset=['keyword'])
#            #去重
#            dfss = dfss.drop_duplicates(['keyword']).reset_index(drop=True)
#            dfss['f'] = round(dfss['catewords_num'] / dfss['catewords_num'].sum(), 6)
#            df_c['df_words'][i] = dfss
#            print(str(df_c['2_category'][i]) + 'Done!')
#        # df_c.to_pickle('dfc_5118_clean.pkl')
#        df_c = pd.read_pickle('dfc_5118_clean.pkl')
#        df_c['wyyb'] = df_c.apply(lambda r: 1 if r['2_category'] in [change_category(c) for c in wyyblis] else 0, axis=1)
#        df_c = df_c[df_c['wyyb'] == 1].reset_index(drop=True)
#        del(df_c['wyyb'])
#        df_c['wordslist'] = df_c.apply(lambda r: list(r.df_words['keyword']), axis=1)
#        print('***********5118词典done!***********')
#        # 汇总
#        df2_sm = pd.concat([df_c, df2_sm]).reset_index(drop=True)
#        # 行业唯一词典/交叉词典的建立
#        df2_sm = df2_sm.reindex(columns=list(df2_sm.columns) + ['uniqewords', 'crosswords',
#                                                                'cross_category'], fill_value=0)
#        start = time.clock()
#        for i in tqdm(range(df2_sm.shape[0])):
#            l = []
#            for p1 in range(0, i):
#                l = l + df2_sm['wordslist'][p1]
#            for p2 in range(i + 1, df2_sm.shape[0]):
#                l = l + df2_sm['wordslist'][p2]
#            df2_sm['uniqewords'][i] = list(set(df2_sm['wordslist'][i]).
#                                           difference(set(l)))
#            df2_sm['crosswords'][i] = list(set(df2_sm['wordslist'][i]).
#                                           intersection(set(l)))
#        end = time.clock()
#        print(str(end - start)) # 6s
#        # df2_sm.to_pickle('df2smmm.pkl')
#        # df2_sm = pd.read_pickle('df2smmm.pkl')
#        start = time.clock()
#        # df2_sm['cross_category'] = df2_sm.apply(lambda r: self.find_cross_category(r.crosswords, df2_sm), axis=1)
#        df2_sm['cross_category'] = df2_sm.apply(lambda r: multi_find_cross(r['2_category'], r.crosswords, df2_sm, 8, '2_category'), axis=1)
#
#        end = time.clock()
#        print(str(end - start)) # 66s
#        df2_sm['total_words'] = df2_sm.apply(lambda r: len(r.wordslist), axis=1)
#        df2_sm['uniwords_num'] = df2_sm.apply(lambda r: len(r.uniqewords), axis=1)
#        df2_sm['cross_num'] = df2_sm.apply(lambda r: len(r.crosswords), axis=1)
#        # df2_sm.to_pickle('df2_sm_cross.pkl')
#        print('****************唯一词典/交叉词典建立 Done!****************')
#        df2_sm = df2_sm[['2_category', 'df_words', 'uniqewords', 'crosswords', 'cross_category',
#                         'uniwords_num', 'cross_num']]
#        df1 = self.wyyb_portion_caculate([change_category(c) for c in wyyblis], df2_sm)
##        df1 = wyyb_portion_caculate([change_category(c) for c in wyyblis], df2_sm)
##         df1.to_pickle('df1.pkl')
#        df2 = self.fm_portion_caculate(fmlis, df2_sm)
##        df2 = fm_portion_caculate(fmlis, df2_sm)
##         df2.to_pickle('df2.pkl')
#        df1 = df1[['2_category', 'df_words', 'uniqewords', 'crosswords', 'cross_category',
#       'uniwords_num', 'cross_num', 'cate_dict', 'cross_dict']]
#        df2 = df2[['2_category', 'df_words', 'uniqewords', 'crosswords', 'cross_category',
#                   'uniwords_num', 'cross_num', 'cate_dict', 'cross_dict']]
#        df_sum = pd.concat([df1, df2]).reset_index(drop=True)
#        # df_sum.to_pickle('dfsum.pkl')
#        # 将行业自身及其分词加入词典
#        df_sum['search_dict'] = 0
#        for i in range(df_sum.shape[0]):
#            df_sum['search_dict'][i] = list(df_sum['cate_dict'][i]['word']) + [df_sum['2_category'][i]] + \
#                                       jieba.lcut_for_search(df_sum['2_category'][i])
#        df_sum['words_size'] = df_sum.apply(lambda r: len(r.cate_dict), axis=1)
#        print('****************ALL Done!****************')
#        df_sum.to_pickle('dfsumalldone.pkl')
#        start = time.clock()
#        # 生成行业唯一词典（txt）
#        for i in range(df_sum.shape[0]):
#            name = df_sum['2_category'][i]
##            f = open(path + '/' + str(name) + dt + 'update.txt', 'w')
#            f = open(self.path + '/' + str(name) + dt + 'update.txt', 'w')
#            for j in range(df_sum['cate_dict'][i].shape[0]):
#                f.write(df_sum['cate_dict'][i]['word'][j] + '\t')
#                f.write(str(df_sum['cate_dict'][i]['freq'][j]) + '\t')
#                f.write(str(df_sum['cate_dict'][i]['portion'][j]) + '\n')
#            print('********{} 唯一词典写入done！********'.format(name))
#            f.close()
#        # 生成行业交叉词典（txt）
#        for i in range(df_sum.shape[0]):
#            name = df_sum['2_category'][i]
##            f = open(pathc + '/' + str(name) + '交叉' + dt + 'update.txt', 'w')
#            f = open(self.pathc + '/' + str(name) + '交叉' + dt + 'update.txt', 'w')
#            for j in range(df_sum['cross_dict'][i].shape[0]):
#                f.write(df_sum['cross_dict'][i]['word'][j] + '\t')
#                f.write(str(df_sum['cross_dict'][i]['freq'][j]) + '\t')
#                f.write(str(df_sum['cross_dict'][i]['portion'][j]) + '\t' + str(df_sum['cross_category'][i]) + '\n')
#            print('********{} 交叉词典写入done！********'.format(name))
#            f.close()
#        end = time.clock()
#        print("******** 词典建立耗时：{}s ********".format((end - start)))
#        return df_sum

if __name__ == '__main__':
    df_dicts = DictBuildMethod().apply_dictionary_build(dt, df_dict, 2)
#    ls1 = ['外语培训',
# '农林牧副渔',
# '网络安防',
# '招商加盟',
# '早教',
# '翻译服务',
# '汽配',
# '工业设备',
# '仪器仪表',
# '猫猫',
# '电子元器件',
# '家居家纺',
# '驾校',
# '设计培训',
# '线缆照明',
# '狗狗',
# '宠物服务配种',
# '橡塑涂料',
# '学历教育',
# '化工能源',
# '水暖电工',
# '电脑培训',
# '五金配件',
# '冶金矿产',
# '职业技能']
#    ls2 = ['维修保养',
# '管道维修',
# '出国劳务',
# '公司注册',
# '货运物流',
# '租车服务',
# '白事服务',
# '高价收车',
# '家电维修',
# '电脑维修',
# '房屋维修',
# '设备租赁',
# '开锁修锁',
# '物品回收',
# '生活配送',
# '保洁清洗',
# '搬家服务',
# '休闲娱乐',
# '建材服务']
#    df = d.apply_dictionary_build('20201203', ls1, ls2)

