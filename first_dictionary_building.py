# -*- coding: utf-8 -*-
"""
Created on Mon Dec  7 11:23:04 2020

@author: baixing
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 10:06:26 2020

@author: baixing
"""
import pandas as pd
import jieba
import time
import re
# import threading
from tqdm import tqdm
from apply_muliti_process import multi_city_clean
from apply_muliti_process import multi_city_cleanwy
from apply_muliti_process import multi_find_cross
from apply_muliti_process import multi_cal_freq
from cut_words_module import CutWords
from clean_wy_words import CleanWyWords
from utils import change_category
import warnings
import pickle


class DictBuildMethod:
    def __init__(self):
        self.pathf = r'C:\Users\baixing\Desktop\BX\dictf'
        self.pathcf = r'C:\Users\baixing\Desktop\BX\cross_dictf'
    def apply_firstdictionary_build(self, df_first):
        print('**************apply_dictionary_build start!**************')
        start = time.clock()
        print('***************地名过滤 可能会有报错****************')
        #地名识别并替换
        #多进程过滤地名！
        df_data = multi_city_clean(df_data, 8)
        df_data = df_data.dropna(subset=['keyword']).reset_index(drop=True)
        df_data['key_extr'] = df_data.apply(lambda r: CutWords().fs(r.keyword, '1'), axis=1)
        print('****************地名过滤Done! ****************')
        
        start = time.clock()
        # 生成行业唯一词典（txt）
        for i in range(df_first.shape[0]):
            name = df_first['first_category'][i]
            f = open(pathf + '/' + str(name) + dt + 'update.txt', 'w')
#            f = open(self.pathf + '/' + str(name) + dt + 'update.txt', 'w')
            for j in range(df_first['cate_dict'][i].shape[0]):
                f.write(df_first['cate_dict'][i]['keyword'][j] + '\t')
                f.write(str(df_first['cate_dict'][i]['freq'][j]) + '\t')
                f.write(str(df_first['cate_dict'][i]['portion'][j]) + '\n')
            print('********{} 唯一词典写入done！********'.format(name))
            f.close()
        # 生成行业交叉词典（txt）
        for i in range(df_first.shape[0]):
            name = df_first['first_category'][i]
            f = open(pathcf + '/' + str(name) + '交叉' + dt + 'update.txt', 'w')
#            f = open(self.pathcf + '/' + str(name) + '交叉' + dt + 'update.txt', 'w')
            for j in range(df_first['cross_dict'][i].shape[0]):
                f.write(df_first['cross_dict'][i]['keyword'][j] + '\t')
                f.write(str(df_first['cross_dict'][i]['freq'][j]) + '\t')
                f.write(str(df_first['cross_dict'][i]['portion'][j]) + '\t' + str(df_first['cross_category'][i]) + '\n')
            print('********{} 交叉词典写入done！********'.format(name))
            f.close()
        end = time.clock()
        print("******** 词典建立耗时：{}s ********".format((end - start)))
        return df_first

    def apply_dictionary_buildold(self,
                               dt,
                               flist,
                               wyyblis,
                               fmlis):
        print('**************apply_dictionary_build start!**************')
        start = time.clock()
        # 行业词典
        wordsdict2_search = {}
        # df_v.to_csv("verify_data.csv", encoding = 'utf-8-sig')
        df_t = pd.read_pickle(r'C:\Users\baixing\Desktop\BX\re\dft.pkl')
        print('***************地名过滤 可能会有报错****************')
        #地名识别并替换
        df_data = df_t.loc[df_t['category'] == '建材服务', :].reset_index(drop=True)
        df_data['okeyword'] = df_data['keyword']
        #多进程过滤地名！
        df_data = df_data.rename(columns={'keyword': 'word'})
        df_data = multi_city_clean(df_data, 8)
        df_data = df_data.rename(columns={'word': 'keyword'})
        df_data = df_data.dropna(subset=['keyword']).reset_index(drop=True)
        df_data['key_extr'] = df_data.apply(lambda r: CutWords().fs(r.keyword, '1'), axis=1)
        print('****************地名过滤Done! ****************')
        # df_data.to_pickle('df_t_clean.pkl')
        df_data = pd.read_pickle(r'C:\Users\baixing\Desktop\BX\re\df_t_clean.pkl')
        # 建立词典0
        for key, group in tqdm(df_data.groupby('category')):
            group = group.reset_index(drop=True)
            # 合并所有分词
            l = []
            for j in list(group['key_extr']):
                l = l + j
            ol = list(set(l))  # 去重
            wdict2s = {}
            # 统计词频
            for s in ol:
                # 单个字以及数字或字母一般无法判断行业
                if len(s) >= 2 and s[0] not in [chr(i).upper() for i in range(97, 123)] \
                        + [chr(i) for i in range(97, 123)] + ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']:
                    wdict2s[s] = l.count(s)
            wordsdict2_search[key] = [wdict2s, list(wdict2s.keys())]
            print(key + 'Done!')
        # dict转dataframe
        df2_ = pd.DataFrame.from_dict(wordsdict2_search, orient='index',
                                        columns=['df_words', 'wordslist']).reset_index().\
            rename(columns={'index': '2_category'})
        df2_['df_words'] = df2_.apply(lambda r: pd.DataFrame.from_dict(r.df_words, orient='index', columns=['f']).
                                          reset_index().
                                          rename(columns={'index': 'keyword'}), axis=1)     
        df2_['first_category'] = '能源材料'

        print('****************凤鸣初步一级词典0 Done!****************')
        # df2_sm.to_pickle('df2fm.pkl')
        # 5118词库清洗
        df_c = CleanWyWords().get_5118words_cleaned()
        # 清洗
        for i in tqdm(range(len(df_c))):
            dfss = df_c['df_words'][i]
            if dfss.shape[0] != 0:
                warnings.filterwarnings('ignore')
                dfss = dfss.rename(columns={'keyword': 'word'})
                dfss = multi_city_cleanwy(dfss, 8)
                dfss = dfss.rename(columns={'word': 'keyword'})
            #去空值
            dfss = dfss.dropna(subset=['keyword'])
            #去重
            dfss = dfss.drop_duplicates(['keyword']).reset_index(drop=True)
            df_c['df_words'][i] = dfss
            print(str(df_c['2_category'][i]) + 'Done!')
        # df_c.to_pickle('dfc_5118_clean.pkl')
        # 校正2级行业名称
        df_c['2_category'] = df_c.apply(lambda r: change_category(r['2_category']), axis=1)
        # 挑选出凤鸣中没有的2级类目
        df_c['wyyb'] = df_c.apply(lambda r: 1 if r['2_category'] in [change_category(c) for c in wyyblis] else 0, axis=1)
        df_c = df_c[df_c['wyyb'] == 1].reset_index(drop=True)
        del(df_c['wyyb'])
        # 生成关键词列表以及相应一级行业
        df_c['wordslist'] = df_c.apply(lambda r: list(r.df_words['keyword']), axis=1)
        df_c['first_category'] = df_c.apply(lambda r:  [c for c, ls in flist.items() if r['2_category'] in ls][-1], axis=1)
        # 关键词dataframe只保留 word 和 f 两列
        df_c['df_words'] = df_c.apply(lambda r: r.df_words.rename(columns={'catewords_num': 'f'}), axis=1)
        df_c['df_words'] = df_c.apply(lambda r: r.df_words.drop(columns=['longwords_num']), axis=1)
        print('***********5118词典done!***********')
        # 5118 和 凤鸣 汇总 得到总dataframe-df_first
        df2_sm = pd.concat([df2_, df_c]).reset_index(drop=True)
        dff = pd.concat(list(df2_sm.loc[df2_sm['2_category'] == '建材服务', 'df_words'])).reset_index(drop=True)
        df2_sm['df_words'][0] = dff
        df2_sm['df_words'][11] = dff
        df2_sm = df2_sm.drop_duplicates(['2_category']).reset_index(drop=True)
        del(dff)
        first_dict = {}
        for key, group in tqdm(df2_sm.groupby('first_category')):
            group = group.reset_index(drop=True)
            # 合并所有分词
            l = []
            for j in list(group['wordslist']):
                l = l + j
            ol = list(set(l))  # 去重
            if '' in ol:
                ol.remove('')
            if ' ' in ol:
                ol.remove(' ')
            first_dict[key] = [ol]
            print(key + 'Done!')
        
        # dict转dataframe
        df_first = pd.DataFrame.from_dict(first_dict, orient='index',
                                        columns=['wordslist']).reset_index().\
            rename(columns={'index': 'first_category'})
        del(first_dict)
        # B2B 的2级
        second_dict = {}
        for key, group in tqdm(df2_sm.groupby('2_category')):
            group = group.reset_index(drop=True)
            # 合并所有分词
            l = []
            for j in list(group['wordslist']):
                l = l + j
            ol = list(set(l))  # 去重
            if '' in ol:
                ol.remove('')
            if ' ' in ol:
                ol.remove(' ')
            second_dict[key] = [ol]
            print(key + 'Done!')
        # dict转dataframe
        df_second = pd.DataFrame.from_dict(second_dict, orient='index',
                                        columns=['wordslist']).reset_index().\
            rename(columns={'index': 'second_category'})
        # 一级行业唯一词典/交叉词典的建立
        df_first = df_first.reindex(columns=list(df_first.columns) + ['uniqewords', 'crosswords',
                                                                'cross_category'], fill_value=0)
        start = time.clock()
        for i in tqdm(range(df_first.shape[0])):
            l = []
            for p1 in range(0, i):
                l = l + df_first['wordslist'][p1]
            for p2 in range(i + 1, df_first.shape[0]):
                l = l + df_first['wordslist'][p2]
            df_first['uniqewords'][i] = list(set(df_first['wordslist'][i]).
                                           difference(set(l)))
            df_first['crosswords'][i] = list(set(df_first['wordslist'][i]).
                                           intersection(set(l)))
        end = time.clock()
        print(str(end - start)) # 6s
        # 找交叉词所在集合
        start = time.clock()
        # df2_sm['cross_category'] = df2_sm.apply(lambda r: self.find_cross_category(r.crosswords, df2_sm), axis=1)
        df_first['cross_category'] = df_first.apply(lambda r: multi_find_cross(r.first_category, r.crosswords, df_first, 8, 'first_category'), axis=1)

        end = time.clock()
        print(str(end - start)) # 66s
        
        df_first['total_words'] = df_first.apply(lambda r: len(r.wordslist), axis=1)
        df_first['uniwords_num'] = df_first.apply(lambda r: len(r.uniqewords), axis=1)
        df_first['cross_num'] = df_first.apply(lambda r: len(r.crosswords), axis=1)
        # 2级行业唯一词典/交叉词典的建立
        df_second = df_second.reindex(columns=list(df_second.columns) + ['uniqewords', 'crosswords',
                                                                'cross_category'], fill_value=0)
        start = time.clock()
        for i in tqdm(range(df_second.shape[0])):
            l = []
            for p1 in range(0, i):
                l = l + df_second['wordslist'][p1]
            for p2 in range(i + 1, df_first.shape[0]):
                l = l + df_second['wordslist'][p2]
            df_second['uniqewords'][i] = list(set(df_second['wordslist'][i]).
                                           difference(set(l)))
            df_second['crosswords'][i] = list(set(df_second['wordslist'][i]).
                                           intersection(set(l)))
        end = time.clock()
        print(str(end - start)) # 6s
        # 找交叉词所在集合
        start = time.clock()
        # df2_sm['cross_category'] = df2_sm.apply(lambda r: self.find_cross_category(r.crosswords, df2_sm), axis=1)
        df_second['cross_category'] = df_second.apply(lambda r: multi_find_cross(r.second_category, r.crosswords, df_second, 8, 'second_category'), axis=1)

        end = time.clock()
        print(str(end - start)) # 66s
        
        df_second['total_words'] = df_second.apply(lambda r: len(r.wordslist), axis=1)
        df_second['uniwords_num'] = df_second.apply(lambda r: len(r.uniqewords), axis=1)
        df_second['cross_num'] = df_second.apply(lambda r: len(r.crosswords), axis=1)
        # 1级
        print('****************唯一词典/交叉词典建立 Done!****************')
        df_first = df_first[['first_category', 'wordslist', 'uniqewords', 'crosswords', 'cross_category',
                         'uniwords_num', 'cross_num']]
        ''' 统计词频
        cate_dict - 唯一词典 dataframe形式
        cross_dict - 交叉词典 dataframe形式
        '''
        df_first['cate_dict'] = 0
        df_first['cate_dict'] = pd.DataFrame(columns=['keyword', 'f'])
        df_first['cross_dict'] = 0
        df_first['cross_dict'] = pd.DataFrame(columns=['keyword', 'f'])
        df_first['df_sum'] = 0
        df_first['df_sum'] = pd.DataFrame(columns=['keyword', 'f'])
        for i in range(len(df_first)):
            df_first['df_sum'][i] = pd.concat(list(df2_sm.loc[df2_sm['first_category'] == df_first['first_category'][i], 'df_words'])).reset_index(drop=True)

        df_first.to_pickle('todo.pkl')
        df_first = pd.read_pickle('todo.pkl')
        dfcatelis = []
        dfcrosslis = []
        # 唯一词频
        for i in tqdm(range(df_first.shape[0])):
            df = df_first['df_sum'][i]
            df['ind'] = df.apply(lambda r: 1 if r.keyword in df_first['uniqewords'][i] else 0, axis=1)
            df['cind'] = df.apply(lambda r: 1 if r.keyword in df_first['crosswords'][i] else 0, axis=1)
            # 唯一性词
            dfcate = df[df['ind'] == 1].reset_index(drop=True)
            del(dfcate['ind'], dfcate['cind'])
            dfcatelis.append(dfcate)
            # 交叉性词
            dfcross = df[df['cind'] == 1].reset_index(drop=True)
            del(dfcross['ind'], dfcross['cind'])
            dfcrosslis.append(dfcross)
            back_df = multi_cal_freq(dfcate, list(df_first['uniqewords'][i]), 8)
            df_first['cate_dict'][i] = back_df.drop_duplicates(['keyword']).reset_index(drop=True)
            back_df = multi_cal_freq(dfcross, list(df_first['crosswords'][i]), 8)
            df_first['cross_dict'][i] = back_df.drop_duplicates(['keyword']).reset_index(drop=True)
            print('*******{}********'.format(df_first['first_category'][i]))
            del(back_df)
        for i in tqdm(range(df_first.shape[0])):
            ds = df_first['cate_dict'][i]
            m = sum(list(ds['freq']))
            try:
                ds['portion'] = round(ds['freq']/m, 6)
            except:
                ds['portion'] = ds['freq']
            df_first['cate_dict'][i] = ds
            del(ds)
            ds = df_first['cross_dict'][i]
            m = sum(list(ds['freq']))
            try:
                ds['portion'] = round(ds['freq']/m, 6)
            except:
                ds['portion'] = ds['freq']
            df_first['cross_dict'][i] = ds
            del(ds)
     
        df_first = df_first[['first_category', 'wordslist', 'cate_dict', 'cross_dict', 'cross_category',
       'uniwords_num', 'cross_num']]
        # df_sum.to_pickle('dfsum.pkl')
        print('****************ALL Done!****************')
        df_first.to_pickle('df_first_B2B.pkl')
        # 2级
        print('****************唯一词典/交叉词典建立 Done!****************')
        df_second = df_second[['second_category', 'wordslist', 'uniqewords', 'crosswords', 'cross_category',
                         'uniwords_num', 'cross_num']]
        ''' 统计词频
        cate_dict - 唯一词典 dataframe形式
        cross_dict - 交叉词典 dataframe形式
        '''
        df_second['cate_dict'] = 0
        df_second['cate_dict'] = pd.DataFrame(columns=['keyword', 'f'])
        df_second['cross_dict'] = 0
        df_second['cross_dict'] = pd.DataFrame(columns=['keyword', 'f'])
        df_second['df_sum'] = 0
        df_second['df_sum'] = pd.DataFrame(columns=['keyword', 'f'])
        for i in range(len(df_second)):
            df_second['df_sum'][i] = pd.concat(list(df2_sm.loc[df2_sm['2_category'] == df_second['second_category'][i], 'df_words'])).reset_index(drop=True)

        dfcatelis = []
        dfcrosslis = []
        # 唯一词频
        for i in tqdm(range(df_second.shape[0])):
            df = df_second['df_sum'][i]
            df['ind'] = df.apply(lambda r: 1 if r.keyword in df_second['uniqewords'][i] else 0, axis=1)
            df['cind'] = df.apply(lambda r: 1 if r.keyword in df_second['crosswords'][i] else 0, axis=1)
            # 唯一性词
            dfcate = df[df['ind'] == 1].reset_index(drop=True)
            del(dfcate['ind'], dfcate['cind'])
            dfcatelis.append(dfcate)
            # 交叉性词
            dfcross = df[df['cind'] == 1].reset_index(drop=True)
            del(dfcross['ind'], dfcross['cind'])
            dfcrosslis.append(dfcross)
            back_df = multi_cal_freq(dfcate, list(df_second['uniqewords'][i]), 8)
            df_second['cate_dict'][i] = back_df.drop_duplicates(['keyword']).reset_index(drop=True)
            back_df = multi_cal_freq(dfcross, list(df_second['crosswords'][i]), 8)
            df_second['cross_dict'][i] = back_df.drop_duplicates(['keyword']).reset_index(drop=True)
            print('*******{}********'.format(df_second['second_category'][i]))
            del(back_df)
        for i in tqdm(range(df_second.shape[0])):
            ds = df_second['cate_dict'][i]
            m = sum(list(ds['freq']))
            try:
                ds['portion'] = round(ds['freq']/m, 6)
            except:
                ds['portion'] = ds['freq']
            df_second['cate_dict'][i] = ds
            del(ds)
            ds = df_second['cross_dict'][i]
            m = sum(list(ds['freq']))
            try:
                ds['portion'] = round(ds['freq']/m, 6)
            except:
                ds['portion'] = ds['freq']
            df_second['cross_dict'][i] = ds
            del(ds)
     
        df_second = df_second[['second_category', 'wordslist', 'cate_dict', 'cross_dict', 'cross_category',
       'uniwords_num', 'cross_num']]
        # df_sum.to_pickle('dfsum.pkl')
        print('****************ALL Done!****************')
        
        start = time.clock()
        
        # 生成行业唯一词典（txt）
        for i in range(df_first.shape[0]):
            name = df_first['first_category'][i]
            pathf = r'C:\Users\baixing\Desktop\BX\Industrydetarmine_v1\dicts\B2B_first_unidicts'
            f = open(pathf + '/' + str(name) + dt + 'update.txt', 'w')
#            f = open(self.pathf + '/' + str(name) + dt + 'update.txt', 'w')
            for j in range(df_first['cate_dict'][i].shape[0]):
                f.write(df_first['cate_dict'][i]['keyword'][j] + '\t')
                f.write(str(df_first['cate_dict'][i]['freq'][j]) + '\t')
                f.write(str(df_first['cate_dict'][i]['portion'][j]) + '\n')
            print('********{} 唯一词典写入done！********'.format(name))
            f.close()
        # 生成行业交叉词典（txt）
        for i in range(df_first.shape[0]):
            name = df_first['first_category'][i]
            pathcf = r'C:\Users\baixing\Desktop\BX\Industrydetarmine_v1\dicts\B2B_first_crossdicts'
            f = open(pathcf + '/' + str(name) + '交叉' + dt + 'update.txt', 'w')
#            f = open(self.pathcf + '/' + str(name) + '交叉' + dt + 'update.txt', 'w')
            for j in range(df_first['cross_dict'][i].shape[0]):
                f.write(df_first['cross_dict'][i]['keyword'][j] + '\t')
                f.write(str(df_first['cross_dict'][i]['freq'][j]) + '\t')
                f.write(str(df_first['cross_dict'][i]['portion'][j]) + '\t' + str(df_first['cross_category'][i]) + '\n')
            print('********{} 交叉词典写入done！********'.format(name))
            f.close()
        end = time.clock()
        print("******** 词典建立耗时：{}s ********".format((end - start)))
        
        start = time.clock()
        
        # 2 级生成行业唯一词典（txt）
        for i in range(df_second.shape[0]):
            name = df_second['second_category'][i]
            pathf = r'C:\Users\baixing\Desktop\BX\Industrydetarmine_v1\dicts\B2B_sencond_unidicts'
            f = open(pathf + '/' + str(name) + dt + 'update.txt', 'w')
#            f = open(self.pathf + '/' + str(name) + dt + 'update.txt', 'w')
            for j in range(df_second['cate_dict'][i].shape[0]):
                f.write(df_second['cate_dict'][i]['keyword'][j] + '\t')
                f.write(str(df_second['cate_dict'][i]['freq'][j]) + '\t')
                f.write(str(df_second['cate_dict'][i]['portion'][j]) + '\n')
            print('********{} 唯一词典写入done！********'.format(name))
            f.close()
        # 生成行业交叉词典（txt）
        for i in range(df_second.shape[0]):
            name = df_second['second_category'][i]
            pathcf = r'C:\Users\baixing\Desktop\BX\Industrydetarmine_v1\dicts\B2B_second_crossdicts'
            f = open(pathcf + '/' + str(name) + '交叉' + dt + 'update.txt', 'w')
#            f = open(self.pathcf + '/' + str(name) + '交叉' + dt + 'update.txt', 'w')
            for j in range(df_second['cross_dict'][i].shape[0]):
                f.write(df_second['cross_dict'][i]['keyword'][j] + '\t')
                f.write(str(df_second['cross_dict'][i]['freq'][j]) + '\t')
                f.write(str(df_second['cross_dict'][i]['portion'][j]) + '\t' + str(df_second['cross_category'][i]) + '\n')
            print('********{} 交叉词典写入done！********'.format(name))
            f.close()
        end = time.clock()
        print("******** 词典建立耗时：{}s ********".format((end - start)))
        return df_first

if __name__ == '__main__':
    d = DictBuildMethod()
    df = d.apply_dictionary_build('20201203', ls1, ls2)

