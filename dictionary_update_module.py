# -*- coding: utf-8 -*-
"""
Created on Fri Dec  4 15:57:53 2020

@author: baixing
"""
#Index(['word', 'second_category', 'first_category', 'B2B_ind', 'cut_phrase1',
#       'f_result', 'hit_words', 'cross_set', 'plist', 'source',
#       'f_success_ind', 'result_ind', 'amb_success_ind', 'cut_phrase',
#       'result', 'second_hit_words', 'second_cross_set', 'second_plist',
#       'source_2', 'success_ind'],
#      dtype='object')

import pandas as pd
from apply_muliti_process import multi_city_clean

def update_fcategory_dicts(df_test=df_testdone):
    try:
        df_test.loc[df_test['first_category'] == '兼职', 'first_category'] = '招聘'
        df_test.loc[df_test['f_result'] == '兼职', 'f_result'] = '招聘'
        df_test['f_success_ind'] = df_test.apply(lambda r: 1 if r.
                                           f_result == r.first_category else 0, axis=1)
    except:
        pass
    # 1级新词集合
    newwords_dict = {}
    df_newwords = df_test.loc[df_test['result'] == '', :].reset_index(drop=True)
    for f, group in df_newwords.groupby(df_newwords.first_category):
        
        sum_word = []
        for l in list(group['cut_phrase']):
            sum_word = sum_word + l
        sum_word_set = list(set(sum_word))
        sum_word_set = [w for w in sum_word_set if w[0] not in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']]

        newwords_dict[f] = [sum_word, sum_word_set]
        
    df_newwords = pd.DataFrame.from_dict(newwords_dict, orient='index', columns=[
        'wordlis', 'wordlis_set']).reset_index().rename(columns={'index': 'first_category'})
    df_newwords = df_newwords.loc[df_newwords['first_category'] != '其他服务', :].reset_index(drop=True)
    # 唯一交叉
    df_newwords = df_newwords.reindex(columns=list(df_newwords.columns) + ['uniqewords', 'crosswords',
                                                            ], fill_value=0)
    for i in tqdm(range(df_newwords.shape[0])):
        l = []
        for p1 in range(0, i):
            l = l + df_newwords['wordlis_set'][p1]
        for p2 in range(i + 1, df_newwords.shape[0]):
            l = l + df_newwords['wordlis_set'][p2]
        df_newwords['uniqewords'][i] = list(set(df_newwords['wordlis_set'][i]).
                                       difference(set(l)))
        df_newwords['crosswords'][i] = list(set(df_newwords['wordlis_set'][i]).
                                       intersection(set(l)))
    df_newwords['uniwords'] = pd.DataFrame(columns=['word'])
    for i in range(df_newwords.shape[0]):
        df = pd.DataFrame(columns=['word'])
        for j in range(len(df_newwords['uniqewords'][i])):
            df.loc[len(df)] = [df_newwords['uniqewords'][i][j]]
        df_newwords['uniwords'][i] = df
        del(df)
    # 进行地名过滤
    df_newwords['uniwords'] = df_newwords.apply(lambda r: multi_city_clean(r.uniwords, 8), axis=1)
    for i in range(df_newwords.shape[0]):
        df_newwords['uniwords'][i]['word'] = df_newwords['uniwords'][i].apply(lambda r: '' if len(r.word) <= 2 else r.word, axis=1)
        df_newwords['uniwords'][i] = df_newwords['uniwords'][i].loc[df_newwords['uniwords'][i]['word'] != '', :].reset_index(drop=True)
    # 1级错词
    update_dic = {}
    df_notice = df_test.loc[df_test['f_success_ind'] == 0, :].reset_index(drop=True)    
    for f, group in df_notice.groupby(df_notice.first_category):
        sum_word = []
        for l in list(group['cut_phrase']):
            sum_word = sum_word + l
        sum_word_set = list(set(sum_word))
        sum_word_set = [w for w in sum_word_set if w[0] not in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']]
        update_dic[f] = [sum_word, sum_word_set]
    df_update = pd.DataFrame.from_dict(update_dic, orient='index', columns=[
        'wordlis', 'wordlis_set']).reset_index().rename(columns={'index': 'first_category'})
    df_update = df_update.loc[df_update['first_category'] != '其他服务', :].reset_index(drop=True)
    
    # 行业唯一词典/交叉词典的建立
    df_update = df_update.reindex(columns=list(df_update.columns) + ['uniqewords', 'crosswords',
                                                            ], fill_value=0)
    for i in tqdm(range(df_update.shape[0])):
        l = []
        for p1 in range(0, i):
            l = l + df_update['wordlis_set'][p1]
        for p2 in range(i + 1, df_update.shape[0]):
            l = l + df_update['wordlis_set'][p2]
        df_update['uniqewords'][i] = list(set(df_update['wordlis_set'][i]).
                                       difference(set(l)))
        df_update['crosswords'][i] = list(set(df_update['wordlis_set'][i]).
                                       intersection(set(l)))
    print('************2级*************')   
    # 2级错词
    update_dic2 = {}
    df_notice2 = df_test.loc[df_test['success_ind'] == 0, :].reset_index(drop=True)    
    for f, group in df_notice2.groupby(df_notice2.second_category):
        sum_word = []
        for l in list(group['cut_phrase']):
            sum_word = sum_word + l
        sum_word_set = list(set(sum_word))
        sum_word_set = [w for w in sum_word_set if w[0] not in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']]
        update_dic2[f] = [sum_word, sum_word_set]
    # 错词
    df_update2 = pd.DataFrame.from_dict(update_dic2, orient='index', columns=[
        'wordlis', 'wordlis_set']).reset_index().rename(columns={'index': 'second_category'})
        
    # 行业唯一词典/交叉词典的建立
    df_update2 = df_update2.reindex(columns=list(df_update2.columns) + ['uniqewords', 'crosswords',
                                                            ], fill_value=0)
    for i in tqdm(range(df_update2.shape[0])):
        l = []
        for p1 in range(0, i):
            l = l + df_update2['wordlis_set'][p1]
        for p2 in range(i + 1, df_update2.shape[0]):
            l = l + df_update2['wordlis_set'][p2]
        try:
            df_update2['uniqewords'][i] = list(set(df_update2['wordlis_set'][i]).difference(set(l)))
        except:
            df_update2['uniqewords'][i] = 0
        df_update2['crosswords'][i] = list(set(df_update2['wordlis_set'][i]).
                                       intersection(set(l)))
    # 1级新词集合
    newwords_dict2 = {}
    df_newwords2 = df_test.loc[df_test['result'] == '', :].reset_index(drop=True)
    for f, group in df_newwords2.groupby(df_newwords2.second_category):
        
        sum_word = []
        for l in list(group['cut_phrase']):
            sum_word = sum_word + l
        sum_word_set = list(set(sum_word))
        sum_word_set = [w for w in sum_word_set if w[0] not in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']]

        newwords_dict2[f] = [sum_word, sum_word_set]
        
    df_newwords2 = pd.DataFrame.from_dict(newwords_dict2, orient='index', columns=[
        'wordlis', 'wordlis_set']).reset_index().rename(columns={'index': 'second_category'})
    # 唯一交叉
    df_newwords2 = df_newwords2.reindex(columns=list(df_newwords2.columns) + ['uniqewords', 'crosswords',
                                                            ], fill_value=0)
    for i in tqdm(range(df_newwords2.shape[0])):
        l = []
        for p1 in range(0, i):
            l = l + df_newwords2['wordlis_set'][p1]
        for p2 in range(i + 1, df_newwords2.shape[0]):
            l = l + df_newwords2['wordlis_set'][p2]
        
        df_newwords2['uniqewords'][i] = list(set(df_newwords2['wordlis_set'][i]).
                                       difference(set(l)))
        try:
            df_newwords2['crosswords'][i] = list(set(df_newwords2['wordlis_set'][i]).
                                       intersection(set(l)))
        except:
            df_newwords2['crosswords'][i] = 0
            
def tongji_freq(df, col_name='first_category'):
    # 唯一词频
    df['df_uniquewords'] = 0
    df['df_uniquewords'] = pd.DataFrame(columns=['word', 'freq'])
    for i in tqdm(range(df.shape[0])):
        wdict2s = {}
        # 统计词频
        for s in df['uniqewords'][i]:
            # 单个字以及数字或字母一般无法判断行业
            if len(s) >= 2 and s[0] not in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']:
                wdict2s[s] = df['wordlis'][i].count(s)
        print(df[col_name][i] + 'Done!') 
        # dict转dataframe
        dfmini = pd.DataFrame.from_dict(wdict2s, orient='index',
                                        columns=['freq']).reset_index().\
            rename(columns={'index': 'word'})
        df['df_uniquewords'][i] = dfmini
        del(dfmini)
        
def fill_newwords_todicts(manual_ind, cross_ind, cateory_name, first_ind, wordslis, df_manual, df_manual_second):
    if manual_ind == 1:
        if first_ind == 1:
            df_manual.loc[df_manual['first category'] == cateory_name, 
                          'manual_words'] = list(df_manual.loc[df_manual['first category'] == 
                                                 cateory_name, 'manual_words']) + wordslis
        else:
            df_manual_second.loc[df_manual_second['second_category'] == cateory_name, 
                                 'manual_words'] = list(df_manual_second.loc[
                                         df_manual_second['second_category'] == 
                                         cateory_name, 'manual_words']) + wordslis
    elif first_ind == 1:
        if cross_ind == 0:
            df_first_dict
        
if __name__ == '__main__':
    tongji_freq(df_newwords)