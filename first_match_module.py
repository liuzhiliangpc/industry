# -*- coding: utf-8 -*-
"""
Created on Tue Dec  8 13:51:57 2020

@author: baixing
"""

import pandas as pd

# 优先级
def priotity_func(longwords):
    if '招' in longwords:
        for w in ['生', '校']:
            if w in longwords:
                return ['培训', longwords.index('招')]
        return ['招聘', longwords.index('招')]
    if '租' in longwords:
        for w in ['房', '写字楼', '办公室', '仓库']:
            if w in longwords:
                return ['房屋', longwords.index('租')]
        return ['生活服务', longwords.index('租')]
    if '收车' in longwords:
        return ['生活服务', longwords.index('收车')]
    if '车' in longwords:
        if '租' in longwords:
            return ['生活服务', longwords.index('车')]
        else:
            return ['车辆', longwords.index('车')]
    if '快递' in longwords:
        if '员' in longwords:
            return ['招聘', longwords.index('快递')]
        else:
            return ['商务服务', longwords.index('快递')]
    if '设计' in longwords:
        if '师' in longwords:
            return ['招聘', longwords.index('设计')]
        else:
            return ['商务服务', longwords.index('设计')]
    if '摄像' in longwords:
        for w in ['头', '监控']:
            if w in longwords:
                return ['工业制造', longwords.index('摄像')]
        return ['生活服务', longwords.index('摄像')]


def search_manual_rules(df_manual, longwords):
    tricky_words = ['快递', '设计', '租', '收车', '车', '招', '摄像']
    tw_hitlist = [tw for tw in tricky_words if tw in longwords]
    if len(tw_hitlist) > 0:
        resultlis = priotity_func(longwords)
#        return [result, ht_word, 0, 0, '优先级匹配']
    try:
        get_result = resultlis[0]
    except:
        get_result = '404'
    df_manual['m'] = df_manual.apply(lambda r: [w for w in r.manual_words if w in longwords], axis=1)
    df_manual['ind'] = df_manual.apply(lambda r: len(r.m), axis=1)
    df = df_manual.query('ind >= {}'.format(1)).reset_index(drop=True)
    del(df_manual['m'], df_manual['ind'])
    if df.shape[0] == 0:
        if get_result == '404':
            return [0, 0, 0, 0, 0]
        else:
            return [resultlis[0], tw_hitlist, 0, 0, '优先级匹配'] 
    elif df.shape[0] == 1:
        if get_result == '404':
            return [list(df['first category'])[-1], list(df['m'])[-1], 0, 0, '人工规则']
        else:
            index_lis = [longwords.index(w) for w in list(df['m'])[-1]]
            if resultlis[-1] >= max(index_lis):
                return [resultlis[0], tw_hitlist, 0, 0, '优先级匹配']
            else:
                return [list(df['first category'])[-1], list(df['m'])[-1], 0, 0, '人工筛选']
    else:
        df['len_m'] = df.apply(lambda r: len(r.m), axis=1)
        M = df['len_m'].max()
        df_selected = df.query('len_m == {}'.format(M)).reset_index(drop=True)
        if df_selected.shape[0] == 1:
            if get_result == '404':
                return [list(df_selected['first category'])[-1], list(df_selected['m'])[-1], 0, 0, '人工筛选']
            else:
                index_lis = [longwords.index(w) for w in list(df_selected['m'])[-1]]
                if resultlis[-1] >= max(index_lis):
                    return [resultlis[0], tw_hitlist, 0, 0, '优先级匹配']
                else:
                    return [list(df_selected['first category'])[-1], list(df_selected['m'])[-1], 0, 0, '人工筛选']
        else:
            fcl = []
            indexlis = []
            for i in range(df_selected.shape[0]):
                for j in range(len(df_selected['m'][i])):
                    fcl.append(df_selected['first category'][i])
                    indexlis.append(longwords.index(df_selected['m'][i][j]))
            if get_result == '404':
                return [fcl[indexlis.index(max(indexlis))], list(df_selected['m']), list(df_selected['first category']), 0, '人工筛选']
            else:
                if resultlis[-1] >= max(indexlis):
                    return [resultlis[0], tw_hitlist, 0, 0, '优先级匹配']
                else:
                    return [fcl[indexlis.index(max(indexlis))], list(df_selected['m']), list(df_selected['first category']), 0, '人工筛选']
#print(search_manual_rules(df_manual=df_manual, longwords='我有一辆车出租'))
#print(search_manual_rules(df_manual=df_manual, longwords='我有一辆车'))
#print(search_manual_rules(df_manual=df_manual, longwords='进行房屋出租'))
#print(search_manual_rules(df_manual=df_manual, longwords='进行表演培训'))
#print(search_manual_rules(df_manual=df_manual, longwords='学校表演场地'))

def first_category_func(longwords, lis, df_first, df_manual):
#    CutWords().gene_own_firstdict(df_first)
    res = search_manual_rules(df_manual=df_manual, longwords=longwords)
    if res[0] != 0:
        return res
    else:
        res2 = match_first_category(lis, df_first)
        return res2
    
# multi_unique match fuction
def choose_multi_unicategory(words, rs, df):
    p = []
    # 按比重进行筛选
    for i in range(len(words)):
        # 找出该词所对应行业的词典及其该词在其行业词典中的比重
        cdf = list(df.loc[df['first_category'] == rs[i], 'cate_dict'])[-1]
        p.append(max(list(cdf.loc[cdf['word'] == words[i], 'portion'])[-1], 0.000001))
    # 找出比重最大的词对对应的行业
    return rs[p.index(max(p))]

# cross category match fuction
def get_cross_categoryset(crossls, hitws, df):
    # 首先选出候选行业中，候选次数最多的行业集合
    houxuanls = []
    for ls in crossls:
        try:
            houxuanls = houxuanls + ls
        except:
            houxuanls = houxuanls + [ls]
    houxuanset = list(set(houxuanls))
    counter = [houxuanls.count(cate) for cate in houxuanset]
    selected_cross = []
    for c in houxuanset:
        if houxuanls.count(c) == max(counter):
            selected_cross.append(c)
    # 若经过筛选只剩一个交叉候选行业，则直接返回：
    if len(selected_cross) == 1:
        return [selected_cross[-1], hitws, crossls, 0, '交叉筛选']
    else:
        # 应用概率模型
        r, plist = apply_probability_method(selected_cross, hitws, df)
    return [r, hitws, selected_cross, plist, '交叉概率']
def apply_probability_method(ls, hitls, df):
    plist = []
    for c in ls:
        p_kbelong2c = 1
        for w in hitls:
            cd = list(df.loc[df['first_category'] == c, 'cross_dict'])[-1]
            if w in list(cd['word']):
                p_wbelng2c = list(cd.loc[cd['word'] == w, 'portion'])[-1]
            else:
                p_wbelng2c = 0.1 * max(cd['portion'].min(), 0.000001)
                # list(cd.loc[cd['word'] == 'default', 'portion'])[-1]
            p_kbelong2c = p_kbelong2c * p_wbelng2c
        plist.append(p_kbelong2c * (1/19))
    
    return ls[plist.index(max(plist))], plist


    
def match_first_category(lis, df_first):
    hit_words = []
    hit_cross = []
    result = []
    crosslis = []
    for word in lis:
        # 进入唯一or交叉匹配
        df_first['m_ind'] = 0
        df_first['c_ind'] = 0
        df_first['m_ind'] = df_first.apply(lambda r: 1 if word in list(set(list(
            r.cate_dict['word'])+[r['first_category']])) else 0, axis=1)
        # 如果m_ind都是0,则说明该词在唯一词典没匹配上，若匹配上则加入匹配词列表
        if df_first['m_ind'].max() == 0:
            df_first['c_ind'] = df_first.apply(lambda r: 1 if word in list(r.cross_dict['word']) else 0, axis=1)
            if df_first['c_ind'].max() == 1:
                hit_cross.append(word)
                crosslis.append(list(df_first.loc[df_first['c_ind'] == 1, 'first_category']))
        else:
            hit_words.append(word)
#            print(list(df_dic.loc[df_dic['m_ind'] == 1, '2_category'])[-1])
            result.append(list(df_first.loc[df_first['m_ind'] == 1, 'first_category'])[-1])
        df_first.drop(columns=['m_ind', 'c_ind'])
    # 匹配结果-类目集合
    if len(result) == 1:
#        print(result[-1])
        return [result[-1], hit_words, 0, 0, 0, '唯一dict']
    # 若候选类目多于一个，则按候选词的长度进行选择
    elif len(result) > 1:
        # 如果候选词类目中有B2B行业，就返回B2B行业
#        B2B_list = [category for category in result if category in ('能源材料', '工业制造', '生活消费')]
#        if len(B2B_list) >= 1:
#            if len(B2B_list) == 1:
#                return [B2B_list[-1], hit_words, result, 0, 'B2B唯一筛选']
#            else:
#                r = choose_multi_unicategory(hit_words, B2B_list, df_first)
#                return [r, hit_words, result, 0, 'B2B唯一筛选']
#        else:
        selected_words = []
        selected_result = []
        n = max([len(w) for w in hit_words])
        for i in range(len(result)):
            if len(hit_words[i]) == n:
                selected_words.append(hit_words[i])
                selected_result.append(result[i])
#           如果候选词中仅有一个最长的词，则返回其对应的类目；否则继续进行筛选
        if len(selected_result) == 1:
#                print(selected_result[-1])
            return [selected_result[-1], hit_words, 0, 0, '唯一筛选']
        else:
            r = choose_multi_unicategory(selected_words, selected_result, df_first)
#                print(r)
            return [r, hit_words, selected_result, 0, '唯一筛选']
    else:
           # get_cross_categoryset
        if len(crosslis) == 0 or len(hit_cross) == 0:
            
            return ['其他服务', '', 0, 0, '新词']
        # 如果候选词类目中有B2B行业，就返回B2B行业
#        B2B_list = [cate for ls in crosslis for cate in ls if cate in ('能源材料', '工业制造', '生活消费')]
#        if len(B2B_list) >= 1:
#            if len(B2B_list) == 1:
#                return [B2B_list[-1], hit_words, crosslis, 0, 'B2B交叉筛选0']
#            else:
#                rs = get_cross_categoryset(B2B_list, hit_cross, df_first)
#                rs[-1] = 'B2B' + str(rs[-1])
#                return rs
#        else:
        rs = get_cross_categoryset(crosslis, hit_cross, df_first)
#            print(rs[0])
        return rs