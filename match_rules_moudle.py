# -*- coding: utf-8 -*-
"""
Created on Mon Nov 23 17:10:46 2020

@author: baixing
"""
import pandas as pd
from tqdm import tqdm
from textwrap import dedent

# 优先级
def priotity_second_func(longwords):
    if '猫' in longwords:
        if '粮' in longwords:
            return '宠物用品食品'
        else:
            return '猫猫'
    if '狗' in longwords:
        if '粮' in longwords:
            return '宠物用品食品'
        else:
            return '狗狗'
    if '鱼' in longwords:
        if '干' in longwords:
            return '宠物用品食品'
        else:
            return '花鸟虫鱼'
    if '收' in longwords:
        if '车' in longwords:
            return '高价收车'
        else:
            return '物品回收'
'''
进行行业判断
search_second_manual_rules(df_manual_second, longwords)
df_manual_second —— 2级行业人工规则（dataframe）, 
longwords —— 待判定的长尾词
'''
def search_second_manual_rules(df_manual_second, longwords):
    # 涉及到优先级问题的词 —— tricky_words
    tricky_words = ['猫', '狗', '鱼', '收']
    # 如果可以在优先级里判定出行业就直接返回 ，否则进入2行业人工规则中去找
    if len([tw for tw in tricky_words if tw in longwords]) > 0:
        result = priotity_second_func(longwords)
        return result
    else:
        # 匹配人工规则 ，若某一个或多个2级的人工规则词根在长尾词中出现则进行筛选
        df_manual_second['m'] = df_manual_second.apply(lambda r: [w for w in r.manual_words if w in longwords], axis=1)
        df_manual_second['ind'] = df_manual_second.apply(lambda r: len(r.m), axis=1)
        df = df_manual_second.query('ind >= 1').reset_index(drop=True)
        del(df_manual_second['m'], df_manual_second['ind'])
        # 若在人工规则词根中无法匹配则进入行业唯一、交叉词典进行匹配
        if df.shape[0] == 0:
            return 0
        # 若人工匹配出唯一一个2级行业，则直接返回行业结果
        elif df.shape[0] == 1:
            return list(df['second_category'])[-1]
        # 若同时有多个2级行业人工词根命中，则按如下逻辑进行筛选
        else:
            df['len_m'] = df.apply(lambda r: len(r.m), axis=1)
            M = df['len_m'].max()
            df_selected = df.query('len_m == {}'.format(M)).reset_index(drop=True)
            if df_selected.shape[0] == 1:
                return list(df_selected['second_category'])[-1]
            else:
                fcl = []
                indexlis = []
                for i in range(df_selected.shape[0]):
                    for j in range(len(df_selected['m'][i])):
                        fcl.append(df_selected['second_category'][i])
                        indexlis.append(str(longwords).index(df_selected['m'][i][j]))
                return fcl[indexlis.index(max(indexlis))]

def match_second_func(longwords, lis, df_dict, df_manual_second):
    # 在一级行业结果已判定的情况下，直接在其所对应的二级行业范围内寻找
    if df_manual_second.shape[0] > 0:
        res = search_second_manual_rules(df_manual_second, longwords)
    else:
        res = 0
    if res!= 0:
        return [res] + ['人工', 0, 0, '人工2级规则']
    else:
        r = get_match_category(lis, df_dict)
        return r
        


def get_match_category(lis, df_dic):        
    hit_words = []
    hit_cross = []
    result = []
    crosslis = []
    for word in lis:
        df_dic['m_ind'] = 0
        df_dic['c_ind'] = 0
        df_dic['m_ind'] = df_dic.apply(lambda r: 1 if word in list(set(list(
            r.cate_dict['word'])+[r['2_category']])) else 0, axis=1)
        # 如果m_ind都是0,则说明该词在唯一词典没匹配上，若匹配上则加入匹配词列表
        if df_dic['m_ind'].max() == 0:
            df_dic['c_ind'] = df_dic.apply(lambda r: 1 if word in list(r.cross_dict['word']) else 0, axis=1)
            if df_dic['c_ind'].max() == 1:
                hit_cross.append(word)
                crosslis.append(list(df_dic.loc[df_dic['c_ind'] == 1, '2_category']))
        else:
            hit_words.append(word)
            result.append(list(df_dic.loc[df_dic['m_ind'] == 1, '2_category'])[-1])
        df_dic.drop(columns=['m_ind', 'c_ind'])
    # 匹配结果-类目集合
    if len(result) == 1:
        print(result[-1])
        return [result[-1], hit_words, 0, 0, '唯一dict']
    # 若候选类目多于一个，则按候选词的长度进行选择
    elif len(result) > 1:
        selected_words = []
        selected_result = []
        n = max([len(w) for w in hit_words])
        for i in range(len(result)):
            if len(hit_words[i]) == n:
                selected_words.append(hit_words[i])
                selected_result.append(result[i])
        # 如果候选词中仅有一个最长的词，则返回其对应的类目；否则继续进行筛选
        if len(selected_result) == 1:
            print(selected_result[-1])
            return [selected_result[-1], hit_words, 0, 0, '唯一dict']
        else:
            r = choose_multi_unicategory(selected_words, selected_result, df_dic)
            print(r)
            return [r, hit_words, 0, 0, '唯一dict']
    else:
           # get_cross_categoryset
        if len(crosslis) == 0 or len(hit_cross) == 0:
            return ['', '', '', 0, '新词']
        rs = get_cross_categoryset(crosslis, hit_cross, df_dic)
        print(rs[0])
        return rs + ['交叉dict']
    
    
# multi_unique match fuction
def choose_multi_unicategory(words, rs, df):
    p = []
    # 按比重进行筛选
    for i in range(len(words)):
        # 找出该词所对应行业的词典及其该词在其行业词典中的比重
        # 若该词正好是行业词本身或其本身的分词， 即（该行业的search_dict有，而cate_dict中没有），则将其portion认为是1（一定最大）
        s = list(df.loc[df['2_category'] == rs[i], 'search_dict'])[-1]
        cdf = list(df.loc[df['2_category'] == rs[i], 'cate_dict'])[-1]
        if words[i] in list(set(s).difference(set(list(cdf['word'])))):
            p.append(1)
        else:
            p.append(list(cdf.loc[cdf['word'] == words[i], 'portion'])[-1])
    # 找出比重最大的词对对应的行业
    return rs[p.index(max(p))]
def choose_multi_firstcategory(fresult):
    # 首先选出候选行业中，候选次数最多的行业集合
    houxuanf = list(set(fresult))
    counter = [fresult.count(f) for f in houxuanf]
    selected_f = []
    for c in houxuanf:
        if fresult.count(c) == max(counter):
            selected_f.append(c)
    # 若经过筛选只剩一个交叉候选行业，则直接返回：
    if len(selected_f) == 1:
        return selected_f[-1]
    else:
        return 0
# cross category match fuction
def get_cross_categoryset(crossls, hitws, df):
    # 首先选出候选行业中，候选次数最多的行业集合
    houxuanls = []
    for ls in crossls:
        houxuanls = houxuanls + ls
    houxuanset = list(set(houxuanls))
    counter = [houxuanls.count(cate) for cate in houxuanset]
    selected_cross = []
    for c in houxuanset:
        if houxuanls.count(c) == max(counter):
            selected_cross.append(c)
    # 若经过筛选只剩一个交叉候选行业，则直接返回：
    if len(selected_cross) == 1:
        return [selected_cross[-1], hitws, crossls, 0]
    else:
        # 应用概率模型
        r, plist = apply_probability_method(selected_cross, hitws, df)
    return [r, hitws, selected_cross, plist]
def apply_probability_method(ls, hitls, df):
    plist = []
    for c in ls:
        p_kbelong2c = 1
        for w in hitls:
            cd = list(df.loc[df['2_category'] == c, 'cross_dict'])[-1]
            if w in list(cd['word']):
                p_wbelng2c = list(cd.loc[cd['word'] == w, 'portion'])[-1]
            else:
                p_wbelng2c = 0.1 * max(cd['portion'].min(), 0.000001)
            p_kbelong2c = p_kbelong2c * p_wbelng2c
        plist.append(p_kbelong2c * (1/19))
    
    return ls[plist.index(max(plist))], plist