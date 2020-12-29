# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 10:06:26 2020

@author: baixing
"""
import pandas as pd
import jieba
from multiprocessing import Pool
from first_match_module import first_category_func
import warnings
from match_rules_moudle import match_second_func
from utils import split_dataframe
from tqdm import tqdm, tqdm_notebook
from location_tagger import LocationTagger
tqdm_notebook().pandas()
from newword_detection import baikewords_match_func
  

# 安装词库切词
def cutwords_with_dic(df, usedict):
    """

    Parameters
    ----------
    df : TYPE   dataframe
        DESCRIPTION. 待分词的dataframe
    usedict : TYPE  txt文件
        DESCRIPTION. 自定义分词词典（行业词典）

    Returns
    -------
    df : TYPE   dataframe
        DESCRIPTION. 分好词的dataframe

    """
    jieba.load_userdict(usedict)
    df['cut_phrase'] = df.apply(lambda r: '/'.join(jieba.cut(r.word.upper())), axis=1)
    df['cut_phrase'] = df.apply(lambda r: [str(w) for w in r.cut_phrase.split('/')], axis=1)
    return df
# 匹配行业
def get_match_cate_plus(df, df_dic, df_manual, mode):
    """
    

    Parameters
    ----------
    df : TYPE   dataframe
        DESCRIPTION. 待进行行业判定的dataframe
    df_dic : TYPE   dataframe
        DESCRIPTION.    行业词根
    df_manual : TYPE    dataframe
        DESCRIPTION.    人工规则词根
    mode : TYPE     int 1或2
        DESCRIPTION.    1-1级；2-2级

    Returns
    -------
    df : TYPE   dataframe
        DESCRIPTION.    进行完行业判定的dataframe

    """
    if mode == 1:
        df['rlist'] = df.apply(lambda r: first_category_func(r.word, r.cut_phrase, df_dic, df_manual), axis=1)
    if mode == 2:
        df['rlist'] = df.apply(lambda r: match_second_func(r.word, r.cut_phrase, df_dic, df_manual), axis=1)
    return df
# 计算各个词在所在词典中的频率
def calculate_freq(dftodo, df, uniwlis):
    """
    

    Parameters
    ----------
    dftodo : TYPE   dataframe
        DESCRIPTION.    待统计频率的dataframe
    df : TYPE   dataframe
        DESCRIPTION.    包含重复项的datafrme
    uniwlis : TYPE  list
        DESCRIPTION.    唯一词根集合

    Returns
    -------
    df : TYPE   dataframe
        DESCRIPTION.    计算完频率的dataframe

    """
    dicts = {}
    for w in uniwlis:
        if w in list(dftodo['keyword']):
            dicts[w] = sum(list(df.loc[df['keyword'] == w, 'f']))
        else:
            continue
    df = pd.DataFrame.from_dict(dicts, orient='index', columns=['freq']).reset_index().\
        rename(columns={'index': 'keyword'})
    return df
# 寻找交叉的集合们
def find_cross_category(cross_ls, df, col_name):
    """
    

    Parameters
    ----------
    cross_ls : TYPE     list
        DESCRIPTION.    交叉的行业集合
    df : TYPE   dataframe
        DESCRIPTION.    待寻找交叉行业集合的dataframe
    col_name : TYPE     string
        DESCRIPTION.    列名

    Returns
    -------
    TYPE    list
        DESCRIPTION.    找到的交叉行业集合

    """
    warnings.filterwarnings('ignore')
    df['cross_ind'] = 0
    for word in cross_ls:
        df['cross_ind'] = df.apply(lambda r: 1 if word in r.wordslist else r.cross_ind, axis=1)
    return list(df.loc[df['cross_ind'] == 1, col_name])

# 地名替换为空
def youfunc(category, df):
    warnings.filterwarnings('ignore')
    print('{}地名识别开始'.format(str(category)))
    LT = LocationTagger()
    # 检测出的地名列表 ret['geo_nouns']
    df['word'] = df.apply(lambda r: LT.clean_sentence_city(r.word), axis=1)
    print('{}地名识别结束'.format(str(category)))
    # print(11111111111111, df_n)
    return df

# 多进程计算各个词在所在词典中的频率
def multi_cal_freq(df, uniwlis, job_num):
    """
    

    Parameters
    ----------
    df : TYPE   dataframe
        DESCRIPTION.    待计算频率的dataframe
    uniwlis : TYPE  list
        DESCRIPTION.    唯一词list
    job_num : TYPE  int
        DESCRIPTION.    进程数

    Returns
    -------
    dff : TYPE  dataframe
        DESCRIPTION.    计算完频率的dataframe

    """
    processor = job_num
    res = []
    p = Pool(processor)
    # dfls = [df[: int(len(df) * (1/8))], df[int(len(df) * (1/8)): int(len(df) * (2/8))],
    #         df[int(len(df) * (2/8)): int(len(df) * (3/8))],
    #         df[int(len(df) * (3/8)): int(len(df) * (4/8))],
    #         df[int(len(df) * (4/8)): int(len(df) * (5/8))],
    #         df[int(len(df) * (5/8)): int(len(df) * (6/8))],
    #         df[int(len(df) * (6/8)): int(len(df) * (7/8))],
    #         df[int(len(df) * (7/8)):]]
    dfls = split_dataframe(df, job_num)
    for dfp in tqdm(dfls):
        res.append(p.apply_async(calculate_freq, args=(dfp, df, uniwlis,)))
    p.close()
    p.join()
    lss = [i.get() for i in res]
    dff = pd.concat(lss).reset_index(drop=True)
    del(dfls, lss)
    return dff
# 多进程匹配行业
def multi_match_category(df, df_dic, df_manual, mode, job_num):
    """
    

    Parameters
    ----------
    df : TYPE   dataframe
        DESCRIPTION.    待进行行业判定的dataframe
    df_dic : TYPE   dataframe
        DESCRIPTION.    行业词根dataframe
    df_manual : TYPE    dataframe
        DESCRIPTION.    人工规则词根dataframe
    mode : TYPE     int(1或2)
        DESCRIPTION.    1-1级； 2-2级
    job_num : TYPE  int
        DESCRIPTION.    进程数

    Returns
    -------
    dff : TYPE  dataframe
        DESCRIPTION.    行业判定结果的dataframe

    """
    processor = job_num
    res = []
    p = Pool(processor)
    dfls = split_dataframe(df, job_num) 
    for dfp in tqdm(dfls):
        res.append(p.apply_async(get_match_cate_plus, args=(dfp, df_dic, df_manual, mode,)))
 
    p.close()
    p.join()
    lss = [i.get() for i in res]
    dff = pd.concat(lss).reset_index(drop=True)
    del(dfls, lss)
    return dff
# 多进程切词
def multi_cut_with_dic(df, usedicts, job_num):
    """
    

    Parameters
    ----------
    df : TYPE   dataframe
        DESCRIPTION.    待分词的dataframe
    usedicts : TYPE     txt文件
        DESCRIPTION.    自定义分词词典
    job_num : TYPE      int
        DESCRIPTION.    进程数

    Returns
    -------
    dff : TYPE      dataframe
        DESCRIPTION.    分好词的dataframe

    """
    processor = job_num
    res = []
    p = Pool(processor)
    dfls = split_dataframe(df=df, nums=job_num)
    print([len(dfp) for dfp in dfls])
    for dfp in tqdm(dfls):
        res.append(p.apply_async(cutwords_with_dic, args=(dfp, usedicts,)))

    p.close()
    p.join()
    lss = [i.get() for i in res]
    dff = pd.concat(lss).reset_index(drop=True)
    del(dfls, lss)
    return dff
# 多进程进行地名过滤
def multi_city_clean(df, job_num):
    """
    

    Parameters
    ----------
    df : TYPE       dataframe
        DESCRIPTION.    待过滤的dataframe
    job_num : TYPE  int
        DESCRIPTION.    进程数

    Returns
    -------
    df : TYPE       dataframe
        DESCRIPTION.    地名过滤完成后的dataframe

    """
    processor = job_num
    res = []
    if len(df) > job_num:
        p = Pool(processor)
        dfls = split_dataframe(df=df, nums=job_num)
        for dfp in tqdm(dfls):
            warnings.filterwarnings('ignore')
            res.append(p.apply_async(youfunc, args=(len(dfp), dfp,)))
           
        p.close()
        p.join()
        lss = [i.get() for i in res]
        df = pd.concat(lss).reset_index(drop=True)
    else:
        df = youfunc(len(df), df)
    return df

# 多进程过滤5118原始词包
def multi_city_cleanwy(df, job_num):
    """
    

    Parameters
    ----------
    df : TYPE   dataframe
        DESCRIPTION.    原始5118行业词包dataframe
    job_num : TYPE  int
        DESCRIPTION.    进程数

    Returns
    -------
    df : TYPE   dataframe
        DESCRIPTION.    数据清洗过的5118行业词包dataframe

    """
    processor = job_num
    res = []
    p = Pool(processor)
    dfls = split_dataframe(df, job_num)
    for dfp in tqdm(dfls):
        warnings.filterwarnings('ignore')
        res.append(p.apply_async(youfunc, args=(len(dfp), dfp,)))
 
    p.close()
    p.join()
    lss = [i.get() for i in res]
    df = pd.concat(lss).reset_index(drop=True)
    return df

# 多进程寻找交集
def multi_find_cross(cate, ls, df_dict, job_num, col_name):
    """
    

    Parameters
    ----------
    cate : TYPE     string
        DESCRIPTION.    行业名称
    ls : TYPE       list
        DESCRIPTION.    交叉的行业集合
    df_dict : TYPE  dataframe
        DESCRIPTION.    行业词典
    job_num : TYPE  int
        DESCRIPTION.    进程数
    col_name : TYPE     string
        DESCRIPTION.    列名

    Returns
    -------
    TYPE
        DESCRIPTION.

    """
    processor = job_num
    res = []
    p = Pool(processor)
    ls_pices = split_dataframe(ls, job_num)
    for dfp in tqdm(ls_pices):
        warnings.filterwarnings('ignore')
        res.append(p.apply_async(find_cross_category, args=(dfp, df_dict, col_name,)))
    p.close()
    p.join()
    lss = [i.get() for i in res]
    back = []
    for l in lss:
        back = back + l
    print(cate + 'Done!')
    return list(set(back))




