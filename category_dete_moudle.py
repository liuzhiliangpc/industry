# -*- coding: utf-8 -*-
"""
Created on Thu Nov  5 09:43:04 2020

@author: baixing
"""
import pandas as pd
import jieba
import time
# from data_moudle import Data
from tqdm import tqdm
import pickle
from dictionary_build_module import DictBuildMethod
from get_dictionary_module import GetCategoryDict
from match_rules_moudle import get_match_category
from apply_test_moudle import apply_test
from cut_words_module import CutWords
from utils import change_category
from city_clean_module import CountryClean
from apply_muliti_process import cutwords_with_dic
from first_match_module import first_category_func
from match_rules_moudle import match_second_func
from location_tagger import LocationTagger
import os

import sys
import importlib
# importlib.reload(sys)

# 获取训练数据并划分
# def get_data(categorylist, amt, db):
#     data = Data('1', amt, db)
#     start = time.clock()
#     df_trainlist = [] 
#     df_verifylist = [] 
#     for c in tqdm(categorylist):
#         df_train = data.get_train_data(c)
#         if df_train.shape[0] == 0:
#             print(c + '数据获取失败 ！')
#         else:
#             # 划分训练集与验证集
#             df_verify_part, df_train_part = df_train.iloc[: int(len(df_train)*0.2)], \
#                                             df_train.iloc[int(len(df_train)*0.2):]
#             df_trainlist.append(df_train_part)
#             df_verifylist.append(df_verify_part)
#     end = time.clock()
#     print('*********** 数据获取与划分耗时：'+ str(end-start) + 's *************')
#     df_t = pd.concat(df_trainlist).reset_index(drop=True)
#     df_v = pd.concat(df_verifylist).reset_index(drop=True)
#     del(df_verifylist)
#     df_t.to_pickle('dft.pkl')
#     df_v.to_pickle('dfv.pkl')
#     return df_t, df_v

# 行业判定
def category_determine(sen,
                       flist,
                       df_first_manual,
                       df_second_manual):
    # 获取1级词典——dataframe
#    df_first_dict = get_first_category_dict('20201208', dicts_path1, dicts_path1c)
    p = os.getcwd()
    df_first_dict= pd.read_pickle(p + '\\' + 'dict_pkl\\df_first_dict.pkl')
    # 获取二级词典——dataframe
    df_dict= pd.read_pickle(p + '\\' + 'dict_pkl\\df_dict.pkl')
    starts = time.clock()
    # 国家识别并替换
    sen = CountryClean().country_clean(sen)
    new = pd.DataFrame({'word': sen}, index=[1])
    jiebadicts_path1 = p + '//' + 'own_cutdicts//own_first_dict.txt'
    jiebadicts_path2 = p + '//' + 'own_cutdicts//owndict.txt'
    # 一级行业分词
    cut_phrase1 = list(cutwords_with_dic('*', new, jiebadicts_path1)['cut_phrase'])[-1]
    # 2级行业分词
    cut_phrase2 = list(cutwords_with_dic('*', new, jiebadicts_path2)['cut_phrase'])[-1]
    # 一级行业判定
    rlist = first_category_func(sen, cut_phrase1, df_first_dict, df_first_manual)
    print(sen)
    if rlist[0] == '其他服务':
        print('****发现一级行业新词！无法判定一级行业*****')
        ends = time.clock()
        return cut_phrase1
    else:
        print(rlist[0])
        # 二级行业判定
        rlist2 = match_second_func(sen, cut_phrase2, df_dict, df_second_manual)
        ends = time.clock()
        print("判定耗时：{}s".format((ends - starts)))
        if rlist2 == '':
            print('****发现2级行业新词！无法判定2级行业*****')
            return [rlist, cut_phrase2]
        else:
            print(rlist2[0])
            return [rlist, rlist2]


# main()
if __name__ == '__main__':
    ''' 获取人工规则
            一级规则 —— df_manual
            二级规则 —— df_manual_second
        '''
    # 行业关系（1级 2级）
    df_cate = pd.read_excel(r'C:\Users\baixing\Desktop\BX\data\cate_info.xlsx').reset_index(drop=True)
    flist = {}
    for f, group in df_cate.groupby(df_cate.first_cate):
        flist[f] = [change_category(s) for s in list(group['second_cate'])]
    p = os.getcwd()
    # 一级规则
    df_manual = pd.read_excel(r'C:\Users\baixing\Desktop\BX\re\manual_first_rules.xlsx')
    df_manual['manual_words'] = df_manual.apply(lambda r: r['人工词库'].split('、'), axis=1)  
    df_manual['manual_words'] = df_manual.apply(lambda r: [w for w in r.manual_words if w != ''], axis=1)
    try:
        df_manual.to_pickle(p + '//dict_pkl//df_manual.pkl')
    except:
        os.remove(p + '//dict_pkl//df_manual.pkl')
        df_manual.to_pickle(p + '//dict_pkl//df_manual.pkl')
    # 2级行业规则
    df_manual_second = pd.read_excel(r'C:\Users\baixing\Desktop\BX\re\manual_second_rules.xlsx').reset_index(drop=True)
    df_manual_second['second_category'] = df_manual_second.apply(lambda r: change_category(r.second_category), axis=1)
    df_manual_second['manual_words'] = df_manual_second.apply(lambda r: r.manual_words.split('、'), axis=1)
    df_manual_second['manual_words'] = df_manual_second.apply(lambda r: [w for w in r.manual_words if w != ''], axis=1)
    try:
        df_manual_second.to_pickle(p + '//dict_pkl//df_manual_second.pkl')
    except:
        os.remove(p + '//dict_pkl//df_manual_second.pkl')
        df_manual_second.to_pickle(p + '//dict_pkl//df_manual_second.pkl')
    if str(input()) == '1':
        dt = time.strftime("%Y%m%d", time.localtime())
        dtt = time.strftime("%Y%m%d%H%M%S", time.localtime())
        # 数据获取
        # data = Data('1', category, 100000, db)
        # df_category = data.get_category()
        fm_category = pd.read_csv(r'C:\Users\baixing\Desktop\BX\fm_category.csv')
        df_category = pd.read_excel(r'C:\Users\baixing\Desktop\BX\re\second_category.xlsx')
        # categorylist 是此次要建立词典的行业列表
        categorylist = list(df_category['second_cate'])
        fmcatelist = list(fm_category['category'])
        wyyblist = list(set(categorylist).difference(set(fmcatelist)))
        # 测试数据
#        df_test = pd.read_excel(r'C:\Users\baixing\Desktop\BX\testdata\testdata_1204.xlsx').reset_index(drop=True)
        df_test = pd.read_excel(r'C:\Users\baixing\Desktop\BX\testdata\yihuitui_words.xlsx').reset_index(drop=True)
                
        # 获取dict
#        df_dict = GetCategoryDict().get_2category_dict(dt=dt)
##        df_dict.to_pickle(r'C:\Users\baixing\Desktop\BX\dict_pkl\df_dict.pkl')
#        df_first_dict = GetCategoryDict().get_first_category_dict(dt=dt)
##        df_first_dict.to_pickle(r'C:\Users\baixing\Desktop\BX\dict_pkl\df_first_dict.pkl')
#        # 过滤dict
#        df_dict = DictBuildMethod().apply_dictionary_build(dt, df_dict, 2)
#        df_first_dict = DictBuildMethod().apply_dictionary_build(dt, df_first_dict, 1)
        # 过滤后再次获取dict
        df_dict = GetCategoryDict().get_2category_dict(dt='20201222')
#        df_dict.to_pickle(r'C:\Users\baixing\Desktop\BX\dict_pkl\df_dict.pkl')
        df_first_dict = GetCategoryDict().get_first_category_dict(dt='20201222')
        # 生成2级分词词典
        CutWords().gene_own_dict(df_dict)
        # 生成1级分词词典
        CutWords().gene_own_firstdict(df_first_dict)
        #测试集 
        df_testdone = apply_test(df_test=df_test,
               df_first=df_first_dict,
               df_dict=df_dict,
               df_manual=df_manual,
               df_manual_second=df_manual_second,
               flist=flist,
               dt=dtt)
    else:
        #行业判定（/测试）
        print('********行业判定*********')
        category_determine(sen='固态二氧化碳',
                       flist=flist,
                       df_first_manual=df_manual,
                       df_second_manual=df_manual_second)
