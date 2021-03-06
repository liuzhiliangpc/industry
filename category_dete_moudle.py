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
from utils import B2B_tatoo
from utils import get_first_category
from city_clean_module import CountryClean
from apply_muliti_process import cutwords_with_dic
from first_match_module import first_category_func
from match_rules_moudle import match_second_func
from location_tagger import LocationTagger
from newword_detection import get_baike_result
import os

import sys
import importlib
# importlib.reload(sys)

# 行业判定
def category_determine(sen,
                       flist,
                       df_first_manual,
                       df_second_manual):
    """
    

    Parameters
    ----------
    sen : TYPE  string
        DESCRIPTION.    待判定的句子
    flist : TYPE    dict
        DESCRIPTION.    1级2级对应关系
    df_first_manual : TYPE  dataframe
        DESCRIPTION.    1级人工规则
    df_second_manual : TYPE     dataframe
        DESCRIPTION.    2级人工规则

    Returns
    -------
    TYPE list
        DESCRIPTION. [1级行业结果， 2级行业结果]

    """
    # 获取1级词典——dataframe
    p = os.getcwd()
    df_first_dict= pd.read_pickle(p + '\\' + 'dict_pkl\\df_first_dict.pkl')
    # 获取二级词典——dataframe
    df_dict= pd.read_pickle(p + '\\' + 'dict_pkl\\df_dict.pkl')
    starts = time.clock()
    # 国家识别并替换
    sen = CountryClean().country_clean(sen)
    new = pd.DataFrame({'word': sen, 'result_ind': 999}, index=[1])
    jiebadicts_path1 = p + '//' + 'own_cutdicts//own_first_dict.txt'
    jiebadicts_path2 = p + '//' + 'own_cutdicts//owndict.txt'
    # 一级行业分词
    cut_phrase1 = list(cutwords_with_dic(new, jiebadicts_path1)['cut_phrase'])[-1]
    # 2级行业分词
    cut_phrase2 = list(cutwords_with_dic(new, jiebadicts_path2)['cut_phrase'])[-1]
    # 一级行业判定
    # rlist, rlist2 是list [行业名称， 命中词list， 交叉行业List， 概率list, 判定依据]
    rlist = first_category_func(sen, cut_phrase1, df_first_dict, df_first_manual)
    # 二级行业判定
    rlist2 = match_second_func(sen, cut_phrase2, df_dict, df_second_manual)
    # 如果1级和2级的结果都是新词，则提前百科信息进行行业判定
    if rlist[0] == '其他服务' and rlist2[0] == '':
        print('****发现行业新词！提取百科信息*****')
        df_ = get_baike_result(df=new, df_first=df_first_dict, df_dict=df_dict, \
                               df_manual=df_first_manual, \
                                   df_second_manual=df_second_manual, flist=flist)
        result_list = df_['baike_result'][0]
        ends = time.clock()
        print("判定耗时：{}s".format((ends - starts)))
        return result_list
    # 如果2级行业判定结果是B2B,则直接按2级所对应的1级作为1级行业行业判定结果
    elif B2B_tatoo(rlist2[0]) == 'B2B':
        ends = time.clock()
        print("判定耗时：{}s".format((ends - starts)))
        return [rlist2[0], get_first_category(rlist2[0], flist)]
    # 如果以上情况均不是，则直接返回行业判定的1级和2级结果
    else:
        ends = time.clock()
        print("判定耗时：{}s".format((ends - starts)))
        return[rlist[0], rlist2[0]]


# main()
if __name__ == '__main__':
    ''' 获取人工规则
            一级规则 —— df_manual
            二级规则 —— df_manual_second
        '''
    # 行业关系（1级 2级）
    path = os.getcwd()
    df_cate = pd.read_excel(path + '//' + 'dicts/cate_info.xlsx').reset_index(drop=True)
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
    # todo
    # '1' 代表测试； 不是‘1’的都是行业判断
    if str(input()) == '1':
        dt = time.strftime("%Y%m%d", time.localtime())
        dtt = time.strftime("%Y%m%d%H%M%S", time.localtime())
        # 数据获取
        # data = Data('1', category, 100000, db)
        # df_category = data.get_category()
        # 测试数据
        df_test = pd.read_excel(path + '/' + 'dict_pkl/yihuitui_words.xlsx').reset_index(drop=True)
                
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
        print(category_determine(sen='聚苯板',
                       flist=flist,
                       df_first_manual=df_manual,
                       df_second_manual=df_manual_second))
