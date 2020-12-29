# -*- coding: utf-8 -*-
"""
Created on Thu Dec 24 11:29:24 2020

@author: baixing
"""

import pandas as pd
import jieba
import os
import urllib
from bs4 import BeautifulSoup
import traceback
import time
import re
import data_utils
import logging
from utils import B2B_tatoo
from utils import get_first_category
from apply_muliti_process import cutwords_with_dic
from first_match_module import first_category_func
from match_rules_moudle import match_second_func

logger = logging.getLogger(__name__)
local_file = os.path.split(__file__)[-1]
logging.basicConfig(
    format='%(asctime)s : %(filename)s : %(funcName)s : %(levelname)s : %(message)s',
    level=logging.INFO)

# 生成对应词语的百科网址 
def url_parse(url, word):
    """
    

    Parameters
    ----------
    url : TYPE  字符串
        DESCRIPTION.    百科头部网址 https://baike.baidu.com/item/
    word : TYPE     字符串
        DESCRIPTION.    要搜索的词

    Returns
    -------
    url : TYPE  网址字符串
        DESCRIPTION.    全网址

    """
    word = urllib.parse.quote(word)
    url = url.format(a=word)
    return url

# 获取文本内容
def get_text_from_tag(tag):
    return tag.get_text()

# 筛选百科网页上的关键信息
def get_info_box(soup):
    """
    

    Parameters
    ----------
    soup : TYPE     百科网页文本
        DESCRIPTION.

    Raises
    ------
    Exception
        DESCRIPTION. 长度不符，即可能获取文本出错

    Returns
    -------
    new_dict : TYPE     dict
        DESCRIPTION.    包含：中文名、英文名、别称（又名、学名等）、用途、领域

    """
    new_dict = dict()
    # base_info
    base_info = soup.find(attrs={"class": "basic-info cmn-clearfix"})
    if base_info is not None:
        all_name = base_info.find_all(attrs={"class": "basicInfo-item name"})
        all_value = base_info.find_all(attrs={"class": "basicInfo-item value"})
        if len(all_name) != len(all_value):
            logging.error('name and value not equal')
            raise Exception('name and value not equal')
        info_size = len(all_name)
        for i in range(info_size):
            name, value = all_name[i], all_value[i]
            name, value = name.get_text(strip=True).replace(u'\xa0', ''), value.get_text(strip=True)
            new_dict[name] = value
    return new_dict

# 获取网页上该词的简介内容
def get_description(soup):
    new_dict = dict()
    desc_label = soup.select('meta[name="description"]')
    if not desc_label: new_dict['description'] = ''
    else:
        description = soup.select('meta[name="description"]')[0].get('content')
        new_dict['description'] = description
    return new_dict


def baike_synonym_detect(word_code_list):
    p = os.getcwd()
    out_path = p + '\\' + 'output\\baike_synonym.txt'
    if os.path.exists(out_path):
        os.remove(out_path)

    multi_thread_search(word_code_list)


# @thread_utils.run_thread_pool(50)
def multi_thread_search(params):
    baike_search(params)


def baike_search(params):
    key_word, word_code = params
    key_word = data_utils.remove_parentheses(key_word)
    p = os.getcwd()
    out_path = p + '\\' + 'output\\baike_synonym.txt'
    file = open(out_path, 'a', encoding='utf8')
    try:
        base_url = 'https://baike.baidu.com/item/{a}'
        url = url_parse(base_url, key_word)
        print('url:' + str(url))
        response = urllib.request.urlopen(url)
        data = response.read()
        soup = BeautifulSoup(data)
        item_json = dict()

        des_dict = get_description(soup)
        item_json.update(des_dict)

        info_box_dict = get_info_box(soup)
        item_json.update(info_box_dict)

        synonym_list = get_synonym(item_json)
        if len(synonym_list) > 0:
            write_line = word_code + '\t' + key_word + '\t' + '|'.join(synonym_list) + '\n'
            file.write(write_line)

        logger.info(' input word = {a}, find {b} synonyms...'.format(a=key_word,b=len(synonym_list)))
        return synonym_list, item_json
    except Exception:
        logger.error(' input word = {a}, occur an error!'.format(a=key_word))
        traceback.print_exc()
    time.sleep(0.1)

# 获取词语的同义词信息
def get_synonym(baike_json):
    info_key = ['别称', '别名', '又名', '英文名称', '又称', '英文别名', '西医学名', '用途']
    pattern_list = ['俗称', '简称', '又称']

    info_set = set()
    for key in info_key:
        if key in baike_json:
            value = baike_json[key]
            if value[-1] == '等':
                value = value[:-1]
            value = seg(value)
            info_set = info_set | set(value)

    description = baike_json['description']
    for p in pattern_list:
        pattern = r'' + p
        result = re_match(pattern, description)
        for r in result:
            value = seg(r)
            info_set = info_set | set(value)

    info_set = [s.strip().replace(u'\xa0', '').replace('"', '').replace('“', '').replace('”', '').
                    replace('（', '').replace('：', '')   for s in info_set]
    return info_set

# 删除文本中的一些非法字符串
def re_match(word, text):
    p_str = r'{a}(.+?)[，。）\s)（(、]'.format(a=word)
    pattern = re.compile(p_str)
    result = re.findall(pattern, text)
    return result


def seg(text):
    segment = [',', '，', '、', '；']
    current_seg = '&&'
    for seg in segment:
        if seg in text:
            current_seg = seg

    return text.split(current_seg)

def ty_words_format(word_list):
    ty_lis = []
    json_lis = []
    for w in word_list:
        lis, json = baike_search((w, str(w)))
        if len(lis) != 0:
            ty_lis.append(lis)
        json_lis.append(json)
    return ty_lis, json_lis

def baikewords_match_func(json_list, 
                          df_first, 
                          df_dict, 
                          df_manual, 
                          df_second_manual,
                          flist):
    p = os.getcwd()
    jiebadicts_path1 = p + '//' + 'own_cutdicts//own_first_dict.txt'
    jiebadicts_path2 = p + '//' + 'own_cutdicts//owndict.txt'
    for json in json_list:
        for k, v in json.items():
            new = pd.DataFrame({'word': v}, index=[1])
            # 一级行业分词
            cut_phrase1 = list(cutwords_with_dic(new, jiebadicts_path1)['cut_phrase'])[-1]
            # 2级行业分词
            cut_phrase2 = list(cutwords_with_dic(new, jiebadicts_path2)['cut_phrase'])[-1]
            # 一级行业判定
            rlist = first_category_func(v, cut_phrase1, df_first, df_manual)
            # 二级行业判定
            rlist2 = match_second_func(v, cut_phrase2, df_dict, df_second_manual)
            if B2B_tatoo(str(rlist[0])) == 'B2B' or \
                B2B_tatoo(str(rlist2[0])) == 'B2B':
                    if B2B_tatoo(str(rlist2[0])) == 'B2B':
                        first_result = get_first_category(rlist2[0], flist)
                        return [first_result, rlist2[0]]
                    else:
                        return [rlist[0], rlist2[0]]
            else:
                non_B2B_result = [rlist[0], rlist2[0]]
                continue
    try:        
        return non_B2B_result
    except:
        return ['其他服务', '']

def get_baike_result(df, df_first, df_dict, df_manual, df_second_manual, flist):
    # 先获取新词集合
    df = df.loc[df['result_ind'] == 999, :].reset_index(drop=True)
    # 获取同义词和baike相关信息
    df['baike_info'] = 0
    df['baike_info'] = df.apply(lambda r: ty_words_format(([r.word] + r.cut_phrase)), axis=1)
    df['baike_words_list'] = 0
    df['baike_json_list'] = 0
    df['baike_words_list'] = df.apply(lambda r: r.baike_info[0], axis=1)
    df['baike_json_list'] = df.apply(lambda r: r.baike_info[1], axis=1)
    # 根据baike信息进行行业判定
    df['baike_result'] = 0
    df['baike_result'] = df.apply(lambda r: \
                                  baikewords_match_func(r.baike_json_list, 
                                                          df_first,
                                                          df_dict, 
                                                          df_manual, 
                                                          df_second_manual,
                                                          flist), axis=1)
    df['baike_first_result'] = 0
    df['baike_second_result'] = 0
    df['baike_first_result'] = df.apply(lambda r: r.baike_result[0], axis=1)
    df['baike_second_result'] = df.apply(lambda r: r.baike_result[1], axis=1)
    del(df['baike_info'], df['baike_result'])
    return df
