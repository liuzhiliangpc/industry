# -*- coding: utf-8 -*-
"""
Created on Mon Nov 23 15:53:04 2020

@author: baixing
"""
import os
import pandas as pd
import time
from tqdm import tqdm
from cut_words_module import CutWords
from utils import change_category

from apply_muliti_process import multi_cut_with_dic
from apply_muliti_process import multi_match_category
from first_match_module import match_first_category
from newword_detection import get_baike_result
from utils import B2B_tatoo
from utils import get_first_category


def apply_test(df_test,
               df_first,
               df_dict,
               df_manual,
               df_manual_second,
               flist,
               dt):
    """
    

    Parameters
    ----------
    df_test : TYPE      dataframe 
        DESCRIPTION.    测试集
    df_first : TYPE     dataframe
        DESCRIPTION.    1级行业词典
    df_dict : TYPE      dataframe
        DESCRIPTION.    2级行业词典
    df_manual : TYPE    dataframe
        DESCRIPTION.    2级人工规则
    df_manual_second : TYPE  dataframe    
        DESCRIPTION.    1级人工规则
    flist : TYPE        dict
        DESCRIPTION.    1级2级对应关系
    dt : TYPE           string
        DESCRIPTION.    日期时间

    Returns
    -------
    None.

    """
    '''标识意义
    一级行业明细 —— ftestresult
    二级行业明细 —— testresult
    data_num —— 数据总数
    f_success_num —— 判定出一级行业结果的数据数量
    f_right_num —— 判定出的一级行业结果是准确的 数据数量
    success_rate —— 覆盖率
    first_acu_rate —— 一级行业判定准确率
    amb_acu_rate —— B2B粒度判定准确率
    success_num —— 判定出2级行业结果的数据数量
    right_num —— 判定出的2级行业结果是准确的 数据数量
    amb_right_num —— 判定出的B2B结果是准确的 数据数量
    acu_rate —— 2级行业判定准确率
    '''
    p = os.getcwd()
    jiebadict1 = p + '//' + 'own_cutdicts//own_first_dict.txt'
    jiebadict2 = p + '//' + 'own_cutdicts//owndict.txt'
    pathv = p + '//' + 'log'
    starts = time.clock()
    # 去重
    df_test = df_test.drop_duplicates(['word']).reset_index(drop=True)
    df_test['second_category'] = df_test.apply(lambda r: change_category(r.second_category), axis=1)
    # 多进程分词
    print('********多进程1级分词**********')
    # 按 1级 词典分词
    dfirst = multi_cut_with_dic(df_test, jiebadict1, 8)
    print('********1级分词Done!**********')
    #多进程匹配词典-一级
    print('********多进程1级行业判定**********')
    df_match = multi_match_category(df=dfirst, df_dic=df_first, df_manual=df_manual, mode=1, job_num=8)
    print('********1级行业判定Done！**********')
    df_match['f_result'] = df_match.apply(lambda r: r.rlist[0], axis=1)
    df_match['hit_words'] = df_match.apply(lambda r: r.rlist[1], axis=1)
    df_match['cross_set'] = df_match.apply(lambda r: r.rlist[2], axis=1)
    df_match['plist'] = df_match.apply(lambda r: r.rlist[3], axis=1)
    df_match['source'] = df_match.apply(lambda r: r.rlist[-1], axis=1)
    df_match = df_match.rename(columns={'rlist': 'rlist1'})
    df_test = df_match.copy()
    ends = time.clock()
    print("一级测试耗时平均：{}s/条".format((ends - starts)/len(df_test)))
#    df_test.to_csv('dftest_first_done_last.csv', encoding='utf-8-sig')
    # 按 2级 词典分词
    print('********多进程2级分词**********')
    df_test = df_test.rename(columns={'cut_phrase': 'cut_phrase1'})
    dff = multi_cut_with_dic(df_test, jiebadict2, 8)
    print('********2级分词Done！**********')
    #多进程匹配词典-2级
    df_dict['2_category'] = df_dict.apply(lambda r: change_category(r['2_category']), axis=1)
#    dff.to_pickle('dff.pkl')
    print('********多进程2级行业判定**********')
    df_match_s = multi_match_category(dff, df_dict, df_manual_second, 2, 8)
    print('********多进程2级行业判定Done！**********')
    print("2级测试耗时平均：{}s/条".format((ends - starts)/len(df_test)))
    df_match_s['result'] = df_match_s.apply(lambda r: r.rlist[0], axis=1)
    df_match_s['second_hit_words'] = df_match_s.apply(lambda r: r.rlist[1], axis=1)
    df_match_s['second_cross_set'] = df_match_s.apply(lambda r: r.rlist[2], axis=1)
    df_match_s['second_plist'] = df_match_s.apply(lambda r: r.rlist[3], axis=1)
    df_match_s['source_2'] = df_match_s.apply(lambda r: r.rlist[-1], axis=1)
    df_test = df_match_s.copy()
    # 新词
    df_test_baike = get_baike_result(df=df_test, df_first=df_first, df_dict=df_dict, df_manual=df_manual, df_second_manual=df_manual_second, flist=flist)
    df_test_baike = df_test_baike[['word', 'baike_words_list', 'baike_json_list', \
                   'baike_first_result', 'baike_second_result']]
    # merge
    df_test = pd.merge(df_test, df_test_baike, on=['word'], how='left').\
        reset_index(drop=True)
    # 优先B2B:若某个词的1级判定出来是非B2B,而其2级判定出来却是B2B中的行业，那么我们按照2级去修正1级的行业
    df_test['f_result'] = df_test.apply(lambda r: get_first_category(r.result, flist) \
                                        if B2B_tatoo(str(r.f_result)) != 'B2B' \
                                            and B2B_tatoo(str(r.result)) == 'B2B' else r.f_result, axis=1)
    # 用baike的判定结果填充未能判定的结果
    df_test['f_result'] = df_test.apply(lambda r: r.baike_first_result if \
                                        r.f_result == '其他服务' else r.f_result, \
                                            axis=1)
    df_test['result'] = df_test.apply(lambda r: r.baike_second_result if \
                                        r.result == '' else r.result, \
                                            axis=1)    
    # 1级或2级若有属于B2B的，就是B2B
    df_test['result_ind'] = df_test.apply(lambda r: 'B2B' if (B2B_tatoo(str(r.result)) == 'B2B' or \
              B2B_tatoo(str(r.f_result)) == 'B2B') else B2B_tatoo(r.f_result), axis=1)
    df_test['f_success_ind'] = df_test.apply(lambda r: 1 if r.f_result \
                                             == r.first_category else 0, axis=1)
    df_test['success_ind'] = df_test.apply(lambda r: 1 if r.
                                           result == r.second_category else 0, axis=1)
    df_test['amb_success_ind'] = df_test.apply(lambda r: 1 if r.
                                           result_ind == r.B2B_ind else 0, axis=1)
    df_test.to_csv('dftest_second_done_{}.csv'.format(dt), encoding='utf-8-sig')
    '''统计结果
    一级行业明细 —— ftestresult
    二级行业明细 —— testresult
    data_num —— 数据总数
    f_success_num —— 判定出一级行业结果的数据数量
    f_right_num —— 判定出的一级行业结果是准确的 数据数量
    success_rate —— 覆盖率
    first_acu_rate —— 一级行业判定准确率
    amb_acu_rate —— B2B粒度判定准确率
    success_num —— 判定出2级行业结果的数据数量
    right_num —— 判定出的2级行业结果是准确的 数据数量
    amb_right_num —— 判定出的B2B结果是准确的 数据数量
    acu_rate —— 2级行业判定准确率
    '''
    # 一级行业
    ftestresult = {}
    for key, group in tqdm(df_test.groupby('first_category')):
        data_num = len(group)
        f_success_num = len(group[group['f_result'] != '其他服务'])
        f_right_num = len(group[group['f_success_ind'] == 1])
        amb_right_num = len(group[group['amb_success_ind'] == 1])
        amb_success_num = len(group[group['result_ind'] != 999])
        success_rate = round((f_success_num / data_num) * 100, 3)
        try:
            first_acu_rate = round((f_right_num / f_success_num) * 100, 3)
        except:
            first_acu_rate = 0
        try:
            amb_acu_rate = round((amb_right_num / amb_success_num) * 100, 3)
        except:
            amb_acu_rate = 0
        print('{} 一级行业测试data共{}条'.format(key, data_num) + ' ' +
              '判定成功data共{}条'.format(f_success_num))
        print('{} 一级行业判定成功率：{}%'.format(key,
              round(success_rate)) + '\t'
              + '准确率：{}%'.format(first_acu_rate) + '\t' + 'B2B粒度准确率：{}%'.format(amb_acu_rate))
        ftestresult[key] = [data_num, f_success_num, success_rate, first_acu_rate, amb_acu_rate]
    print('一级行业总成功率：{}%'.format(round((len(df_test[df_test['f_result'] != '其他服务'])
          /len(df_test)) * 100, 3)))
    print('一级行业总准确率：{}%'.format(round((len(df_test[df_test['f_success_ind']
          == 1])/ len(df_test[df_test['f_result'] != '其他服务'])) * 100, 3)))
    print('B2B粒度总准确率：{}%'.format(round((len(df_test[df_test['amb_success_ind']
                                               == 1]) / len(df_test[df_test['result_ind'] != 999])) * 100, 3)))
    
    ftestresult['total'] = [len(df_test), len(df_test[df_test['f_result'] != '其他服务']),
                           round((len(df_test[df_test['f_result'] != '其他服务'])/len(df_test)) * 100, 3),
                           round((len(df_test[df_test['f_success_ind'] == 1]) /
                                  len(df_test[df_test['f_result'] != '其他服务'])) * 100, 3), 
                                  round((len(df_test[df_test['amb_success_ind'] == 1]) /
                                  len(df_test[df_test['result_ind'] != 999])) * 100, 3)]
    
    df_ftestresult = pd.DataFrame.from_dict(ftestresult, orient='index', columns=[
        'data_num', 'success_num', 'cover_rate', 'first_accurcy', 'B2B_accuracy']).reset_index().rename(
        columns={'index': 'first_category'})
    df_ftestresult.to_csv(pathv+'/'+str(dt)+'一级测试.csv', encoding='utf-8-sig')
    # 2级测试结果明细
    testresult = {}
    for key, group in tqdm(df_test.groupby('second_category')):
        data_num = len(group)
        success_num = len(group[group['result'] != ''])
        right_num = len(group[group['success_ind'] == 1])
        success_rate = round((success_num / data_num) * 100, 3)
        amb_right_num = len(group[group['amb_success_ind'] == 1])
        amb_success_num = len(group[group['result_ind'] != 999])
        try:
            acu_rate = round((right_num / success_num) * 100, 3)
        except:
            acu_rate = 0
        try:
            amb_acu_rate = round((amb_right_num / amb_success_num) * 100, 3)
        except:
            amb_acu_rate = 0
        print('{} 行业测试data共{}条'.format(key, data_num) + ' ' +
              '判定成功data共{}条'.format(success_num))
        print('{} 行业判定成功率：{}%'.format(key,
              round(success_rate)) + '\t'
              + '准确率：{}%'.format(acu_rate) + '\t' + 'B2B粒度准确率：{}%'.format(amb_acu_rate))
        testresult[key] = [data_num, success_num, success_rate, acu_rate, amb_acu_rate]
    
    testresult['total'] = [len(df_test), len(df_test[df_test['result'] != '']),
                           round((len(df_test[df_test['result'] != ''])/len(df_test)) * 100, 3),
                           round((len(df_test[df_test['success_ind'] == 1]) /
                                  len(df_test[df_test['result'] != ''])) * 100, 3), 
                                  round((len(df_test[df_test['amb_success_ind'] == 1]) /
                                  len(df_test[df_test['result_ind'] != 999])) * 100, 3)]
    df_testresult = pd.DataFrame.from_dict(testresult, orient='index', columns=[
        'data_num', 'success_num', 'cover_rate', 'second_accurcy', 'B2B_accuracy']).reset_index().rename(
        columns={'index': 'category'})
    df_testresult.to_csv(pathv+'/'+str(dt)+'2级测试.csv', encoding='utf-8-sig')
    del(df_test['rlist'], df_test['rlist1'], dfirst, dff, df_match, df_match_s, 
        df_testresult, df_ftestresult)
    return df_test

    
