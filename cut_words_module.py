# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 10:06:26 2020

@author: baixing
"""
import jieba
from tqdm import tqdm 
import os

class CutWords:
    def __init__(self):
        dir_path = os.path.dirname(os.path.abspath(__file__))
        self.seconddicts = os.path.join(dir_path, 'own_cutdicts/owndict.txt')
        self.firstdicts = os.path.join(dir_path, 'own_cutdicts/own_first_dict.txt')

    def fs(self, x, code):
        if code == '1':
            return jieba.lcut_for_search(x.upper())
        else:
            return jieba.lcut(x.upper())
    # 生成1级分词词典
    def gene_own_dict(self, df):
        """
        

        Parameters
        ----------
        df : TYPE       dataframe
            DESCRIPTION.    1级行业词根dataframe

        Returns
        -------
        int 
            DESCRIPTION.

        """
        # 生成分词词典（txt）
        f = open(self.seconddicts, 'w', encoding="utf-8")
        for i in tqdm(range(len(df))):
            for j in range(len(df['cate_dict'][i])):
                f.write(df['cate_dict'][i]['word'][j] + ' ')
                #暂时按频数来
                f.write(str(df['cate_dict'][i]['freq'][j]) + '\n')
        print('********唯一词典写入分词词典生成done！********')
        f.close()
        return 0
    # 生成2级分词词典
    def gene_own_firstdict(self, df):
        # 生成分词词典（txt）
        f = open(self.firstdicts, 'w', encoding="utf-8")
        for i in tqdm(range(len(df))):
            for j in range(len(df['cate_dict'][i])):
                f.write(df['cate_dict'][i]['word'][j] + ' ')
                #保证比jieba自带词典频数大才行 先按频数来
                f.write(str(df['cate_dict'][i]['freq'][j]) + '\n')
        print('********唯一词典写入分词词典生成done！********')
        f.close()
        return 0
