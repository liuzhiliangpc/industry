# -*- coding: utf-8 -*-
"""
Created on Thu Nov 19 13:42:31 2020

@author: baixing
"""

import os
import pandas as pd
from tqdm import *
import re


class CleanWyWords:
    def __init__(self):
        dir_path = os.path.dirname(os.path.abspath(__file__))
        self.fp = os.path.join(dir_path, 'B2Bdict')

    def get_5118words_cleaned(self):
        # warnings.filterwarnings('ignore')
        pathDir = os.listdir(self.fp)
        wfile = [file for file in pathDir if '.' in file]
        cflie = [file for file in pathDir if '.' not in file]
        D = {}
        clist = []
        for w in tqdm(wfile):
            if 'csv' in w:
                df = pd.read_csv(open(self.fp + '\\' + w)).rename(columns={"根词名称": "keyword",
                                                                           '行业词数量': 'catewords_num',
                                                                           '长尾词数量': 'longwords_num'})
                df['keyword'] = df.apply(lambda r: str(r.keyword).upper(), axis=1)
                D[w.replace('词库根词数据.csv', '')] = df
                clist.append(w.replace('词库根词数据.csv', ''))
            else:
                df = pd.read_excel(self.fp + '\\' + w).rename(columns={"根词名称": "keyword",
                                                                       '行业词数量': 'catewords_num',
                                                                       '长尾词数量': 'longwords_num'})
                df['keyword'] = df.apply(lambda r: str(r.keyword).upper(), axis=1)
                D[w.replace('词库根词数据.xlsx', '')] = df
                clist.append(w.replace('词库根词数据.xlsx', ''))
        for f in tqdm(cflie):
            templis = []
            files = os.listdir(self.fp + '\\' + f)
            for c in files:
                if 'csv' in c:
                    df = pd.read_csv(open(self.fp + '\\' + f + '\\' + c)).rename(columns={"根词名称": "keyword",
                                                                                       '行业词数量': 'catewords_num',
                                                                                       '长尾词数量': 'longwords_num'})
                    templis.append(df)
                else:
                    df = pd.read_excel(self.fp + '\\' + f + '\\' + c).rename(columns={"根词名称": "keyword",
                                                                                      '行业词数量': 'catewords_num',
                                                                                      '长尾词数量': 'longwords_num'})
                    templis.append(df)
            df = pd.concat(templis).reset_index(drop=True)
            df['keyword'] = df.apply(lambda r: str(r.keyword).upper(), axis=1)
            D[f] = df
            clist.append(f)
        df_c = pd.DataFrame.from_dict(D, orient='index', columns=['df_words']).reset_index().rename(
            columns={'index': '2_category'})
        for i in tqdm(range(len(df_c))):
            dfs = df_c['df_words'][i]
            # 单个字默认无法作为关键词
            dfs['keyword'] = dfs.apply(lambda r: r.keyword if len(r.keyword) >= 2 else '0', axis=1)
            # 数字不作为关键词根
            dfs['keyword'] = dfs.apply(lambda r: r.keyword if len(re.findall(r"\d+", r.keyword)) == 0 else '0', axis=1)
            # 含字母的不作为关键词    [chr(i) for i in range(97,123)]
            dfs['keyword'] = dfs.apply(lambda r: '0' if r.keyword[0] in [chr(i).upper() for i in range(97, 123)] +
                                                        [chr(i) for i in range(97, 123)] else r.keyword, axis=1)
            dfss = dfs.loc[dfs['keyword'] != '0', :].reset_index(drop=True)
            df_c['df_words'][i] = dfss
        return df_c
if __name__ == '__main__':
    c = CleanWyWords()
    dffff = c.get_5118words_cleaned()
    print('Done!')
