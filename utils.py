# -*- coding: utf-8 -*-
"""
Created on Tue Dec 29 11:52:24 2020

@author: baixing
"""

def split_dataframe(df, nums):
    dfls = []
    for i in range(nums):
        if i == 0:
            dfls.append(df[: int(len(df) * ((i + 1)/nums))])
        elif i == nums-1:
            dfls.append(df[int(len(df) * (i/nums)):])
        else:
            dfls.append(df[int(len(df) * (i/nums)): int(len(df) * ((i + 1)/nums))])
    return dfls

def change_category(ors):
    if ors == '早教':
        return '婴幼儿教育'
    elif ors == '汽配':
        return '汽车配件'
    elif ors == '驾校':
        return '驾校服务'
    elif '/' in ors:
        ors = ors.replace('/', '')
        return ors
    else:
        return ors
    
def B2B_tatoo(s):
    if s in ['仪器仪表', '冶金矿产', '橡塑涂料', '线缆照明', '五金配件', '网络安防',
    '水暖电工', '家居家纺', '化工能源', '钢材', '电子元器件', '礼品定制', '鲜花盆景', '建材服务', '农林牧副渔', \
    '工业设备'] or s in ['工业制造', '能源材料', '生活消费']:
        return 'B2B'
    elif s == '' or s == '其他服务':
        return 999
    else:
        return '本地服务'
    
def get_first_category(second_category, cate_dict):
    for k, v in cate_dict.items():
        if second_category in v:
            return k
    print('未见过此行业名称：{}'.format(second_category))
    return 0


    