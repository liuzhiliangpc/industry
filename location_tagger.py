#!/usr/bin/env python3
# encoding: utf-8

import os
import json
import jieba
import re

# 地名有歧义情形下，自动关联为热点地区
myumap = {'南关区': '长春市',
    '南山区': '深圳市',
    '宝山区': '上海市',
    '普陀区': '上海市',
    '朝阳区': '北京市',
    '河东区': '天津市',
    '白云区': '广州市',
    '西湖区': '杭州市',
    '铁西区': '沈阳市',
    '鼓楼区': '南京市'}

class LocationTagger(object):

    def __init__(self):
        dir_path = os.path.dirname(os.path.abspath(__file__))
        loc_set_path = os.path.join(dir_path, 'dicts/loc.txt')
        loc_tree_path = os.path.join(dir_path, 'dicts/loc_tree.txt')

        jieba.load_userdict(loc_set_path)
        self._jieba = jieba
        self.loc_dict = self.loadLocSet(loc_set_path)
        self.id2loc, self.name2ids, self.pre2ids = self.loadLocTree(loc_tree_path)

    def loadLocSet(self, path):
        loc_dict = set()
        with open(path, 'r', encoding='utf-8') as fin:
            for line_id,line in enumerate(fin):
                w = line.strip()
                if w in ['社区','街道','新区','市区','通道','公安','延长','开发区',
                           '高明','会同','平安','比如','资源','凤凰','安康','延寿','城口',
                         '中方','合作','同心','紫阳','和平','互助','西区','正安','合计','经济开发区']:
                    continue
                loc_dict.add(w)
        return loc_dict

    def loadLocTree(self, path):
        id2loc = {}
        name2ids = {}
        pre2ids = {}
        with open(path, 'r', encoding='utf-8') as fin:
            for line_id, line in enumerate(fin):
                line = line.strip().split('\t')
                line = [l for l in line if l]
                if line:
                    lid, lname, lparent, llevel, lstatus = line
                    if lid == lparent:
                        continue
                    id2loc[lid] = {'name':lname, 'parent':lparent, 'level':llevel}

                    if lname not in name2ids:
                        name2ids[lname] = []
                    name2ids[lname].append(lid)

                    if lname[0] not in pre2ids:
                        pre2ids[lname[0]] = []
                    pre2ids[lname[0]].append(lid)
        return id2loc, name2ids, pre2ids

    def getLocWord(self, text):
        text = text.replace('政府', ' ')
        text = text.replace('当局', ' ')
        ws = self._jieba.lcut(text)
        locs = {}
        for w in ws:
            if w in self.loc_dict:
                if w not in locs:
                    locs[w]=0
                locs[w]+=1
        locs = sorted(locs.items(), key=lambda kv:kv[1], reverse=True)[:4]
        locs = [l for l in locs if l[1] > 0]
        return locs

    def traceLoc(self, node, node_weight):
        path = [(node['name'], node['level'])]
        key = [node['name']]
        pre = node
        while pre['parent'] != '0':
            pre = self.id2loc[pre['parent']]
            path.append((pre['name'], pre['level']))
            key.append(pre['name'])
        path = path[::-1]
        key = key[::-1]
        ret = {'path':path, 'key':''.join(key), 'weight':node_weight}
        return ret

    def combinePaths(self, paths):
        p_len = len(paths)
        del_key = []
        
        for i in range(p_len):
            for j in range(i+1, p_len):
                # if i == j:
                #     continue
                if i in del_key or j in del_key:
                    continue
                
                p1,p2 = paths[i], paths[j]
                p1k, p2k = p1['key'], p2['key']
                #print(p1k, p2k, i, j)
                if p1k == p2k:
                    del_key.append(j)
                    p1['weight']+=p2['weight']
                elif p1k.startswith(p2k):
                    del_key.append(j)
                    p1['weight']+=p2['weight']
                elif p2k.startswith(p1k):
                    del_key.append(i)
                    p2['weight']+=p1['weight']
                else:
                    pass
        npath = []
        for i in range(p_len):
            if i not in del_key:
                npath.append(paths[i])
        return npath

    def tagArticle(self, title):
        text = title
        locs = self.getLocWord(text)
        # print(locs)
        if locs:
            temp_locs = [{i[0]:i[1]} for i in locs]
        else:
            temp_locs = locs
        geo_location = {'geo_nouns':temp_locs}
        # geo_location
        try:
            if locs:
                loc_paths = []

                for item in locs:
                    lname = item[0]
                    lweight = item[1]
                    node_ids = self.name2ids.get(lname, [])

                    if len(node_ids) == 1:
                        node = self.id2loc[node_ids[0]]
                        #print(node)
                        path = self.traceLoc(node, lweight)
                        loc_paths.append(path)
                    elif len(node_ids) == 0:
                        node_ids = self.pre2ids.get(lname[0], [])
                        pre_nodes = [self.id2loc[idx] for idx in node_ids]
                        ok_pre_nodes = [node for node in pre_nodes if node['name'].startswith(lname)]
                        #print(ok_pre_nodes)
                        ok_node = None
                        if ok_pre_nodes:
                            ok_pre_nodes = sorted(ok_pre_nodes, key=lambda kv:int(kv['level']))
                            ok_node = ok_pre_nodes[0]
                            path = self.traceLoc(ok_node, lweight)
                            loc_paths.append(path)
                    elif len(node_ids) > 1:
                        nodes = [self.id2loc[idx] for idx in node_ids]
                        for n in nodes:
                            #print(n)
                            path = self.traceLoc(n, lweight)
                            loc_paths.append(path)
                #print(locs)
                npaths = self.combinePaths(loc_paths)
                npaths = sorted(npaths, key=lambda kv:kv['weight'], reverse=True)

                #print(loc_paths)
                # print(npaths)
                best_path = None
                if npaths:
                    if len(npaths) > 1:
                        max_weight = npaths[0]['weight']
                        # 等权重的前n项优先选择热门地点
                        common_ids = []
                        common_areas = {}
                        for i in range(len(npaths)):
                            if npaths[i]['weight'] == max_weight:
                                common_ids.append(i)
                                common_areas[i] = [temp[0] for temp in npaths[i]['path']]
                        # print(common_ids)
                        # print(common_areas)
                        flag = False # 标记是否找到热门路径
                        for hot_county in myumap.keys():
                            for j in common_ids:
                                if hot_county in common_areas[j] and myumap[hot_county] in common_areas[j]:
                                    best_path = npaths[j]
                                    flag = True
                                    break
                            if flag:  # 提前返回结果
                                break
                        if not best_path:
                            best_path = npaths[0]
                    else:
                        best_path = npaths[0]

                if best_path:
                    loc_level_dict = {v: k for k, v in best_path['path']}
                    loc1 = loc_level_dict.get('1', '')
                    loc2 = loc_level_dict.get('2', '')
                    loc3 = loc_level_dict.get('3', '')
                    geo_location['locations'] = {'state': loc1, 'city': loc2, 'county': loc3}
        except Exception as e:
            pass
        return geo_location
    
    def clean_sentence_city(self, sen):
        ret = self.tagArticle(sen)
        # 检测出的地名列表 ret['geo_nouns']
        citys = [[]+list(cd.keys()) for cd in ret['geo_nouns']]
        if len(citys) > 0:
            print(citys[-1])
            for c in citys[-1]:
                sen = sen.replace(str(c), '')
                return sen
        else:
            return sen

if __name__ == '__main__':
    
    title = "赵王庙会"
    import time
    time1 = time.time()
    # for i in range(1000):
    print(clean_sentence_city(title))


