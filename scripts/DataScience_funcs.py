
import pandas as pd
import os


class SimpleSR(object):

    def __init__(self, filename):
        self.file_path = filename

    def SR_method(self):
        file = self.file_path
        secondary_point = pd.read_excel(file, sheet_name='节点表')
        secondary_relation = pd.read_excel(file, sheet_name='关系表')

        # 处理节点名称
        points = secondary_point.iloc[1:, :].reset_index(drop=True)
        points.loc[:, 'name'] = points.loc[:, 'properties'].apply(lambda x: eval(x)['name'])
        points.loc[:, 'labels'] = points.loc[:, 'labels'].apply(lambda x: eval(x)[0])

        # 处理节点类型
        for i in range(len(points)):
            if points.loc[i,'labels'] == 'Company':
                keys = ['分公司', '办事处', '营业部', '支公司']
                if any(key in points.loc[i, 'name'] for key in keys):
                    points.loc[i, '节点类型'] = '分支机构'
                else:
                    points.loc[i,'节点类型'] = '关联企业'
            elif points.loc[i,'labels'] == 'Human':
                points.loc[i,'节点类型'] = '关联人员'
            else:
                points.loc[i,'节点类型'] = '未知'
            if points.loc[0,'ent_id'] in points.loc[:,'id'].values:
                points.loc[points['id'] == points.loc[0,'ent_id'],'节点类型'] = '目标企业'
            else:
                row={'id':points.loc[0,'ent_id'],'ent_id':points.loc[0,'ent_id'],'节点类型':'目标企业'}
                points = points.append(row, ignore_index=True)

        self.points = points

        # 处理节点之间的关系
        start_points = points['name'].to_frame(name='开始节点')
        start_points.index = points['id']

        end_points = points[['name','节点类型']].rename(columns={'name':'结束节点','节点类型':'结束节点类型'})
        end_points.index = points['id']

        # 处理关系
        relations = secondary_relation.iloc[1:,:].reset_index(drop=True)
        relations.loc[:,'占股'] = relations.loc[:,'properties'].apply(lambda x:eval(x)['percent'])
        relations.loc[:,'labels'] = relations.loc[:,'properties'].apply(lambda x:eval(x)['labels'][0])

        for i in range(len(relations)):
            if relations.loc[i,'labels'] == '法人':
                pass
            elif relations.loc[i,'labels'] == '分支机构':
                pass
            elif relations.loc[i,'labels'] == '参股':
                relations.loc[i,'labels'] = '股东'
            else:
                relations.loc[i,'职位'] = relations.loc[i,'labels']
                relations.loc[i,'labels'] = '高管'

        # 处理关系关键代码
        fenzu = relations.groupby([relations['startNode'],relations['endNode']])
        # temp['labels'].value_counts().rename('count').reset_index()
        points_relations = []
        for (a, b), group in fenzu:
            temp = set(group['labels'].tolist())
            if temp == {'法人'}:
                c = '法人'
            elif temp == {'股东'}:
                c = '股东'
                points_relations.append({'startNode':a, 'endNode':b, 'label':c, 'labeltext':'占股比{0:.2f}%'.format(group['占股'].astype(float).sum()*100)})
                continue
            elif '分支机构' in temp:
                c = '分支机构'
            elif temp == {'高管'}:
                c = '高管'
            elif temp == {'法人','股东','高管'}:
                c = '法人股东高管'
            elif temp == {'法人','高管'}:
                c = '法人高管'
            elif temp == {'法人','股东'}:
                c = '法人股东'
            elif temp == {'股东','高管'}:
                c = '股东高管'
            else:
                c = '未知'
            p = group['占股'].astype(float).sum()#.tolist()
            z = '、'.join(group['职位'].dropna().tolist())
        #     print(p,z)  占股比55%/监事
            if p != 0:
                labeltext = '占股比{0:.2f}%/{1}'.format(p*100,z)
            else:
                labeltext = z
            points_relations.append({'startNode':a, 'endNode':b, 'label':c, 'labeltext':labeltext})
        #     points_relations.append({'startNode':a, 'endNode':b, 'label':c, 'labeltext':labeltext, '占股':p, '职位':z})
        points_relations = pd.DataFrame(points_relations)

        final = points_relations.merge(start_points,how='left',left_on='startNode',right_index=True).merge(end_points,how='left',left_on='endNode',right_index=True)

        final.loc[(final['结束节点类型']=='分支机构')&(final['label'].str.contains('高管|法人')),'label'] = '高管'
        final.drop('结束节点类型', axis=1, inplace=True)

        self.final_relations = final

    def to_develop(self):
        self.SR_method()
        out_file = pd.ExcelWriter(os.path.join(filefolder, 'outfile', filename.split('.')[0]+'清洗后.xlsx'), engine='xlsxwriter')
        self.points.drop(['properties'], axis=1).to_excel(out_file, sheet_name='节点', index=0)
        self.final_relations.to_excel(out_file, sheet_name='关系', index=0)
        out_file.save()

    def to_gephi(self):
        self.SR_method()
        # out_file = pd.ExcelWriter(os.path.join(filefolder, 'outfile', filename.split('.')[0] + 'Gephi版.xlsx'), engine='xlsxwriter')
        out_file = pd.ExcelWriter(self.file_path, engine='xlsxwriter')
        self.points['id'] = self.points['name']
        self.points['labels'] = self.points['name']
        # self.points.drop(['properties', 'name'], axis=1).to_excel(out_file, sheet_name='节点', index=0)
        self.points[['id', 'labels', '节点类型']].to_excel(out_file, sheet_name='节点', index=0)
        self.final_relations.rename(columns={'开始节点': 'source', '结束节点': 'target', 'label': '关系类型'}, inplace=True)
        self.final_relations.drop(['endNode', 'startNode', 'labeltext'], axis=1).to_excel(out_file, sheet_name='关系', index=0)
        out_file.save()

        return out_file


class FinalSR(object):

    def __init__(self, filename):
        self.file_path = filename

    def SR_methodPlus(self):
        file = self.file_path
        secondary_point = pd.read_excel(file, sheet_name='节点表').iloc[1:, :].reset_index(drop=True)
        secondary_relation = pd.read_excel(file, sheet_name='关系表').iloc[1:, :].reset_index(drop=True)
        secondary_gaoguan = pd.read_excel(file, sheet_name='高管基本信息').iloc[1:, :].reset_index(drop=True)
        secondary_biangeng = pd.read_excel(file, sheet_name='企业变更信息').iloc[1:, :].reset_index(drop=True)
        secondary_zhaomian = pd.read_excel(file, sheet_name='企业基本信息').iloc[1:, :].reset_index(drop=True)

        secondary_point.loc[:, 'name'] = secondary_point.loc[:, 'properties'].apply(lambda x: eval(x)['name'])
        secondary_point.loc[:, 'labels'] = secondary_point.loc[:, 'labels'].apply(lambda x: eval(x)[0])
        fenzhi_points = secondary_zhaomian[secondary_zhaomian['entType'].str.contains('分公司', na=False)]['entName'].tolist()
        ent_id = secondary_point.loc[0, 'ent_id']

        ## 处理照面的法人
        secondary_zhaomian.loc[:, 'labels'] = 'Human'

        def change_faren(x):
            if x['entName'] in fenzhi_points:
                return '高管'
            else:
                return '法人'

        secondary_zhaomian.loc[:, 'properties'] = secondary_zhaomian.apply(change_faren, axis=1)
        faren_points = secondary_zhaomian[['frName', 'legalPersonId', 'properties', 'labels']].rename(
            columns={'legalPersonId': 'id', 'frName': 'name'})

        ## 添加高管
        gaoguan_points = secondary_gaoguan[['perName', 'id', 'position']].rename(
            columns={'perName': 'name', 'position': 'properties'})
        gaoguan_points.loc[:, 'labels'] = 'Human'
        # print(gaoguan_points['properties'])
        gaoguan_points['properties'] = gaoguan_points['properties'].apply(lambda x: ''.join(eval(x)))

        ## 合并节点
        self.points = secondary_point.append(faren_points).append(gaoguan_points).dropna(axis=0, how='any',
                                                                                    subset=['name']).astype(
            str).drop_duplicates(subset=['name', 'id']).drop(['ent_id', 'storage_time'], axis=1).reset_index(drop=True)

        # --------------------------------------处理节点类型-----------------------------------------------------------------
        def add_label(x):
            if x['labels'] == 'Company':
                if x['id'] == ent_id:
                    label = '目标企业'
                elif x['name'] in fenzhi_points:
                    label = '分支机构'
                else:
                    label = '关联企业'
            elif x['labels'] == 'Human':
                label = '关联人员'
            else:
                label = '未知'
            return label

        self.points['节点类型'] = self.points.apply(add_label, axis=1)

        # 处理关系 ###
        start_points = pd.DataFrame(self.points['name'].tolist(), columns=['开始节点'], index=(self.points['id']))
        end_points = pd.DataFrame(self.points['name'].tolist(), columns=['结束节点'], index=(self.points['id']))
        start_nodes = pd.DataFrame(self.points['id'].tolist(), columns=['startNode'], index=(self.points['name']))
        end_nodes = pd.DataFrame(self.points['id'].tolist(), columns=['endNode'], index=(self.points['name']))

        # 处理关系表
        secondary_relation.rename(columns={'id': 'relationId'}, inplace=True)
        secondary_relation.loc[:, '占股'] = secondary_relation.loc[:, 'properties'].apply(lambda x: eval(x)['percent'])
        secondary_relation.loc[:, 'labels'] = secondary_relation.loc[:, 'properties'].apply(lambda x: eval(x)['labels'][0])
        secondary_relation.drop(['storage_time', 'properties', 'ent_id'], axis=1, inplace=True)
        for i in range(len(secondary_relation)):
            if secondary_relation.loc[i, 'labels'] == '法人':
                secondary_relation.loc[i, '关系类型'] = '法人'
            elif secondary_relation.loc[i, 'labels'] == '分支机构':
                secondary_relation.loc[i, '关系类型'] = '分支机构'
            elif secondary_relation.loc[i, 'labels'] == '参股':
                secondary_relation.loc[i, '关系类型'] = '股东'
            else:
                secondary_relation.loc[i, '关系类型'] = '高管'

        # 处理照面表的法人关系
        faren_relations = secondary_zhaomian[['legalPersonId', 'frName', 'id', 'entName', 'properties']].rename(
            columns={'legalPersonId': 'startNode', 'id': 'endNode', 'properties': '关系类型', 'frName': '开始节点',
                     'entName': '结束节点'})

        # 处理高管表
        gaoguan_relations = secondary_gaoguan[['perName', 'id', 'entNameMain', 'position']].rename(
            columns={'perName': '开始节点', 'id': 'startNode', 'position': 'labels', 'entNameMain': '结束节点'}).merge(end_nodes,
                                                                                                               how='left',
                                                                                                               left_on='结束节点',
                                                                                                               right_index=True)
        gaoguan_relations['labels'] = gaoguan_relations['labels'].apply(lambda x: ''.join(eval(x)))
        gaoguan_relations['关系类型'] = '高管'

        # 找出带有缺失值的行
        # faren_relations[faren_relations.isnull().T.any()]
        # 合并关系表
        relations = secondary_relation.merge(start_points, how='left', left_on='startNode', right_index=True).merge(
            end_points, how='left', left_on='endNode', right_index=True) \
            .append(faren_relations).append(gaoguan_relations).dropna(axis=0, how='any', subset=['startNode', 'endNode'])
        relations[['endNode', 'startNode', '开始节点', '结束节点']] = \
            relations[['endNode', 'startNode', '开始节点', '结束节点']].astype(str)
        relations = relations.drop_duplicates().reset_index(drop=True).drop(['relationId', 'type'], axis=1)
        relations['占股'] = relations['占股'].str.strip().fillna(0).replace('', 0).astype(float).apply(lambda x: format(x, '.2%')).replace('0.00%', '')
        # 检查一列中是否有多个类型的数据，有的话报错
        # sorted(relations["startNode"].unique())

        # 对关系表进行透视分析
        self.final_relations = relations.pivot_table(index=['endNode', 'startNode', '开始节点', '结束节点'],
                                                values=['labels', '关系类型', '占股'],
                                                aggfunc={'labels': lambda x: x.str.cat(sep='、'),
                                                         '关系类型': lambda x: x.str.cat(sep='、'), '占股': sum}).reset_index()
        self.final_relations['labels'] = self.final_relations['labels'].apply(lambda x: x.split("、"))
        self.final_relations['关系类型'] = self.final_relations['关系类型'].apply(lambda x: set(x.split("、")))

        for i in self.final_relations.index:
            zhangu = ''
            temp = set(self.final_relations.loc[i, '关系类型'])
            zhiwei = set(self.final_relations.loc[i, 'labels'])
            zhiwei.discard('法人')
            zhiwei.discard('参股')
            if temp == {'法人'}:
                c = '法人'
            elif temp == {'股东'}:
                c = '股东'
                zhangu = '占股比{}'.format(self.final_relations.loc[i, '占股'])
            elif '分支机构' in temp:
                c = '分支机构'
            elif temp == {'高管'}:
                c = '高管'
            elif temp == {'法人', '股东', '高管'}:
                c = '法人股东高管'
                zhangu = '占股比{}'.format(self.final_relations.loc[i, '占股'])
            elif temp == {'法人', '高管'}:
                c = '法人高管'
            elif temp == {'法人', '股东'}:
                c = '法人股东'
                zhangu = '占股比{}'.format(self.final_relations.loc[i, '占股'])
            elif temp == {'股东', '高管'}:
                c = '股东高管'
                zhangu = '占股比{}'.format(self.final_relations.loc[i, '占股'])
            else:
                c = '未知'

            #     z = '、'.join(group['职位'].dropna().tolist())
            # #     print(p,z)  占股比55%/监事
            #     if p != 0:
            #         labeltext = '占股比{0:.2f}%/{1}'.format(p*100,z)
            #     else:
            #         labeltext = z

            self.final_relations.loc[i, '关系类型'] = c
            if zhangu and zhiwei:
                self.final_relations.loc[i, 'labeltext'] = zhangu + '/' + '、'.join(zhiwei)
            elif zhangu and not zhiwei:
                self.final_relations.loc[i, 'labeltext'] = zhangu
            elif zhiwei and not zhangu:
                self.final_relations.loc[i, 'labeltext'] = '、'.join(zhiwei)
        self.final_relations.drop(['labels', '占股'], axis=1, inplace=True)

    def to_develop(self):
        self.SR_methodPlus()
        out_file = pd.ExcelWriter(os.path.join(out_path, filename.split('.')[0]+'清洗后.xlsx'), engine='xlsxwriter')
        self.points.drop(['properties'], axis=1).to_excel(out_file, sheet_name='节点', index=0)
        self.final_relations.to_excel(out_file, sheet_name='关系', index=0)
        out_file.save()

    def to_gephi(self):
        self.SR_methodPlus()
        # out_file = pd.ExcelWriter(os.path.join(out_path, filename.split('.')[0] + 'Gephi版.xlsx'), engine='xlsxwriter')
        out_file = pd.ExcelWriter(self.file_path, engine='xlsxwriter')
        self.points['id'] = self.points['name']
        self.points['labels'] = self.points['name']
        self.points.drop(['properties', 'name'], axis=1).to_excel(out_file, sheet_name='节点', index=0)
        self.final_relations.rename(columns={'开始节点': 'source', '结束节点': 'target'}, inplace=True)
        self.final_relations.drop(['endNode','startNode','labeltext'], axis=1).to_excel(out_file, sheet_name='关系', index=0)
        out_file.save()
        return out_file


if __name__ == '__main__':
    filefolder = '/Users/admin/Documents/临时文件/工商关联处理'
    filename = '2020-07-14_北京恒昌利通投资管理有限公司_二级关联.xlsx'
    out_path = os.path.join(filefolder, 'outfile')
    if not os.path.exists(out_path):
        os.makedirs(out_path)
    # print([i for i in os.listdir(path=filefolder) if (i[-4:] in ["xlsx", ".xls"] and not i.startswith("~$"))])
    # for filename in os.listdir(path=filefolder):
    #     if filename[-4:] in ["xlsx", ".xls"] and not filename.startswith("~$"):
    #         print(filename)
    #         hehe = SimpleSR(filename)
    #         hehe.to_gephi()
    hehe = FinalSR(filename)
    # hehe = SimpleSR(filename)
    # hehe.to_gephi()
    hehe.to_develop()
