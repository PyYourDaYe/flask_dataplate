import pandas as pd
import numpy as np
from manage import mongo_conn
import pymysql
import re

my_host = '192.168.1.119'
my_user = 'data_division'
my_password = 'jxwy@2018!'
my_port = 3306
my_db = 'data_platform'


class GetCorrespondingFile:
    # 基本信息
    @staticmethod
    def getJibenxinxi(companys):

        col = mongo_conn['download']['corp_lkl_faceinfo']
        headers_jibenxinxi = ['企业名称', '法定代表人', '统一社会信用代码', '工商注册号', '注册资金（万元）',
                              '开业时间', '经营期限自', '企业类型', '经营状态', '登记机关', '注册地址',
                              '经营范围', '实收资本', '简称', '核准时间', '省份简称', '邮箱', '行业', '是否是小微企业（0不是 1是）',
                              '法人id', '组织机构代码', '电话号码', '人员规模', '纳税人识别号', '类型', '网址', '注销原因',
                              '币种', '曾用名', '吊销日期', '吊销原因', '经营期限至', '主管机构名称', '注销日期']
        data_jibenxinxi = col.aggregate([
            {'$match': {'entName': {'$in': companys}}},
            {'$sort': {'storage_time': -1}},
            {'$group': {'_id': "$entName",
                        '企业名称': {'$first': '$entName'},
                        '法定代表人': {'$first': '$frName'},
                        '统一社会信用代码': {'$first': '$authorityCode'},
                        '工商注册号': {'$first': '$regNo'},
                        '注册资金（万元）': {'$first': '$regCap'},
                        '开业时间': {'$first': '$esDate'},
                        '经营期限自': {'$first': '$opFrom'},
                        '企业类型': {'$first': '$entType'},
                        '经营状态': {'$first': '$entStatus'},
                        '登记机关': {'$first': '$regOrg'},
                        '注册地址': {'$first': '$dom'},
                        '经营范围': {'$first': '$opScope'},
                        '实收资本': {'$first': '$recCap'},
                        '简称': {'$first': '$alias'},
                        '核准时间': {'$first': '$approvedTime'},
                        '省份简称': {'$first': '$base'},
                        '邮箱': {'$first': '$email'},
                        '行业': {'$first': '$industry'},
                        '是否是小微企业（0不是 1是）': {'$first': '$isMicroEnt'},
                        '法人id': {'$first': '$legalPersonId'},
                        '组织机构代码': {'$first': '$orgNo'},
                        '电话号码': {'$first': '$phone'},
                        '人员规模': {'$first': '$staffNum'},
                        '纳税人识别号': {'$first': '$taxNumber'},
                        '类型': {'$first': '$type'},
                        '网址': {'$first': '$websiteList'},
                        '注销原因': {'$first': '$cancelReason'},
                        '币种': {'$first': '$regCapCur'},
                        '曾用名': {'$first': '$historyNames'},
                        '吊销日期': {'$first': '$revDate'},
                        '吊销原因': {'$first': '$revokeReason'},
                        '经营期限至': {'$first': '$opTo'},
                        '主管机构名称': {'$first': '$authorityName'},
                        '注销日期': {'$first': '$canDate'},
                        }
             },
            {'$project': {
                '_id': 0,
            }}
        ])
        temp_df = pd.DataFrame(list(data_jibenxinxi), columns=headers_jibenxinxi)

        return temp_df

    # 取冒烟指数
    @staticmethod
    def getSmokingindex(obj):

        col = mongo_conn.dataplat.financecase
        headers = ['监测企业来源', '监测时间', '排查人', '企业名称', '简称', '官网', '省', '市', '区', '类别', '状态',
                   '收益率（%）', '概述', '传播检索条件', '冒烟指数', '非法性指数', '特征性指数', '利诱性指数',
                   '传播力指数', '投诉率指数']
        if isinstance(obj, list):  # 传的是公司列表的情况
            company_list = col.aggregate([
                {'$match': {'name': {'$in': obj}}},
                # {'$group': {'_id':{'company':'$name','time':'$lastmodifiedtime'},
                #             'time':
                #             'count': {'$sum': 1}
                #             }},
                # {'$sort': {'count': -1}},
                {'$project': {
                    '_id': 0,
                    '监测企业来源': '$monitorCompanySource',
                    '监测时间': '$monitorTime',
                    '排查人': '$checkPeople',
                    '企业名称': '$name',
                    '简称': '$shortname',
                    '官网': '$url',
                    '省': '$province',
                    '市': '$city',
                    '区': '$cityarea',
                    '类别': '$classify',
                    '状态': '$state',
                    '收益率（%）': '$rateofreturn',
                    '概述': '$abstract',
                    '传播检索条件': '',
                    '冒烟指数': '$score',
                    '非法性指数': '$indicatorOfIllegal',
                    '特征性指数': '$indicatorOfAdvert',
                    '利诱性指数': '$indicatorOfIncome',
                    '传播力指数': '$indicatorOfInfluence',
                    '投诉率指数': '$indicatorOfComplaint'
                }}
            ])
        elif isinstance(obj, dict):  # 传的是行政区域的情况
            if 'province' in obj.keys():
                pattern = re.compile(obj['province'])
                company_list = col.aggregate([
                    {'$match': {'province': pattern}},
                    {'$project': {
                        '_id': 0,
                        '监测企业来源': '$monitorCompanySource',
                        '监测时间': '$monitorTime',
                        '排查人': '$checkPeople',
                        '企业名称': '$name',
                        '简称': '$shortname',
                        '官网': '$url',
                        '省': '$province',
                        '市': '$city',
                        '区': '$cityarea',
                        '类别': '$classify',
                        '状态': '$state',
                        '收益率（%）': '$rateofreturn',
                        '概述': '$abstract',
                        '传播检索条件': '',
                        '冒烟指数': '$score',
                        '非法性指数': '$indicatorOfIllegal',
                        '特征性指数': '$indicatorOfAdvert',
                        '利诱性指数': '$indicatorOfIncome',
                        '传播力指数': '$indicatorOfInfluence',
                        '投诉率指数': '$indicatorOfComplaint'
                    }}
                ])
            elif 'city' in obj.keys():
                pattern = re.compile(obj['city'])
                company_list = col.aggregate([
                    # {'$match': {'province': pattern}},
                    # {'$match': {'cityarea': pattern}},
                    {'$match': {'city': pattern}},
                    {'$project': {
                        '_id': 0,
                        '监测企业来源': '$monitorCompanySource',
                        '监测时间': '$monitorTime',
                        '排查人': '$checkPeople',
                        '企业名称': '$name',
                        '简称': '$shortname',
                        '官网': '$url',
                        '省': '$province',
                        '市': '$city',
                        '区': '$cityarea',
                        '类别': '$classify',
                        '状态': '$state',
                        '收益率（%）': '$rateofreturn',
                        '概述': '$abstract',
                        '传播检索条件': '',
                        '冒烟指数': '$score',
                        '非法性指数': '$indicatorOfIllegal',
                        '特征性指数': '$indicatorOfAdvert',
                        '利诱性指数': '$indicatorOfIncome',
                        '传播力指数': '$indicatorOfInfluence',
                        '投诉率指数': '$indicatorOfComplaint'
                    }}
                ])
            else:
                pattern = re.compile(obj['cityarea'])
                company_list = col.aggregate([
                    {'$match': {'cityarea': pattern}},
                    {'$project': {
                        '_id': 0,
                        '监测企业来源': '$monitorCompanySource',
                        '监测时间': '$monitorTime',
                        '排查人': '$checkPeople',
                        '企业名称': '$name',
                        '简称': '$shortname',
                        '官网': '$url',
                        '省': '$province',
                        '市': '$city',
                        '区': '$cityarea',
                        '类别': '$classify',
                        '状态': '$state',
                        '收益率（%）': '$rateofreturn',
                        '概述': '$abstract',
                        '传播检索条件': '',
                        '冒烟指数': '$score',
                        '非法性指数': '$indicatorOfIllegal',
                        '特征性指数': '$indicatorOfAdvert',
                        '利诱性指数': '$indicatorOfIncome',
                        '传播力指数': '$indicatorOfInfluence',
                        '投诉率指数': '$indicatorOfComplaint'
                    }}
                ])
        else:
            return None

        list_DF = pd.DataFrame(list(company_list), columns=headers)
        list_DF['传播检索条件'] = np.where(list_DF['简称'] == '', list_DF['企业名称'],
                                     (list_DF['企业名称'].str.cat(list_DF['简称'], sep='+')))

        return list_DF

    # 取广告数据
    @staticmethod
    def getAdv(obj):
        company_col = mongo_conn['advertisement']['companyFinal']
        adv_col = mongo_conn['advertisement']['final_adv_all']

        def __getADV(mongo_list):
            df_company = pd.DataFrame(list(mongo_list))
            ori_list = df_company['company'].tolist()
            list_length = len(ori_list)
            print(list_length)
            iter_size = 1000
            current = 0
            df_all_datas = pd.DataFrame()
            # 根据广告主查广告数据=======
            while current < list_length:
                end = current + iter_size
                recruit_segment = ori_list[current:end]
                result_cursor = adv_col.find({'e_nameOfAdvertiser': {'$in': recruit_segment}})
                df_segment = pd.DataFrame(list(result_cursor))
                df_mix = pd.merge(df_segment, df_company, left_on='e_nameOfAdvertiser', right_on='company')
                df_mix.drop(['_id', 'company'], axis=1, inplace=True)
                df_all_datas = df_all_datas.append(df_mix, ignore_index=True)
                print(end)
                current = current + iter_size

            return df_all_datas

        if isinstance(obj, list):  # 传的是公司列表的情况
            list_length = len(obj)
            print(list_length)
            iter_size = 1000
            current = 0
            df_all_datas = pd.DataFrame()
            # 根据广告主查广告数据,以1000家为查询批次
            while current < list_length:
                end = current + iter_size
                recruit_segment = obj[current:end]
                result_cursor = adv_col.find({'e_nameOfAdvertiser': {'$in': recruit_segment}})
                df_segment = pd.DataFrame(list(result_cursor))
                df_all_datas = df_all_datas.append(df_segment, ignore_index=True)
                print(end)
                current = current + iter_size

            return df_all_datas

        elif isinstance(obj, dict):  # 传的是行政区域的情况
            if 'province' in obj.keys():
                mongo_list = company_col.find(
                    {'address_province': {'$regex': obj['province']}},
                    {'company': 1, 'registered_address': 1, '_id': 0,
                     'address_province': 1, 'address_city': 1, 'address_region': 1})
                list_DF = __getADV(mongo_list)
                return list_DF
            elif 'city' in obj.keys():
                mongo_list = company_col.find(
                    {'address_city': {'$regex': obj['city']}},
                    {'company': 1, 'registered_address': 1, '_id': 0,
                     'address_province': 1, 'address_city': 1, 'address_region': 1})
                list_DF = __getADV(mongo_list)
                return list_DF
            else:
                mongo_list = company_col.find(
                    {'address_region': {'$regex': obj['cityarea']}},
                    {'company': 1, 'registered_address': 1, '_id': 0,
                     'address_province': 1, 'address_city': 1, 'address_region': 1})
                list_DF = __getADV(mongo_list)
                return list_DF
        else:
            return None

    # 给公司做分类
    @staticmethod
    def company_sort(obj):
        if isinstance(obj, list):  # list对象的处理
            companylist_all = pd.DataFrame(obj, columns=['企业名称'])
        else:
            companylist_all = pd.DataFrame(obj)  # sheet对象的处理
            if '企业名称' in companylist_all.iloc[0, :].tolist():
                companylist_all.columns = companylist_all.loc[0, :]
                companylist_all.drop(0, inplace=True)
            else:
                companylist_all.rename(columns={0: '企业名称'}, inplace=True)
        # 清洗中英文括号及换行符
        companylist_all['企业名称'].replace(['\r\n', '\(', '\)', '\s'], ['', '（', '）', ''], regex=True, inplace=True)
        companylist_all.drop_duplicates(inplace=True)
        companylist_all.reset_index(drop=True, inplace=True)

        # 扩充总公司列， 插入列['总公司']，与['企业名称']相同，索引从0开始；
        companylist_all.insert(1, '总公司', companylist_all['企业名称'])
        # 通过公司名，将包含分公司的部分分开
        companylist_all['总公司'] = companylist_all['总公司'].str.replace('公司', '公司#')
        # 设置为切割2次，生成新的df，一家总公司都没有的，会添加一列空列
        try:
            companylist_all['总公司'] = companylist_all['总公司'].str.split('#', 2, expand=True)[0]  # tmp —— [0总公司, 1切分, 2切分 ]
        except Exception as e:
            print(e)
        companylist_all.insert(2, '分好的企业类型', None)
        companylist_all.insert(3, '立案状态', None)

        # 处理公司的分类信息
        '''
         将企业名称与分类名单进行比对，关键词命中，在原表基础上新增列“分好的企业类型”
        '''
        df = pd.read_excel('scripts/新企业分类--20190409--v17（王德军更新）.xlsx', sheet_name='Sheet4')
        key_list = list(df['关键词'])
        class_list = list(df['企业类别'])
        category = dict(zip(key_list, class_list))  # 将关键词与企业类别一一对应
        # 先初步分类
        print(companylist_all.head())
        for k, v in category.items():
            companylist_all.loc[companylist_all['企业名称'].astype(str).str.contains(k), '分好的企业类型'] = v  # 使用series函数 以及 df的过滤
        # companylist_all.loc[companylist_all['企业名称'].str.contains(k),'细分行业']= k
        #  ['企业名称'，'总公司','分好的企业类型']

        # 处理私募类别数据
        '''
         从数据库查询私募公司数据，若企业名称匹配到私募数据库的公司名，则设置类别为私募股权投资基金
        '''
        db = pymysql.connect(host=my_host, user=my_user, password=my_password, db='data_hangye', port=my_port,
                             charset='utf8')
        cur = db.cursor()
        sql_simu = 'select fundName from data_hangye.simuchanpin as managerName union distinct select managerName from data_hangye.simuchanpin;'
        try:
            cur.execute(sql_simu)
            results = cur.fetchall()
            simu = []
            for row in results:
                simu.append(row[0])  # 取每条数据的第1个元素
            # 以下步骤可省略
            db.commit()
        except Exception as e:
            db.rollback()
            raise e
        finally:
            db.close()  # 关闭连接
        companylist_all.loc[(companylist_all['企业名称'].isin(simu)) | (companylist_all['总公司'].isin(simu)), '分好的企业类型'] = '私募股权投资基金'

        # 处理网贷类别数据
        '''
         从数据库查询网贷公司数据，若企业名称匹配到数据库网贷的公司名，则设置类别为网络借贷平台
        '''
        db = pymysql.connect(host=my_host, user=my_user, password=my_password, db=my_db, port=my_port, charset='utf8')
        cur = db.cursor()
        sql_wd = "SELECT company FROM data_hangye.2_p2p_wdty_wdzj;"
        try:
            cur.execute(sql_wd)
            results = cur.fetchall()
            wd = []
            for row in results:
                wd.append(row[0])
            db.commit()
        except Exception as e:
            db.rollback()
            raise e
        finally:
            db.close()  # 关闭连接
        companylist_all.loc[companylist_all['企业名称'].isin(wd) | companylist_all['总公司'].isin(wd), '分好的企业类型'] = '网络借贷平台'
        # 将其他没有匹配到的类别填充为‘其他’
        companylist_all['分好的企业类型'].fillna('其他', inplace=True)

        # # 立案信息
        con = pymysql.connect(host=my_host, user=my_user, password=my_password, port=my_port, db=my_db, charset='utf8',
                              use_unicode=True)
        sql = 'select distinct company_name company from data_platform.offline_case where offline_case.register_flag ="是";'
        LA = pd.read_sql(sql, con).company.to_list()
        companylist_all.loc[companylist_all['企业名称'].isin(LA), '立案状态'] = '立案'
        # 将其他没有匹配到的类别填充为‘其他’
        companylist_all['立案状态'].fillna('健康', inplace=True)
        companylist_all.drop(['总公司'], axis=1, inplace=True)
        # ===========================================================================================================
        return companylist_all
