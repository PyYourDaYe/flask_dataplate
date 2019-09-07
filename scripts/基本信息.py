from pymongo import MongoClient
import pandas as pd


client = MongoClient('192.168.1.110', 17017)
db = client['download']


class GetAllBusinessCSV:

    def __init__(self, file):

        self.companys = file

    # 基本信息
    def getJibenxinxi(self):

        col = db['corp_lkl_faceinfo']

        headers_jibenxinxi = ['企业名称', '法定代表人', '统一社会信用代码', '工商注册号', '注册资金（万元）',
                              '开业时间', '经营期限自', '企业类型', '经营状态', '登记机关', '注册地址',
                              '经营范围', '实收资本', '简称', '核准时间', '省份简称', '邮箱', '行业', '是否是小微企业（0不是 1是）',
                              '法人id', '组织机构代码', '电话号码', '人员规模', '纳税人识别号', '类型', '网址', '注销原因',
                              '币种', '曾用名', '吊销日期', '吊销原因', '经营期限至', '主管机构名称', '注销日期']
        data_jibenxinxi = col.aggregate([
            {'$match': {'entName': {'$in': self.companys}}},
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