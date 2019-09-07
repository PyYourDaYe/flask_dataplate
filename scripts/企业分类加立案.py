
# coding:utf-8

import pandas as pd
import pymysql
from os import path
from setting import rootPath

my_host = '192.168.1.119'
my_user = 'data_division'
my_password = 'jxwy@2018!'
my_port = 3306
my_db = 'data_platform'


def company_sort(filename):

    companylist_ori = pd.read_excel(path.join(rootPath, filename))  # 需要调的名单

    companylist_all = companylist_ori.copy()    # 复制原始企业名单['企业名称'，'总公司']
    companylist_all.drop_duplicates(inplace=True)
    companylist_all = companylist_all.reset_index(drop=True)
    # 清洗中英文括号及换行符
    companylist_all = companylist_all.replace(['\r\n', '\(', '\)', '\s'], ['', '（', '）', ''], regex=True)

    companylist_all.insert(1, '分好的企业类型', None)
    companylist_all.insert(2, '立案状态', None)

    # 处理公司的分类信息
    '''
     将企业名称与分类名单进行比对，关键词命中，在原表基础上新增列“分好的企业类型”
    '''
    df = pd.read_excel('新企业分类--20190409--v17（王德军更新）.xlsx', sheet_name='Sheet4')
    key_list = list(df['关键词'])
    class_list = list(df['企业类别'])
    category = dict(zip(key_list, class_list))  # 将关键词与企业类别一一对应
    # 先初步分类
    for k, v in category.items():
        companylist_all.loc[companylist_all['企业名称'].str.contains(k), '分好的企业类型'] = v  # 使用series函数 以及 df的过滤
    # companylist_all.loc[companylist_all['企业名称'].str.contains(k),'细分行业']= k
    #  ['企业名称'，'总公司','分好的企业类型']

    # 处理私募类别数据
    '''
     从数据库查询私募公司数据，若企业名称匹配到私募数据库的公司名，则设置类别为私募股权投资基金
    '''
    db = pymysql.connect(host=my_host, user=my_user, password=my_password, db='data_hangye', port=my_port, charset='utf8')
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
    companylist_all.loc[companylist_all['企业名称'].isin(simu), '分好的企业类型'] = '私募股权投资基金'

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
    companylist_all.loc[companylist_all['企业名称'].isin(wd), '分好的企业类型'] = '网络借贷平台'
    # 将其他没有匹配到的类别填充为‘其他’
    companylist_all['分好的企业类型'].fillna('其他', inplace=True)

    # # 立案信息
    con = pymysql.connect(host=my_host, user=my_user, password=my_password, port=my_port, db=my_db, charset='utf8',
                          use_unicode=True)
    sql = 'select distinct company from data_platform.`黑名单-新` where state in ("立案","公安立案");'
    cur = con.cursor()
    cur.execute(sql)
    results = cur.fetchall()
    LA = []
    for row in results:
        LA.append(row[0])
    companylist_all.loc[companylist_all['企业名称'].isin(LA), '立案状态'] = '立案'
    # 将其他没有匹配到的类别填充为‘其他’
    companylist_all['立案状态'].fillna('健康', inplace=True)
# ===========================================================================================================
    writer = pd.ExcelWriter(path.join(rootPath, filename))
    companylist_ori.to_excel(writer,sheet_name='原名单无改动', index=False)
    companylist_all.to_excel(writer,sheet_name='企业分类加立案', index=False)
    writer.save()
    writer.close()


if __name__ == '__main__':

    filename = input('请输入位于桌面的需要分类的文件名：')
    company_sort(filename)
