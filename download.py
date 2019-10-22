from flask import url_for, redirect, render_template, flash, \
    send_from_directory, send_file, session, Blueprint, request, current_app
from io import BytesIO
from scripts.funcs import GetCorrespondingFile

download = Blueprint('download', __name__, url_prefix='/download')


@download.route('/jibenxinxi')
def jibenxinxi():
    # if app.config.companylist:
    # companylist = session.get('companylist')
    # filename = session.get('filename')
    # session.clear()
    companylist = current_app.config.pop('companylist')
    filename = current_app.config.pop("filename")

    if companylist != [''] and companylist is not None:
        # app.config.hehe = ''
        # return send_file(r'C:\Users\zhd\Desktop\flask_hehe\project\uploads\持牌机构广告主.xlsx')
        get_jibenxinxi = GetCorrespondingFile.getJibenxinxi(companys=companylist)
        bf = BytesIO()
        get_jibenxinxi.to_excel(bf, index=0)
        bf.seek(0)
        if filename:
            return send_file(bf, as_attachment=True, attachment_filename='反馈基本信息-%s' % filename,
                             mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        else:
            return send_file(bf, as_attachment=True, attachment_filename='反馈-基本信息.xlsx',
                             mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    flash('未传入公司列表')  # redirect的话flash没用
    return render_template('flask首页.html')
    # return redirect(url_for('demo_page1'))


@download.route('/smokingindex')
def smokingindex():
    # companylist = session.get('companylist')  # 通过session保存名单
    # filename = session.get('filename')
    # companylist = request.cookies.get('companylist')  # 通过cookie保存名单
    # filename = request.cookies.get('filename')
    companylist = current_app.config.pop('companylist')
    filename = current_app.config.pop("filename")
    print(companylist)
    print(type(companylist))

    if companylist != [''] and companylist is not None:
        smoking = GetCorrespondingFile.getSmokingindex(companylist)
        bf = BytesIO()
        smoking.to_excel(bf, index=0)
        bf.seek(0)
        if filename:
            return send_file(bf, as_attachment=True, attachment_filename='反馈冒烟指数-%s' % filename,
                             mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        else:
            return send_file(bf, as_attachment=True, attachment_filename='反馈冒烟指数.xlsx',
                             mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    flash('未传入公司列表')
    return render_template('flask首页.html')


@download.route('/fenlei')
def fenlei():
    # companylist = session.get('companylist')
    # filename = session.get('filename')
    companylist = current_app.config.pop('companylist', None)
    filename = current_app.config.pop("filename", None)
    sheet_ob = current_app.config.pop("sheet_ob", None)
    if companylist != [''] and companylist is not None:
        sheet = sheet_ob.values
        get_fenlei = GetCorrespondingFile.company_sort(sheet)
        bf = BytesIO()
        get_fenlei.to_excel(bf, index=0)
        bf.seek(0)
        if filename:
            return send_file(bf, as_attachment=True, attachment_filename='反馈公司分类-%s' % filename,
                             mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        else:
            return send_file(bf, as_attachment=True, attachment_filename='反馈公司分类.xlsx',
                             mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    flash('未传入公司列表')
    return render_template('flask首页.html')
