# -*- coding: utf-8 -*-
""" このスクリプトをHotpepper Beautyのオープンデータ(tsv形式)があるフォルダにいれ、
python preprocess_solr_import.py
とコマンド
用途、Solrにインポートできる形に元ファイルを整形
"""
import csv

fName = ['CouponData', 'ReviewData', 'StoreData', 'MenuData',
         'SetMenuData', 'StylistData', 'BlogData']
hName = []
hName.append(['id', 'S_storeID', 'S_couponID', 'I_number',
              'S_when', 'S_who', 'T_description', 'T_condition',
              'T_coupon_name', 'D_limit_day', 'I_time', 'I_type',
              'I_price', 'I_discount_rate', 'I_discount_price'])
hName.append(['id', 'S_storeID', 'S_reviewID', 'S_stylistID',
              'S_memberID', 'S_nickname', 'S_gender', 'S_age',
              'T_review', 'I_mode', 'I_service', 'I_technique',
              'I_menu', 'I_general', 'D_date'])
hName.append(['id', 'S_storeID', 'T_store_name', 'T_store_name_kana',
              'T_address', 'L_location', 'T_open', 'T_holyday',
              'T_represent_name', 'T_represent_position',
              'T_message1', 'T_message2', 'T_catch', 'T_menu_note', 'S_url'])
hName.append(['id', 'S_storeID', 'S_menuID', 'I_number',
              'T_name', 'I_price', 'I_time'])
hName.append(['id', 'S_storeID', 'S_setmenuID', 'I_number',
              'T_name', 'I_price', 'I_time'])
hName.append(['id', 'S_storeID', 'S_stylistID', 'T_carrer',
              'T_catch', 'T_technique', 'T_PR', 'S_gender'])
hName.append(['id', 'S_storeID', 'S_blogID', 'S_stylistID',
              'S_category', 'T_title', 'T_body', 'D_date'])

# StylistData 以外を処理
for i in [0, 1, 3, 4, 6]:
    file1 = open(fName[i]+'.tsv', 'rb')
    tsvFile = csv.reader(file1, delimiter='\t')
    saveCSV = open('new' + fName[i] + '.csv', 'wb')
    csvWriter = csv.writer(saveCSV, delimiter=',')
    csvWriter.writerow(hName[i])
    id_ = 0
    print i
    for line in tsvFile:
        # CouponDataの場合、時刻のフォーマットを変換
        if i == 0:
            line[8] = line[8] + 'T00:00:00Z'
        # ReviewDataの場合、時刻のフォーマット以外に欠損値の処理
        if i == 1:
            if line[13] == "":
                line[13] = "00-00-00"
            line[13] = line[13] + 'T00:00:00Z'
        # MenuData, SetMenuDataの場合、値段が数値でない場合に負の値をいれる
        if i == 3 or i == 4:
            if str.isdigit(line[4]) is False:
                line[4] = -1
        # BlogDataの場合、titleとbodyの欠損の応急処理他と時刻のフォーマット変換
        if i == 6:
            if len(line) > 7:
                line = line[0:5] + [line[5]+line[6]] + [line[7]]
            elif len(line) < 7:
                line = line[0:4] + ['none'] + line[4:6]
            line[6] = line[6] + 'T00:00:00Z'
            #  元データにバグがあり、前半と後半に同じデータが入っているあため処理            
            if id_ > 1779160:
                break
        csvWriter.writerow([str(id_)] + line)
        id_ = id_+1
    saveCSV.close()

# StyListDataのタブ区切りがcsv.readerでうまく読めないケースがあるので、ここだけpandas.read_csvを使用
import pandas
stylist = pandas.read_csv(fName[5]+'.tsv', delimiter="\t", header=False, names=hName[5][1:])
idlist = pandas.DataFrame(data=range(0, len(stylist)), columns=['id'])
stylist2 = pandas.concat((idlist, stylist), axis=1)
stylist2.to_csv('new' + fName[5] + '.csv')

# StoreDataの場合、pandasでもtsvの読み込みがうまくいかない場所があるので応急処置 また緯度経度のフォーマット変換
file1 = open(fName[2]+'.tsv', 'rb')
saveCSV = open('new' + fName[2] + '.csv', 'wb')
csvWriter = csv.writer(saveCSV, delimiter=',')
csvWriter.writerow(hName[2])
id_ = 0
for line in file1:
    line = unicode(line, 'utf-8')
    line = line.split('\t')
    for k in range(0, len(line)):
        line[k] = line[k].encode('utf-8')
#        line[k] = line[k][1:]
    a = line[4].split('.')
    line[4] = float(a[0]) + float(a[1])/60 + float(a[2])/3600 + float(a[3])/360000
    a = line[5].split('.')
    line[5] = float(a[0]) + float(a[1])/60 + float(a[2])/3600 + float(a[3])/360000
    line = line[0:4] + [str(line[4])+","+str(line[5])] + line[6:16]
    csvWriter.writerow([str(id_)] + line)
    id_ = id_ + 1
saveCSV.close()
