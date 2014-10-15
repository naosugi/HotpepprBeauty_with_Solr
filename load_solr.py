# -*- coding: utf-8 -*-

import numpy
# この分析のために作られたクラス　
import hb_solr


# ----東京都の2013年の平均ブログ投稿数---------------------------------
hb = hb_solr.HotpepperBeauty()
sl = hb_solr.Solr()

# 東京のお店IDをとってくる お店の位置情報はStoreDataにある
tokyo_query = hb.makeQuery(DB='StoreData',
                           searchFields=['T_address'],
                           searchValues=['\"東京都\"'],
                           outputFields=['S_storeID'],
                           option='')
tokyoStore = sl.getResponse(tokyo_query)

tokyoBlog = 0
# 2013年の1月から12月まで、を検索する条件文
Y2013 = '[0013-01-01T00:00:00Z TO 0013-12-31T00:00:00Z]'
# 東京のお店IDごとに、ブログ投稿が何件あったかを足す連続クエリ
for tokyoStoreID in tokyoStore.df.S_storeID.values:
    tokyo_query = hb.makeQuery(DB='BlogData',
                             searchFields=['S_storeID', 'D_date'],
                             searchValues=[tokyoStoreID, Y2013],
                             outputFields=['S_storeID,id,D_date'],
                             option='')
    tokyoBlog = tokyoBlog + sl.getResponseNumber(tokyo_query)

print "東京のブログ更新数平均:"+str(tokyoBlog/len(tokyoStore.df))
# ------------------------------------------------------------
# ---------月ごとのレビューの数表示-----------------------
hb = hb_solr.HotpepperBeauty()
sl = hb_solr.Solr()

# 結果を格納するところ 24ヶ月 = 2年 分の場所を準備
resultList = numpy.zeros(24)

# 2012/01/01開始 データの仕様によると2012/01から2013/12までは確実にある
startMonth = '0012-01-01T00:00:00Z'
for month in range(0, 24):  # 24ヶ月 = 2年 分検索を繰り返す
    endMonth = startMonth + '+1MONTH'  # 1ヶ月幅で検索
    one_month = '[' + startMonth + ' TO ' + endMonth + ']'  # 検索用に整形
    # クエリのパラメータ, どのDBか?, 何で検索するか?, 検索の値は?, 特殊な検索(今回はなし)は?

    query = hb.makeQuery(DB='ReviewData',
                         searchFields=['D_date'],
                         searchValues=[one_month],
                         outputFields=[''],
                         option='')
    resultList[month] = sl.getResponseNumber(query)
    startMonth = startMonth + '+1MONTH'  # 次の検索のために1ヶ月進める

print resultList
# -------------------------------------------------------------


# 東京の新宿と池袋で「ある美容室の半径500m以内に美容室がある期待値」
hb = hb_solr.HotpepperBeauty()
sl = hb_solr.Solr()
# まず、新宿と池袋の美容院の座標（緯度経度）一覧を獲得
minato_query = hb.makeQuery(DB='StoreData',
                              searchFields=['T_address'],
                              searchValues=['\"東京都港区\"'],
                              outputFields=['L_location'],
                              option='')
minatoStore = sl.getResponse(minato_query)

tyuou_query = hb.makeQuery(DB='StoreData',
                               searchFields=['T_address'],
                               searchValues=['\"東京都中央区\"'],
                               outputFields=['L_location'],
                               option='')
tyuouStore = sl.getResponse(tyuou_query)

# 上記のお店の半径200m以内の美容室の数をもとめ、その平均を表示する
minatoResult = numpy.zeros(len(minatoStore.df))
tyuouResult = numpy.zeros(len(tyuouStore.df))
c = 0
for loc in minatoStore.df.L_location.values:
    # 特殊検索条件 dの値は半径(km)　ptは中心座標
    loc200 = "fq={!geofilt}&sfield=L_location&pt=" + loc + "&d=0.2&"
    query = hb.makeQuery(DB='StoreData',
                         searchFields=['id'],
                         searchValues=['*'],
                         outputFields=[''],
                         option=loc200)
    minatoResult[c] = sl.getResponseNumber(query)
    c = c + 1
c = 0
for loc in tyuouStore.df.L_location.values:
    loc200 = "fq={!geofilt}&sfield=L_location&pt=" + loc + "&d=0.5&"
    query = hb.makeQuery(DB='StoreData',
                         searchFields=['id'],
                         searchValues=['*'],
                         outputFields=[''],
                         option=loc200)
    tyuouResult[c] = sl.getResponseNumber(query)
    c = c + 1

print "港区のお店の数は" + str(len(minatoResult)),  "お店密集度は" + str(minatoResult.mean())
print "中央区のお店の数は" + str(len(tyuouResult)),  "お店密集度は" + str(tyuouResult.mean())
# -------------------------------------------------
