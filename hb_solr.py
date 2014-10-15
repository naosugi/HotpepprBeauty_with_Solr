# -*- coding: utf-8 -*-
"""
SolrにいれたHotpepper Beautyのオープンデータを読み込むためのクラス
参考：URL

"""
import urllib2
import json
import pandas


class HotpepperBeauty:
    def __init__(self):
        self.DB_name = ['CouponData', 'ReviewData', 'StoreData',
                        'MenuData', 'SetMenuData',
                        'StylistData', 'BlogData']
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
                      'T_address', 'ML_location', 'T_open', 'T_holyday',
                      'T_represent_name', 'T_represent_position',
                      'T_message1', 'T_message2', 'T_catch', 'T_menu_note',
                      'S_url'])
        hName.append(['id', 'S_storeID', 'S_menuID', 'I_number',
                      'T_name', 'T_price', 'I_time'])
        hName.append(['id', 'S_storeID', 'S_setmenuID',
                      'I_number', 'T_name', 'T_price', 'I_time'])
        hName.append(['id', 'S_storeID', 'S_stylistID',
                      'T_carrer', 'T_catch', 'T_technique',
                      'T_PR', 'S_gender'])
        hName.append(['id', 'S_storeID', 'S_blogID', 'S_stylistID',
                      'S_category', 'T_title', 'T_body', 'D_date'])
        hName.append(['id', 'S_storeID', 'T_store_name', 'T_store_name_kana',
                      'T_address', 'ML_location1', 'ML_location2', 'T_open',
                      'T_holyday', 'T_represent_name', 'T_represent_position',
                      'T_message1', 'T_message2', 'T_catch', 'T_menu_note',
                      'S_url'])
        self._index_name = hName
        self._basic_URL = 'http://localhost:8983/solr/'
        self._sub_query = 'wt=json&indent=true&fl='

    def makeQuery(self, DB, searchFields, searchValues, outputFields, option):
        """
        Solrのクエリの文字列（str型）を出力する関数
        URLの形で出力される。そのため、この出力クエリをブラウザに直接入力してデバックを行うことも可能
        引数説明
        DB, str型。　どのCoreにアクセスするかを指定。
        searchFields, list型。どのフィールドで検索を行うかを指定。listの中身はstr型。AND検索を行う。
        searchValues, list型。searchFieldsで指定されたフィールドの検索条件を指定。listの中身はstr型。
        例、searchFields=['f1','f2','f3'], searchValues=['aa','bb','[0 TO 1000]']
        f1がaa　かつ f2がbb　かつ f3が0以上1000以下の条件で検索
        outputFields, list型。検索結果として返したいフィールド名を指定。listの中身はstr型。
        option, str型。その他、詳細なクエリを発行したい場合に指定。
        """
        if (len(searchFields) != len(searchValues)):
            print 'searchFiledsとsearchValuesは同じ長さにしてください'
        else:
            full_query = self._basic_URL + DB + '/select?q=(*:*'
            for i in range(0, len(searchFields)):
                full_query += urllib2.quote(' AND ') + searchFields[i] + urllib2.quote(':') + urllib2.quote(searchValues[i])
            full_query += ')&' + option + self._sub_query
            for i in range(0, len(outputFields)):
                full_query += urllib2.quote(str(outputFields[i]) + ',') + '&'
            return full_query


class Solr:
    def getResponse(self, full_query):
        """
        Solrへのクエリに対応した検索結果をSolrDataFrame型で返す。
        SolrDataFrameはその属性のqueryにクエリを、dfに検索結果をpandasのDataFrame型で格納
        引数説明
        full_query, str型。HotpepperBeauty クラスのmakeQueryで作られたhttpから始まる完全なクエリ
        """
        full_query = full_query + 'rows=100000000'
        response = json.loads(urllib2.urlopen(full_query).read())['response']['docs']
        df = pandas.DataFrame(response)
        return SolrDataFrame(df, full_query)
        
    def getResponseNumber(self, full_query):
        """
        Solrへのクエリに対応した検索結果の数をfloat型で返す。
        引数説明
        full_query, str型。HotpepperBeauty クラスのmakeQueryで作られたhttpから始まる完全なクエリ
        """
        full_query = full_query + 'rows=0'
        response = json.loads(urllib2.urlopen(full_query).read())['response']['numFound']
        return float(response)


class SolrDataFrame:
    """
    queryにクエリを、dfに検索結果をpandasのDataFrame型で格納するためだけのクラス
    """
    def __init__(self, df, query):
        self.df = df
        self.query = query
