#!/usr/bin/python
# -*- coding: utf-8 -*-
import psycopg2
import psycopg2.extras
import pandas as pd
import os
import urllib.request
import urllib.parse
from urllib.parse import urlencode
import json
import pickle
import numpy as np
from sklearn.neighbors import NearestNeighbors

cbt = 1
cby = 1
cta = 1
cd = 1


def normalize(df):
    result = df.copy()
    coeff = 1
    for feature_name in df.columns:
        # max_value = df[feature_name].max()
        # min_value = df[feature_name].min()
        mean = df[feature_name].median()
        std = df[feature_name].std()
        if feature_name == 'BuildingType':
            coeff = cbt
        elif feature_name == 'BuildingYear':
            coeff = cby
        elif feature_name == 'Lon':
            coeff = cd
        elif feature_name == 'Lat':
            coeff = cd
        elif feature_name == 'TotalArea':
            coeff = cta
        result[feature_name] = (df[feature_name] - mean) / std
        result[feature_name] = coeff * result[feature_name]
    return result


def get_appraisal(lat, lon, totalarea, buildingtype, buildingyear, analogscount):
    import psycopg2.extras
    conn_string = "host='37.18.75.197' dbname='omnia' user='postgres' password='Qwerty123'"
    conn = psycopg2.connect(conn_string)
    lat = float(lat)
    lon=float(lon)
    radius = 0.003
    while True:
        sql = 'select "Region" from "newParsers2" where "Lat" > ' + str(lat - radius) + ' and "Lat" < ' +\
            str(lat + radius) + ' and "Lon">' + str(lon-2* radius) + ' and "Lon"<' + str(lon+2*radius)
        df_city = pd.read_sql(sql, con=conn)
        if df_city.count().Region > 0:
            break
        else:
            radius *= 2
    region = df_city['Region'][0]
    filename = 'calcdata/knn_' + region + '_' + str(analogscount) + '.data'
    dffile = 'calcdata/df_' + region + '_' + '.data'

    if os.path.isfile(dffile):
        data = pickle.load(open(dffile, "rb"))
    if os.path.isfile(filename):
        neigh = pickle.load(open(filename, "rb"))
    else:
        sql = 'select id, "Lat", "Lon", "TotalArea", ' + \
              'case when "BuildingType" = \'элитный\' then 20 ' + \
              'when "BuildingType" =  \'монолитный\' then 18 ' + \
              'when "BuildingType" =  \'кирпично-монолитный\' then 19 ' + \
              'when "BuildingType" =  \'сталинский\' then 12 ' + \
              'when "BuildingType" =  \'панельный\' then 5 ' + \
              'when "BuildingType" =  \'кирпичный\' then 15 ' + \
              'when "BuildingType" =  \'блочный\' then 17  ' + \
              'when "BuildingType" =  \'металлоконструкции \' then 3 ' + \
              'when "BuildingType" =  \'железобетон\' then 18 ' + \
              'else 4 end as "BuildingType",  ' + \
              '"BuildingYear", case when lower(currency) = \'рубль\' then "Price"/"TotalArea" ' + \
              'when currency is null then "Price"/"TotalArea" ' + \
              'when lower(currency) = \'доллар\' then 65 * "Price"/"TotalArea" ' + \
              'when lower(currency) = \'евро\' then 75 * "Price"/"TotalArea" ' + \
              'end as "Price" from "newParsers2" where ' + \
              '"Region" = \'' + region + '\' and "Lat" is not null and "Lon" is not NULL ' + \
              'and "BuildingYear" is not null and "BuildingYear" > 0 and ' + \
              '"TotalArea" is not null and "TotalArea" > 1 ' + \
              'and "BuildingType" != \'\' and "Price" is not null and "ScreenshotFilePath" != \'\' '
        df = pd.read_sql(sql, con=conn)
        if os.path.isfile(dffile) == False:
            data = {}
            data['stdlat'] = df['Lat'].std()
            data['medlat'] = df['Lat'].median()
            data['maxlat'] = df['Lat'].max()
            data['minlat'] = df['Lat'].min()

            data['stdlon'] = df['Lon'].std()
            data['medlon'] = df['Lon'].median()
            data['maxlon'] = df['Lon'].max()
            data['minlon'] = df['Lon'].min()

            data['stdarea'] = df['TotalArea'].std()
            data['medarea'] = df['TotalArea'].median()
            data['maxarea'] = df['TotalArea'].max()
            data['minarea'] = df['TotalArea'].min()

            data['stdyear'] = df['BuildingYear'].std()
            data['medyear'] = df['BuildingYear'].median()
            data['maxyear'] = df['BuildingYear'].max()
            data['minyear'] = df['BuildingYear'].min()

            data['stdtype'] = df['BuildingType'].std()
            data['medtype'] = df['BuildingType'].median()
            data['maxtype'] = df['BuildingType'].max()
            data['mintype'] = df['BuildingType'].min()
            data['ids'] = df['id']
            print(2, data)
            pickle.dump(data, open(dffile, "wb"))
        normdf = normalize(df)
        X = normdf.drop(['id'], axis=1).values.tolist()
        neigh = NearestNeighbors(n_neighbors=analogscount)
        neigh.fit(X)
        pickle.dump(neigh, open(filename, "wb"))

    std = data['stdlat']
    mean = data['medlat']
    newlat = cd * (float(lat) - mean) / std

    std = data['stdlon']
    mean = data['medlon']
    newlon = cd * (float(lon) - mean) / std

    std = data['stdarea']
    mean = data['medarea']
    newtotalarea = cta * (totalarea - mean) / std

    std = data['stdyear']
    mean = data['medyear']
    newbuildingyear = cby * (buildingyear - mean) / std

    std = data['stdtype']
    mean = data['medtype']
    newbuildingtype = cbt * (buildingtype - mean) / std

    newprice = 0.0

    res = neigh.kneighbors([[newlat, newlon, newtotalarea, newbuildingtype, newbuildingyear, newprice]])
    wstr = ''
    for i in range(len(res[1][0])):
        if len(wstr) > 0:
            wstr = wstr + ' or '
        wstr = wstr + ' id = ' + str(data['ids'][res[1][0][i]])

    sql = 'select "BuildingType", case when lower(currency) = \'рубль\' then "Price" ' + \
          'when lower(currency) is null then "Price" ' + \
          'when lower(currency) = \'доллар\' then 65 * "Price" ' + \
          'when lower(currency) = \'евро\' then 75 * "Price" ' + \
          'end as "Price", "TotalArea", "City", "Lat", "Lon", "id", "RoomsNum", "Storey", ' + \
          ' "StoreysNum", "RawAddress", "RegionDistrict", "RepairRaw", ' +\
          ' "BuildingYear" as "BuildingPeriod", "LivingSpaceArea", ' + \
          ' "KitchenArea", "SubwayDistance", 0, 0, ' + \
          ' "Source" || \'/\' || replace("ScreenshotFilePath", \'\\\', \'/\') from public."newParsers2" where '

    sql = sql + wstr
    # ' and "Lon" > ' + str(lon1) + ' and "Lon" < ' +  str(lon2)
    df = pd.read_sql_query(sql, con=conn)
    df['pricepermetr'] = df['Price'] / df['TotalArea']
    #df = df.sort_values(by=['pricepermetr'])
    totalcount = df.count().BuildingType
    calcdf = df[:10]  # df[(int (totalcount / 2) - 5):(int (totalcount / 2) + 5)]
    analogs = []
    calcanalogs = []
    for i in range(totalcount):
        housetype = df.iloc[i][0]
        price = df.iloc[i][1]
        totalsquare = df.iloc[i][2]
        city = df.iloc[i][3]
        lat = df.iloc[i][4]
        lon = df.iloc[i][5]
        id = df.iloc[i][6]
        roomsnum = df.iloc[i][7]
        storey = df.iloc[i][8]
        storeysnum = df.iloc[i][9]
        fulladdress = df.iloc[i][10]
        district = df.iloc[i][11]
        repair = df.iloc[i][12]
        buildingyear = df.iloc[i][13]
        leavingsquare = df.iloc[i][14]
        kitchensquare = df.iloc[i][15]
        metrodistance = df.iloc[i][16]
        analogstatus = df.iloc[i][17]
        analogindex = df.iloc[i][18]
        screenshot = df.iloc[i][19]
        analogs.append([housetype, price, totalsquare, city, lat, lon, id, roomsnum,
                        storey, storeysnum, fulladdress, district, repair, buildingyear, leavingsquare,
                        kitchensquare, metrodistance, analogstatus, analogindex, screenshot])

    for i in range(calcdf.count().Price):
        housetype = calcdf.iloc[i][0]
        price = calcdf.iloc[i][1]
        totalsquare = calcdf.iloc[i][2]
        city = calcdf.iloc[i][3]
        lat = calcdf.iloc[i][4]
        lon = calcdf.iloc[i][5]
        id = calcdf.iloc[i][6]
        roomsnum = calcdf.iloc[i][7]
        storey = calcdf.iloc[i][8]
        storeysnum = calcdf.iloc[i][9]
        fulladdress = calcdf.iloc[i][10]
        district = calcdf.iloc[i][11]
        repair = calcdf.iloc[i][12]
        buildingyear = calcdf.iloc[i][13]
        leavingsquare = calcdf.iloc[i][14]
        kitchensquare = calcdf.iloc[i][15]
        metrodistance = calcdf.iloc[i][16]
        analogstatus = calcdf.iloc[i][17]
        analogindex = calcdf.iloc[i][18]
        screenshot = calcdf.iloc[i][19]
        calcanalogs.append([housetype, price, totalsquare, city, lat, lon, id, roomsnum,
                            storey, storeysnum, fulladdress, district, repair, buildingyear, leavingsquare,
                            kitchensquare, metrodistance, analogstatus, analogindex, screenshot])
    sql = 'select AVG ("Price"/"TotalArea") as price from "newParsers2" where "Region" = \'' + region + '\'' +\
        ' and "Price" is not NULL and "Price" > 1000 and "TotalArea" is not NULL and "TotalArea" > 10'
    df_price = pd.read_sql(sql, con=conn)
    regionAvrgPrice = df_price['price'][0]
    radius = 0.003
    while True:
        sql = 'select city_area from "zkh" where lat > ' + str(lat - radius) + ' and lat < ' +\
            str(lat + radius) + ' and lon>' + str(lon-2* radius) + ' and lon<' + str(lon+2*radius)

        df_city = pd.read_sql(sql, con=conn)
        if df_city.count().city_area > 0:
            break
        else:
            radius *= 2
    sql = 'select AVG ("Price"/"TotalArea") as price from "newParsers2" where "Region" = \'' + region + '\'' +\
          ' and "District" = \'' + df_city['city_area'][0] + \
          '\' and "Price" is not NULL and "Price" > 1000 and "TotalArea" is not NULL and "TotalArea" > 10'
    df_price = pd.read_sql(sql, con=conn)
    cityAvrgPrice = df_price['price'][0]
    if cityAvrgPrice is None:
        cityAvrgPrice = 0.0
    conn.close()
    result = {'analogs': analogs, 'pricePerMetr': df['pricepermetr'].median(), 'houseAvrgPrice': 0,
              'regionAvrgPrice': regionAvrgPrice, 'cityAvrgPrice': cityAvrgPrice,
              'data': df['pricepermetr'].median() * totalarea,
              'calcanalogs': calcanalogs}
    conn.close()
    return result


def getzkh(address):
    conn_string = "host='37.18.75.197' dbname='omnia' user='postgres' password='Qwerty123'"
    conn = psycopg2.connect(conn_string)

    project = ''
    house = ''
    foundation = ''
    housetype = ''
    region = ''
    buildingyear = 0
    sql = "SELECT project_type, house_type, foundation_type, wall_material, \
      floor_type, floor_count_max, built_year, \
      exploitation_start_year, formalname_region from zkh where fulladdress = '" + address + "'"
    df = pd.read_sql(sql, con=conn)
    storeysnum = 9
    if df.count().project_type > 0:
        if df['foundation_type'][0] == 'Каменные, кирпичные':
            housetype = "кирпичный"
        elif df['foundation_type'][0] == 'Панельные':
            housetype = 'панельный'
        elif df['foundation_type'][0] == 'Монолитные':
            housetype = 'монолитный'
        elif df['foundation_type'][0] == 'Блочные':
            housetype = 'блочный'
        elif df['foundation_type'][0] == 'Деревянные':
            housetype = 'деревянный'
        elif df['foundation_type'][0] == 'Смешанные':
            housetype = 'кирпично-монолитный'
        else:
            housetype = 'сталинский'

        buildingyear = df['built_year'][0]
        storeysnum = df['floor_count_max'][0]
        project = df['project_type'][0]
        house = df['house_type'][0]
        foundation = df['foundation_type'][0]
        region = df['formalname_region'][0]
    conn.close()
    return {'project': project, 'house': house, 'foundation': foundation,
            'housetype': housetype, 'storeysnum': int(storeysnum), 'buildingyear': int(buildingyear),
            'region': region}


def getyandex(address):
    yandex_url = 'https://geocode-maps.yandex.ru/1.x/?geocode='
    address = urllib.parse.quote_plus(address)
    yandex_url = yandex_url + address
    yandex_url = yandex_url + '&format=json&result=1'
    req = urllib.request.Request(yandex_url)
    contents = urllib.request.urlopen(req).read()
    result = json.loads(contents.decode('utf-8'))
    house = ''
    city = ''
    street_name = ''
    address = \
    result['response']['GeoObjectCollection']['featureMember'][0]['GeoObject']['metaDataProperty']['GeocoderMetaData'][
        'Address']['Components']
    for comp in address:
        if comp['kind'] == 'province':
            area = comp['name']
            region = comp['name']
        elif comp['kind'] == 'locality':
            city = comp['name']
        elif comp['kind'] == 'street':
            street_name = comp['name']
        elif comp['kind'] == 'house':
            house = comp['name']

    fulladdress = \
    result['response']['GeoObjectCollection']['featureMember'][0]['GeoObject']['metaDataProperty']['GeocoderMetaData'][
        'AddressDetails']['Country']['AddressLine']

    address = result['response']['GeoObjectCollection']['featureMember'][0]['GeoObject']['Point']['pos']
    longitude = address.split(' ')[0]
    latitude = address.split(' ')[1]
    if house is not None:
        bindex = house.find('к')
    else:
       bindex = -1
    building = ''
    if bindex >= 0:
        building = house[(bindex + 1):]
        house = house[:bindex]

    return {'region': region, 'area': area, 'city': city, 'street_name': street_name, \
            'house': house, 'building': building, \
            'fulladdress': fulladdress, 'longitude': longitude, 'latitude': latitude}


def getdadata(address):
    dadata_url = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/suggest/address"
    values = {"query": address, "count": 1}

    req = urllib.request.Request(dadata_url)
    req.add_header('Content-Type', 'application/json; charset=utf-8')
    req.add_header('X-Secret', '7848d777e41199081323970dbbfa00f5a8e71b3b')
    req.add_header('Authorization', 'Token 2ea0d7a2da0b9161f2106694203348ffd91a39df')

    jsondata = json.dumps(values)
    jsondataasbytes = jsondata.encode('utf-8')
    contents = urllib.request.urlopen(req, jsondataasbytes).read()

    result = json.loads(contents.decode('utf-8'))
    region = result['suggestions'][0]['data']['region']
    area = result['suggestions'][0]['data']['area']
    city = result['suggestions'][0]['data']['city']
    if city == 'Санкт-Петербург' or city == 'Москва' or city == 'Санкт-Петербург' or city == 'Москва':
        city = result['suggestions'][0]['data']['city_area']

    street_type = result['suggestions'][0]['data']['street_type_full']
    if street_type is None or len(street_type) == 0:
        street_type = result['suggestions'][0]['data']['settlement_type_full']
        street_name = result['suggestions'][0]['data']['settlement']
    else:
        street_name = result['suggestions'][0]['data']['street']
    house = result['suggestions'][0]['data']['house']
    building = result['suggestions'][0]['data']['block']
    apartment = result['suggestions'][0]['data']['flat']
    fulladdress = result['suggestions'][0]['value']
    longitude = result['suggestions'][0]['data']['geo_lon']
    latitude = result['suggestions'][0]['data']['geo_lat']

    return {'region': region, 'area': area, 'city': city, 'street_name': street_name, \
            'house': house, 'building': building, 'apartment': apartment, \
            'fulladdress': fulladdress, 'longitude': longitude, 'latitude': latitude}


def getparams(address, analogscount, objsquare=100):
    newanalogscount = analogscount
    if analogscount < 10:
        newanalogscount = 10
    if analogscount > 50:
        newanalogscount = 50
    macroRegionId = 0  # int('146000000000')
    regionId = 0  # int('146234000000')

    result = getdadata(address)
    fulladdress = result['fulladdress']
    longitude = result['longitude']
    latitude = result['latitude']
    region = result['region']
    city = result['city']
    street_name = result['street_name']
    house = result['house']
    building = result['building']
    apartment = result['apartment']
    conn_string = "host='37.18.75.197' dbname='omnia' user='postgres' password='Qwerty123'"
    conn = psycopg2.connect(conn_string)

    sql = "SELECT R.region_id, C.city_id \
      from rosreestr_regions R join rosreestr_cities C on R.region_id = C.region_id \
      where lower(R.region_name) like '%" + region.lower() + "%'"

    if city is not None and len(city) > 0:
        sql1 = sql + " and lower(C.city_name) = '" + city.lower() + "' LIMIT 1"
        df = pd.read_sql(sql1, con=conn)
        if df.count().region_id == 0:
            # if city != 'Санкт-Петербург' and city != 'Москва' and city != 'Санкт-Петербург' and city != 'Москва':
            sql = sql + " and lower(C.city_name) like '%" + city[:-1].lower() + "%'"
            # else:
            #    sql = sql + ' and C.city_name like "%' + city[:-1] + '%"'

            sql = sql + ' LIMIT 1'
    df = pd.read_sql(sql, con=conn)
    if df.count().region_id > 0:
        macroRegionId = df['region_id'][0]
        regionId = df['city_id'][0]
    else:
        print('did not found:', sql)
    # street = "Октябрьский"
    # house = "10В"
    # apartment = 213
    conn.close()
    values = {"macroRegionId": macroRegionId, "regionId": regionId}
    if len(street_name) > 0:
        values['street'] = street_name

    if len(house) > 0:
        values['house'] = house

    if apartment != None:
        if len(apartment) > 0:
            values['apartment'] = apartment

    rosreestr_url = 'http://rosreestr.ru/api/online/address/fir_objects'
    req = urllib.request.Request(rosreestr_url)
    req.add_header('Content-Type', 'application/json; charset=utf-8')
    jsondata = json.dumps(values)
    jsondataasbytes = jsondata.encode('utf-8')
    is_error = False
    try:
        contents = urllib.request.urlopen(req, jsondataasbytes, timeout=5).read()

    except Exception as exception:
        is_error = True
    if is_error:
        square = objsquare
        cadCost = 0.01
        cadNomer = ''
    else:
        result = ''
        if len(contents) < 1:
            res = getyandex(address)
            if len(res['street_name']) > 0:
                values['street'] = res['street_name'].replace(' улица', '')

            if len(res['house']) > 0:
                values['house'] = res['house']

            if len(res['building']) > 0:
                values['building'] = res['building']

            jsondata = json.dumps(values)
            jsondataasbytes = jsondata.encode('utf-8')
            contents = urllib.request.urlopen(req, jsondataasbytes).read()
            if len(contents) > 0:
                result = json.loads(contents.decode('utf-8'))

        else:
            result = json.loads(contents.decode('utf-8'))

        if len(result) > 0:
            try:
                objectId = result[0]['objectId']
                rosreestr_url = 'http://rosreestr.ru/api/online/fir_object/' + objectId
                req = urllib.request.Request(rosreestr_url)
                req.add_header('Content-Type', 'application/json; charset=utf-8')
                contents = urllib.request.urlopen(req).read()
                result = json.loads(contents.decode('utf-8'))
                square = result['parcelData']['areaValue']
                cadCost = result['parcelData']['cadCost']
                cadNomer = objectId
            except Exception as e:
                square = 0.0
                cadCost = 0.01
                cadNomer = ''
            if square < 0.01:
                square = objsquare
        else:
            square = objsquare
            cadCost = 0.01
            cadNomer = ''

    flatpos = fulladdress.find(", кв")
    if flatpos >= 0:
        address = fulladdress[:flatpos]
    else:
        address = fulladdress

    zkh = getzkh(address)
    project = zkh['project']
    foundation = zkh['foundation']
    buildingtype = zkh['housetype']
    buildingyear = zkh['buildingyear']

    if zkh['storeysnum'] > 1:
        storey = zkh['storeysnum'] - 1
    else:
        storey = 1
    storeysnum = zkh['storeysnum']

    ceilingheight = '2.70'
    #fincase_url = 'http://127.0.0.1:3006/api/getparams'

    if region == 'Санкт-Петербург' or region == 'Москва' or region == 'Санкт-Петербург' or region == 'Москва':
        city = region

    leavingsquare = square * 0.75
    kitchensquare = square * 0.20
    metrodistance = 15
    # parameters = {'totalsquare': square,
    #               'repairRaw': 'косметический',
    #               'longitude': longitude,
    #               'latitude': latitude,
    #               'housetype': housetype,
    #               'city': region,
    #               'buildingyear': buildingyear,
    #               'ceilingheight': ceilingheight,
    #               'storey': storey,
    #               'storeysNumb': storeysnum,
    #               'leavingsquare': leavingsquare,
    #               'kitchensquare': kitchensquare,
    #               'metrodistance': metrodistance,
    #               'analogscount': newanalogscount,
    #               'param': ["RoomsNum", "Storey",
    #                         "StoreysNum", "RawAddress",
    #                         "MicroDistrict", "RepairRaw", "BuildingYear",
    #                         "LivingSpaceArea", "KitchenArea", "SubwayTime"]
    #               }
    #url = '{}?{}'.format(fincase_url, urlencode(parameters))

    #req = urllib.request.Request(url)
    #req.add_header('Content-Type', 'application/json; charset=utf-8')
    #contents = urllib.request.urlopen(req).read()
    #result = json.loads(contents.decode('utf-8'))
    if buildingtype == 'элитный':
        buildingtype = 20
    elif buildingtype == 'монолитный':
       buildingtype = 18
    elif buildingtype == 'сталинский':
       buildingtype = 12
    elif buildingtype == 'панельный':
       buildingtype = 5
    elif buildingtype == 'кирпичный':
       buildingtype = 15
    elif buildingtype == 'блочный':
       buildingtype = 17
    elif buildingtype == 'металлоконструкции':
       buildingtype = 3
    elif buildingtype == 'железобетон':
       buildingtype = 18
    elif buildingtype == 'кирпично-монолитный':
       buildingtype = 19
    elif buildingtype == 'кирпично-монолитный':
       buildingtype = 19
    else:
       buildingtype = 4

    if analogscount < 10:
        analogscount = 10
    if analogscount > 50:
        analogscount = 50
    result = get_appraisal(latitude, longitude, square, buildingtype, buildingyear, analogscount)
    newanalogs = list(map(map_analog, result['analogs']))
    newanalogs = newanalogs[:analogscount]
    result['analogs'] = newanalogs[:analogscount]
    if result['data'] > 0:
        result['priceDivergency'] = 100.0 * (cadCost / result['data'] - 1.0)
    else:
        result['priceDivergency'] = 100.0
    result['cadCost'] = cadCost
    result['totalsquare'] = square
    result['kadastrovy_nomer'] = cadNomer
    result['price'] = result.pop('data')
    result.pop('calcanalogs')

    return result


def map_analog(analog):
    return {'houusetype': analog[0],
            'price': analog[1],
            'totalsquare ': analog[2],
            'city': analog[3],
            'lat': analog[4],
            'lon': analog[5],
            'roomsnum': analog[7],
            'storey': analog[8],
            'storeysnum': analog[9],
            'fulladdress': analog[10],
            'district': analog[11],
            'repair': analog[12],
            'buildingyear': analog[13],
            'leavingsquare': analog[14],
            'kitchensquare': analog[15],
            'metrodistance': analog[16],
            'analogstatus': analog[17],
            'analogindex': analog[18]
            }

#getparams('г. Москва, ул. Ухтомская, д.13, кв.11', 10)
#getparams('г. Смоленск, ул. Николаева, д.31, кв. 8', 10)
