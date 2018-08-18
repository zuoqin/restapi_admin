#!/usr/bin/python
# -*- coding: utf-8 -*-
import psycopg2
import psycopg2.extras
import pandas as pd

from flask import Flask, jsonify, request
from flasgger import Swagger
from flask_jwt_extended import (create_access_token,
    create_refresh_token, jwt_required, jwt_refresh_token_required,
    get_jwt_identity, get_raw_jwt)
from datetime import datetime, timedelta
app = Flask(__name__)
Swagger(app)


from flask_jwt_extended import JWTManager
app.config['JWT_SECRET_KEY'] = 'jwt-secret-string'
#app.config['JWT_EXPIRATION_DELTA'] = timedelta(days=10)
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(days=10)
jwt = JWTManager(app)


import sys
import urllib.request
import urllib.parse
from urllib.parse import urlencode
import json

from flask_restful import reqparse, request, abort, Api, Resource
from flask_cors import CORS



CORS(app)

conn_string = "host='37.18.75.197' port=5432 dbname='omnia' user='postgres' password='Qwerty123'"

def getzkh(address):
  #import MySQLdb
  #db = MySQLdb.connect("localhost","root","Qwerty123","fincase" )
  #db.set_character_set('utf8')
  #print('will search zkh for address: ' + address)
  conn_string = "host='37.18.75.197' dbname='omnia' user='postgres' password='Qwerty123'"
  #print ("Connecting to database\n  ->%s" % (conn_string))
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
  #cursor = db.cursor()
  #cursor.execute('SET NAMES utf8;')
  #cursor.execute('SET CHARACTER SET utf8;')
  #cursor.execute('SET character_set_connection=utf8;')
  #print(sql)
  #cursor.execute(sql)
  # Fetch all the rows in a list of lists.
  #results = cursor.fetchall()
  df = pd.read_sql(sql, con = conn)
  storeysnum = 9
  if df.count().project_type > 0:
      #print('Retrieved data from zkh')
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
  return {'project': project, 'house': house, 'foundation': foundation,
    'housetype': housetype, 'storeysnum' : storeysnum, 'buildingyear': buildingyear,
    'region': region}


def getyandex(address):
    yandex_url = 'https://geocode-maps.yandex.ru/1.x/?geocode='
    address = urllib.parse.quote_plus(address)
    yandex_url = yandex_url + address
    yandex_url = yandex_url + '&format=json&result=1'
    req = urllib.request.Request(yandex_url)
    contents = urllib.request.urlopen(req).read()
    result = json.loads(contents.decode('utf-8'))

    address = result['response']['GeoObjectCollection']['featureMember'][0]['GeoObject']['metaDataProperty']['GeocoderMetaData']['Address']['Components']
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

    fulladdress = result['response']['GeoObjectCollection']['featureMember'][0]['GeoObject']['metaDataProperty']['GeocoderMetaData']['AddressDetails']['Country']['AddressLine']

    address = result['response']['GeoObjectCollection']['featureMember'][0]['GeoObject']['Point']['pos']
    longitude = address.split(' ')[0]
    latitude = address.split(' ')[1]
    bindex = house.find('к')
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
  req.add_header('X-Secret', 'd6bc0e31ee4b878691e051e7fb3ac1cd787fe7d9')
  req.add_header('Authorization', 'Token 22a558542ffe2926bc0b384fa349ecad8885365b')

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
  if street_type == None or len(street_type) == 0:
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

  #print('fulladdress from dadata: ' + fulladdress + '; longitude: ' + longitude + '; latitude: ' + latitude)

  return {'region': region, 'area': area, 'city': city, 'street_name': street_name, \
   'house': house, 'building': building, 'apartment': apartment, \
   'fulladdress': fulladdress, 'longitude': longitude, 'latitude': latitude}


def getparams(address, analogscount):
  #address = request.args.get('address', type = str)
  #import MySQLdb
  #db = MySQLdb.connect("localhost","root","Qwerty123","fincase" )
  #db.set_character_set('utf8')
  parameters = {'totalsquare': 89.9,
          'cadCost': 2000000.08,
          'longitude': 55.7,
          'latitude': 88.3,
          'housetype': 'new type',
          'city': 'new city',
          'buildingyear': 2078,
          'ceilingheight': 2.89,
          'storey': 7,
          'storeysnum': 99,
          'pricePerMetr': 76567.91,
          'houseAvrgPrice': 56456.98,
          'regionAvrgPrice': 234567.67,
          'cityAvrgPrice': 9874567.32
        }
  #return parameters
  newanalogscount = analogscount
  if analogscount < 10:
      newanalogscount = 10
  if analogscount > 50:
      newanalogscount = 50
  macroRegionId = 0 #int('146000000000')
  regionId = 0 #int('146234000000')


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
  #print ("Connecting to database\n  ->%s" % (conn_string))
  conn = psycopg2.connect(conn_string)

  sql = "SELECT R.region_id, C.city_id \
      from rosreestr_regions R join rosreestr_cities C on R.region_id = C.region_id \
      where lower(R.region_name) like '%" + region.lower() + "%'"

  #if city != 'Санкт-Петербург' and city != 'Москва' and city != 'Санкт-Петербург' and city != 'Москва':
  if city is not None and len(city) > 0:
      sql = sql + " and lower(C.city_name) like '%" + city[:-1].lower() + "%'"
  #else:
  #    sql = sql + ' and C.city_name like "%' + city[:-1] + '%"'

  sql = sql + ' LIMIT 1'
  # prepare a cursor object using cursor() method
  #cursor = db.cursor()
  #cursor.execute('SET NAMES utf8;')
  #cursor.execute('SET CHARACTER SET utf8;')
  #cursor.execute('SET character_set_connection=utf8;')
  #print(sql)
  #cursor.execute(sql)
  # Fetch all the rows in a list of lists.
  #results = cursor.fetchall()
  df = pd.read_sql(sql, con = conn)
  if df.count().region_id > 0:
      macroRegionId = df['region_id'][0]
      regionId = df['city_id'][0]

      #print('Obtained macroRegionId and regionId from rosreestr')
  else:
      print('did not found:')
      print(sql)



  #street = "Октябрьский"
  #house = "10В"
  #apartment = 213

  values = {"macroRegionId": macroRegionId, "regionId": regionId}
  if len(street_name) > 0:
      values['street'] = street_name

  if len(house) > 0:
      values['house'] = house

  if apartment != None:
      if len(apartment) > 0:
          values['apartment'] = apartment


  #print('Send values to rosreestr:')
  print(values)

  rosreestr_url = 'http://rosreestr.ru/api/online/address/fir_objects'
  req = urllib.request.Request(rosreestr_url)
  req.add_header('Content-Type', 'application/json; charset=utf-8')
  jsondata = json.dumps(values)
  jsondataasbytes = jsondata.encode('utf-8')
  contents = urllib.request.urlopen(req, jsondataasbytes).read()

  print(contents)
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
      objectId = result[0]['objectId']
      print('Obtained from rosreestr objectId: ' + objectId)
      rosreestr_url = 'http://rosreestr.ru/api/online/fir_object/' + objectId
      req = urllib.request.Request(rosreestr_url)
      req.add_header('Content-Type', 'application/json; charset=utf-8')
      contents = urllib.request.urlopen(req).read()
      result = json.loads(contents.decode('utf-8'))

      square = result['parcelData']['areaValue']
      cadCost = result['parcelData']['cadCost']
      cadNomer = objectId
  else:
      square = 0.01
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
  housetype = zkh['housetype']
  buildingyear = zkh['buildingyear']



  if zkh['storeysnum'] > 1:
    storey = zkh['storeysnum'] - 1
  else:
    storey = 1
  storeysnum = zkh['storeysnum']

  #print('before 127')
  ceilingheight = '2.70'
  #fincase_url = 'http://127.0.0.1:3006/api/getparams'

  if region == 'Санкт-Петербург' or region == 'Москва' or region == 'Санкт-Петербург' or region == 'Москва':
      city = region

  leavingsquare = square * 0.75
  kitchensquare = square * 0.20
  metrodistance = 15
  parameters = {'totalsquare': square,
          'repairRaw': 'косметический',
          'longitude': longitude,
          'latitude': latitude,
          'housetype': housetype,
          'city': city,
          'buildingyear': buildingyear,
          'ceilingheight': ceilingheight,
          'storey': storey,
          'storeysNumb': storeysnum,
          'leavingsquare': leavingsquare,
          'kitchensquare': kitchensquare,
          'metrodistance': metrodistance,
          'analogscount': newanalogscount,
          'param': ["RoomsNum", "Storey",
               "StoreysNum",  "RawAddress",
               "MicroDistrict", "RepairRaw","BuildingYear",
               "LivingSpaceArea", "KitchenArea", "SubwayTime"]
        }
  #url = '{}?{}'.format(fincase_url, urlencode(parameters))

  #print('Will obtain price for: ' + url)
  #req = urllib.request.Request(url)
  #req.add_header('Content-Type', 'application/json; charset=utf-8')
  #contents = urllib.request.urlopen(req).read()
  #result = json.loads(contents.decode('utf-8'))


  newanalogs = list(map(map_analog, result['analogs']))
  newanalogs = newanalogs[:analogscount]
  result['analogs'] = newanalogs[:analogscount]
  result['priceDivergency'] = 100.0 * (cadCost / result['data'] - 1.0)
  result['cadCost'] = cadCost
  result['totalsquare'] = square
  result['kadastrovy_nomer'] = cadNomer
  result['price'] = result.pop('data')
  result.pop('calcanalogs')

  #print(result)

  return result

def map_analog(analog):
    return {'houusetype': analog[0],
        'price': analog[1],
        'totalsquare ': analog[2],
        'city': analog[3],
        'lat': analog[4],
        'lon': analog[5],
        'roomsnum': analog[6],
        'storey': analog[7],
        'storeysnum': analog[8],
        'fulladdress': analog[9],
        'district': analog[10],
        'repair': analog[11],
        'buildingyear': analog[12],
        'leavingsquare': analog[13],
        'kitchensquare': analog[14],
        'metrodistance': analog[15],
        'analogstatus': analog[16],
        'analogindex': analog[17]
    }

@app.route('/api/getparams', methods=['GET'])
@jwt_required
def get_params_by_address():
    """
    This is FinCase RealEstate API
    Вызовите этот метод и передайте адрес в качестве параметра
    ---
    tags:
      - Финкейс Жилая Недвижимость API
    parameters:
      - in: header
        name: Authorization
        type: string
        required: true
      - name: address
        in: query
        type: string
        required: true
        description: Адрес объекта жилой недвижимости в любом формате
      - name: analogscount
        in: query
        type: number
        required: true
        default: 10
        description: Количество возвращаемых аналогов
    definitions:
      Analog:
            type: object
            properties:
              id:
                type: integer
                description: Идентификатор объекта
                default: 10
              housetype:
                type: string
                description: Материал здания
                default: 'панельный'
              price:
                type: number
                description: Цена объекта
                default: 2000.99
              totalsquare:
                type: number
                description: Общая площадь объета
                default: 100.0
              leavingsquare:
                type: number
                description: Жилая площадь объета
                default: 100.0
              kitchensquare:
                type: number
                description: Площадь кухни объета
                default: 100.0
              city:
                type: string
                description: Город
                default: 'Москва'
              repair:
                type: string
                description: Тип ремонта
                default: 'косметический'
              lat:
                type: number
                description: Широта расположения объекта
                default: 55.89
              lon:
                type: number
                description: Долгота расположения объекта
                default: 37.89
              buildingyear:
                type: integer
                description: Год постройки
                default: 2000
              ceilingheight:
                type: number
                description: Высота потолков
                default: 2.71
              floor:
                type: integer
                description: Этаж расположения
                default: 2
              storeys:
                type: integer
                description: Количество этажей в здании
                default: 9
              fulladdress:
                type: string
                description: Адрес объекта
                default: 'Москва'
    responses:
      500:
        description: Ошибка, адрес некорректный
      200:
        description: Наиболее полный набор параметров и оценка стоимости объекта
        schema:
          id: object_params
          properties:
            analogs:
              type: array
              description: Аналогичные объекты искомому
              items:
                  $ref: '#/definitions/Analog'
            buildingyear:
              type: integer
              description: Год постройки объекта
              default: 2000
            ceilingheight:
              type: number
              description: Высота потолков
              default: 2.70
            city:
              type: string
              description: Город нахождения объекта
              default: 'Москва'
            housetype:
              type: string
              description: Материал здания
              default: 'панельный'
            latitude:
              type: number
              description: Географическая широта объекта
              default: 55.751244
            longitude:
              type: number
              description: Географическая долгота объекта
              default: 37.618423
            storey:
              type: integer
              description: Этаж расположения объекта
              default: 7
            storeysnum:
              type: integer
              description: Количество этажей в здании
              default: 9
            totalsquare:
              type: number
              description: Общая площадь объета
              default: 100.0
            cadCost:
              type: number
              description: Кадастровая стоимость объекта
              default: 1000000.00
            kadastrovy_nomer:
              type: string
              description: Кадастровый номер объекта
              default: ''
            priceDivergency:
              type: number
              description: Отклонение кадастровой стоимости от рыночной в процентах
              default: 3.09
            price:
              type: number
              description: Текущая цена объекта
              default: 1000000.00
            pricePerMetr:
              type: number
              description: Стоимость квадратного метра объекта
              default: 1000000.00
            houseAvrgPrice:
              type: number
              description: Средняя цена квадратного метра в доме
              default: 1000000.00
            regionAvrgPrice:
              type: number
              description: Средняя цена квадратного метра в районе
              default: 1000000.00
            cityAvrgPrice:
              type: number
              description: Средняя цена квадратного метра в городе
              default: 1000000.00

    """
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    current_user = get_jwt_identity()
    sql  = 'select id from accounts_customuser where username=\'' + current_user + '\''
    df = pd.read_sql(sql, con=conn)

    address = request.args.get('address', default = '*', type = str)
    analogscount = request.args.get('analogscount', default = 10, type = int)

    sql = 'insert into user_actions (user_id, action, time, comment) values(' +\
        str(df['id'][0]) + ', 1, current_timestamp' + ', \'' +\
        '/api/getparams address=' + address + '\')'
    cur.execute(sql)
    conn.commit()
    cur.close()
    result = getparams(address, analogscount)
    return jsonify(
        result
    )


@app.route('/api/login', methods=['POST'])
def get_token():
    """
    This is FinCase RealEstate API
    Вызовите этот метод и передайте имя пользователя и пароль в качестве параметра
    ---
    tags:
      - Финкейс Жилая Недвижимость API
    parameters:
      - in: body
        name: user
        schema:
            type: object
            required:
                - username
            properties:
                username:
                    type: string
                password:
                    type: string
    responses:
      500:
        description: Ошибка, адрес некорректный
      200:
        description: Токен для доступа к ресурсам
        schema:
          id: login_params
          properties:
            token:
              type: string
              description: Код досутпа
              default: ''
    """

    import os

    # We defer to a DJANGO_SETTINGS_MODULE already in the environment. This breaks
    # if running multiple sites in the same mod_wsgi process. To fix this, use
    # mod_wsgi daemon mode with each site in its own daemon process, or use
    # os.environ["DJANGO_SETTINGS_MODULE"] = "django_custom_user_example.settings"
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "customuser.settings")

    import django
    django.setup()
    from django.contrib.auth import authenticate

    parser = reqparse.RequestParser()
    parser.add_argument('username', help='This field cannot be blank', required=True)
    parser.add_argument('password', help='This field cannot be blank', required=True)
    data = parser.parse_args()

    user = authenticate(username=data.username, password=data.password)

    if user is not None:
        access_token = create_access_token(identity=data['username'])
    else:
        access_token = ''
    result = {'token': access_token}
    return jsonify(
        result
    )

@app.route('/api/getzkh', methods=['GET'])
@jwt_required
def get_zkh_by_address():
    """
    This is FinCase RealEstate API
    Вызовите этот метод и передайте адрес в качестве параметра
    ---
    tags:
      - Финкейс Жилая Недвижимость API
    parameters:
      - in: header
        name: Authorization
        type: string
        required: true
      - name: address
        in: query
        type: string
        required: true
        description: Адрес объекта жилой недвижимости в любом формате
    responses:
      500:
        description: Ошибка, адрес некорректный
      200:
        description: Наиболее полный набор параметров БТИ объекта
        schema:
          id: zkh_params
          properties:
            buildingyear:
              type: integer
              description: Год постройки объекта
              default: 2000
            region:
              type: string
              description: Регион нахождения объекта
              default: 'Москва'
            foundation:
              type: string
              description: Основание здания
              default: 'Ленточный'
            house:
              type: string
              description: Тип дома
              default: 'Многоквартирный дом'
            project:
              type: string
              description: Проект или серия дома
              default: 'Индивидуальный проект'
            housetype:
              type: string
              description: Материал здания
              default: 'панельный'
            storeysnum:
              type: integer
              description: Количество этажей в здании
              default: 9
    """

    address = request.args.get('address', default = '*', type = str)
    fulladdress = getdadata(address)['fulladdress']
    flatpos = fulladdress.find(", кв")
    if flatpos >= 0:
      address = fulladdress[:flatpos]
    else:
      address = fulladdress

    result = getzkh(address)
    #print(result)
    return jsonify(
        result
    )

app.run(debug=True,host='127.0.0.1', port=3005)
#getyandex('город Москва, улица Большая Набережная, д 19 корп 1  кв 20')
#getparams('город Москва, улица Большая Набережная, д 19 корп 1  кв 20', 10)