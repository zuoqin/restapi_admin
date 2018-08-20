#!/usr/bin/python
# -*- coding: utf-8 -*-
import psycopg2
import psycopg2.extras
import random
from flask import Flask, jsonify, request, make_response
from flasgger import Swagger
from datetime import datetime, timedelta
import pandas as pd
from flask_jwt_extended import (create_access_token,
    create_refresh_token, jwt_required, jwt_refresh_token_required,
    get_jwt_identity, get_raw_jwt)

app = Flask(__name__)
Swagger(app)
import apilib
import pickle
import os.path
from flask_jwt_extended import JWTManager
app.config['JWT_SECRET_KEY'] = 'jwt-secret-string'
jwt = JWTManager(app)


from sklearn.neighbors import KNeighborsClassifier
from sklearn.neighbors import NearestNeighbors

cbt = 1
cby = 1
cta = 1
cd = 1


import sys
import urllib.request
import urllib.parse
from urllib.parse import urlencode
import json

from flask_restful import reqparse, request, abort, Api, Resource
from flask_cors import CORS

conn_string = "host='37.18.75.197' port=5432 dbname='omnia' user='postgres' password='Qwerty123'"

CORS(app)

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
        return make_response('Неверное имя пользователя или пароль', 401)
    result = {'token': access_token}
    return jsonify(
        result
    )

@app.route('/api/getparams', methods=['GET'])
@jwt_required
def get_params():
    """
    This is FinCase RealEstate API
    Вызовите этот метод и передайте адрес в качестве параметра
    ---
    tags:
      - Финкейс Жилая Недвижимость API
    parameters:
    parameters:
      - in: header
        name: Authorization
        type: string
        required: true
      - name: totalsquare
        in: query
        type: number
        required: true
        description: Площадь квартиры
      - name: repairRaw
        in: query
        type: string
        required: true
        description: Ремонт
      - name: longitude
        in: query
        type: number
        required: true
        description: Долгота расположения
      - name: latitude
        in: query
        type: number
        required: true
        description: Широта расположения
      - name: housetype
        in: query
        type: string
        required: true
        description: Тип дома
      - name: city
        in: query
        type: string
        required: true
        description: Город
      - name: buildingyear
        in: query
        type: integer
        required: true
        description: Год постройки
      - name: storey
        in: query
        type: integer
        required: true
        description: Этаж
      - name: storeysnum
        in: query
        type: integer
        required: true
        description: Количество этажей в здании
      - name: storeysnum
        in: query
        type: integer
        required: true
        description: Количество этажей в здании
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
                type: float
                description: Общая площадь объета
                default: 100.0
              leavingsquare:
                type: float
                description: Жилая площадь объета
                default: 100.0
              kitchensquare:
                type: float
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
                type: float
                description: Широта расположения объекта
                default: 55.89
              lon:
                type: float
                description: Долгота расположения объекта
                default: 37.89
              buildingyear:
                type: integer
                description: Год постройки
                default: 2000
              ceilingheight:
                type: float
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
              type: float
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
              type: float
              description: Географическая широта объекта
              default: 55.751244
            longitude:
              type: float
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
              type: float
              description: Общая площадь объета
              default: 100.0
            cadCost:
              type: float
              description: Кадастровая стоимость объекта
              default: 1000000.00
            priceDivergency:
              type: float
              description: Отклонение кадастровой стоимости от рыночной в процентах
              default: 3.09
            price:
              type: float
              description: Текущая цена объекта
              default: 1000000.00
            pricePerMetr:
              type: float
              description: Стоимость квадратного метра объекта
              default: 1000000.00
            houseAvrgPrice:
              type: float
              description: Средняя цена квадратного метра в доме
              default: 1000000.00
            regionAvrgPrice:
              type: float
              description: Средняя цена квадратного метра в районе
              default: 1000000.00
            cityAvrgPrice:
              type: float
              description: Средняя цена квадратного метра в городе
              default: 1000000.00

    """

    lat = request.args.get('latitude', default = 55.751244, type = float)
    lon = request.args.get('longitude', default = 37.618423, type = float)
    buildingyear = request.args.get('buildingyear', default = 2000, type = int)
    buildingtype = request.args.get('housetype', default = 'кирпичный', type = str)
    totalarea = request.args.get('totalsquare', default = 37.618423, type = float)
    region = request.args.get('city', default = 'Москва', type = str)
    analogscount = request.args.get('analogscount', default = 30, type = int)
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


    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    current_user = get_jwt_identity()
    sql = 'select id from accounts_customuser where username=\'' + current_user + '\''
    df = pd.read_sql(sql, con=conn)

    sql = 'insert into user_actions (user_id, action, time, comment) values(' +\
        str(df['id'][0]) + ', 1, current_timestamp' + ', \'' +\
        '/api/getappraisal lat={}, lon={}, totalarea={}, buildingtype={}, buildingyear={}, analogscount={}'.\
        format(lat, lon, totalarea, buildingtype, buildingyear, analogscount) + '\')'
    cur.execute(sql)
    conn.commit()
    cur.close()

    result = apilib.get_appraisal(lat, lon, totalarea, buildingtype, buildingyear, analogscount)
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
    fulladdress = apilib.getdadata(address)['fulladdress']
    flatpos = fulladdress.find(", кв")
    if flatpos >= 0:
      address = fulladdress[:flatpos]
    else:
      address = fulladdress

    result = apilib.getzkh(address)
    return jsonify(
        result
    )

app.run(debug=True,host='0.0.0.0', port=3001)

