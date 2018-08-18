#!/usr/bin/python
# -*- coding: utf-8 -*-
import psycopg2
import psycopg2.extras
import pandas as pd
import random
import apilib
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
conn_string = "host='37.18.75.197' port=5432 dbname='omnia' user='postgres' password='Qwerty123'"

from flask_restful import reqparse, request
from flask_cors import CORS



CORS(app)

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
      - name: totalsquare
        in: query
        type: number
        required: false
        default: 100
        description: Площадь объекта (по умолчанию 100 если отсутствует в росреестре)
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
    sql = 'select id from accounts_customuser where username=\'' + current_user + '\''
    df = pd.read_sql(sql, con=conn)

    address = request.args.get('address', default = '*', type = str)
    analogscount = request.args.get('analogscount', default = 10, type = int)
    totalsquare = request.args.get('square', default = 100, type = float)

    sql = 'insert into user_actions (user_id, action, time, comment) values(' +\
        str(df['id'][0]) + ', 1, current_timestamp' + ', \'' +\
        '/api/getparams address=' + address + '\')'
    cur.execute(sql)
    conn.commit()
    cur.close()

    result = apilib.getparams(address, analogscount, totalsquare)
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


app.run(debug=True, host='127.0.0.1', port=3005)
