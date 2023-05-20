import requests
import json


def get_cour(offset):

    url_cour = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers'
    headers = {'X-Nickname': 'sandrotsin',
        'X-Cohort': '12',
        'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'}
    
    params_cour = {
        'sort_field': '_id',
        'sort_direction': 'asc',
        'limit': 50,
        'offset': offset
    }

    resp = requests.get(url_cour, headers = headers, params = params_cour).json()
    return resp


def get_del(offset, date_from, date_to):

    url_del = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries'
    headers = {'X-Nickname': 'sandrotsin',
        'X-Cohort': '12',
        'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'}

    params_del = {
        'sort_field': 'date',
        'sort_direction': 'asc',
        'limit': 50,
        'offset': offset,
        'from': date_from,
        'to': date_to
    }

    resp = requests.get(url_del, headers = headers, params = params_del).json()
    return resp



