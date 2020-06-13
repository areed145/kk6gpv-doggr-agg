import numpy as np
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import time
import json
import os

client = MongoClient(os.environ['MONGODB_CLIENT'])
db = client.petroleum


def agg():
    df_prod = pd.DataFrame(list(db.doggr.aggregate([
        {'$unwind': '$prod'},
        {'$match': {'prod.oil': {'$gt': 0}}},
        {'$group': {
            '_id': {"api": "$api"},
            'api': {'$first': '$api'},
            'latitude': {'$first': '$latitude'},
            'longitude': {'$first': '$longitude'},
            'api': {'$first': '$api'},
            'oil': {'$sum': '$prod.oil'},
            'water': {'$sum': '$prod.water'},
            'gas': {'$sum': '$prod.gas'},
        }}
    ])))

    df_inj = pd.DataFrame(list(db.doggr.aggregate([
        {'$unwind': '$inj'},
        {'$match': {'inj.wtrstm': {'$gt': 0}}},
        {'$group': {
            '_id': {"api": "$api"},
            'api': {'$first': '$api'},
            'wtrstm': {'$sum': '$inj.wtrstm'},
        }},
    ])))

    apis = set(list(df_prod['api'])+list(df_inj['api']))
    for api in apis:
        cums = {}
        try:
            row_prod = df_prod[df_prod['api'] == api]
            for col in ['oil', 'water', 'gas']:
                try:
                    cums[col+'_cum'] = row_prod[col].values[0]
                except:
                    pass
        except:
            pass
        try:
            row_inj = df_inj[df_inj['api'] == api]
            for col in ['wtrstm']:
                try:
                    cums[col+'_cum'] = row_inj[col].values[0]
                except:
                    pass
        except:
            pass

        try:
            db.doggr.update_one({'api': api}, {'$set': cums})
            print(api)
        except:
            print('failed')
            pass


if __name__ == '__main__':
    last_hour = datetime.now().hour - 1
    while True:
        if datetime.now().hour != last_hour:
            agg()
            last_hour = datetime.now().hour
            print('got long')
        else:
            print('skipping updates')
        time.sleep(10)
