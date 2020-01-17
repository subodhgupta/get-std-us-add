# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

import requests
import urllib
import time
from dataiku.customrecipe import *
import sys

# disable InsecureRequestWarning.
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

print ('## Running Plugin v0.5.0 ##')

input_name = get_input_names_for_role('input')[0]

# Recipe out

output_ = get_output_names_for_role('output')[0]
output_dataset = dataiku.Dataset(output_)

schema = [
    {'name': 'matchedAddress', 'type': 'string'}
    , {'name': 'latitude', 'type': 'double'}
    , {'name': 'longitude', 'type': 'double'}
    , {'name': 'tigerLineId', 'type': 'string'}
    , {'name': 'side', 'type': 'string'}
    , {'name': 'preDirection', 'type': 'string'}
    , {'name': 'streetName', 'type': 'string'}
    , {'name': 'suffixType', 'type': 'string'}
    , {'name': 'suffixDirection', 'type': 'string'}
    , {'name': 'city', 'type': 'string'}
    , {'name': 'state', 'type': 'string'}
    , {'name': 'zip', 'type': 'string'}
]

P_PAUSE = int(get_recipe_config()['param_api_throttle'])
P_COL_ADDRESS = get_recipe_config()['p_col_address']
# P_LON = get_recipe_config()['p_col_lon']

P_BENCHMARK = get_recipe_config()['p_benchmark']
P_VINTAGE = get_recipe_config()['p_vintage']

if P_BENCHMARK == "9":
    P_VINTAGE = "910"

print ('[+] BENCHMARK = {} ; VINTAGE = {} '.format(P_BENCHMARK, P_VINTAGE))

P_BATCH_SIZE_UNIT = int(get_recipe_config()['param_batch_size'])
if P_BATCH_SIZE_UNIT is None:
    P_BATCH_SIZE_UNIT = 50000

strategy = get_recipe_config()['param_strategy']

if get_recipe_config().get('p_id_column', None) is not None and get_recipe_config().get('p_id_column', None) <> '':
    use_column_id = True
    id_column = get_recipe_config().get('p_id_column', None)
    id_as_int = get_recipe_config().get('param_id_as_int', None)

    if id_as_int:
        schema.append({'name': id_column, 'type': 'int'})
    else:
        schema.append({'name': id_column, 'type': 'string'})
else:
    use_column_id = False

output_dataset.write_schema(schema)

b = -1
with output_dataset.get_writer() as writer:
    for df in dataiku.Dataset(input_name).iter_dataframes(chunksize=P_BATCH_SIZE_UNIT):

        b = b + 1
        n_b = b * P_BATCH_SIZE_UNIT

        df = df[abs(df[P_COL_ADDRESS] > 0)]

        if strategy == 'make_unique':
            dfu = df.groupby([P_COL_ADDRESS]).count().reset_index()
        else:
            dfu = df.copy()

        n__ = -1
        for v in dfu.to_dict('records'):

            n__ = n__ + 1
            n_record = n_b + n__

            address = v[P_COL_ADDRESS]
            # lon = v[P_LON]

            if use_column_id:
                id_ = v[id_column]

            # p = {'format': 'json',
            #      'y': lat,
            #      'x': lon,
            #      'benchmark': P_BENCHMARK,
            #      'vintage': P_VINTAGE,
            #      'layers': '10'
            #      }

            # Encode parameters 
            params = urllib.urlencode(
                {'format': 'json',
                 'address': address,
                 'benchmark': P_BENCHMARK,
                 'vintage': P_VINTAGE,
                 'layers': '1'
                 }
            )
            # Construct request URL
            url = 'https://geocoding.geo.census.gov/geocoder/locations/onelineaddress?' + params
            # print '%s - processing: (%s,%s,%s)' % (n_record,lat, lon, url)

            call_count = 0
            for call_count in range(0, 3):
                call = requests.get(url, verify=False)
                if call.status_code == 200:
                    break
                else:
                    time.sleep(2)

            if call.status_code == 200:

                data = call.json()

                try:
                    d = {}
                    s_geo = data['result'][u'addressMatches'][0]
                    d['matchedAddress'] = s_geo[u'matchedAddress']

                    s_geo = data['result'][u'addressMatches'][u'coordinates'][0]
                    d['latitude'] = s_geo[u'x']
                    d['longitude'] = s_geo[u'y']

                    s_geo = data['result'][u'addressMatches'][u'tigerLine'][0]
                    d['tigerLineId'] = s_geo[u'tigerLineId']
                    d['side'] = s_geo[u'side']

                    s_geo = data['result'][u'addressMatches'][u'addressComponents'][0]
                    d['preDirection'] = s_geo[u'preDirection']
                    d['streetName'] = s_geo[u'streetName']
                    d['suffixType'] = s_geo[u'suffixType']
                    d['suffixDirection'] = s_geo[u'suffixDirection']
                    d['city'] = s_geo[u'city']
                    d['state'] = s_geo[u'state']
                    d['zip'] = s_geo[u'zip']

                    col_list_ = ['matchedAddress',
                                 'latitude',
                                 'longitude',
                                 'tigerLineId',
                                 'side',
                                 'preDirection',
                                 'streetName',
                                 'suffixType',
                                 'suffixDirection',
                                 'city',
                                 'state',
                                 'zip'
                                 ]

                    if use_column_id is True:
                        if id_as_int:
                            d[id_column] = int(id_)
                        else:
                            d[id_column] = id_

                    writer.write_row_dict(d)

                except:
                    print 'Unable to find these coordinates in the US Census API: Record #:%s, address:%s, url:%s' % (
                    n_record, address, url)

            else:
                print 'Failed. API status: %s' % (call.status_code)
                print 'The plugin will write the output dataset where the process stopped. You should probably consider filtering your input dataset where the plugin stopped and select the append mode for the input/output panel.'
                sys.exit(1)

            time.sleep(P_PAUSE)
