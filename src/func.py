
import os
import requests
import re
import json
import datetime
import pytz
import pickle
import pandas as pd
import geopandas as gpd
from numpy import nan
from shapely.geometry import Polygon, multilinestring, MultiLineString, LineString
taipei_tz = pytz.timezone('Asia/Taipei')


class TDX_AUTH():
    '''
    Get TDX API token
    Optimize token access by obtaining it once, saving it as a pickle file,
    and refreshing it only when it has expired.
    '''
    def __init__(self):
        self.client_id = '[your_tdx_client_id]'
        self.client_secret = '[your_tdx_client_secret]'

    def get_token(self, token_path):
        
        now_time = datetime.datetime.now()
        if os.path.exists(token_path):
            with open(token_path, 'rb') as handle:
                res = pickle.load(handle)

            if res:
                expired_time = res['expired_time']
                not_expired = (now_time < expired_time)
                if not_expired:
                    token = res['access_token']
                    return token

        token_url = 'https://tdx.transportdata.tw/auth/realms/TDXConnect/protocol/openid-connect/token'
        headers = {'content-type': 'application/x-www-form-urlencoded'}
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        
        response = requests.post(token_url, headers=headers, data=data)
        res_json = response.json()
        # print(response.status_code)
        # print(response.json())
        token = res_json['access_token']
        expired_time = now_time + datetime.timedelta(seconds=res_json['expires_in'])
        res = {'access_token': token, 'expired_time': expired_time}
        with open(token_path, 'wb') as handle:
            pickle.dump(res, handle)
            
        return token


def to_time_contain_chinese_string(x):
    '''
    處理時間欄位裡包含上午/下午，甚至後面附帶.000

    Example
    ----------
    to_time_contain_chinese_string(None)
    to_time_contain_chinese_string("2022/7/14 上午 12:00:00")
    to_time_contain_chinese_string("2022/7/14 下午 12:00:00")
    to_time_contain_chinese_string("2022/7/14 下午 12:00:00.000")
    '''
    if x:
        x = x.replace('.000', '')
        x = x.replace('  ', ' ')
        split_x = x.split(' ')
        if split_x[1] == '上午':
            hour = int(split_x[2][0:2])
            if hour == 12:  # 上午12=00點
                fine_x = split_x[0] + ' ' + '00'+ split_x[2][2:]
            else:  # 不用轉換
                fine_x = split_x[0] + ' ' + split_x[2]
        elif split_x[1] == '下午':
            hour = int(split_x[2][0:2])+12  # 下午 = +12
            if hour == 24:  # 下午12點=12點
                hour = 12
            fine_x = split_x[0] + ' ' + str(hour) + split_x[2][2:]
        else:
            # print(x)
            pattern = '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
            if re.match(pattern, x)[0]:
                fine_x = x
            else:
                fine_x = re.findall(pattern, x)[0]
        return fine_x
    else:
        return None


def _parse_from_format(from_format):
    '''
    解析from_format以做後續利用

    Example
    ----------
    time_column = pd.Series(['111/12/31', '110/12/31'])
    pattern, items = _parse_from_format(from_format='cy/m/d')

    time_column = pd.Series(['111-12-31', '110-12-31'])
    pattern, items = _parse_from_format(from_format='cy-m-d')

    time_column = pd.Series(['2022/12/31', '2021/1/31'])
    pattern, items = _parse_from_format(from_format='y/m/d')

    time_column = pd.Series(['110/12/31 00:12:21', '111/1/31 01:02:03'])
    pattern, items = _parse_from_format(from_format='cy/m/d H:M:S')
    '''
    from_format += ';'
    sep_list = [':', ' ', ',', '/', '-']

    items = []
    pattern = ''
    temp = ''
    for char in from_format:
        if char in sep_list:
            sep = char
            items.append(temp)
            pattern += f'([0-9]+){sep}'
            temp = ''
        elif char == ';':
            pattern += '([0-9]+)'
            items.append(temp)
        else:
            temp += char

    return pattern, items


def _standardize_time_string(column, from_format):
    '''
    根據提供的form_format，將input處理成標準時間格式

    Example
    ----------
    time_column = pd.Series(['111/12/31', '110/12/31'])
    datetime_str = _standardize_time_string(time_column, from_format='cy/m/d')

    time_column = pd.Series(['111-12-31', '110-12-31'])
    datetime_str = _standardize_time_string(time_column, from_format='cy-m-d')

    time_column = pd.Series(['2022/12/31', '2021/1/31'])
    datetime_str = _standardize_time_string(time_column, from_format='y/m/d')

    time_column = pd.Series(['110/12/31 00:12:21', '111/1/31 01:02:03'])
    datetime_str = _standardize_time_string(time_column, from_format='cy/m/d H:M:S')
    '''

    pattern, items = _parse_from_format(from_format)
    splited_column = column.str.extract(pattern)
    splited_column.columns = items

    for item in items:
        if item == 'cy':
            temp_column = splited_column[item].copy()
            temp_column = temp_column.astype(float) + 1911
            splited_column['y'] = temp_column.astype(int).astype(str)

    datetime_col = pd.Series(['']*splited_column.shape[0])
    item_founded = ''
    pre_time_item = ''
    for time_item in ['y', 'm', 'd', 'H', 'M', 'S']:
        try:
            if pre_time_item == '':
                pass
            elif pre_time_item == 'y':
                datetime_col += '-'
            elif pre_time_item == 'm':
                datetime_col += '-'
            elif pre_time_item == 'd':
                datetime_col += ' '
            elif pre_time_item == 'H':
                datetime_col += ':'
            elif pre_time_item == 'M':
                datetime_col += ':'
            else:
                raise ValueError(f'Not valid previous time format code *{pre_time_item}*!')
            datetime_col += splited_column[time_item]
            item_founded += time_item
            # print(splited_column[time_item])
        except KeyError:
            print(f'*{time_item}* not found, only *{item_founded}*')
            break
        pre_time_item = time_item
    return datetime_col


def convert_str_to_time_format(
    column: pd.Series,
    from_format=None,
    output_level='datetime',
    output_type='time',
    is_utc=False, from_timezone='Asia/Taipei'
    ) -> pd.Series:
    '''
    時間處理 function
    Input should be pd.Series with string.
    Output type depending on para output_level and output_type.

    Parameters
    ----------
    output_level: "date" or "datetime", default "datetime".
    output_type: "str" or "time", default "time".
    from_format: defalut None, means format were common, let function automatically parse.
        Or, you can given string like "ty/m/d" or "y-m-d",
        function will split input string by "/" then convert to time format.
        Format "ty" is taiwan year, ty will +1911 to western year.
        All allowed code is [y, m, d, H, M, S].
    is_utc: defalut False, which means input is not UTC timezone.
    from_timezone: defalut "Asia/Taipei", if is_utc=False, from_timezone must be given.
        if is_utc=True, from_timezone will be ignored.

    Example
    ----------
    t1 = to_time_contain_chinese_string("2022/7/14 上午 12:00:00")
    t2 = to_time_contain_chinese_string("2022/7/14 下午 12:00:00.000")
    time_column = pd.Series([t1, t2])
    date_col = convert_str_to_time_format(time_column, output_level='date')

    time_column = pd.Series(['111/12/31', '110/12/31'])
    datetime_col = convert_str_to_time_format(time_column, from_format='cy/m/d')

    time_column = pd.Series(['111-12-31', '110-12-31'])
    datetime_col = convert_str_to_time_format(time_column, from_format='cy-m-d')

    time_column = pd.Series(['2022/12/31', '2021/1/31'])
    datetime_col = convert_str_to_time_format(time_column, from_format='y/m/d')
    datetime_col = convert_str_to_time_format(time_column, from_format='y/m/d', is_utc=True)

    time_column = pd.Series(['110/12/31 00:12:21', '111/1/31 01:02:03'])
    datetime_col = convert_str_to_time_format(time_column, from_format='cy/m/d H:M:S')
    date_col = convert_str_to_time_format(time_column, from_format='cy/m/d H:M:S', output_level='date')
    datetime_col = convert_str_to_time_format(time_column, from_format='cy/m/d H:M:S', output_type='str')
    '''
    if from_format:
        column = _standardize_time_string(column, from_format)

    if is_utc:
        column = pd.to_datetime(column)
    else:
        try:
            column = pd.to_datetime(column, utc=is_utc).dt.tz_localize(from_timezone)
        except TypeError:
            column = column.astype(str).str.replace('\+08:00', '')
            column = pd.to_datetime(column, utc=is_utc).dt.tz_localize(from_timezone)

    if output_level == 'date':
        column = column.dt.date

    if output_type == 'str':
        column = column.astype(str)

    return column


def get_datataipei_api(rid):
    '''
    Get Data.taipei API，自動遍歷所有資料。
    (data.taipei的API單次return最多1000筆，因此需利用offset定位，取得所有資料)
    '''
    url = f"""https://data.taipei/api/v1/dataset/{rid}?scope=resourceAquire"""
    response = requests.request("GET", url)
    data_dict = response.json()
    count = data_dict['result']['count']
    res = list()
    offset_count = int(count/1000)
    for i in range(offset_count+1):
        i = i* 1000
        url = f"""https://data.taipei/api/v1/dataset/{rid}?scope=resourceAquire&offset={i}&limit=1000"""
        response = requests.request("GET", url)
        get_json = response.json()
        res.extend(get_json['result']['results'])
    return pd.DataFrame(res)

