import pandas as pd
import geopandas as gpd
from numpy import nan
from shapely.geometry import Polygon, multilinestring, MultiLineString, LineString
import re
import json
import requests
import datetime
import pytz
taipei_tz = pytz.timezone('Asia/Taipei')


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


def convert_str_to_time_format(column: pd.Series, from_format=None,
                               output_level='datetime', output_type='time',
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


def convert_to_float(column):
    '''
    無論原本欄位的格式，轉成float格式

    Example
    ----------
    data = pd.DataFrame({'name': ['a', 'b', 'c', 'd'],
                         'type': ['A', 'B', 'C', 'D']})
    x = pd.Series([121.123, 123.321, '', None])
    y = pd.Series([25.123, 26.321, None, ''])
    xx = convert_to_float(x)
    gdf = add_point_wkbgeometry_column_to_df(data, x, y, from_crs=4326)

    x = pd.Series([262403.2367, 481753.6091, '', None])
    y = pd.Series([2779407.0527, 2914189.1837, None, ''])
    convert_to_float(x)
    convert_to_float(y)
    '''
    try:
        column = column.astype(float)
    except ValueError:
        is_empty = (column=='')
        is_na = pd.isna(column)
        column.loc[is_empty|is_na] = nan
        column = column.astype(float)
    return column


def get_tpe_now_time_str():
    '''
    Get now time with tz = 'Asia/Taipei'.
    Output is a string truncate to seconds.
    output Example: '2022-09-21 17:56:18'

    Example
    ----------
    get_tpe_now_time_str()
    '''
    now_time = str(datetime.datetime.now(tz=taipei_tz)).split('.')[0]
    return now_time


def get_datataipei_data_updatetime(url):
    '''
    Request lastest update time of given data.taipei url.
    Output is a string truncate to seconds.
    output Example: '2022-09-21 17:56:18'

    Example
    ----------
    url = 'https://data.taipei/api/frontstage/tpeod/dataset/change-history.list?id=4fefd1b3-58b9-4dab-af00-724c715b0c58'
    get_datataipei_data_updatetime(url)
    '''
    # 抓data.taipei的更新時間
    res = requests.get(url)
    update_history = json.loads(res.text)
    lastest_update = update_history['payload'][0]
    lastest_update_time = lastest_update.split('更新於')[-1]
    return lastest_update_time.strip()


def get_datataipei_data_file_last_modeified_time(url, rank=0):
    '''
    Request lastest modeified time of given data.taipei url.
    Output is a string truncate to seconds.
    The json can contain more than one data last modifytime, "rank" para chose which one.
    output Example: '2022-09-21 17:56:18'

    Example
    ----------
    '''
    # 抓data.taipei的更新時間
    res = requests.get(url)
    data_info = json.loads(res.text)
    lastest_modeified_time = data_info['payload']['resources'][rank]['last_modified']
    return lastest_modeified_time


def linestring_to_multilinestring(geo):
    '''
    將LineString轉換為MultiLineString

    Example
    ----------
    line_a = LineString([[0,0], [1,1]])
    line_b = LineString([[1,1], [1,0]])
    multi_line = MultiLineString([line_a, line_b])
    linestring_to_multilinestring(None)
    type(linestring_to_multilinestring(multi_line))
    type(linestring_to_multilinestring(line_a))
    '''
    is_multistring = (type(geo)==multilinestring.MultiLineString)
    is_na = pd.isna(geo)
    if (is_multistring) or (is_na):
        return geo
    else:
        return MultiLineString([geo])

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


def given_string_to_none(input_str, given_str, mode='start'):
    '''
    輸入任意string，若符合指定文字，則轉成None，不符合則保持原樣
    此funciton能igonre data type的問題

    Example
    ----------
    given_string_to_none('-990.00', '-99')
    given_string_to_none('-90.00', '-99')
    given_string_to_none('-990.00', '-99', mode='end')
    given_string_to_none('-990.00', '-99', mode='test')
    '''
    if mode == 'start':
        try:
            is_target = input_str.startswith(given_str)
        except:
            is_target = False
    elif mode == 'end':
        try:
            is_target = input_str.endswith(given_str)
        except:
            is_target = False
    else:
        is_target = False

    if is_target:
        return None
    else:
        return input_str


def get_tpe_now_time_timestamp(minutes_delta=None):
    '''
    Get now time with tz = 'Asia/Taipei'.
    '''
    from datetime import datetime, timedelta
    if minutes_delta:
        now_timestamp = (datetime.now(tz=taipei_tz)+timedelta(minutes=minutes_delta)).timestamp() * 1e3
    else:
        now_timestamp = datetime.now(tz=taipei_tz).timestamp() * 1e3
    return now_timestamp