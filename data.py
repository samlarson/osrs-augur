import os
import time
import json
import datetime
import pandas as pd
import urllib.error
import urllib.request
from pprint import pprint


def load_tradeable_item_dict(item_file):
    with open(item_file, 'r') as file:
        result_dict = json.load(file)

    df = pd.DataFrame.from_dict(data=result_dict, orient='index', columns=['id', 'name', 'members'])
    df.set_index(keys='id', inplace=True)

    return df


def call_item_api(item_name):
    user_agent = "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) " \
                 "Chrome/45.0.2454.85 Safari/537.36"
    headers = {'User-Agent': user_agent, }

    wiki_endpoint = "https://oldschool.runescape.wiki/w/Module:Exchange/"
    uri = wiki_endpoint + item_name.replace(" ", "_") + "/Data?action=raw"

    request = urllib.request.Request(uri, None, headers)
    response = urllib.request.urlopen(request)
    data = response.read()

    x = data.decode("utf-8")
    data_list = x.split(sep=',')

    first_row = data_list[0].split(sep='\'')
    data_list[0] = "\'" + first_row[1] + "\'"

    last_row = data_list[-1].split(sep='\n')
    data_list[-1] = last_row[0]
    data_dict = {}
    del data_list[-1]

    for i in range(1, len(data_list)):
        trunc_str = data_list[i][6:-1]
        data_row = trunc_str.split(sep=':')

        if len(data_row) < 3:
            data_row.append('NaN')

        data_dict[data_row[0]] = data_row[1], data_row[2]

    return data_dict


def wiki_dict_to_df(data_dict):
    df = pd.DataFrame.from_dict(data=data_dict, orient='index', columns=['price', 'volume'])
    df.index = pd.to_datetime(df.index, unit='s')

    return df


# recursive random sub-select
def sel_item_sample(df, x):
    df = df.sample(n=x)
    sample_list = df['name'].to_list()

    for item in sample_list:
        print(item)
        wiki_api_handler(item_name=item)
        print("\n")


def wiki_api_handler(item_name):
    try:
        ex_dict = call_item_api(item_name=item_name)
        ex_df = wiki_dict_to_df(data_dict=ex_dict)

        df_size = len(ex_df.index)
        n = 5
        x, y = int((df_size / 1.5) - 0.5 * n), int((df_size / 1.5) + 0.5 * n)
        print(ex_df.iloc[x:y])

    # The only error I've seen so far is an HTTP 404 NotFound, but there may be others to catch
    except Exception as e:
        print("WARNING: API call unsuccessful for item: " + item_name)
        print(e)


def get_top_100_ids(local=True):
    if local:
        try:
            df = pd.read_csv(filepath_or_buffer='data/top_100_ids.csv')
            item_list = df['0'].tolist()

            return item_list

        except FileNotFoundError as e:
            print("WARNING: Pandas could not retrieve a local copy of Top 100 Items Traded by Volume. Generating new "
                  "list now...")

            return get_top_100_ids(local=False)

    else:
        user_agent = "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) " \
                     "Chrome/45.0.2454.85 Safari/537.36"
        headers = {'User-Agent': user_agent, }

        uri = "https://secure.runescape.com/m=itemdb_oldschool/top100"
        request = urllib.request.Request(uri, None, headers)
        response = urllib.request.urlopen(request)
        data = response.read()
        x = data.decode("utf-8")

        raw_list = x.split(sep='\n')
        item_list = []

        for item in raw_list:
            if 'class=\'table-item-link\'' in item:
                sub_str_list = item.split(sep='viewitem?obj=')
                sub_str = sub_str_list[1]
                id_list = sub_str.split(sep='\" class')
                item_id = id_list[0]
                item_list.append(str(item_id))

        df = pd.DataFrame(item_list)
        df.to_csv(path_or_buf='data/top_100_ids.csv', index=False)

        return item_list


def get_top_100_names(id_list, ref_df):
    name_list = []
    for item in id_list:
        item_name = id_to_name(item_id=item, ref_df=ref_df)
        name_list.append(item_name)

    return name_list


def id_to_name(item_id, ref_df):
    row = ref_df.loc[ref_df.index == int(item_id)]
    item_name = row['name'].iloc[0]

    return item_name


def wiki_df_to_csv(item_name, df):
    path = "data/historical_wiki_data/" + item_name.replace(" ", "_") + ".csv"

    # If the data is already stored, check which one is more up-to-date slice of time
    if os.path.isfile(path):
        stored_df = pd.read_csv(filepath_or_buffer=path)
        stored_df.rename(columns={'Unnamed: 0': 'index'}, inplace=True)
        stored_df.set_index('index', inplace=True)
        stored_last = stored_df.index[-1]

        stored_time = datetime.datetime.strptime(stored_last, '%Y-%m-%d')
        curr_time = df.index[-1]

        if stored_time > curr_time:
            print("WARNING: Current dataframe attempting to store is less recent than stored data for " + item_name +
                  " - retaining stored data\n")

        elif curr_time > stored_time:
            df.to_csv(path_or_buf=path)
            print("WARNING: Overwriting existing data stored for " + item_name + " with more recent dataframe\n")

        else:
            print("WARNING: Current dataframe attempting to store is no more recent than stored data for " + item_name +
                  " - retaining stored data\n")

    else:
        df.to_csv(path_or_buf=path)
        print("Successfully saved historical " + item_name + " data as .csv file\n")


def concat_hist_datasets():
    data_path = 'data/historical_wiki_data'
    db_path = 'data/all_historical_data.csv'

    if os.path.isfile(db_path):

        with os.scandir(data_path) as scan:
            for entry in scan:
                if entry.name.endswith(".csv") and entry.is_file():
                    item_name = entry.name.split(sep='.csv')[0]

                    master_df = pd.read_csv(filepath_or_buffer=db_path)
                    master_df.set_index('index', inplace=True)

                    stored_df = pd.read_csv(filepath_or_buffer=entry.path)
                    price_col, vol_col = item_name + "_P", item_name + "_V"
                    stored_df.rename(columns={'Unnamed: 0': 'index', 'price': price_col, 'volume': vol_col},
                                     inplace=True)
                    stored_df.set_index('index', inplace=True)

                    merged = master_df.join(other=stored_df, how='left')
                    merged.to_csv(path_or_buf=db_path)
                    print(merged.head(10))
                    print("Merged " + item_name + " data with master historical dataset")

    else:
        print("WARNING: No master historical dataset detected. Creating new dataset entry...")

        today = pd.Timestamp('today').floor('D')
        dates = pd.date_range(start='3/28/2015', end=today)

        master = pd.DataFrame(dates, columns=['index'])
        master.set_index('index', inplace=True)

        master.to_csv(path_or_buf=db_path)
        concat_hist_datasets()


def slice_hist_dataset(start=None, end='today', save_local=False):
    # If no start time specified, will default to the known beginning of the dataset
    if not start:
        start = datetime.datetime.strptime('2015-03-28', '%Y-%m-%d')
    else:
        start = datetime.datetime.strptime(start, '%Y-%m-%d')

    # If end time specified is today, will set the current day
    if end == "today":
        end = datetime.datetime.now().date()
    else:
        end = datetime.datetime.strptime(end, '%Y-%m-%d')

    db_path = 'data/all_historical_data.csv'
    master_df = pd.read_csv(filepath_or_buffer=db_path)
    master_df['index'] = pd.to_datetime(arg=master_df['index'], format='%Y-%m-%d')

    df = master_df.loc[(master_df['index'] >= start) & (master_df['index'] <= end)]
    df.set_index('index', inplace=True)

    if save_local:
        slice_path = 'data/all_hist_data_' + str(start.date()) + '_-_' + str(end.date()) + '.csv'
        df.to_csv(path_or_buf=slice_path)
        print("Saved truncated historical dataset locally as: " + slice_path)

    return df
