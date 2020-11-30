import os
import time
import json
import datetime
import warnings
import pandas as pd
from pprint import pprint
import urllib.error
import urllib.request
from pandas.tseries.offsets import DateOffset


def full_print_df(df):
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(df)


# TODO: class item - object for a given item with different attributes
# TODO: function that takes an id, checks table, and returns name
# TODO: function that takes a name, checks table, and returns id (fuzzy search for no result exception)
def get_single_item():
    api_endpoint = "https://secure.runescape.com/m=itemdb_oldschool/api/catalogue/detail.json?item="
    item_id = "2"

    request_str = api_endpoint + item_id
    uri = request_str

    try:
        response = urllib.request.urlopen(uri)
    except urllib.error.HTTPError:
        raise Exception("Unable to find item with id %d." % id)

    osb_data = response.read()
    encoding = response.info().get_content_charset("utf-8")
    response.close()

    osb_json_data = json.loads(osb_data.decode(encoding))
    # osb_price = osb_json_data["overall"]

    pprint(osb_json_data)

    current_item = osb_json_data['item']
    current_price = current_item['current']
    # print(current_price)


def test_high_vol():
    uri = "https://secure.runescape.com/m=itemdb_oldschool/top100"
    response = urllib.request.urlopen(uri)
    osb_data = response.read()
    encoding = response.info().get_content_charset("utf-8")
    response.close()
    osb_json_data = json.loads(osb_data.decode(encoding))
    pprint(osb_json_data)


def get_wiki_item():
    wiki_endpoint = "https://oldschool.runescape.wiki/w/Module:Exchange/"
    item_name = "Bones"
    uri = wiki_endpoint + item_name + "/Data?action=raw"

    try:
        response = urllib.request.urlopen(uri)
    except urllib.error.HTTPError:
        raise Exception("Unable to find data at URI: " + uri)

    pprint(response)


def get_wiki_response():
    user_agent = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7'
    actual = "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36"

    url = "https://oldschool.runescape.wiki/w/Module:Exchange/Bones/Data?action=raw"
    headers = {'User-Agent': user_agent, }

    request = urllib.request.Request(url, None, headers)  # The assembled request
    response = urllib.request.urlopen(request)
    data = response.read()  # The data needed

    pprint(data)


def get_wiki_exch_data():
    # TODO: dynamically retrieve this, for future-proofing
    user_agent = "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) " \
                 "Chrome/45.0.2454.85 Safari/537.36"
    headers = {'User-Agent': user_agent, }

    wiki_endpoint = "https://oldschool.runescape.wiki/w/Module:Exchange/"
    item_name = "Bones"
    uri = wiki_endpoint + item_name + "/Data?action=raw"

    request = urllib.request.Request(uri, None, headers)
    response = urllib.request.urlopen(request)
    data = response.read()

    # Decode response from a bytes object to an unformatted list object
    x = data.decode("utf-8")
    data_list = x.split(sep=',')

    # Remove plaintext garbage from first row of data ('return {\n    ')
    first_row = data_list[0].split(sep='\'')
    data_list[0] = "\'" + first_row[1] + "\'"

    # Remove plaintext garbage from last row of data ('\n}')
    last_row = data_list[-1].split(sep='\n')
    data_list[-1] = last_row[0]

    # Construct dictionary through extracted list elements
    data_dict = {}
    # Iterate through the list
    for i in range(0, len(data_list)):
        # trunc_str = data_list[i].split(sep='\n    ')
        # data_grouped = trunc_str[1]
        trunc_str = data_list[i][1:-1]
        data = trunc_str[5:-1].split(sep=':')
        print(data)
        # if not data[2]:
        #     data[2] = 0
        # data_dict[data[0]] = data[1], data[2]

    pprint(data_dict)

    pprint(data_list[:1])


def call_item_api(item_name):
    user_agent = "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) " \
                 "Chrome/45.0.2454.85 Safari/537.36"
    headers = {'User-Agent': user_agent, }

    wiki_endpoint = "https://oldschool.runescape.wiki/w/Module:Exchange/"
    # item_name = "Bones"
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


def load_unsorted_item_dict(item_file):
    with open(item_file, 'r') as file:
        result_dict = json.load(file)

    df = pd.DataFrame.from_dict(data=result_dict, orient='index', columns=['cosmetic', 'members', 'model_id', 'name',
                                                                           'tradeable', 'value'])

    # df = pd.DataFrame.from_dict(data=result_dict, orient='columns')

    # print(df.head())
    return df


def load_tradeable_item_dict(item_file):
    with open(item_file, 'r') as file:
        result_dict = json.load(file)

    df = pd.DataFrame.from_dict(data=result_dict, orient='index', columns=['id', 'name', 'members'])
    df.set_index(keys='id', inplace=True)

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

        # Printing middle n rows requires more manual work then the df head/tail
        # Currently, we're going 3/4 down the price history, because we know this typically contains volume info
        # See for performance differences:
        # https://stackoverflow.com/questions/15943769/how-do-i-get-the-row-count-of-a-pandas-dataframe
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
            # full_print_df(df=df)
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

        #  'href="https://secure.runescape.com/m=itemdb_oldschool/Fire+rune/viewitem?obj=554" '
        raw_list = x.split(sep='\n')
        item_list = []
        for item in raw_list:
            # if 'href=\"https://secure.runescape.com/m=itemdb_oldschool/' in item and item.endswith('\" \''):
            #     item_list.append(item)
            #     print(item)
            # TODO: parse the below structure
            # <a href="https://secure.runescape.com/m=itemdb_oldschool/Fire+rune/viewitem?obj=554" class='table-item-link'>
            if 'class=\'table-item-link\'' in item:
                sub_str_list = item.split(sep='viewitem?obj=')
                sub_str = sub_str_list[1]
                id_list = sub_str.split(sep='\" class')
                item_id = id_list[0]
                item_list.append(str(item_id))

        df = pd.DataFrame(item_list)
        # TODO: possibly append current date to file for version control
        df.to_csv(path_or_buf='data/top_100_ids.csv', index=False)

        return item_list


def id_to_name(item_id, ref_df):
    row = ref_df.loc[ref_df.index == int(item_id)]
    item_name = row['name'].iloc[0]
    return item_name


def get_top_100_names(id_list, ref_df):
    name_list = []
    for item in id_list:
        item_name = id_to_name(item_id=item, ref_df=ref_df)
        name_list.append(item_name)
    return name_list


def wiki_df_to_csv(item_name, df):
    path = "data/historical_wiki_data/" + item_name.replace(" ", "_") + ".csv"

    # If the data is already stored, check which one is more up-to-date slice of time
    if os.path.isfile(path):
        stored_df = pd.read_csv(filepath_or_buffer=path)
        stored_df.rename(columns={'Unnamed: 0': 'index'}, inplace=True)
        stored_df.set_index('index', inplace=True)
        stored_last = stored_df.index[-1]
        # the current df gets indexed converted to dt obj in wiki_dict_to_df, no need to convert here
        # curr_last = df.index[-1]
        # curr_time = datetime.datetime.strptime(curr_last, '%Y-%m-%d')

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


# TODO: iterate through data dir, split filename for item name, add 2 features to monolithic df
def concat_hist_datasets():
    data_path = 'data/historical_wiki_data'
    db_path = 'data/all_historical_data.csv'

    if os.path.isfile(db_path):

        # more performant than pathlib or glob implementations
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
                    # merged = pd.concat(objs=[master_df, stored_df], axis='index', join='outer')
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


# TODO: graph a specified item using historical wiki prices
def graph_single_history(item_name):
    return


# TODO: get current item price from official api or ge-tracker
def get_curr_price(item_name):
    return


# TODO: interpolate missing volume values
def interpolate_hist_volume(item_name):
    return


def slice_hist_dataset(start=None, end='today', save_local=False):
    # If no start time specified, will default to the known beginning of the dataset
    if not start:
        # start = pd.Timestamp('now').floor('D') + DateOffset(months=-12)
        start = datetime.datetime.strptime('2015-03-28', '%Y-%m-%d')
    else:
        start = datetime.datetime.strptime(start, '%Y-%m-%d')

    # If end time specified is today, will set the current day
    if end == "today":
        # end = pd.Timestamp('today').floor('D')
        end = datetime.datetime.now().date()
    else:
        end = datetime.datetime.strptime(end, '%Y-%m-%d')

    db_path = 'data/all_historical_data.csv'
    master_df = pd.read_csv(filepath_or_buffer=db_path)
    master_df['index'] = pd.to_datetime(arg=master_df['index'], format='%Y-%m-%d')
    # master_df.set_index('index', inplace=True)

    # master_df.drop([master_df.index < start], inplace=True)
    # print(master_df.head())

    df = master_df.loc[(master_df['index'] >= start) & (master_df['index'] <= end)]
    df.set_index('index', inplace=True)

    if save_local:
        slice_path = 'data/all_hist_data_' + str(start.date()) + '_-_' + str(end.date()) + '.csv'
        df.to_csv(path_or_buf=slice_path)
        print("Saved truncated historical dataset locally as: " + slice_path)

    return df


def main():
    # Initial test of individual functions and API endpoints
    get_single_item()
    test_high_vol()
    get_wiki_item()
    get_wiki_response()
    get_wiki_exch_data()

    # Transforming single item call to wiki into dict->df
    # data_dict = call_item_api(item_name='Bones')
    # item_ex = wiki_dict_to_df(data_dict=data_dict)
    items = load_unsorted_item_dict(item_file='metadata/unsorted_item_dict.json')
    sel_item_sample(df=items, x=5)

    # Using dict of all tradeable items, randomly select 5 and try wiki API call
    items = load_tradeable_item_dict(item_file='metadata/tradeable_item_dict.json')
    sel_item_sample(df=items, x=5)

    # Testing full DB pull functionality, for single entry or for top 100 high-volume items of the week
    items = load_tradeable_item_dict(item_file='metadata/tradeable_item_dict.json')
    top_ids = get_top_100_ids(local=True)
    top_names = get_top_100_names(id_list=top_ids, ref_df=items)
    # Single item (top traded of the week)
    x = top_names[0]
    wiki_data = call_item_api(item_name=x)
    data_df = wiki_dict_to_df(data_dict=wiki_data)
    wiki_df_to_csv(item_name=x, df=data_df)
    # Top 100 items of the week (cached)
    for item in top_names:
        wiki_data = call_item_api(item_name=item)
        data_df = wiki_dict_to_df(data_dict=wiki_data)
        wiki_df_to_csv(item_name=item, df=data_df)
        # There aren't strict API limits for Wiki and it's only 100 entries, just being polite
        time.sleep(5)

    # Convert singular .csv files in data/historical_wki_data to single table of concatenated time-series
    concat_hist_datasets()

    # Slice historical dataset table to just 2020, and interpolate missing volume
    # Dates are both inclusive, possible to set end value as 'today', possible to save local csv copy
    start, end = "2020-01-01", "2020-11-15"
    trunc_df = slice_hist_dataset(start=start, end=end, save_local=True)
    print(trunc_df.tail(15))


if __name__ == '__main__':
    main()
