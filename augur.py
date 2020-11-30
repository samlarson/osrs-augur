import data
import time


# Using dict of all tradeable items, randomly select 5 and try wiki API call
def test_tradeable_sample():
    items = data.load_tradeable_item_dict(item_file='metadata/tradeable_item_dict.json')
    data.sel_item_sample(df=items, x=5)


# Testing full DB pull functionality, for single entry
def get_single_data_csv():
    items = data.load_tradeable_item_dict(item_file='metadata/tradeable_item_dict.json')
    top_ids = data.get_top_100_ids(local=True)
    top_names = data.get_top_100_names(id_list=top_ids, ref_df=items)

    # Single item (top traded of the week)
    x = top_names[0]
    wiki_data = data.call_item_api(item_name=x)
    data_df = data.wiki_dict_to_df(data_dict=wiki_data)
    data.wiki_df_to_csv(item_name=x, df=data_df)


# Testing full DB pull functionality, for top 100 high-volume items of the week
def get_t100_data_csv():
    items = data.load_tradeable_item_dict(item_file='metadata/tradeable_item_dict.json')
    top_ids = data.get_top_100_ids(local=True)
    top_names = data.get_top_100_names(id_list=top_ids, ref_df=items)

    # Top 100 items of the week (cached)
    for item in top_names:
        wiki_data = data.call_item_api(item_name=item)
        data_df = data.wiki_dict_to_df(data_dict=wiki_data)
        data.wiki_df_to_csv(item_name=item, df=data_df)
        # There aren't explicit API limits for Wiki, preventative measure to not get IP blocked
        time.sleep(5)


# Convert singular .csv files in data/historical_wki_data to single table of concatenated time-series
def merge_historical_db():
    data.concat_hist_datasets()


# Slice historical dataset table to just 2020, and interpolate missing volume
# Dates are both inclusive, possible to set end value as 'today', possible to save local csv copy
def slice_historical_db():
    start, end = "2020-01-01", "2020-11-15"
    trunc_df = data.slice_hist_dataset(start=start, end=end, save_local=True)
    print(trunc_df.tail(15))
