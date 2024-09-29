import pandas as pd
import numpy as np
import re

default_data_path = '../data/'

# Initial data cleaning functions

def start_pipeline(df):
    '''Make a copy of the pipeline to prevent corrupting the original data'''
    return df.copy()

def drop_deprecated_columns(df, col_list):
    return (df
            .drop(col_list, axis=1)
           )

def remove_missing_data(df, col):
    return df[~df[col].isna()]

def rename_columns(df, col_name_dict):
    return (df
            .rename(columns=col_name_dict)
           )

def create_short_col(df):
    """
    Add a new column named 'short' to orderlines which correspondes to the 'short' column in brands.
    This column contains the first three characters of the str values in the 'sku' column. 
    
    Args:
        df (pd.DataFrame): The orderlines data
    
    Returns:
        pd.DataFrame: orderlines with a new column named 'short'
    """
    return df.assign(short = lambda row: row['sku'].str[:3])

missing_product_descriptions = [
    '2TB Mac hard drive and Nas',
    'Apple keyboard for iPad 9.7',
    'NAS server with 10GB RAM',
    'Ethernet adapter for Macbook 12',
    'Luxury power bank combined with powder, 2 mirrors - normal and 3x magnification, Illuminated under mirror with LED, Low weight and compact dimensions',
    'Battery capacity: 20,000 mAh; ultra-stable: outer shell made of durable synthetic rubber (military standard, withstands drops from up to 2 metres) ; protection: dust and splash proof: military standard iP54; battery level indicator and super fast charging; USB port can be connected to charger and other devices',
    'Smart thermostat designed to provide automatic time and temperature control of heating systems in homes and apartments. '
]

def add_missing_product_descriptions(df):
    # Check for products without descriptions
    names_of_products_without_descriptions = df[df.desc.isna()].name.tolist()
    
    for i in range(len(names_of_products_without_descriptions)):
        df.loc[df.name == names_of_products_without_descriptions[i], 'desc'] = missing_product_descriptions[i]
    return df
    
def drop_duplicate_rows_by_column(df, col):
    return df.drop_duplicates(subset=col)

def import_brands(data_path=None):
    path = data_path if data_path else default_data_path
    return pd.read_csv(path + 'brands.csv',
                      dtype={'short': str,
                             'long': str})

def import_orders(data_path=None):
    path = data_path if data_path else default_data_path
    return pd.read_csv(path + 'orders.csv',
                       dtype={'order_id': int, 
                              'total_paid': float, 
                              'state': str}, 
                       parse_dates=['created_date'])

def import_orderlines(data_path=None):
    path = data_path if data_path else default_data_path
    return pd.read_csv(path + 'orderlines.csv', 
                       dtype={'id': int, 
                              'id_order': int, 
                              'product_id': int,
                              'product_qunatity': int,
                              'sku': str, 
                              'unit_price': str}, 
                       parse_dates=['date'])

def import_products(data_path=None):
    path = data_path if data_path else default_data_path
    return pd.read_csv(path + 'products.csv',
                       dtype={'sku': str, 
                              'name': str, 
                              'desc': str,
                              'price': str,
                              'promo_price': str, 
                              'in_stock': int,
                              'type': str})

def clean_brands(data_path=None):
    print("0 missing values were removed from brands.")
    print("This represents 0.00% of the data.")
    print("\n")
    return import_brands(data_path)

def clean_orders(data_path=None):
    orders = import_orders(data_path)
    
    orders_clean = (orders
                    .pipe(start_pipeline)
                    .pipe(remove_missing_data, col='total_paid')
                    )
    
    print(f"{orders.shape[0]-orders_clean.shape[0]} missing values were removed from orders.")
    print(f"This represents {(orders.shape[0]-orders_clean.shape[0])/orders.shape[0] * 100:.4f}% of the data.")
    print("\n")

    return orders_clean

def clean_orderlines(data_path=None):
    orderlines = import_orderlines(data_path)
    
    orderlines_clean = (orderlines
                        .pipe(start_pipeline)
                        .pipe(drop_deprecated_columns, col_list=['product_id'])
                        .pipe(rename_columns, {'id_order': 'order_id'})
                        .pipe(create_short_col)
                        )
    print(f"{orderlines.shape[0]-orderlines_clean.shape[0]} missing values were removed from orderlines.")
    print(f"This represents {(orderlines.shape[0]-orderlines_clean.shape[0])/orderlines.shape[0] * 100:.2f}% of the data.")
    print("\n")

    return orderlines_clean

def clean_products(data_path=None):
    products = import_products(data_path)

    # Check for products without descriptions
    names_of_products_without_descriptions = products[products.desc.isna()].name.tolist()

    products_clean = (products
                      .pipe(start_pipeline)
                      .pipe(drop_deprecated_columns, col_list=['type', 'in_stock', 'promo_price']) 
                      .pipe(add_missing_product_descriptions)
                      .pipe(remove_missing_data, col='price')
                      .pipe(drop_duplicate_rows_by_column, 'sku')
                     )
    
    print(f"{products.shape[0]-products_clean.shape[0]} missing values were removed from products")
    print(f"This represents {(products.shape[0]-products_clean.shape[0])/products.shape[0] * 100:.2f}% of the data.")
    print("\n")
    
    return products_clean


# Data merging functions

col_order = [
    'order_id',
    'orderline_id',
    'date',
    'name',
    'desc',
    'brand',
    'sku',
    'category',
    'total_paid',
    'product_quantity',
    'regular_price',
    'sale_price'
]

def reorder_columns(df, col_list):
    return df[col_list]

def rename_columns(df, col_dict):
    return (df
            .rename(columns=col_dict)
           )
    
def assign_product_categories(df):
    apple_regexp_dict = {
        'iPod': '^.{0,7}apple ipod',
        'iPhone':  'apple iphone',
        'iPad':  'apple ipad',
        'Mac':  'apple macbook|apple iMac|apple Mac mini|desktop computer',
    }
    
    other_regexp_dict = {        
        'Smartwatch':'withings|watch|fitbit|apple watch|smartwatch|smart watch',
        'Accessories': 'kit|strap|armband|belt|bracelet|stylus|pen|Bamboo Wacom Intuos|pencil|pen|rubber pointers|screwdriver|case|funda|housing|casing|folder|bag|backpack|cable|connector|Lightning to USB|Wall socket|power strip|adapter|battery|headset|headphones|mouse|trackpad|stand|support|protect|cover|sleeve|Screensaver|shellhub|dock|microphone|keyboard|keypad',
        'Hardware': 'Philips Hue|temperature sensor|display|monitor|camera|charger|speaker|router|repeater|Synology|nas|server|Parrot FPV Glasses|Command Pack 2 Skycontroller|Apple TV',
        'Software':  'adobe|Office 365|Office Home and Student|software|parallels',
        'Memory': 'hard disk|hard drive|flash drive|USB 2.0 key|USB 2.0 pen|SSD|pendrive|raid|SDHC|sata|memory card|Portable Hard Thunderbolt',
        'Repairs & warranties': 'repair|parts and labor|warranty|applecare|license|protection|installation',
    }
    
    df = df.assign(category = 'unknown')
    
    # Find main apple items
    for label, val in apple_regexp_dict.items(): 
        regexp = re.compile(val, flags=re.IGNORECASE)
        df = (
            df
            .assign(
                category = lambda x: np.where(
                    ((x['desc'].str.contains(regexp, regex=True))|(x['name'].str.contains(regexp, regex=True))) &
                    (x['category'] == 'unknown') & (x['brand'] == 'Apple'), 
                    label, x['category'])
            )
        )
    
    # Find other items
    for label, val in other_regexp_dict.items(): 
        regexp = re.compile(val, flags=re.IGNORECASE)
        df = (
            df
            .assign(
                category = lambda x: np.where(
                    ((x['desc'].str.contains(regexp, regex=True))|(x['name'].str.contains(regexp, regex=True))) &
                    (x['category'] == 'unknown'), label, x['category'])
            )
        )
    
    return df

def merge_dataframes(df, merge_df, col):
    return df.merge(merge_df, on=col)

def drop_uncompleted_orders(df):
    return df[df.state=='Completed']

def merge_data(orders_clean, orderlines_clean, products_clean, brands_clean):

    completed_sales = (orders_clean
                       .pipe(start_pipeline)
                       .pipe(drop_uncompleted_orders)
                       .pipe(merge_dataframes, orderlines_clean, 'order_id')
                       .pipe(merge_dataframes, products_clean, 'sku')
                       .pipe(merge_dataframes, brands_clean, 'short')
                       .pipe(rename_columns, col_dict={'long': 'brand', 'unit_price': 'sale_price', 'price': 'regular_price', 'id': 'orderline_id'})
                       .pipe(drop_deprecated_columns, col_list=['short', 'created_date', 'state'])
                       .pipe(assign_product_categories)
                       .pipe(reorder_columns, col_order)
                 )
    
    return completed_sales

    