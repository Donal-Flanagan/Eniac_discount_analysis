import pandas as pd
import numpy as np
import re

path = '../data/'

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
    
def transform_unit_price_to_floats(df):
    """
    Transform the orderlines.unit_price price column to floats.
    Some of the values have two decimal points. 
    For these values we will remove the leftmost decimal and transform all values to floats.
    The correct position of the decimal point will be determined by merging orderlines, 
    products, orders and brands, and comparing the price values.
    
    Args:
        df (pd.DataFrame): The orderlines data
    
    Returns:
        pd.DataFrame: The orderlines data with the unit_price column transformed from str to float values.
    """
    return (
        df.assign(unit_price = df.unit_price.str.split('.')
                  .apply(lambda x : x[0]+x[1]+'.'+x[2] if len(x)==3 else x[0]+'.'+ x[1])
                  .astype(float)
        )
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

def import_brands():
    return pd.read_csv(path + 'brands.csv',
                      dtype={'short': str,
                             'long': str})

def import_orders():
    return pd.read_csv(path + 'orders.csv',
                       dtype={'order_id': int, 
                              'total_paid': float, 
                              'state': str}, 
                       parse_dates=['created_date'])

def import_orderlines():
    return pd.read_csv(path + 'orderlines.csv', 
                       dtype={'id': int, 
                              'id_order': int, 
                              'product_id': int,
                              'product_qunatity': int,
                              'sku': str, 
                              'unit_price': str}, 
                       parse_dates=['date'])

def import_products():
    return pd.read_csv(path + 'products.csv',
                       dtype={'sku': str, 
                              'name': str, 
                              'desc': str,
                              'price': str,
                              'promo_price': str, 
                              'in_stock': int,
                              'type': str})

def clean_brands():
    print("0 missing values were removed from brands.")
    print("This represents 0.00% of the data.")
    print("\n")
    return import_brands()

def clean_orders():
    orders = import_orders()
    
    orders_clean = (orders
                    .pipe(start_pipeline)
                    .pipe(remove_missing_data, col='total_paid')
                    )
    
    print(f"{orders.shape[0]-orders_clean.shape[0]} missing values were removed from orders.")
    print(f"This represents {(orders.shape[0]-orders_clean.shape[0])/orders.shape[0] * 100:.4f}% of the data.")
    print("\n")

    return orders_clean

def clean_orderlines():
    orderlines = import_orderlines()
    
    orderlines_clean = (orderlines
                        .pipe(start_pipeline)
                        .pipe(drop_deprecated_columns, col_list=['product_id'])
                        .pipe(rename_columns, {'id_order': 'order_id'})
                        .pipe(transform_unit_price_to_floats)
                        .pipe(create_short_col)
                        )
    print(f"{orderlines.shape[0]-orderlines_clean.shape[0]} missing values were removed from orderlines.")
    print(f"This represents {(orderlines.shape[0]-orderlines_clean.shape[0])/orderlines.shape[0] * 100:.2f}% of the data.")
    print("\n")

    return orderlines_clean

def clean_products():
    products = import_products()

    # Check for products without descriptions
    names_of_products_without_descriptions = products[products.desc.isna()].name.tolist()

    products_clean = (products
                      .pipe(start_pipeline)
                      .pipe(drop_deprecated_columns, col_list=['type', 'in_stock']) 
                      .pipe(add_missing_product_descriptions)
                      .pipe(remove_missing_data, col='price')
                      .pipe(drop_duplicate_rows_by_column, 'sku')
                     )
                      
    print(f"{products.shape[0]-products_clean.shape[0]} missing values were removed from products")
    print(f"This represents {(products.shape[0]-products_clean.shape[0])/products.shape[0] * 100:.2f}% of the data.")
    print("\n")
    
    return products_clean



    