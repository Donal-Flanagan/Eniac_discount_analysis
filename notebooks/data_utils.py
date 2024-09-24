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

    
orders_clean = (orders
                .pipe(start_pipeline)
                .pipe(remove_missing_data, col='total_paid')
                )

print(f"{orders.shape[0]-orders_clean.shape[0]} missing values were removed from orders.")
print(f"This represents {(orders.shape[0]-orders_clean.shape[0])/orders.shape[0] * 100:.2f}% of the data.")

# Save the data
orders_clean.to_csv(path + 'orders_clean.csv', index=False)




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
                      )

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
    for i in range(len(names_of_products_without_descriptions)):
        df.loc[df.name == names_of_products_without_descriptions[i], 'desc'] = missing_product_descriptions[i]
    return df

def remove_missing_prices(df, col):
    return df[~df[col].isna()]
    
def drop_duplicate_rows_by_column(df, col):
    return df.drop_duplicates(subset=col)

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
    print(f"This represents {(products.shape[0]-products_clean.shape[0])/products.shape[0] * 100:.2f}% of the data. \n")
    
    return products_clean



    