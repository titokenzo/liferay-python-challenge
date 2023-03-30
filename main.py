import pandas as pd
from datetime import datetime
import os

import definitions as rates

# Read sample files
df_orders = pd.read_json('sample_data/Orders.json')
df_orders = pd.concat([df_orders.drop('Product', axis=1), df_orders['Product'].apply(pd.Series)], axis=1)
df_accounts = pd.read_csv('sample_data/Accounts.csv')
df_products = pd.read_csv('sample_data/Products.tsv', delimiter='\t')

# filter by sale date then build Dataframe joining all sources
df_orders['SoldAt'] = pd.to_datetime(df_orders['SoldAt'], unit='ms')
df = df_orders[df_orders['SoldAt'] >= datetime.strptime('20200101', '%Y%m%d')]
df = pd.merge(df, df_accounts, how='left', on='AccountID')
df = pd.merge(df, df_products, how='left', on='ProductID')
df.reset_index()

print(df.info())

if not os.path.exists('output'):
    os.makedirs('output')

rates.generate_rates_file()
print('\n> Getting Dollar values')
df['DollarValue'] = df.apply(rates.value_to_dollar, axis=1)

print('\n> Getting Real values')
df['RealValue'] = df.apply(rates.value_to_real, axis=1)

# transform datatypes
df['StartDate'] = pd.to_datetime(df['StartDate'], utc=True, format='%Y-%m-%d', errors='coerce')
df['EndDate'] = pd.to_datetime(df['EndDate'], utc=True, format='%Y-%m-%d', errors='coerce')
df['SoldAt'] = pd.to_datetime(df['SoldAt'], utc=True, format='%Y-%m-%d', errors='coerce')

print('\n> Writing files')
# INCLUDE accountID, account name, orderID, productID, product name, sale price, sale date, start date, and end date.
df['SalesPrice'] = df['DollarValue']
df_dollar = df[[
    'AccountID',
    'Name',
    'OrderID',
    'ProductID',
    'ProductName',
    'SalesPrice',
    'SoldAt',
    'StartDate',
    'EndDate',
    'DollarValue']]
df_dollar.to_csv('output/SalesInDollar.csv')

df['SalesPrice'] = df['RealValue']
df_real = df[[
    'AccountID',
    'Name',
    'OrderID',
    'ProductID',
    'ProductName',
    'SalesPrice',
    'SoldAt',
    'StartDate',
    'EndDate',
    'RealValue']]
df_real.to_csv('output/SalesInReal.csv')
