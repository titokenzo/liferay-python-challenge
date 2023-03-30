from datetime import datetime
import json
import requests
import pandas as pd


source = 'https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/rates_of_exchange?'


def generate_rates_file() -> None:
    url = source
    url += 'sort=-record_date'
    url += '&fields=country_currency_desc,exchange_rate,record_date'
    url += '&filter=record_date:gte:2019-01-01'
    url += '&page[number]=1&page[size]=5000'
    api_response = requests.get(url)
    data = api_response.text
    rates = json.loads(data)
    df_rates = pd.DataFrame(rates['data'])

    df_rates['country_currency_desc'] = df_rates['country_currency_desc'].str.upper()
    df_rates['record_date'] = pd.to_datetime(df_rates['record_date'], format='%Y-%m-%d')
    df_rates.to_csv('output/ImportedRates.csv')
    print('\n> Exchange Rates file generated')


def get_dollar_online(df_row: pd.DataFrame) -> float | None:
    if not isinstance(df_row['CurrencyDescription'], str):
        return None
    country_currency = df_row['CurrencyDescription'].upper()
    if country_currency == 'United States-Dollar'.upper():
        return df_row['SalesPrice']

    url = source
    url += 'sort=-record_date'
    url += '&fields=country_currency_desc,exchange_rate,record_date'
    url += '&filter=country_currency_desc:eq:' + df_row['CurrencyDescription']
    url += ',record_date:lte:' + datetime.strftime(df_row['SoldAt'], format='%Y-%m-%d')
    url += '&page[number]=1&page[size]=1'
    api_response = requests.get(url)
    data = api_response.text
    rates = json.loads(data)
    df_rates = pd.DataFrame(data=rates['data'], )
    df_rates = df_rates.reset_index()
    if df_rates.shape[0] == 0:
        return None
    df_rates['exchange_rate'] = df_rates['exchange_rate'].astype(float)
    return df_rates.iloc[0]


def get_rates_data(df_row: pd.DataFrame, currency: str = None) -> pd.DataFrame | None:
    if currency is None:
        currency = df_row['CurrencyDescription'].upper()

    df_rates = pd.read_csv('output/ImportedRates.csv', parse_dates=['record_date'])
    df_rates = df_rates[(df_rates['country_currency_desc'] == currency) &
                        (df_rates['record_date'] <= df_row['SoldAt'])]
    df_rates = df_rates.sort_values('record_date', ascending=False)
    df_rates.reset_index()
    if df_rates.shape[0] == 0:
        return get_dollar_online(df_row)
    return df_rates.iloc[0]


def value_to_dollar(df_row: pd.DataFrame) -> float | None:
    if not isinstance(df_row['CurrencyDescription'], str):
        print('\tWarning: missing CurrencyDescription, OrderID=' + str(df_row['OrderID']))
        return None
    if df_row['CurrencyDescription'].upper() == 'United States-Dollar'.upper():
        return df_row['SalesPrice']

    df_rates = get_rates_data(df_row)
    if df_rates is None:
        print('\tWarning: currency not found, CurrencyDescription=' + df_row['CurrencyDescription'])
        return None
    return round(df_row['SalesPrice'] / df_rates['exchange_rate'], 2)


def value_to_real(df_row: pd.DataFrame) -> float | None:
    if not isinstance(df_row['CurrencyDescription'], str):
        return None
    if df_row['CurrencyDescription'].upper() == 'Brazil-Real'.upper():
        return df_row['SalesPrice']
    if df_row['DollarValue'] is None:
        return None

    df_rates = get_rates_data(df_row, 'Brazil-Real'.upper())
    if df_rates is None:
        print('\tWarning: currency not found, CurrencyDescription=' + df_row['CurrencyDescription'])
        return None
    return round(df_row['DollarValue'] * df_rates['exchange_rate'], 2)
