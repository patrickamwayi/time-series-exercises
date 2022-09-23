import requests
import pandas as pd
from datetime import datetime
import os


def store_items():
    #pull data locally
    combined = acquire.acquire_all()
    #combined.sale_date.apply(lambda date: date[ : -13]).head(2)
    combined.sale_date = combined.sale_date.apply(lambda date: date[ : -13])
    # we can get ride of GMT time as it is all same
    combined.sale_date = pd.to_datetime(combined.sale_date, format='%a, %d %b %Y')
    combined = combined.set_index('sale_date').sort_index()
    #create month in dataframe
    combined['month'] = combined.index.strftime('%m-%b')
    #create week in dataframe
    combined['weekday'] = combined.index.strftime('%w-%a')
    #create new column sales total 
    combined['sales_total'] = combined.sale_amount * combined.item_price
    return combined



def data():
    #read csv
    data = pd.read_csv('https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv')
    #acquire data
    data = acquire.acquire_power_data()
    data.columns = [col.replace('+', '_').lower() for col in data.columns]
    data.date = pd.to_datetime(data.date)
    data = data.set_index('date').sort_index()
    data['month'] = data.index.strftime('%m-%b')
    data['year'] = data.index.year
    data.wind = data.wind.fillna(0)
    data.solar= data.solar.fillna(0)
    data["wind_solar"] = data["wind_solar"].fillna(0)
    
    return data
