import requests
import pandas as pd
import os
################################################################################
def acquire_items():
    
    if os.path.exists('./items.csv'):
        # Read data from csv if it exists
        items_df = pd.read_csv('./items.csv')
        
        return items_df
    
    else:
        # If not querry API for data if does not exist in csv
        domain = 'https://api.data.codeup.com/'
        endpoint = 'api/v1/items'
        items_list = []

        url = domain + endpoint
        response = requests.get(url)
        data = response.json()

        items_list.extend(data['payload']['items'])

        for page in range(data['payload']['max_page']-1):
            url = domain + data['payload']['next_page']
            response = requests.get(url)
            data = response.json()

            items_list.extend(data['payload']['items'])

        items = pd.DataFrame(items_list)

        print("Shape of items dataframe ", items.shape)
    
        return items
 #######################################################################################################   
def acquire_stores():
    
    if os.path.exists('./stores.csv'):
        # Read data from csv if it exists
        stores_df = pd.read_csv('./stores.csv')

        return stores_df
    
    else:
        domain = 'https://api.data.codeup.com/'
        endpoint = 'api/v1/stores'
        stores_list = []

        url = domain + endpoint
        response = requests.get(url)
        data = response.json()

        stores_list.extend(data['payload']['stores'])

        for page in range(data['payload']['max_page']-1):
            print("Checking page: " , page)
            url = domain + data['payload']['next_page']
            response = requests.get(url)
            data = response.json()

            stores_list.extend(data['payload']['stores'])

        stores = pd.DataFrame(stores_list)

        print("Shape of stores df: ",stores.shape)

        return stores
###################################################################################################
def acquire_sales():
    
    if os.path.exists('./sales.csv'):
        
        sales_df = pd.read_csv('./sales.csv')
        
        return sales_df
    
    else:
        domain = 'https://api.data.codeup.com/'
        endpoint = 'api/v1/sales'
        sales_list = []

        url = domain + endpoint
        response = requests.get(url)
        data = response.json()

        # Get initial data 
        sales_list.extend(data['payload']['sales'])

        for page in range(data['payload']['max_page']-1):
            print("Checking page: " , page, "of ", data['payload']['max_page'])

            url = domain + data['payload']['next_page']
            response = requests.get(url)
            print("Downloading ", url)
            data = response.json()
            print(len(data['payload']['sales']), "Number of Records for this page")

            sales_list.extend(data['payload']['sales'])

            print("Records saved: ", len(sales_list))

        sales = pd.DataFrame(sales_list)

        print("Shape of sales df: ",sales.shape)
        
        return sales
 #######################################################################################################   
def acquire_all():
    """ This function returns combined dataframe of all sales, items, and stores data """
    
    
    sales = acquire_sales()
    items = acquire_items()
    stores = acquire_stores()

    combined = sales.merge(items, 
                           left_on = 'item', 
                           right_on = 'item_id').merge(stores,
                                                       left_on = 'store', 
                                                       right_on = 'store_id')
    return combined

def acquire_power_data():
    """ Acquire Open Power Systems Data for Germany """
    
    if os.path.exists('power_systems.csv'):
        data = pd.read_csv('power_systems.csv')
        return data
    
    else:
        data = pd.read_csv('https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv')
        data.to_csv('power_systems.csv', index=False)
        
        return data
