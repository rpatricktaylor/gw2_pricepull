import json
import time

import bs4
import numpy    as np
import pandas   as pd
import requests as rqs

url_apiV2   = r'https://api.guildwars2.com/v2'
working_dir = r'/home/ubuntu/'

def gw2_request(request_type, item_nums, retries = 25):
    item_num_str = ','.join([str(num) for num in item_nums])
    request_url = '{}/{}?ids={}'.format(url_apiV2, request_type, item_num_str)
    
    retry   = 0
    success = False
    
    while True:
        try:
            r = rqs.get(request_url)
        except:
            retry += 1
        else:
            break
        finally:
            if retry == retries:
                break
    
    j = r.json()

    return j
    
def item_request(item_nums):
    return gw2_request('items', item_nums)

def price_request(item_nums):
    return gw2_request('commerce/prices', item_nums)

def listings_request(item_nums):
    return gw2_request('commerce/listings', item_nums)

def avg_prices(item_nums, min_quantity=1000, max_quantity=50000, max_price=500000):
    
    def listing_avg_price(listings):
        quantity_total = 0
        quantity       = 0
        cost           = 0
        
        for listing in listings:    
            q = listing['quantity']
            p = listing['unit_price']
            
            quantity_total += q

            if (quantity < max_quantity) and (cost < max_price):
                quantity += q
                cost     += p * q
                
        return cost, quantity, quantity_total
    
    listings = listings_request(item_nums)
    
    out = []
    
    request_time = time.strftime('%Y-%m-%d %H:00')
    
    if listings:
        for listing in listings:
            if 'buys' not in listing or 'sells' not in listing or 'id' not in listing:
                continue
            bid_listings = listing['buys']
            ask_listings = listing['sells']
            item_id      = listing['id']
            
            bid_cost, bid_quantity, bid_quantity_total = listing_avg_price(bid_listings)
            ask_cost, ask_quantity, ask_quantity_total = listing_avg_price(ask_listings)
            
            if bid_quantity_total < min_quantity or ask_quantity_total < min_quantity:
                continue
    
            out.append((str(item_id),            request_time,
                        bid_cost / bid_quantity, bid_quantity_total,
                        ask_cost / ask_quantity, ask_quantity_total))
            
    return out
	
with open(working_dir + 'item_nums', 'r') as f:
    item_nums = [int(line[:-1]) for line in f]
	
price_data_fetch = []

k = 0

while k < len(item_nums):
    price_data_fetch += avg_prices(item_nums[k:(k+200)])
    k += 200
	
price_hist = pd.DataFrame(price_data_fetch, 
                          columns=['item_id',   'time',
                                   'bid_price', 'bid_quantity', 
                                   'ask_price', 'ask_quantity'])
								   
price_hist['liquidity_score'] = price_hist['ask_price'] / price_hist['bid_price'] - 1.1

out_path = '{}price_hist_{}.csv'.format(working_dir, time.strftime('%Y-%m-%d_%H00'))
price_hist.to_csv(out_path)