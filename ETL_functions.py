from yahooquery import Ticker
import pandas as pd
import numpy as np
import datetime as dt
import pytz

def extract_data(query_result, selected_items, query_time):
    """
    Extracts items from a query with yahooquery library. 
    Returns a list with one dict per ticker in the query containing query_time and selected_items datas 
       
    Arguments
    ----------
    query_result: dict
        Result of a query with yahooquery library with a method applied (ex: yahooquery.Ticker(ticker).financial_data)
    selected_items: list of str
        Names of items to extract (ex: ['regularMarketPrice', 'regularMarketDayHigh'])
    query_time: timestamp
        Time at which the query has been performed
    """
    
    tickers = list(query_result.keys())   
    results_list = []
    
    # For each ticker extract selected items
    for ticker in tickers:
        
        # Get query result for the current ticker
        query_result_ticker = query_result[ticker]
        
        # Instantiante result with time and ticker
        ticker_result = {'timestamp': query_time, 'ticker': ticker}
        
        # Collect name of available items for the ticker (yahooquery doesn't return the same items depending on the ticker)
        if isinstance(query_result_ticker, str): # If the query doesn't find any results it resturns a string
            available_items = []
        
        else: # Else get nales of items returned
            available_items = query_result_ticker.keys()
        
        # Now extract items if available
        for item in selected_items:
        
            # Check if data is available, and append items
            if item in available_items:
                ticker_result[item] = query_result_ticker[item]

            # If not available, fill with NaN
            else:
                ticker_result[item] = np.NaN
              
        # Append results for the current ticker to the final list          
        results_list.append(ticker_result)
    
    return results_list

def load_data(database, data_to_insert):
    
    temp_df = pd.DataFrame(data_to_insert)
    database = pd.concat([database, temp_df])
    
    return database

def ETL(path_history, path_last, query_result_list, selected_items_list, query_time):
    """
    Extracts items from a query with yahooquery library, and save extracts in the specified path.
    The history database will be updated with extracts and the new database of last extracts will replace the older one. 
       
    Arguments
    ----------
    path_history: str
        Path where the history extracts database is located
    path_last: str or None
        Path where the last extracts database is located. If None, only the history database will be updated.
    query_result: list containing dicts
        Result of a queries with yahooquery library with a method applied (ex: yahooquery.Ticker(ticker).financial_data)
    selected_items: list containing list of str
        Names of items to extract (ex: [['priceHint', 'previousClose', 'open'], ['regularMarketChangePercent', 'regularMarketChange']])
    query_time: timestamp
        Time at which the query has been performed
    """
    
    # Load existing database
    history_db = pd.read_csv(path_history)
    
    # Extract datas
    i = 0
    
    for query_result, selected_items in zip(query_result_list, selected_items_list):
        
        extract = pd.DataFrame(extract_data(query_result, selected_items, query_time))
        
        # If it is the first loop we need to create a DataFrame with the extract
        if i == 0:
            combined_extract = extract.copy()
            i += 1
        
        # Else we merge the new extract with the existing DataFrame from first loop
        else:
            combined_extract = pd.merge(combined_extract, extract, on=['ticker', 'timestamp'])
    
    # Update history database
    history_db = pd.concat([history_db, combined_extract])
    history_db.to_csv(path_history, index=False)
    
    # If dedicated database storing only last updates, replace this database
    if path_last:
        combined_extract.to_csv(path_last, index=False)