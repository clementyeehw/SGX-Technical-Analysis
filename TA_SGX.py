# Import Python libraries #
from bs4 import BeautifulSoup
import datetime as dt
import gc
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
from random import choice
import re
import requests
import talib as ta
from yahooquery import Ticker

######################################################################################################################################
''' 
##########          ########## 
          INITIATION 
##########          ##########  
'''

def get_proxy():
    '''
    Generate working proxy address
    Return:
        proxy       : Working proxy address and port  
    '''
    # Initiate parameter #
    proxy_url = 'https://www.sslproxies.org/'
    
    # Send GET request and scrape all proxy addresses #
    response = requests.get(proxy_url)
    soup = BeautifulSoup(response.content, 'html5lib')
    rows = soup.find('tbody').find_all('tr')
    proxies = [(row.find('td').text.strip() + ':' + 
               row.find('td').find_next('td').text.strip())
               for row in rows]
    
    # Randomise to return one proxy address #
    proxy = {'https': choice(proxies)}
    
    return proxy 


def proxy_request(request_type, target_url, **kwargs):
    '''
    Send GET request to target website using proxy address
    Parameters:
        request_type  : Type of request
        target_url    : Target website
    '''
    # Initiate parameter #
    headers = ['Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0',
               'Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/42.0',
               ('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) ' + 
               'Chrome/51.0.2704.103 Safari/537.36'),
               ('Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_1 like Mac OS X) AppleWebKit/603.1.30 ' +
               '(KHTML, like Gecko) Version/10.0 Mobile/14E304 Safari/602.1'),
               ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 ' +
               '(KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'),
               'Mozilla/5.0 (compatible; MSIE 9.0; Windows Phone OS 7.5; Trident/5.0; IEMobile/9.0)']
    header = {'User-Agent': choice(headers)}
    
    while True:
        try:
            # Generate one proxy address #
            proxy = get_proxy()
            # Send request #
            response = requests.request(request_type, target_url, 
                                        proxies=proxy, headers=header,
                                        timeout=5, **kwargs)
            # Break out of while loop #
            break
        except:
            pass
    
    return response

######################################################################################################################################
''' 
##########          ########## 
          STOCK PRICES
##########          ##########  
'''
   
def get_tickers(api_path=None, exchange='SGX'):
    '''
    Send GET request to query exchange traded stocks and their tickers.    
    Parameter:
        api_path      : Filename (incl path) containing World Trading Data's API token
                        Default value is None
        exchange      : Short name for stock exchange. Default value is SGX
    Return:
        tickers       : DataFrame containing exchange stock tickers
    '''
    # Initialise parameters #
    ## Local download path ##
    download_path = os.environ['USERPROFILE'] + r'\\\Dropbox\\Personal\\Trading'
    ## World Trading Data's URL ##
    wtd_url = 'https://api.worldtradingdata.com/api/v1/ticker_list'
    ## World Trading Data's API Token ##
    wtd_token = get_wtd_token(api_path=download_path + r'\API_TOKENS.xlsx', 
                              token_num=1)
    ## WTD's parameters ##
    params = {
        'type'          : 'stocks',
        'api_token'     : wtd_token,
        'symbol_only'   : 'false',
        'stock_exchange': exchange,
        }
    ## Exchange ##
    exchange = exchange.upper()
    
    # Send GET request to query all stocks #
    response = requests.get(wtd_url, params=params)
    results = response.json()
    
    # Free up memory #
    clean_responses(response)
    
    # Convert results into DataFrame #
    tickers = pd.DataFrame(results)
    tickers.drop_duplicates(subset=['symbol'], inplace=True)
    tickers = tickers[['symbol', 'name', 'currency']]
    tickers.columns = ['TICKER', 'COMPANY', 'CURRENCY']
    
    # Download DataFrame #
    tickers.to_excel('%s_TICKERS.xlsx' % exchange, sheet_name='TICKERS', index=False)
    
    return tickers
    

def get_wtd_token(api_path=None, token_num=None):
    '''
    Return wtd_token.
    Parameters:
        api_path      : Filename (incl path) containing World Trading Data's API tokens
        token_num     : WTD token to be used. Pick any number from 1 to 4.
    Return:
        wtd_token     : WTD's API token
    '''
    # WTD token is set by user's input #
    if api_path is None:
        wtd_token = input("Please key in WTD's token that was assigned to you: ")
    # WTD token is set by Excel's inputs #
    else:
        api_tokens = pd.read_excel(api_path)
        wtd_token = api_tokens.loc[api_tokens['APPLICATION']=='WorldTradingData_' + \
                                   str(token_num), 'KEY'].iloc[0]
    
    return wtd_token


def get_historical(exchange='SGX', start_year=2018, interval='1d'):
    '''
    Query latest historical prices of stocks.    
    Parameters:
        exchange      : Short name for stock exchange. Default value is SGX
        start_year    : Reference start year. Default value is 2018
        interval      : Reference interval period. Default value is daily
    '''
    # Initialise parameter #
    ## Tickers ##
    filepath = re.split(exchange, download_path)[0]
    stocks = pd.read_excel(filepath + exchange + '_TICKERS.xlsx')
    target_tickers = list(stocks['TICKER'])

    # Get historical prices - Default end date is now #
    tickers = Ticker(target_tickers)
    if interval == '1d':
        historical = tickers.history(start=dt.datetime(start_year,1,1),
                                     end=dt.datetime.now())
    else:
        first_recent = get_working_day()
        second_recent = first_recent - dt.timedelta(7)
        historical = tickers.history(interval=interval,
                                     start=second_recent,
                                     end=first_recent)
    
    # Access each ticker and output data #
    for target_ticker in target_tickers:
        target = historical[target_ticker]
        company = stocks.loc[stocks['TICKER'] == target_ticker, 'COMPANY'].iloc[0] 
        try:
            # target.index = [timestamp.date() for timestamp in list(target.index)]
            target.index.name = 'date'
            target.sort_index(ascending=False, inplace=True)
            if interval == '1d':
                target.to_csv(download_path + '\\' + exchange + \
                              r'\Prices\Daily\%s_%s.csv' % (target_ticker, company))
            else:
                target.dropna(inplace=True)
                target.to_csv(download_path + '\\' + exchange + \
                              r'\Prices\Minute\%s_%s.csv' % (target_ticker, company))
        except:
            pass
    
######################################################################################################################################
''' 
##########          ########## 
       STOCK FUNDAMENTALS 
##########          ##########  
'''

def get_financials(exchange='SGX', delimiter='.SI'):
    '''
    Query stock financials (quarterly and annual).    
    Parameter:
        exchange      : Short name for stock exchange. Default value is SGX
        delimiter     : RIC code accompanying tickers
    '''
    # Initialise parameters #
    ## Tickers ##
    filepath = re.split(exchange, download_path)[0]
    stocks = pd.read_excel(filepath + exchange + '_TICKERS.xlsx')
    target_tickers = list(stocks['TICKER'])
    ## Modules ##
    modules = ['balanceSheetHistory', 'balanceSheetHistoryQuarterly',
               'incomeStatementHistory', 'incomeStatementHistoryQuarterly',
               'cashflowStatementHistory', 'cashflowStatementHistoryQuarterly',
               'defaultKeyStatistics']
    ## Errata tickers ##
    errata = []
    ## Empty DataFrames ##
    bal_sheet = pd.DataFrame([{'symbol' : np.nan, 'periodType': np.nan}])
    inc_statement = pd.DataFrame([{'symbol' : np.nan, 'periodType': np.nan}])
    cf_statement = pd.DataFrame([{'symbol' : np.nan, 'periodType': np.nan}])
    fin_ratios = pd.DataFrame([{'symbol' : np.nan}])
    
    # Get stock fundamnetals - Balance Sheet, Income Statement, Cash Flow #
    while True:
        if errata:
            target_tickers = [target_ticker for target_ticker in target_tickers 
                              if target_ticker not in errata]
        tickers = Ticker(target_tickers)
        try:
            data = tickers.get_modules(modules)
        except Exception as e:
            ## Get errata tickers ##
            errata.append(re.split('/', re.split(r'\%s' % delimiter, str(e))[0])[-1] + delimiter)
            print(errata)
        else:
            ## Retrieve working data for errata_tickers ##
            print('--Commence processing DataFrames--')
            if errata:
                for ticker in errata:
                    errata_ticker = Ticker(ticker)
                    for module in modules:
                        try:
                            errata_data = errata_ticker.get_modules(module)
                        except:
                            pass
                        else:
                            ## Combine dictionaries ##
                            for key, value in errata_data.items():
                                data.update({key:value})            
                ## Merge tickers ##
                target_tickers += errata
            
            ## Convert dictionaries into DataFrames ##
            for target_ticker in target_tickers:
                for module in modules:
                    ### Balance Sheet ###
                    if re.search('balanceSheetHistory', module):
                        try:
                            bal_sheet = pd.concat([bal_sheet,
                                    pd.DataFrame(data[target_ticker][module]['balanceSheetStatements'])],
                                              axis=0)
                        except:
                            pass
                        else:
                            bal_sheet = get_period_type(bal_sheet, target_ticker, module)
                    ### Income Statement ###
                    elif re.search('incomeStatementHistory', module):
                        try:
                            inc_statement = pd.concat([inc_statement,
                                        pd.DataFrame(data[target_ticker][module]['incomeStatementHistory'])],
                                              axis=0)
                        except:
                            pass
                        else:
                            inc_statement = get_period_type(inc_statement, target_ticker, module)
                    ### Cash Flow Statement ###
                    elif re.search('cashflowStatementHistory', module):
                        try:
                            cf_statement = pd.concat([cf_statement,
                                        pd.DataFrame(data[target_ticker][module]['cashflowStatements'])],
                                              axis=0)
                        except:
                            pass
                        else:
                            cf_statement = get_period_type(cf_statement, target_ticker, module)
                    ### Financial Ratios ###
                    else:
                        try:
                            fin_ratios = pd.concat([fin_ratios,
                                        pd.DataFrame([data[target_ticker][module]])],
                                              axis=0)
                        except:
                            pass
                        else:
                            fin_ratios.loc[fin_ratios['symbol'].isnull(), 'symbol'] = target_ticker
            
            ## Drop the first row ##
            bal_sheet.index = range(0, len(bal_sheet))
            bal_sheet = bal_sheet.iloc[1:]
            inc_statement.index = range(0, len(inc_statement))
            inc_statement = inc_statement.iloc[1:]
            cf_statement.index = range(0, len(cf_statement))
            cf_statement = cf_statement.iloc[1:]
            fin_ratios.index = range(0, len(fin_ratios))
            fin_ratios = fin_ratios.iloc[1:]
            
            ## Download DataFrames ## 
            writer = pd.ExcelWriter(download_path + '\\' + exchange + r'\FIN_STATEMENTS.xlsx', 
                                    engine='xlsxwriter')
            bal_sheet.to_excel(writer, sheet_name='BS', index=False)
            inc_statement.to_excel(writer, sheet_name='IS', index=False)
            cf_statement.to_excel(writer, sheet_name='CFS', index=False)
            fin_ratios.to_excel(writer, sheet_name='RATIOS', index=False)
            writer.save()
            print('--Complete processing--')
            
            ## Break out of while loop ##
            break
            
    return data, bal_sheet, inc_statement, cf_statement, fin_ratios


def get_period_type(fs, ticker, module):
    '''
    Returns the symbol and period type for the target financial statement.
    Parameters:
        fs               : Target financial statement
        ticker           : Target ticker 
        module           : Name of YahooQuery module
    Return:
        fs               : Updated financial statement
    '''
    # Fill up symbol #
    fs.loc[fs['symbol'].isnull(), 'symbol'] = ticker
    
    # Fill up period type #
    if not re.search(r'Quarterly', module):
        fs.loc[fs['periodType'].isnull(), 'periodType'] = '12M'
    else:
        fs.loc[fs['periodType'].isnull(), 'periodType'] = '3M'

    return fs

######################################################################################################################################
''' 
##########          ########## 
         MISCELLANEOUS 
##########          ##########  
'''

def clean_responses(response, soup=None):
    '''
    Clear all the the responses to free up memory 
    Parameters:
        response      : Response from GET requests
        soup          : Parsed BeautifulSoup     
    '''
    # Close response #
    response.close()
    response = None
    
    # Decompose soup if soup is valid #
    if soup is not None:
        soup.decompose()
        
    # Clear all the waste #
    gc.collect()
    

def get_working_day():
    '''
    Returns the previous working day if today is not a working day.
    Return:
        working_day   : Previous working day in datetime format
    '''
    # Initiate parameters #
    ## Today's date ##
    today = dt.datetime.now().date()
    working_day = today
    ## Day of the week ##
    weekday = today.weekday()
    
    while weekday >= 5:
        # Decrease counter #
        working_day -= dt.timedelta(1)
        weekday -= 1
    
    return working_day
    
######################################################################################################################################
''' 
##########          ########## 
       TECHNICAL ANALYSIS 
##########          ##########  
'''

def test_strat(ticker):
    ''' 
    Trading strategy using Parabolic SAR and Stochastic Oscillator.
    
    Brief description of trading strategy:
    We BUY when the (i) parabolic SAR line appears below the closing price and
    (ii) the fast stochastic crosses above the slow stochastic. A SAR line that 
    appears below the market price indicates an uptrend and since fast 
    stochastics is more sensitive to price and when it crosses above the slow 
    stochastic, it indicates the reversal in price direction from downward to 
    upward. Similarly, we SELL when the SAR line appears above closing price 
    and the fast stochastic crosses below the slow  stochastic. A SAR line that 
    appears above the market price indicates a downtrend and when the fast 
    stochastic crosses below the slow stochastic, it indicates the reversal 
    in price direction from upward to downward.
    
    Note: Get TA-Lib from this website if it is not installed in your environment.
    https://blog.quantinsti.com/install-ta-lib-python/
    
    Parameter:
        ticker        : SGX stock ticker, e.g. S68.SI    
    '''
    # Import target ticker's historical prices #
    filepath = [download_path + '\\' + file 
                for file in os.listdir(download_path) 
                if re.search(ticker, file)][0]
    df = pd.read_csv(filepath, 
                     usecols=['date', 'high', 'low', 'adjclose'],
                     parse_dates=['date'])
    df.columns = ['Date', 'High', 'Low', 'Close']
    df.set_index('Date', inplace=True)
    df.sort_index(ascending=True, inplace=True)
    
    # Calculate Parabolic SAR #
    df['SAR']=ta.SAR((df['High'].values), (df['Low'].values), 
                      acceleration = 0.02, maximum = 0.2)
    
    # Calculate Stochastic Oscillator #
    ## Fast Stochastic Oscillator ##
    df['fastk'], df['fastd'] = ta.STOCHF((df['High'].values), (df['Low'].values), (df['Close'].values),
                                          fastk_period=5, fastd_period=3, fastd_matype=0)
    ## Slow Stochastic Oscillator ##
    df['slowk'], df['slowd'] = ta.STOCH((df['High'].values), (df['Low'].values), (df['Close'].values),
                                        fastk_period=5, slowk_period=3, slowk_matype=0, 
                                        slowd_period=3, slowd_matype=0)
    
    # Generate Trading Signal #
    df['Signal'] = np.nan
    ## Buy Signal ##
    df.loc[(df['SAR']< df['Close']) & (df['fastd']>df['slowd']) & (df['fastk']>df['slowk']),'Signal'] = 1
    ## Sell Signal ##
    df.loc[(df['SAR']> df['Close']) & (df['fastd']<df['slowd']) & (df['fastk']<df['slowk']),'Signal'] = -1
    df = df.fillna(method='ffill')
    
    # Calculate Strategy Returns #
    df['Stock_Return'] = df['Close'].pct_change()
    df['strategy_return'] = (df['Stock_Return'] * df['Signal'].shift(1))
    df = df.dropna()
    ## Plot the cumulative strategy returns ##
    (df['strategy_return']+1).cumprod().plot(figsize=(10,5), label = 'Strategy Returns')
    plt.xlabel('Date')
    plt.ylabel('Cumulative Strategy Returns')
    plt.legend()
    plt.show()
    ## Calculate Sharpe Ratio , assuming risk-free rate is 1.35% p.a. ##
    risk_free_rate = 0.0135/252
    sharpe = np.sqrt(252)*(np.mean(df['strategy_return'])- (risk_free_rate))/np.std(df['strategy_return'])
    print ('Sharpe Ratio:', sharpe)
    ## Calculate CAGR ##
    period_in_days = (df.index[-1] - df.index[0]).days
    CAGR = ((df['strategy_return'].cumsum()[-1]+1)**(365.0/period_in_days) - 1)*100
    print ('CAGR:', CAGR)
    
######################################################################################################################################
# Initialise parameters #
## Local download path ##
download_path = os.environ['USERPROFILE'] + r'\Dropbox\Personal\Trading\Historical\SGX\Prices\Daily'
## Headers ##
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6)' + \
           'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}

# Import historical prices from YahooQuery #
get_historical(exchange='SGX', start_year=2018, interval='1d')

# Test Trading Strategy - this example uses SGX ticker as a test-bed #
test_strat('S68.SI')