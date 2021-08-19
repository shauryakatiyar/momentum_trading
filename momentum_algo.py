#!/usr/bin/env python3.9
'''
gatherTlTickets.py
This monitoring script does daily backup of Tl Tickets.
optional arguments:
  -h, --help            show this help message and exit
  -l LOGLEVEL, --logLevel LOGLEVEL
                        Logging Level
'''
import ast
from datetime import datetime
import numpy as np
import pandas as pd
import requests
import xlsxwriter
import math
import signal
from os.path import expanduser
from scipy.stats import percentileofscore as score
from statistics import mean
import time
import os
import argparse
import textwrap
import logging
import sys

#------------------------------------------------
# Module Functions
#------------------------------------------------


def signalCatch(signal, stackframe):
    '''
    This function is used as keyboard handler for the singal.signal(<signal>, <handler>)
    call. Note that in order for keyboard handler to fire, exception cases in
    try statements must at least catch Exception class. Otherwise, other exceptions
    like KeyboardInterrupt or SystemExit will be caught which will make it harder to
    exit programs.
    '''
    print('\n> SCRIPT ABORTED: Keyboard Interrupt Detected !!!\n')
    sys.exit()

# Function sourced from
# https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks


def abort(msg='', code=0, error=False):
    '''
    This function is used by tools to abort a script with a useful message
    '''

    # User Message
    if msg:
        print("\n> %s" % msg)

    # Completion Message
    if error:
        print("\n* Script Aborted due to Errors (check logs and output above)\n")
    else:
        print("\n* Script Complete\n")

    # Status Code
    sys.exit(code)


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def portfolio_input():
    global portfolio_size
    portfolio_size = input("Enter the value of your portfolio:")

    try:
        val = float(portfolio_size)
    except ValueError:
        print("That's not a number! \n Try again:")
        portfolio_size = input("Enter the value of your portfolio:")


#------------------------------------------------
# MAIN
#------------------------------------------------
def main(nasdaq, nse, sandp, allstock, nasdaqkey, nsekey, downloaddata):

    # nasdaq=sk_24c9ec8bbd8048edbf2982a2dae55876
    # nse=sk_24c9ec8bbd8048edbf2982a2dae55876
    #############NASDAQ##############
    stocks_nasdaq = pd.read_csv("NASDAQ.csv")
    IEX_CLOUD_API_TOKEN_NASDAQ = nasdaqkey

    # Dividing Stocks into Chunks of 100
    symbol_groups_nasdaq = list(chunks(stocks_nasdaq['Ticker'], 100))
    symbol_strings_nasdaq = []
    for i in range(0, len(symbol_groups_nasdaq)):
        symbol_strings_nasdaq.append(','.join(symbol_groups_nasdaq[i]))
    #############NASDAQ################

    #############NSE##############
    stocks_nse = pd.read_csv("NSE.csv")
    IEX_CLOUD_API_TOKEN_NSE = nsekey

    # Dividing Stocks into Chunks of 100
    symbol_groups_nse = list(chunks(stocks_nse['Ticker'], 100))
    symbol_strings_nse = []
    for i in range(0, len(symbol_groups_nse)):
        symbol_strings_nse.append(','.join(symbol_groups_nse[i]))
    #############NSE################

    #############S&P500##############
    stocks_sandp500 = pd.read_csv("sp_500_stocks.csv")
    #############S&P500################

    unique_date_time_for_file_name = str(datetime.now().strftime("%d%m%Y"))
    # File name for storing and readind Data
    filename = expanduser(".") + "/" + unique_date_time_for_file_name + "/" + "cumulative_data-" + unique_date_time_for_file_name + ".txt"
    print(filename)
    if downloaddata:
        # Will store all returned data in data2 variable
        data2 = {}
        data = ""
        # Going through NASDAQ List
        print("Going for NASDAQ list")
        print("Total entries in NASDAQ list %s" % (len(stocks_nasdaq.index)))
        c = 0
        for symbol_string_nasdaq in symbol_strings_nasdaq:
            time.sleep(2.4)
            print(c)
            c = c + 1
            batch_api_call_url = f'https://cloud.iexapis.com/stable/stock/market/batch/?types=stats,quote&symbols={symbol_string_nasdaq}&token={IEX_CLOUD_API_TOKEN_NASDAQ}'
            data = requests.get(batch_api_call_url).json()
            data2.update(data)

        # Will store all returned data in data2 variable
        # Going through NSE List
        print("Going for NSE list")
        print("Total entries in NSE list %s" % (len(stocks_nse.index)))
        c = 0
        data = ""
        for symbol_string_nse in symbol_strings_nse:
            time.sleep(2.4)
            print(c)
            c = c + 1
            batch_api_call_url = f'https://cloud.iexapis.com/stable/stock/market/batch/?types=stats,quote&symbols={symbol_string_nse}&token={IEX_CLOUD_API_TOKEN_NSE}'
            data = requests.get(batch_api_call_url).json()
            data2.update(data)

        if not os.path.exists(os.path.dirname(filename)):
            try:
                print("Creating %s" % filename)
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise

        with open(filename, 'w') as file:
            file.write(repr(data2))  # use `json.loads` to do the reverse
        print("Written output to %s" % filename)
        abort()

    cumulative_list = []

    if nasdaq:
        cumulative_list = stocks_nasdaq['Ticker'].tolist()
    elif nse:
        cumulative_list = stocks_nse['Ticker'].tolist()
    elif sandp:
        cumulative_list = stocks_sandp500['Ticker'].tolist()
    elif allstock:
        cumulative_list = stocks_nasdaq['Ticker'].tolist() + stocks_nse['Ticker'].tolist() + stocks_sandp500['Ticker'].tolist()

    file = open(filename, "r")
    contents = file.read()
    data1 = ast.literal_eval(contents)

    # Creating Data Frame
    my_columns = ['Ticker', 'Price', 'One-Year Price Return', 'Number of Shares to Buy']
    final_dataframe = pd.DataFrame(columns=my_columns)

    # Creating Data Frame
    combined_symbol_strings = symbol_strings_nasdaq + symbol_strings_nse
    for symbol_string in combined_symbol_strings:
        for symbol in symbol_string.split(','):
            if symbol in data1 and 'quote' in data1[symbol] and (data1[symbol]['quote'] and data1[symbol]['stats']) and symbol in cumulative_list:
                final_dataframe = final_dataframe.append(
                    pd.Series([symbol,
                               data1[symbol]['quote']['latestPrice'],
                               data1[symbol]['stats']['year1ChangePercent'],
                               'N/A'
                               ],
                              index=my_columns),
                    ignore_index=True)

    # Sorting First Data Frame Based on One-Year Price Return
    final_dataframe.sort_values('One-Year Price Return', ascending=False, inplace=True)
    final_dataframe = final_dataframe[:51]
    final_dataframe.reset_index(drop=True, inplace=True)

    # Calculating the Number of Shares to Buy
    # Asking portfolio size to calculate Number of Stocks to buy
    # portfolio_input()
    portfolio_size = 10000000000

    position_size = float(portfolio_size) / len(final_dataframe.index)
    for i in range(0, len(final_dataframe['Ticker'])):
        final_dataframe.loc[i, 'Number of Shares to Buy'] = math.floor(position_size / final_dataframe['Price'][i])

    print(final_dataframe)

    # Building a Better (and More Realistic) Momentum Strategy

    # Real-world quantitative investment firms differentiate between "high quality" and "low quality" momentum stocks:

    # High-quality momentum stocks show "slow and steady" outperformance over long periods of time
    # Low-quality momentum stocks might not show any momentum for a long time, and then surge upwards.
    # The reason why high-quality momentum stocks are preferred is because low-quality momentum can often be cause by short-term news that is unlikely to be repeated in the future (such as an FDA approval for a biotechnology company).

    # To identify high-quality momentum, we're going to build a strategy that selects stocks from the highest percentiles of:

    # 1-month price returns
    # 3-month price returns
    # 6-month price returns
    # 1-year price returns
    # Let's start by building our DataFrame. You'll notice that I use the abbreviation hqm often. It stands for high-quality momentum.

    # Building HQM Datafram
    hqm_columns = [
        'Ticker',
        'Price',
        'Number of Shares to Buy',
        'One-Year Price Return',
        'One-Year Return Percentile',
        'Six-Month Price Return',
        'Six-Month Return Percentile',
        'Three-Month Price Return',
        'Three-Month Return Percentile',
        'One-Month Price Return',
        'One-Month Return Percentile',
        'HQM Score'
    ]

    hqm_dataframe = pd.DataFrame(columns=hqm_columns)

    for symbol_string in combined_symbol_strings:
        #     print(symbol_strings)
        # batch_api_call_url = f'https://sandbox.iexapis.com/stable/stock/market/batch/?types=stats,quote&symbols={symbol_string}&token={IEX_CLOUD_API_TOKEN}'
        # data = requests.get(batch_api_call_url).json()
        for symbol in symbol_string.split(','):
            if symbol in data1 and 'quote' in data1[symbol] and (data1[symbol]['quote'] and data1[symbol]['stats']) and symbol in cumulative_list:
                hqm_dataframe = hqm_dataframe.append(
                    pd.Series([symbol,
                               data1[symbol]['quote']['latestPrice'],
                               'N/A',
                               data1[symbol]['stats']['year1ChangePercent'],
                               'N/A',
                               data1[symbol]['stats']['month6ChangePercent'],
                               'N/A',
                               data1[symbol]['stats']['month3ChangePercent'],
                               'N/A',
                               data1[symbol]['stats']['month1ChangePercent'],
                               'N/A',
                               'N/A'
                               ],
                              index=hqm_columns),
                    ignore_index=True)

    # Calculating Momentum Percentiles
    # We now need to calculate momentum percentile scores for every stock in the universe. More specifically, we need to calculate percentile scores for the following metrics for every stock:

    # One-Year Price Return
    # Six-Month Price Return
    # Three-Month Price Return
    # One-Month Price Return
    time_periods = [
        'One-Year',
        'Six-Month',
        'Three-Month',
        'One-Month'
    ]

    for row in hqm_dataframe.index:
        for time_period in time_periods:
            if hqm_dataframe.loc[row, f'{time_period} Price Return'] == None:
                hqm_dataframe.loc[row, f'{time_period} Price Return'] = 0

    for row in hqm_dataframe.index:
        for time_period in time_periods:
            change_col = f'{time_period} Price Return'
            percentile_col = f'{time_period} Return Percentile'
            hqm_dataframe.loc[row, percentile_col] = score(hqm_dataframe[change_col], hqm_dataframe.loc[row, change_col]) / 100

    # Print each percentile score to make sure it was calculated properly
    # for time_period in time_periods:
    #    print(hqm_dataframe[f'{time_period} Return Percentile'])

    # Calculating the HQM Score
    # We'll now calculate our HQM Score, which is the high-quality momentum score that we'll use to filter for stocks in this investing strategy.
    # The HQM Score will be the arithmetic mean of the 4 momentum percentile scores that we calculated in the last section.
    # To calculate arithmetic mean, we will use the mean function from Python's built-in statistics module.

    for row in hqm_dataframe.index:
        momentum_percentiles = []
        for time_period in time_periods:
            momentum_percentiles.append(hqm_dataframe.loc[row, f'{time_period} Return Percentile'])
        hqm_dataframe.loc[row, 'HQM Score'] = mean(momentum_percentiles)

    # Print the entire DataFrame
    # print(hqm_dataframe.to_string())

    hqm_dataframe.sort_values(by='HQM Score', ascending=False, inplace=True)
    # hqm_dataframe = hqm_dataframe[:51]

    # Calculating the Number of Shares to Buy
    position_size = float(portfolio_size) / len(hqm_dataframe.index)
    for i in range(0, len(hqm_dataframe['Ticker'])):
        if hqm_dataframe['Price'][i] == None:
            temp = 100000000000000
        else:
            temp = hqm_dataframe['Price'][i]

        hqm_dataframe.loc[i, 'Number of Shares to Buy'] = math.floor(position_size / temp)
    print(hqm_dataframe.to_string())

    if nse:
        filename2 = expanduser(".") + "/" + unique_date_time_for_file_name + "/" + "nse-" + unique_date_time_for_file_name + ".xlsx"
    if sandp:
        filename2 = expanduser(".") + "/" + unique_date_time_for_file_name + "/" + "sandp-" + unique_date_time_for_file_name + ".xlsx"
    if nasdaq:
        filename2 = expanduser(".") + "/" + unique_date_time_for_file_name + "/" + "nasdaq-" + unique_date_time_for_file_name + ".xlsx"
    if allstock:
        filename2 = expanduser(".") + "/" + unique_date_time_for_file_name + "/" + "allstock-" + unique_date_time_for_file_name + ".xlsx"

    # Formatting Our Excel Output
    writer = pd.ExcelWriter(filename2, engine='xlsxwriter')
    hqm_dataframe.to_excel(writer, sheet_name='Momentum Strategy', index=False)

    # Creating the Formats We'll Need For Our .xlsx File
    background_color = '#0a0a23'
    font_color = '#ffffff'

    string_template = writer.book.add_format(
        {
            'font_color': font_color,
            'bg_color': background_color,
            'border': 1
        }
    )

    dollar_template = writer.book.add_format(
        {
            'num_format': '$0.00',
            'font_color': font_color,
            'bg_color': background_color,
            'border': 1
        }
    )

    integer_template = writer.book.add_format(
        {
            'num_format': '0',
            'font_color': font_color,
            'bg_color': background_color,
            'border': 1
        }
    )

    percent_template = writer.book.add_format(
        {
            'num_format': '0.0%',
            'font_color': font_color,
            'bg_color': background_color,
            'border': 1
        }
    )

    column_formats = {
        'A': ['Ticker', string_template],
        'B': ['Price', dollar_template],
        'C': ['Number of Shares to Buy', integer_template],
        'D': ['One-Year Price Return', percent_template],
        'E': ['One-Year Return Percentile', percent_template],
        'F': ['Six-Month Price Return', percent_template],
        'G': ['Six-Month Return Percentile', percent_template],
        'H': ['Three-Month Price Return', percent_template],
        'I': ['Three-Month Return Percentile', percent_template],
        'J': ['One-Month Price Return', percent_template],
        'K': ['One-Month Return Percentile', percent_template],
        'L': ['HQM Score', percent_template]
    }

    for column in column_formats.keys():
        writer.sheets['Momentum Strategy'].set_column(f'{column}:{column}', 25, column_formats[column][1])
        writer.sheets['Momentum Strategy'].write(f'{column}1', column_formats[column][0], string_template)

    # Saving Our Excel Output
    writer.save()


#------------------------------------------------
# MAIN
#------------------------------------------------
if __name__ == '__main__':

    # Initialize Variables
    logFile = os.path.expanduser('~/logs/momentum_algo.log')
    logOptions = {'logFile': logFile,
                  'logLevel': 'INFO',
                  'filterList': 'default'}

    # Parsing Options
    parser = argparse.ArgumentParser(description=textwrap.dedent(__doc__), formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-l', '--logLevel', default=logOptions['logLevel'], help='Logging Level')
    parser.add_argument('--nasdaq', action='store_true', help='IP Address')
    parser.add_argument('--nse', action='store_true', help='MAC address')
    parser.add_argument('--sandp', action='store_true', help='Domain name for A record')
    parser.add_argument('--allstock', action='store_true', help='Domain name for A record')
    parser.add_argument('--downloaddata', action='store_true', help='Read data from sheet')
    parser.add_argument('--nasdaqkey', help='Key for downloading nasdaq data')
    parser.add_argument('--nsekey', help='Key for downloading nse data')
    options = parser.parse_args()

    # Generic Keyboard Interrupt Signal Handler
    signal.signal(signal.SIGINT, signalCatch)

    # Logging
    logOptions['logLevel'] = options.logLevel

    if options.downloaddata and not (options.nasdaqkey and options.nsekey):
        print("Please enter both NSE and NASDAQ Keys")
        abort()

    # Main
    main(options.nasdaq, options.nse, options.sandp, options.allstock, options.nasdaqkey, options.nsekey, options.downloaddata)

    # Exit
    abort()
    # Main
    main()
