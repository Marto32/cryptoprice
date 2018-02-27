#! /usr/bin/env python
import argparse
import datetime
import json
import time
import logging

import pandas as pd
import requests

from pathlib import Path
from retrying import retry


AVAILABLE_CURRENCY_PAIRS = ['BTC_AMP', 'BTC_ARDR', 'BTC_BCH', 'BTC_BCN', 'BTC_BCY', 'BTC_BELA',
                           'BTC_BLK', 'BTC_BTCD', 'BTC_BTM', 'BTC_BTS', 'BTC_BURST', 'BTC_CLAM',
                           'BTC_CVC', 'BTC_DASH', 'BTC_DCR', 'BTC_DGB', 'BTC_DOGE', 'BTC_EMC2', 
                           'BTC_ETC', 'BTC_ETH', 'BTC_EXP', 'BTC_FCT', 'BTC_FLDC', 'BTC_FLO', 'BTC_GAME', 
                           'BTC_GAS', 'BTC_GNO', 'BTC_GNT', 'BTC_GRC', 'BTC_HUC', 'BTC_LBC', 'BTC_LSK', 
                           'BTC_LTC', 'BTC_MAID', 'BTC_NAV', 'BTC_NEOS', 'BTC_NMC', 'BTC_NXC', 'BTC_NXT', 
                           'BTC_OMG', 'BTC_OMNI', 'BTC_PASC', 'BTC_PINK', 'BTC_POT', 'BTC_PPC', 'BTC_RADS', 
                           'BTC_SC', 'BTC_STEEM', 'BTC_STORJ', 'BTC_STR', 'BTC_STRAT', 'BTC_SYS',
                           'BTC_VIA', 'BTC_VRC', 'BTC_VTC', 'BTC_XBC', 'BTC_XCP', 'BTC_XEM', 'BTC_XMR', 
                           'BTC_XPM', 'BTC_XRP', 'BTC_XVC', 'BTC_ZEC', 'BTC_ZRX', 'ETH_BCH', 'ETH_CVC', 
                           'ETH_ETC', 'ETH_GAS', 'ETH_GNO', 'ETH_GNT', 'ETH_LSK', 'ETH_OMG', 'ETH_REP', 
                           'ETH_STEEM', 'ETH_ZEC', 'ETH_ZRX', 'USDT_BCH', 'USDT_BTC', 'USDT_DASH', 
                           'USDT_ETC', 'USDT_ETH', 'USDT_LTC', 'USDT_NXT', 'USDT_REP', 'USDT_STR', 
                           'USDT_XMR', 'USDT_XRP', 'USDT_ZEC', 'XMR_BCN', 'XMR_BLK', 'XMR_BTCD', 'XMR_DASH',
                           'XMR_LTC', 'XMR_MAID', 'XMR_NXT', 'XMR_ZEC', 'BTC_REP', 'BTC_RIC', 'BTC_SBD',]


class CryptoData(object):
    """
    Poloneix Documentation: https://poloniex.com/support/api/

    ## returnChartData
    Returns candlestick chart data. Required GET parameters are "currencyPair",
    "period" (candlestick period in seconds; valid values are 300, 900, 1800, 7200,
    14400, and 86400), "start", and "end". "Start" and "end" are given in UNIX
    timestamp format and used to specify the date range for the data returned. Sample output:
    [{"date":1405699200,"high":0.0045388,"low":0.00403001,"open":0.00404545,"close":0.00427592,"volume":44.11655644,
    "quoteVolume":10259.29079097,"weightedAverage":0.00430015}, ...]
    """

    def __init__(self, currency_pair='USDT_BTC', start_date='2015-01-01', end_date=None,
                 period=14400, destination=None, api='returnChartData', logger=None):

        self.currency_pair = currency_pair.upper()
        self.start_timestamp = self.get_timestamp(date_string=start_date)
        if not end_date:
            self.end_timestamp = 9999999999
        else:
            self.end_timestamp = self.get_timestamp(date_string=end_date)
        self.period = 300
        self.api = api
        self.destination = destination
        self.data = None
        self.logger = logger

        self.url = f'https://poloniex.com/public?command={self.api}&currencyPair' \
                   f'={self.currency_pair}&start={self.start_timestamp}&end=' \
                   f'{self.end_timestamp}&period={self.period}'

    def get_timestamp(self, date_string=None, date_format='%Y-%m-%d'):
        if date_string is None:
            return int(time.mktime(datetime.datetime.utcnow().timetuple()))
        else:
            return int(time.mktime(datetime.datetime.strptime(date_string, date_format).timetuple()))

    def get_api_data(self):
        response = requests.get(self.url)
        return response

    def parse_api_data_text(self, response):
        parsed_data = json.loads(response.text)
        if isinstance(parsed_data, dict) and 'error' in parsed_data.keys():
            if parsed_data['error'] == 'Invalid currency pair.':
                raise Exception(f'{self.currency_pair} is not a valid currency pair. ' \
                                f'You must use one of: \n{AVAILABLE_CURRENCY_PAIRS}')
            else:
                raise Exception(f'API Error: {parsed_data["error"]}')
        return parsed_data

    def build_dataframe(self, parsed_data):
        data = pd.DataFrame(parsed_data)
        data['datetime'] = data['date'].apply(datetime.datetime.utcfromtimestamp)
        data.sort_values('datetime', inplace=True)
        data['datetime_utc'] = data['datetime']
        cols = ['datetime_utc', 'open', 'high', 'low', 'close', 'quoteVolume', 'volume',
                'weightedAverage']
        self.data = data[cols]

    def save_data(self, dataframe):
        dataframe.to_csv(self.destination, index=False)
        return self

    @retry(stop_max_attempt_number=7, wait_random_min=1000, wait_random_max=2000)
    def run(self, save=True):
        if self.data is None:
            response = self.get_api_data()
            self.build_dataframe(self.parse_api_data_text(response))

        if save:
            self.save_data(self.data)
            return self

        else:
            return self.data


if __name__ == '__main__':
    DESCRIPTION = """
        A simple tool to pull price data from Poloneix's API. The data
        can be saved down as a csv or used in memory as a pandas DataFrame.

        Poloneix Documentation: https://poloniex.com/support/api/
        """

    parser = argparse.ArgumentParser(description=DESCRIPTION)

    parser.add_argument('--currency-pair', dest='currency_pair', default='USDT_LTC',
                        type=str, help='A poloneix currency pair. Use --pairs to view pairs')
    parser.add_argument('--period', dest='period', default=14400, help='The timefrime to use '
                        'when pulling data in seconds. Defaults to 14400. Available options' \
                        ' 300, 900, 1800, 7200, 14400, 86400.', type=int)
    parser.add_argument('--dest', dest='dest', type=str, default=None, help='The full path to which '
                        'the output file should be saved. Defaults to the home directory.')
    parser.add_argument('--start-date', dest='start_date', type=str,
                        default=datetime.datetime.strftime(
                            datetime.datetime.utcnow() + datetime.timedelta(-30), format='%Y-%m-%d'),
                        help='The start date for the data pull in the format YYYY-MM-DD. Defaults ' \
                        'to 30 days ago.')
    parser.add_argument('--end-date', dest='end_date', type=str, default=None,
                        help='The end date for the data pull in the format YYYY-MM-DD. Defaults ' \
                        'to now.')
    parser.add_argument('--pairs', dest='pairs', action='store_true',
                        default=False, help='A flag used to view currency pairs.')

    args = parser.parse_args()

    logger = logging.getLogger(__name__)
    logger.setLevel('INFO')

    if args.pairs:
        chunks = [AVAILABLE_CURRENCY_PAIRS[x:x + 3] for x in range(0, len(AVAILABLE_CURRENCY_PAIRS), 3)]
        setup = [[str(e) for e in row] for row in chunks]
        lens = [max(map(len, col)) for col in zip(*setup)]
        fmt = '\t'.join('{{:{}}}'.format(x) for x in lens)
        table = [fmt.format(*row) for row in setup]
        print('\n'.join(table))

    CURRENCY_PAIR = args.currency_pair
    SAVE = True
    PERIOD = args.period
    _dest = args.dest

    if SAVE and _dest is None:
        home_dir = str(Path.home())
        DESTINATION = f'{home_dir}/{CURRENCY_PAIR}_{PERIOD}.csv'

    else:
        DESTINATION = _dest

    START_DATE = args.start_date
    END_DATE = args.end_date

    client = CryptoData(
        currency_pair=CURRENCY_PAIR,
        destination=DESTINATION,
        period=PERIOD,
        start_date=START_DATE,
        end_date=END_DATE,
        logger=logger
    )

    client.run(save=SAVE)

