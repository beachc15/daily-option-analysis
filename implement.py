import pandas as pd
import bson.json_util as bson
import json
import datetime
# import yfinance as yf
from blackScholesPricing import euro_vanilla

'''things which need to be gotten:

S, K, T, r, sigma, option_type

What I am thinking is that we create new datatables with the index as the contract name and then the rest of the features
are all the inputs for the BSM... aka S, K, T, r, sigma and the option_type'''


# TODO fix the fact that we count weekends in my time_delta calculation. use the datetime weekday fucntion to fix this
# TODO fix the fact that we are currently looking up prices only on evaluation date but we are referencing time as a
#  function of the last trade date which could be a previous date.

def main():
    def get_price_list(dt):
        # work left TODO... pass in time side-note only one price needs to be associated with each DataFrame,
        #  should we just keep the two separate or add the same value to every line of the DataFrame?
        with open('all_prices.json', 'r') as my_file:
            reader = my_file.read()
            data = bson.loads(reader)
        for z in data:
            # update with variable
            if z['date'] == dt:
                working_data = z['data']
            else:
                pass
        json_parsed_data = pd.read_json(working_data)
        # we need to change the column data from a tuple (although technically its a string) and convert it so we just
        # get the adjusted close

        # current format: '('Adj close', 'IVV')'
        # wanted format: 'IVV'
        # methodology: split by the apostrophes
        new_columns = []
        reindex_cols = []
        columns = json_parsed_data.columns
        # TODO this is stupid and needs to be replaced. Why would I filter out the Adj Close when I just put it back in.
        #  How can I get this all done more quickly?
        for column in columns:
            work = column.split('\'')
            if work[1] == 'Adj Close':
                new_columns.append(f'(\'Adj Close\', \'{work[3]}\')')
                reindex_cols.append(work[3])

        new_df = pd.DataFrame(json_parsed_data).reindex(new_columns, axis=1)
        # TODO right now we have the tickers parsed for the new columns of the dataframe and we will then correct the
        #  dataframe just to keep the adjsuted closing prices and use that to compare with timing on the options chain

        return new_df
        # TODO create an indexer to pull these prices by date

    def date_str_to_obj(string):
        """takes a string in the format \"%Y-%m-%d\" and returns a datetime object for that string"""
        temp = list(map(int, string.split('-')))
        out = datetime.datetime(year=temp[0], month=temp[1], day=temp[2])
        return out

    # import files
    with open('data.json', 'r') as f:
        reader = f.read()
        data = bson.loads(reader)

    # declare high level variables

    # Who doesn't need to know how many seconds are in a day?
    total_seconds_in_day = 60 * 60 * 24
    # TODO get eval_date_str based on the mongodb lookup maybe?
    # Date in which the data was recorded
    eval_date_str = '2020-06-26'
    options = ['call', 'put']
    S = 150
    all = []
    # The daily price movement DataFrame of each of the 10 ETFs which are followed
    # TODO define the price list at the highest level
    price_list = get_price_list(eval_date_str)
    for n in range(len(data)):
        time = data[n]['time']
        temp_data = data[n]['data']
        for ticker in data[n]['data'].keys():
            # Iterate by each ticker that was recorded
            temp_data_by_ticker = temp_data[ticker]
            for expiration_date_str in data[n]['data'][ticker].keys():
                # Iterate by the different date expiration's
                expiration_date_obj = date_str_to_obj(expiration_date_str)
                temp_data_by_ticker_and_date = temp_data_by_ticker[expiration_date_str]
                for option in options:
                    # Iterate between 'call' and 'put' option type. This is iteration is useful both for looking through
                    # the json file as well as for the option pricing function.
                    temp_data_for_option_type = pd.read_json(temp_data_by_ticker_and_date[option])
                    for contract in range(len(temp_data_for_option_type.index)):
                        # Iterate by the individual contracts inside of each option type
                        sigma = temp_data_for_option_type['impliedVolatility']
                        K = float('{}.{}'.format(temp_data_for_option_type['contractSymbol'][contract][-6:-3],
                                                 temp_data_for_option_type['contractSymbol'][contract][-2:]))
                        # TODO double check accuracy of K measurement

                        # get T from last trade date to the expiration date. in the future it would be interesting to
                        # compare the price from the last trade date vs the current time but that poses an issue for
                        # my code as it currently collects for 30 minutes before markets open

                        # t
                        time_diff = (expiration_date_obj - datetime.datetime.fromtimestamp(
                            temp_data_for_option_type['lastTradeDate'][contract] / 1000))
                        T = time_diff.days + (time_diff.seconds / total_seconds_in_day)

                        # our datetime expiration object has already been created above so now we need to convert the
                        # last trade date to a datetime object then just simply subtract the two figure out date by
                        # UNIX timestamp

                        # ~~ r ~~ # Lets get r from the likely rpime rate offered to big borrowers like hedge funds.
                        # The idea behind this is that the risk-free rate which we use to calculate r should be the
                        # average of the rates paid by investor by volume. and before we know more about prime debt
                        # markets, lets just go with treasury ten-years + 5
                        r = 0.002
                        sigma = temp_data_for_option_type['impliedVolatility']

                        # S
                        # get stock price at time that the stock was last traded
                        # print(temp_data_for_option_type['lastTradeDate'])

                        # TODO to lookup by timestamp we will need to find a way to round to the nearest 5 min
                        #  increment. StackOverflow this question
                        time_stamps_unix = temp_data_for_option_type['lastTradeDate']
                        time_stamps_objs = [(datetime.datetime.fromtimestamp(x / 1000)) for x in time_stamps_unix]
                        # time_stamps_objs = [x for x in time_stamps_objs]
                        temp_price_series = price_list[f'(\'Adj Close\', \'{ticker}\')']

                        # S = df['Price']
                        x = euro_vanilla(S, K, T, r, sigma, option_type='call')
                        all.append(x)
    return all


if __name__ == '__main__':
    start = datetime.datetime.now()
    y = main()
    end = datetime.datetime.now()
