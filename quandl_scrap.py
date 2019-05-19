############################################################################################################
#
#
# Quandl Scraping program
# Nuno de Sousa
# Simia AT
# May 2019
# ver_0.1
#
############################################################################################################


import quandl
import pandas as pd
from datetime import datetime
from datetime import timedelta
import MySQLdb as mdb
from sqlalchemy import create_engine

def download_data(ticker, code, num, start_date=None):
    """
    This function downloads the data from quandl.

    :param ticker: of the instrument
    :param code: quandl code
    :param num: number of the continuous contract
    :param start_date: starting date
    :return: pandas dataframe with data.
    """

    data = pd.DataFrame()

    for gen in range(1, num + 1):
        string = code + str(gen)
        data_temp = quandl.get(string, authtoken="", start_date=start_date)
        data_temp['Generic'] = ticker + str(gen)
        data_temp['Ticker'] = ticker
        data = pd.concat([data, data_temp])
    return data


def checkTableExists(dbcon, database=None, tablename=None):
    """
    This function checks if a table exists in a database.

    Example: checkTableExists(con, database= 'CME', tablename = 'CL')
    :param dbcon: Database communicator
    :param database: name of the database
    :param tablename: name of the table
    :return: booling variable
    """
    if ((tablename != None) & (database != None)):
        dbcur = dbcon.cursor()
        dbcur.execute("""Show tables from {};""".format(database))
        results = dbcur.fetchall()
        tables = []
        for i in range(len(results)):
            tables.append(results[i][0])

        return tablename in tables

    else:
        print('Missing parameters.')
        return None


def get_data(database, name, ticker, code, gen):
    """
    Function responsible to make the connection with the database.
    Also this function checks if the database already exists and decides if we are going to create the
    table or just append new information.

    :param database: name of the database where we are going to record the data
    :param name: name of the table
    :param ticker: ticker of the instrument
    :param code: quandl code to make the download.
    :param gen: Number of the generic
    :return: It doesn't return nothing
    """

    engine = create_engine("mysql://{}:{}@{}:{}/{}".format(credentials['db_user'], \
                                                           credentials['db_pass'], \
                                                           credentials['db_host'], \
                                                           credentials['host_port'],
                                                           database))
    con_engine = engine.connect()

    checkTable = checkTableExists(con, database=database, tablename=name)

    if (checkTable == False):
        print('Table not in the Database.')
        print('Downloading data...')
        data = download_data(ticker, code, gen, start_date=None)
        print('[Done]')

        print('Creating table...')
        data.to_sql(con=con_engine, name=name, if_exists='fail', chunksize=100)
        print('[Done]')

    else:
        print('Table in the Database.')
        print('Checking last record in the table...')
        dbcur = con.cursor()
        dbcur.execute("""select Date from {}.{} order by Date desc limit 1;""".format(database, name))
        last_date = dbcur.fetchall()[0][0]
        print(last_date)
        print('[Done]')

        print('Downloading data...')
        last_date = last_date + timedelta(days=1)
        start_date = "{}-{}-{}".format(last_date.year, last_date.month, last_date.day)
        data_to_append = download_data(ticker, code, 1, start_date=start_date)
        print('[Done]')

        print('Appending to table...')
        data_to_append.to_sql(con=con_engine, name=name, if_exists='append', chunksize=100)
        print('[Done]')
    con_engine.close()

if __name__ == "__main__":


    path = 'data_description/'

    credentials = {'db_host': '192.168.1.x', 'db_user': 'username', 'db_pass': 'yourpassword', 'db_name': 'CME',
                   'host_port': 3306}

    con = mdb.connect(host=credentials['db_host'], user=credentials['db_user'],
                      passwd=credentials['db_pass'], db=credentials['db_name'], port=credentials['host_port'])

    continuous_description = pd.read_csv(path + 'description.csv', header=None)
    continuous_description.columns = ['ticker', 'exchange', 'description', 'months', 'number', 'code']

    WTI_Crude_Oil = {'exchange': 'CME', 'ticker': 'CL'}
    Natural_Gas = {'exchange': 'CME', 'ticker': 'NG'}
    Heating_Oil = {'exchange': 'CME', 'ticker': 'HO'}
    Gasoline = {'exchange': 'CME', 'ticker': 'RB'}

    Energy = [WTI_Crude_Oil, Natural_Gas, Heating_Oil, Gasoline]

    list_to_download = []

    for E in Energy:

        code = continuous_description[(continuous_description['ticker'] == E['ticker']) & \
                                      (continuous_description['exchange'] == E['exchange'])]['code'].values[0]
        num = continuous_description[(continuous_description['ticker'] == E['ticker']) & \
                                     (continuous_description['exchange'] == E['exchange'])]['number'].values[0]
        list_to_download.append((code, num, E['ticker'], E['exchange']))

    for contract_class in list_to_download:
        print('Contract Class: ', contract_class)
        for gen_i in range(1, contract_class[1] + 1):
            print(contract_class[3], contract_class[2] + str(gen_i), contract_class[2], contract_class[0], gen_i)
            get_data(contract_class[3], contract_class[2] + str(gen_i), contract_class[2], contract_class[0], gen_i)
