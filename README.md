# binance_sql
This module downloads historical data from binance via API, writes it to a MySQL database, then returns data.
The downloading occurs concurrently, using asyncio.  
To get the data one should run get_candles method, specifying start and end of period of interest.  
The central idea is to first check if requested data is in the database. But data in DB can be fragmented, e.g.
we can have data from 1 September to 5 Sep, then from 10 Sep to 15 Sep and so on.  
In order to deal with gaps, we search for them with a tricky SQL request. Then, we iterate over gaps, download and write data to a database.  
After all gaps have been filled, data is fetched from the database and returned to the user.  
## Should you use this?
First of all - this is my first published project, and I am no expert at programming.  
The reason for publishing this is to get feedback and to improve programming skills.  
Nevertheless, this module has some advantages:  
* It is easy to use by any trading algorythm. Just type get_candles(start, end), and voila - you have your backtesting data.  
* You don't need to download data every time from the exchange, once downloaded, it is stored in the database.
* Much better than csv files downloaded from Binance. Updating those csv with new data requires downloading them once again.
* Data provided by this module contains number of trades and taker buy volume.  

This module is quite raw and overcomplicated, but if someone finds it useful, I will definetly make it better.
## Requirements
You will need [MySql](https://dev.mysql.com/doc/refman/5.7/en/installing.html)  
And [mysql-connector-python](https://github.com/mysql/mysql-connector-python)  
Here is [the link](https://realpython.com/python-mysql/#installing-mysql-server-and-mysql-connectorpython) to a realpython.com article, that will walk you through the installation and setup.  
Also, a [nice article](https://dev.mysql.com/doc/mysql-getting-started/en/) from mysql.com about getting started with MySQL.  
Basically, all you need is to install MySQL server and set up host, user and password. Database creation can be done via python script, as shown in example.py  
Tables are created automatically, for example for a BTC-USDT pair on 1 hour interval a table named 'BTCUSDT1hHist' will be created. Hist is for historical.  
## Installation  
Just download module folder and import into your python script.  
I think it is not worth uploading to PyPI yet.
## Usage
```python
import binance_sql.historical_data as hst
import binance_sql.helpers as hlp

conn_creds = {
        'host': 'localhost', #  replace with your host name
        'user': 'user', # this too
        'password': 'password', #  and this
        'database': 'DB_name'
    }

candle_getter = hst.data_manager('BTCUSDT', '1d')
candle_getter.set_database_credentials(**conn_creds)

start = hlp.date_to_milliseconds('25 Nov 2018')
end = hlp.date_to_milliseconds('29 Nov 2018')

fetch = candle_getter.get_candles(start_ts=start, end_ts=end, delete_existing_table=False)
```  
get_candles method will return a list of tuples, where each tuple represents a candle.  
There is an example.py file also.
