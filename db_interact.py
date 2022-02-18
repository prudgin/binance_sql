import sys
import logging
import mysql.connector as mysql_connector

import exceptions
import helpers

"""
This module contains a ConnectionDB class, that holds multiple methods, used to interact with a database.
"""

logger = logging.getLogger(__name__)


class ConnectionDB:
    def __init__(self, host: str, user: str, password: str, database: str = None):
        """
        host, user, password, database are credentials, that should be stored elsewhere and passed as strings.
        If database is not created yet, you should run create_database method
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.table = None
        # self.conn is a MySQLConnection object, the connection to the database,
        # it will be set up later on, using connect method, like this: self.conn = mysql_connector.connect
        self.conn = None
        # self.conn_cursor is a MySQLCursor object, uses of MySQLConnection object to interact with your MySQL server.
        # Can exequte SQL queries
        self.conn_cursor = None
        self.connected = False
        # SQL table structure that will be created
        self.data_structure = [
            ['id', 'INT', 'AUTO_INCREMENT PRIMARY KEY'],
            ['open_time', 'BIGINT', 'NOT NULL UNIQUE'],
            ['open', 'DOUBLE(15,8)', 'NOT NULL'],
            ['high', 'DOUBLE(15,8)', 'NOT NULL'],
            ['low', 'DOUBLE(15,8)', 'NOT NULL'],
            ['close', 'DOUBLE(15,8)', 'NOT NULL'],
            ['volume', 'DOUBLE(25,8)', 'NOT NULL'],
            ['close_time', 'BIGINT', 'NOT NULL UNIQUE'],
            ['quote_vol', 'DOUBLE(25,8)', 'NOT NULL'],
            ['num_trades', 'BIGINT', 'NOT NULL'],
            ['buy_base_vol', 'DOUBLE(25,8)', 'NOT NULL'],
            ['buy_quote_vol', 'DOUBLE(25,8)', 'NOT NULL'],
            ['ignored', 'DOUBLE(15,8)', ''],
            ['time_loaded', 'BIGINT']
        ]

    def __str__(self):
        return f'This is an instance of ConnectionDB class,' \
               f'database = {self.database}'

    def create_database(self, db_name: str):
        try:
            with connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
            ) as connection:
                create_db_query = f'CREATE DATABASE {db_name}'
                with connection.cursor() as cursor:
                    cursor.execute(create_db_query)
        except mysql_connector.Error as err:
            err_message = 'error while creating a database'
            logger.error(f'{err_message}; {err}')
            raise exceptions.SQLError(err, err_message)

    def connect(self):
        try:
            self.conn = mysql_connector.connect(host=self.host,
                                                user=self.user,
                                                password=self.password,
                                                database=self.database)
        except mysql_connector.Error as err:
            err_message = 'error while connecting to a database'
            logger.error(f'{err_message} {self.database}; {err}')
            return None
        else:
            self.conn_cursor = self.conn.cursor()
            logger.debug(f'connected to database {self.database}')
            self.connected = True
            return True

    def close_connection(self):
        if self.connected:
            try:
                self.conn_cursor.close()
                self.conn.close()
                self.connected = False
                logger.debug(f'closed connection to database {self.database}')
            except mysql_connector.Error as err:
                err_message = 'error while closing connection to a database'
                logger.error(f'{err_message} {self.database}; {err}')
                raise exceptions.SQLError(err, err_message)

    def execute_query(self, query: str, commit=False, fetch_one=False, fetch_all=False):
        """
        :param query: the query to be executed
        :param commit: in case of writing or deleting, we need to commit to the database
        :param fetch_one: get one entry from cursor
        :param fetch_all: get everything there is from cursor
        :return: if fetch=true, returns database entries, if commit successfull returns True, else None
        """
        # At least one parameter should be True, otherwise executing such a query makes no sense.
        if not any([commit, fetch_one, fetch_all]):
            err_message = 'execute_query: please set one of commit, fetch_one or fetch_all arguments to True'
            logger.error(f'{err_message}; {err}')
            raise exceptions.SQLError(err, err_message)

        result = None
        if not self.connected:
            logger.error(f'cannot execute query: {query}, not connected to a database, run .connect() first')
        else:
            try:
                self.conn_cursor.execute(query)
                if commit:
                    self.conn.commit()
                    result = True
                if fetch_all:
                    result = self.conn_cursor.fetchall()
                if fetch_one:
                    result = self.conn_cursor.fetchone()
                    self.conn_cursor.reset()
            except mysql_connector.Error as err:
                logger.error(f'could not execute query {query}, {err}')
        return result

    # check if table present in the database
    def table_in_db(self, table_name: str):
        show_query = f'SHOW TABLES LIKE \'{table_name}\''
        return self.execute_query(show_query, fetch_all=True)

    def table_create(self, table_name: str):
        #  check if table present
        if not self.table_in_db(table_name):
            logger.debug(f'not found table {table_name}, creating one')

            table_struct_string = ',\n'.join([' '.join(i) for i in self.data_structure])
            create_new_table_query = f"""
                                    CREATE TABLE {table_name} (
                                    {table_struct_string}
                                    )
                                    """
            return self.execute_query(create_new_table_query, commit=True)
        return True #  table already in database



    def table_delete(self, table_name: str):
        if self.table_in_db(table_name):
            delete_table_query = f'DROP TABLE {table_name}'
            if self.execute_query(delete_table_query, commit=True):
                logger.debug(f'deleted table {table_name}')
        else:
            logger.error(f'could not delete table {table_name}, no such table')

    def get_latest_id(self, table_name):
        # return the id of entry that was added last
        last_id = 0
        query = f"""
                    SELECT id FROM {table_name}
                    ORDER BY id DESC
                    LIMIT 1
                """
        fetch = self.execute_query(query, fetch_one=True)
        if fetch:
            last_id = fetch[0]
        return last_id

    def get_start_end_later_than(self, table_name, later_than, only_count=False):
        # get the timestamp of the first and the last entry in existing table added after "later_than"
        # also count entries
        # used only for printing results
        first_entry, last_entry, count = None, None, 0
        count_query = f'SELECT COUNT(id) FROM {table_name} WHERE id > {later_than}'
        count = self.execute_query(count_query, fetch_one=True)
        if count:
            count = count[0]
        else:
            return [first_entry, last_entry, count]
        if only_count:
            return [first_entry, last_entry, count]

        if count < 1:  # if table has no entries later than specified time
            logger.debug(f'tried to get first and last entry from table that'
                         f' has no entries later than specified time {table_name}')
            return [first_entry, last_entry, count]
        else:
            first_query = f"""SELECT open_time from {table_name}
                              WHERE id > {later_than}
                              ORDER BY open_time LIMIT 1"""
            first_entry = self.execute_query(first_query, fetch_one=True)
            last_query = f"""SELECT open_time from {table_name}
                               WHERE id > {later_than}
                               ORDER BY open_time DESC LIMIT 1"""
            last_entry = self.execute_query(last_query, fetch_one=True)
            if first_entry and last_entry:
                first_entry = first_entry[0]
                last_entry = last_entry[0]
        return [first_entry, last_entry, count]

    def write(self, data, table_name):
        if not self.connected:
            logger.error(f'cannot execute query: {query}, not connected to a database, run .connect() first')
            return None
        else:
            headers_list = [i[0] for i in self.data_structure][1:]
            headers_string = ', '.join(headers_list)
            helper_string = ' ,'.join(['%s'] * len(headers_list))
            insert_query = f"""
                            INSERT IGNORE INTO {table_name}
                            ({headers_string})
                            VALUES ( {helper_string})
                           """
            try:
                self.conn_cursor.executemany(insert_query, data)
                self.conn.commit()
            except mysql_connector.Error as err:
                err_message = 'error writing data into table'
                logger.error(f'{err_message} {table_name}, {err}')
                return None

    def read(self, table_name, start_ts, end_ts, time_col_name='open_time', reversed_order=True):
        """returns candles from database in a form of
        (open_time, open, high, low, close, volume, close_time,
         quote_vol, num_trades, buy_base_vol, buy_quote_vol)"""
        if reversed_order:
            order = 'DESC'
        else:
            order = 'ASC'

        read_query = f"""
                         SELECT open_time, open, high, low, close, volume, close_time,
                                quote_vol, num_trades, buy_base_vol, buy_quote_vol FROM {table_name}
                         WHERE {time_col_name} BETWEEN {start_ts} AND {end_ts}
                         ORDER BY {time_col_name} {order};
                     """
        return self.execute_query(read_query, fetch_all=True)

    def check_number_candles_in_period(self, table_name, interval_ms, start, end, time_col_name='open_time'):
        start, end = helpers.round_timings(start, end, interval_ms)
        #  Count number of candles between start and end. If all candles are in place, return True.
        count_request = f"""
                            SELECT COUNT({time_col_name}) FROM {table_name}
                            WHERE {time_col_name} BETWEEN {start} AND {end}
                        """
        entry_count = self.execute_query(count_request, fetch_one=True)
        if entry_count and entry_count[0] == (end - start) / interval_ms + 1:
            return True
        return None

    def get_missing(self, table_name, interval_ms, start, end, time_col_name='open_time'):

        if self.check_number_candles_in_period(table_name, interval_ms, start, end, time_col_name='open_time'):
            return []  # All candles in place, have no gaps.

        missing = None
        last_entry = None
        # get the last entry in start-end range
        query = f"""
                    SELECT MAX({time_col_name}) FROM {table_name}
                    WHERE {time_col_name} BETWEEN {start} AND {end}
                """
        last_entry = self.execute_query(query, fetch_one=True)


        # if there are no records between start and end, we will recieve: (None,)
        if last_entry and last_entry[0] is None:
            return ([(start, end)])

        if last_entry is None:  # self.execute_query produced an error
            return None

        if last_entry:
            last_entry = last_entry[0]

        # find gaps
        mega_query = f"""
                        SELECT CAST((z.expected) AS UNSIGNED INTEGER),
                               IF(z.got-{interval_ms}>z.expected, CAST((z.got-{interval_ms}) AS UNSIGNED INTEGER),
                               CAST((z.expected) AS UNSIGNED INTEGER)) AS missing
                        FROM
                            (
                                SELECT 
                                            /* (3) increace @rownum by step from row to row */
                                    @rownum:=@rownum+{interval_ms} AS expected, 
                                            /* (4) @rownum should be equal to time_col_name unless we find a gap, 
                                            when we do, overwrite @rownum with gap end and continue*/ 
                                    IF(@rownum={time_col_name}, 0, @rownum:={time_col_name}) AS got
                                FROM 
                                            /* (1) set variable @rownum equal to start */
                                    (SELECT @rownum:={start}-{interval_ms}) AS a 

                                            /* (2) join a column populated with variable @rownum, 
                                            for now it doesn't change from row to row */
                                    JOIN
                                    (SELECT * FROM {table_name}
                                    WHERE {time_col_name} BETWEEN {start} AND {end}) AS b

                                ORDER BY {time_col_name}
                           ) AS z
                        WHERE z.got!=0;
                    """

        missing = self.execute_query(mega_query, fetch_all=True)

        if missing is None:
            return None

        #  in case we have empty space between last entry and an "end" we have requested
        if last_entry < end:
            missing.append((last_entry + interval_ms, end))

        return (missing)


if __name__ == '__main__':
    pass
