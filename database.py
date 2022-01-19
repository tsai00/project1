import pyodbc
import logging
from sqlalchemy import create_engine


class DatabaseConnection:
    """
    Connection to MS SQL Server (or other) database
    """

    # Configuring file for logs
    logging.basicConfig(filename='app.log',
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        level=logging.INFO,
                        datefmt='%d.%m.%Y %H:%M:%S')

    # Credentials for database
    __username_database = '*****'
    __password_database = '*****'
    __dsn = 'MYMSSQL'

    # All messages (info, logs, etc.) would start with
    __info_message_start = '[DATABASE]'

    def __init__(self):
        """
        This function initializes the database connection.
        """

        try:
            # Format of string:
            # {Database Type}+{Database Connector}://{login}:{password}@{host}:{port}/{Database}?driver={Driver with spaces replaced with +}

            # Initializing the database
            self.engine = create_engine(
                f'mssql+pyodbc://{self.__username_database}:{self.__password_database}@{self.__dsn}')
            self.conn = pyodbc.connect(
                f'DSN={self.__dsn};UID={self.__username_database};PWD={self.__password_database}')

            with self.conn:
                self.cursor = self.conn.cursor()

            print(f'{self.__info_message_start} Connection to the database was established!')
            logging.info(f'{self.__info_message_start} Connection to the database was established!')

        except pyodbc.ProgrammingError:
            print(f'{self.__info_message_start} Wrong credentials for database!')
            logging.exception()
        except pyodbc.InterfaceError:
            print(f'{self.__info_message_start} Wrong DNS or driver!')
            logging.exception(f'{self.__info_message_start} Wrong DNS or driver!')
        except pyodbc.OperationalError:
            print(f'{self.__info_message_start} Unable to connect to the database!')
            logging.exception(f'{self.__info_message_start} Unable to connect to the database!')
        except Exception as e:
            print(f'{self.__info_message_start} Error while connecting to the database: {e}.')
            logging.exception(f'{self.__info_message_start} Error while connecting to the database: {e}.')

    def close_connection(self):
        """
        This function closes the database connection.
        :return: None
        """
        try:
            self.cursor.close()
        except Exception as e:
            print(f'{self.__info_message_start} Error while closing the connection: {e}.')
            logging.exception(f'{self.__info_message_start} Error while closing the connection: {e}.')