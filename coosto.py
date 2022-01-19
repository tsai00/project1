import pandas as pd
import requests as r
import json
import os
import logging
from datetime import datetime as dt


class CoostoAPI:
    """
    API for social media data provider (Coosto)
    """

    # Configuring file for logs
    logging.basicConfig(filename='app.log',
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        level=logging.INFO,
                        datefmt='%d.%m.%Y %H:%M:%S')

    # Link to the main API
    api_url = 'https://in.coosto.com/api/1/'

    # Change table names (values in dict) only here. Table names further in a script would change automatically
    __tables = {'table_projects': 'projects',
                'table_topics': 'trending_topics',
                'table_sources': 'sources',
                'table_sentiment': 'sentiment',
                'table_authors': 'authors'}

    # Defining possible options for output formats and SQL modes
    __possible_outputs = ['csv', 'sql', 'csv-sql']
    __possible_modes = ['append', 'replace']

    # All messages (info, logs, etc.) would start with
    __info_message_start = '[COOSTO]'

    def __init__(self, path, username, password, db_conn):
        """
        This function initializes the Coosto API connection.
        :param path: Path to folder where the results should be.
        :param username: Username for API
        :param password: Password for API
        :param db_conn: Database connection instance where the results should be exported
        :return: None
        """

        self.path = path
        self.username = username
        self.password = password
        self.__db = db_conn

        # Setting credentials for API
        credentials = {'username': username, 'password': password}

        # Logging into API and getting session id
        try:
            self.login_api = r.get(self.api_url + 'users/login', params=credentials, stream=True)
            if self.login_api.status_code == 200:
                self.__session_id = json.loads(self.login_api.text)['data']['sessionid']
            else:
                print(f'{self.__info_message_start} Wrong credentials for API!')
                logging.exception(f'{self.__info_message_start} Wrong credentials for API!')

            print(f'{self.__info_message_start} Connection to the API was established!')
            logging.info(f'{self.__info_message_start} Connection to the API was established!')

        except Exception as e:
            print(f'{self.__info_message_start} Error while connecting to the API: {e}.')
            logging.exception(f'{self.__info_message_start} Error while connecting to the API: {e}.')

        # Path to results
        self.path_to_results = path + '/coosto_data/'
        if not os.path.exists(self.path_to_results):
            os.makedirs(self.path_to_results)

    def __export(self, df, table, output, mode):
        """
        This function takes a dataframe and then exports it to CSV file or/and MS SQL Server.
        :param df: DataFrame to export (what to export).
        :param table: Table in SQL database to put the result in.
        :param output: Where to export:
                        csv - only in CSV file
                        sql - only in MS SQL Server Database
                        csv-sql - both CSV and database
        :param mode: How to insert data? Only applicable if output contains sql
                        append - add data to the previous rows
                        replace - remove all previous data from table, then add new
        :return: None
        """
        try:
            if output == 'csv':
                df.to_csv(self.path_to_results + self.__tables[table] + '.csv')
            elif output == 'sql':
                df.to_sql(self.__tables[table], self.__db.engine, if_exists=mode, index=False)
            elif output == 'csv-sql':
                df.to_csv(self.path_to_results + self.__tables[table] + '.csv')
                df.to_sql(self.__tables[table], self.__db.engine, if_exists=mode, index=False)

        except FileNotFoundError:
            print(f'{self.__info_message_start} Wrong path to the folder or file: {self.path_to_results}.')
            logging.exception(f'{self.__info_message_start} Wrong path to the folder or file: {self.path_to_results}.')
            return
        except Exception as e:
            print(f'{self.__info_message_start} Error while exporting table <{self.__tables[table]}>: {e}.')
            logging.exception(f'{self.__info_message_start} Error while exporting table <{self.__tables[table]}>: {e}.')
            return

        print(f'{self.__info_message_start} Table <{self.__tables[table]}> was successfully exported to {output}!')
        logging.info(f'{self.__info_message_start} Table <{self.__tables[table]}> was successfully exported to {output}!')

    def __check_output_and_mode(self, output, mode, table):
        """
        This function checks if provided output format and/or SQL mode are in a list of available options.
        :param output: Provided output format. Should be "csv", "sql" or "csv-sql".
        :param mode: Provided SQL mode. Should be "append" or "replace".
        :param table: Table in SQL database to put the result in.
        :return: True or False depending on the result of check.
        """

        if output not in self.__possible_outputs or mode not in self.__possible_modes:
            print(
                f'{self.__info_message_start} Wrong output format or SQL mode for table <{self.__tables[table]}>!')
            logging.exception(
                f'{self.__info_message_start} Wrong output format or SQL mode for table <{self.__tables[table]}>!')
            return False
        else:
            return True

    def export_saved_queries(self, output='csv', mode='replace'):
        """
        This function exports projects (saved queries). Exported ID of query is used later.
        :param output: What format of output file is expected?
                            csv - result is only exported in CSV file (default value)
                            sql - result is only exported in SQL database
                            csv-sql - result is exported both in CSV file and SQL database
        :param mode: Only used if output format contains SQL. How to insert data in SQL database?
                            append - add data to the previous rows (default value)
                            replace - remove all previous data from table, then add new
        :return: None
        """

        table = 'table_projects'

        if self.__check_output_and_mode(output, mode, table):
            with self.login_api:
                try:
                    # Getting response
                    saved_queries_api = json.loads(
                        r.get(self.api_url + 'savedqueries/get_all/', params={'sessionid': self.__session_id}).text)

                    saved_queries = {x['name']: x['id'] for x in saved_queries_api['data']}

                    # Saving result to dataframe
                    projects_df = pd.DataFrame(list(saved_queries.items()), columns=['Name', 'ID'])

                    # Exporting the results
                    self.__export(projects_df, table, output, mode)

                except KeyError:
                    print(f'{self.__info_message_start} Error while loading <{self.__tables[table]}>: check API link or parameters!')
                    logging.exception(f'{self.__info_message_start} Error while loading <{self.__tables[table]}>: check API link or parameters!')
                except Exception as e:
                    print(f'{self.__info_message_start} Error for table <{self.__tables[table]}>: {e}.')
                    logging.exception(f'{self.__info_message_start} Error for table <{self.__tables[table]}>: {e}.')

    def export_trending_topics(self, output='csv', mode='replace'):
        """
        This function exports trending topics.
        :param output: What format of output file is expected?
                            csv - result is only exported in CSV file (default value)
                            sql - result is only exported in SQL database
                            csv-sql - result is exported both in CSV file and SQL database
        :param mode: Only used if output format contains SQL. How to insert data in SQL database?
                            append - add data to the previous rows (default value)
                            replace - remove all previous data from table, then add new
        :return: None
        """

        table = 'table_topics'

        if self.__check_output_and_mode(output, mode, table):
            with self.login_api:
                try:
                    # Getting response
                    trending_api = json.loads(
                        r.get(self.api_url + 'query/trending', params={'sessionid': self.__session_id, 'qid': 131755}).text)

                    topics = [x['topic'] for x in trending_api['data'][0]]
                    scores = [x['score'] for x in trending_api['data'][0]]
                    trending_final_list = zip(topics, scores)

                    # Saving result to DataFrame
                    trending_df = pd.DataFrame(trending_final_list, columns=['Topic', 'Score'])

                    # Exporting the results
                    self.__export(trending_df, table, output, mode)

                except KeyError:
                    print(f'{self.__info_message_start} Error while loading <{self.__tables[table]}>: check API link or parameters!')
                    logging.exception(f'{self.__info_message_start} Error while loading <{self.__tables[table]}>: check API link or parameters!')
                except Exception as e:
                    print(f'{self.__info_message_start} Error for table <{self.__tables[table]}>: {e}.')
                    logging.exception(f'{self.__info_message_start} Error for table <{self.__tables[table]}>: {e}.')

    def export_source_types(self, output='csv', mode='replace'):
        """
        This function exports source types (e.g. Twitter, blog, news etc.).
        :param output: What format of output file is expected?
                            csv - result is only exported in CSV file (default value)
                            sql - result is only exported in SQL database
                            csv-sql - result is exported both in CSV file and SQL database
        :param mode: Only used if output format contains SQL. How to insert data in SQL database?
                            append - add data to the previous rows (default value)
                            replace - remove all previous data from table, then add new
        :return: None
        """

        table = 'table_sources'

        if self.__check_output_and_mode(output, mode, table):
            with self.login_api:
                try:
                    # Getting response
                    sources_api = json.loads(
                        r.get(self.api_url + 'query/sourcetypes', params={'sessionid': self.__session_id, 'qid': 131755}).text)

                    names = [x['sourcetype'] for x in sources_api['data'][0]]
                    freq = [x['freq'] for x in sources_api['data'][0]]
                    sent = [x['sent'] for x in sources_api['data'][0]]
                    sentp = [x['sentp'] for x in sources_api['data'][0]]
                    sentn = [x['sentn'] for x in sources_api['data'][0]]
                    sent0 = [x['sent0'] for x in sources_api['data'][0]]
                    source_types_final_list = zip(names, freq, sent, sentp, sentn, sent0)

                    # Saving result to dataframe
                    source_types_df = pd.DataFrame(source_types_final_list,
                                                   columns=['Name', 'Frequency', 'Overall_sentiment', 'Positive_sentiment',
                                                            'Negative_sentiment', 'Neutral_sentiment'])

                    # Exporting the results
                    self.__export(source_types_df, table, output, mode)

                except KeyError:
                    print(f'{self.__info_message_start} Error while loading <{self.__tables[table]}>: check API link or parameters!')
                    logging.exception(f'{self.__info_message_start} Error while loading <{self.__tables[table]}>: check API link or parameters!')
                except Exception as e:
                    print(f'{self.__info_message_start} Error for table <{self.__tables[table]}>: {e}')
                    logging.exception(f'{self.__info_message_start} Error for table <{self.__tables[table]}>: {e}')

    def export_sentiment_per_day(self, output='csv', mode='append'):
        """
        This function exports sentiment analysis per day.
        :param output: What format of output file is expected?
                            csv - result is only exported in CSV file (default value)
                            sql - result is only exported in SQL database
                            csv-sql - result is exported both in CSV file and SQL database
        :param mode: Only used if output format contains SQL. How to insert data in SQL database?
                            append - add data to the previous rows (default value)
                            replace - remove all previous data from table, then add new
        :return: None
        """

        table = 'table_sentiment'

        if self.__check_output_and_mode(output, mode, table):
            with self.login_api:
                try:
                    # Getting response
                    sentiment_api = json.loads(
                        r.get(self.api_url + 'query/days', params={'sessionid': self.__session_id, 'qid': 131755}).text)

                    days = [dt.utcfromtimestamp(ts['time']).strftime('%Y-%m-%d') for ts in sentiment_api['data'][0]]
                    freq = [x['freq'] for x in sentiment_api['data'][0]]
                    senttot = [x['sent'] for x in sentiment_api['data'][0]]
                    sentp = [x['sentp'] for x in sentiment_api['data'][0]]
                    sentn = [x['sentn'] for x in sentiment_api['data'][0]]
                    sent0 = [x['sent0'] for x in sentiment_api['data'][0]]
                    sent_final_list = zip(days, freq, senttot, sentp, sentn, sent0)

                    # Saving result to dataframe
                    sent_df = pd.DataFrame(sent_final_list,
                                           columns=['Date', 'Frequency', 'Overall_sentiment', 'Positive_sentiment',
                                                    'Negative_sentiment', 'Neutral_sentiment'])

                    # Exporting the results
                    self.__export(sent_df, table, output, mode)

                except KeyError:
                    print(f'{self.__info_message_start} Error while loading <{self.__tables[table]}>: check API link or parameters!')
                    logging.exception(f'{self.__info_message_start} Error while loading <{self.__tables[table]}>: check API link or parameters!')
                except Exception as e:
                    print(f'{self.__info_message_start} Error for table <{self.__tables[table]}>: {e}.')
                    logging.exception(f'{self.__info_message_start} Error for table <{self.__tables[table]}>: {e}.')

    def export_authors(self, output='csv', mode='replace'):
        """
        This function exports most popular authors.
        :param output: What format of output file is expected?
                            csv - result is only exported in CSV file (default value)
                            sql - result is only exported in SQL database
                            csv-sql - result is exported both in CSV file and SQL database
        :param mode: Only used if output format contains SQL. How to insert data in SQL database?
                            append - add data to the previous rows (default value)
                            replace - remove all previous data from table, then add new
        :return: None
        """

        table = 'table_authors'

        if self.__check_output_and_mode(output, mode, table):
            with self.login_api:
                try:
                    # Getting response
                    authors_api = json.loads(
                        r.get(self.api_url + 'query/authors', params={'sessionid': self.__session_id, 'qid': 131755}).text)

                    authors = [x['author'] for x in authors_api['data'][0]]
                    freq = [x['freq'] for x in authors_api['data'][0]]
                    sent = [x['sent'] for x in authors_api['data'][0]]
                    sentp = [x['sentp'] for x in authors_api['data'][0]]
                    sentn = [x['sentn'] for x in authors_api['data'][0]]
                    sent0 = [x['sent0'] for x in authors_api['data'][0]]
                    influence = [x['influence'] for x in authors_api['data'][0]]
                    gender = [x['gender'] for x in authors_api['data'][0]]
                    followers = [x['followers'] for x in authors_api['data'][0]]
                    reactions = [x['reactions'] for x in authors_api['data'][0]]
                    authors_final_list = zip(authors, freq, sent, sentp, sentn, sent0,
                                             influence, gender, followers, reactions)

                    # Saving result to dataframe
                    authors_df = pd.DataFrame(authors_final_list,
                                              columns=['Author', 'Freq', 'Sent', 'SentP', 'SentN', 'Sent0',
                                                       'Influence', 'Gender', 'Followers', 'Reactions'])

                    # Exporting the results
                    self.__export(authors_df, table, output, mode)

                except KeyError:
                    print(f'{self.__info_message_start} Error while loading <{self.__tables[table]}>: check API link or parameters!')
                    logging.exception(f'{self.__info_message_start} Error while loading <{self.__tables[table]}>: check API link or parameters!')
                except Exception as e:
                    print(f'{self.__info_message_start} Error for table <{self.__tables[table]}>: {e}.')
                    logging.exception(f'{self.__info_message_start} Error for table <{self.__tables[table]}>: {e}.')

    def export_all(self, output='csv', mode='replace'):
        """
        This functions exports all available data at once.
        :param output: What format of output file is expected?
                            csv - result is only exported in CSV file (default value)
                            sql - result is only exported in SQL database
                            csv-sql - result is exported both in CSV file and SQL database
        :param mode: Only used if output format contains SQL. How to insert data in SQL database?
                            append - add data to the previous rows (default value)
                            replace - remove all previous data from table, then add new
        :return: None
        """

        if output not in self.__possible_outputs or mode not in self.__possible_modes:
            print(f'{self.__info_message_start} Wrong output format or SQL mode!')
            logging.exception(f'{self.__info_message_start} Wrong output format or SQL mode!')

        else:
            self.export_saved_queries(output=output, mode=mode)
            self.export_trending_topics(output=output, mode=mode)
            self.export_source_types(output=output, mode=mode)
            self.export_sentiment_per_day(output=output, mode='append')
            self.export_authors(output=output, mode=mode)

    def clean_database(self, delete_tables=True):
        """
        This functions cleans the MS SQL Server database
        :param delete_tables: True if tables should be deleted as well as its' content
        :return: None
        """

        for table in self.__tables.values():
            if delete_tables:
                query = f"DROP TABLE {table}"
            else:
                query = f"DELETE FROM {table}"

            try:
                self.__db.cursor.execute(query)
                self.__db.conn.commit()

            except Exception as e:
                print(f'{self.__info_message_start} Error while deleting table <{table}>: {e}.')
                logging.exception(f'{self.__info_message_start} Error while deleting table <{table}>: {e}.')
                continue

            print(f'{self.__info_message_start} Table <{table}> was successfully deleted!')
            logging.info(f'{self.__info_message_start} Table <{table}> was successfully deleted!')


