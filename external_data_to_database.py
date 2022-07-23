from datetime import timedelta
import os

import numpy as np
import pandas as pd
import psycopg2 as pg

import generate_sql_query as qSQL
import align_arrays


###########################################################################
# Name Mappings
parameter_mapping = {
                     'sst': 'sst',
                     'sss': 'sal',
                     'sal': 'sal'
                    }
inverse_parameter_mapping = {
                             'sal': 'sss',
                             'sst': 'sst'
                            }
############################################################################


class DatabaseUploader:
    def __init__(self, host="ourdb", database="database", user="user", password="updatedatabase", testing=False):
        """
        Class to upload external data (typically sst and sss) to database.
        
        :param host: (str) host name to connect to the database.
        :param database: (str) database name.
        :param user: (str) user credentials.
        :param password: (str) password credentials.
        :param testing: (bool) if in testing mode, there will be no updates to the database.
        
        :return:
        """
        self._database_credentials = {
                                      'host': host,
                                      'database': database,
                                      'user': user,
                                      'password': password
                                     }
        self.conn = pg.connect(**self._database_credentials)
        self.cur = self.conn.cursor()
        self.testing = testing
        
    def _get_date_range(self, data):
        """
        Returns start and end datetime range based on provided parameter data.
        End datetime adjusted by 10 minutes so that all data is accounted for in database queries.
        
        :return: (adjusted class attribute) adds start date and end date to self.date_range (list of datetime objects)
        """
        
        start_date = data['datetime'][0]
        end_date = data['datetime'][data.shape[0] - 1] + np.timedelta64(10, 'm')
        return [start_date, end_date]
    
    def _get_engineering_table_names(self, station_name, date_range):
        """
        Builds relevant engineering tables based on site and either time frame (self.date_range) or deployment.
        Removes built table names if tables don't exist in database.
        
        :param deployment: (int or None) Deployment number of parameter data.
        
        :return: (adjusted class attribute) adds engineering table names to self.engineering_tables (list of str)
        """
        
        system_number_sql = qSQL.datasetinfo_query_from_datetime(
                                                                 station_name,
                                                                 date_range[0],
                                                                 date_range[1]
                                                                )
        self.cur.execute(system_number_sql)
        query_results = self.cur.fetchall()
        engineering_tables = []
        for system_num, start_date in query_results:
            engineering_tables.append(qSQL.generate_engineering_table_name(
                                                                           station_name,
                                                                           system_num,
                                                                           start_date
                                                                          )
                                          )
        # Remove any tables that don't exist in database
        self._test_for_eng_table_existence(engineering_tables)
        return engineering_tables
    
    
    def _test_for_eng_table_existence(self, engineering_tables):
        """
        Removes non-existant engineering tables from engineering table list.
        Due to previous database issues, some engineering tables may not exist.
        """
        
        existence_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = (%s));"
        for eng_table in engineering_tables:
            self.cur.execute(existence_query, (eng_table,))
            engExists = self.cur.fetchall()[0][0]
            if not engExists:
                engineering_tables.remove(eng_table)
    
    
    def update_table(self, data, station_name, parameters, test_copy_upload=False):
        """
        Updates engineering tables found during initial parameter data processing.
        
        :param data: (pandas dataframe) dataframe with relevant data. Must have 'datetime' column and parameter column(s) which exist parameter_mapping as keys.
        :param parameters: (list of str) list of parameters to upload to database. Must match column names in data.
        
        :return: (dict) a set of metadata to check that the station, parameters uploaded, and tables changed are consistent with expectations.
        """
        
        date_range = self._get_date_range(data)
        engineering_tables = self._get_engineering_table_names(station_name, date_range)
        testing_queries = {'hdrtime': [], 'update': []}
        
        for eng_table in engineering_tables:
            for p in parameters:
                # remove bad parameter data
                sub_data = data[(data[p] > 0)]
                
                # get hdrtimes from engineering table
                hdrtimes, aligned_dates = self._get_hdrtimes_and_aligned_times(eng_table, date_range, sub_data)
                
                # prepare sql update commands
                hdrtimes_to_update = np.array([htime for htime, atime in zip(hdrtimes, aligned_dates) if atime])
                
                
                param_data_to_update = np.array([sub_data[p][dtime] for dtime in aligned_dates if dtime]).astype(float)
                update_sql = qSQL.generate_update_sql(eng_table, parameter_mapping[p])
                
                # update table
                qSQL.run_update_sql(self.cur, update_sql, hdrtimes_to_update, param_data_to_update)
                self.conn.commit()
        
        metadata = {
                    'Station': station_name,
                    'Parameter': [parameter_mapping[p] for p in parameters],
                    'Date Range': date_range,
                    'Engineering Tables': engineering_tables
                   }

        return metadata
    
    def _preset_parameter_column(self, eng_table, parameter_type, date_range):
        """
        Set the parameter column to -99. Older tables have 0 as missing data rather than -99.
        Should not need this after initial run.
        
        :param eng_table: (str) database table name
        """
        
        reset_sql = qSQL.generate_param_reset_sql(
                                                  eng_table,
                                                  parameter_type,
                                                  date_range[0],
                                                  date_range[1]
                                                 )
        self.cur.execute(reset_sql)
    
    def _get_hdrtimes_and_aligned_times(self, eng_table, date_range, data):
        """
        Queries for the hdrtimes of the eng_table and aligns these times with parameter data.
        
        :param eng_table: (str) database table name
        
        :return: (two numpy arrays) hdrtimes from database and aligned datetimes.
        """
        
        hdrtime_query = qSQL.generate_hdrtime_query(
                                                    eng_table,
                                                    date_range[0],
                                                    date_range[1]
                                                    )
        self.cur.execute(hdrtime_query)
        hdrtimes = [entry[0] for entry in self.cur.fetchall()]
        hdrtimes = pd.Series(hdrtimes, name='hdrtimes')
        
        # align hdrtimes with sst/sss datetimes
        adjusted_hdrtimes = hdrtimes + np.timedelta64(17, 'm')
        aligned_dates = align_arrays.align_arrays(data['datetime'].to_numpy(), adjusted_hdrtimes.to_numpy(), np.timedelta64(20, 'm'))
        return hdrtimes, aligned_dates
    
    def close_db_connection(self):
        self.cur.close()
    
    def reestablish_db_connection(self):
        self.cur.close()
        self.conn = pg.connect(**self._database_credentials)
        self.cur = conn.cursor()
