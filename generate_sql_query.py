import psycopg2 as pg
from psycopg2 import extras


def datasetinfo_query_from_deployment(station_name, deployment_num):
    """
    Generate SQL query of the datasetinfo table, getting the system number of a station-deployment pair.
    
    :param station_name: (str) name of station
    :param deployment_num: (int) deployment number
    
    :return: (str) SQL query
    """
    
    assert type(deployment_num) == int
    
    return f"SELECT systemnum, startdate FROM datasetinfo WHERE LOWER(location) = LOWER('{station_name}') AND deployment = '{deployment_num}';"


def datasetinfo_query_from_datetime(station_name, start_date, end_date):
    """
    Generate SQL query of the datasetinfo table, getting the system number of a station-time series range triplet.
    
    :param station_name: (str) name of station.
    :param start_date: (datetime object) beginning datetime of time series.
    :param end_date: (datetime object) end datetime of time series.
    
    :return: (str) SQL query
    """
    
    return f"SELECT systemnum, startdate FROM datasetinfo WHERE LOWER(location) = LOWER('{station_name}') AND (mintime, maxtime) OVERLAPS (date '{start_date.strftime('%Y-%m-%d')}', date '{end_date.strftime('%Y-%m-%d')}') ORDER BY startdate;"



def generate_engineering_table_name(station_name, system_num, start_date):
    """
    Builds an engineering table name for query 
    :param station_name: (str) name of station
    :param system_num: (int) system number for deployment.
    :param start_date: (str) start date of deployment. Formatted YYYYMMDD
    """
    
    # TODO: Check that the code below is necessary...
    # reformat system_num to add preceeding zeros to total 4 characters
    system_num = '0' * (4 - len(str(system_num))) + str(system_num)
    
    return f"eng_{system_num}_{station_name.lower()}_{start_date}"


def generate_hdrtime_query(engineering_table_name, query_start_date, query_end_date):
    """
    Generate SQL query fom proper engineering table, getting the header datetimes.
    :param engineering_table_name: (str) name of engineering table in database
    :param query_start_date: (datetime object) earliest date for search range
    :param query_end_date: (datetime object) latest date for search range
    
    :return: (str) SQL query
    """
    
    table_query = f"SELECT hdrtime FROM {engineering_table_name}"
    dtime_query = f"WHERE hdrtime BETWEEN '{query_start_date.strftime('%Y-%m-%d')}' and '{query_end_date.strftime('%Y-%m-%d %H:%M')}'"
    
    return table_query + " " + dtime_query + " ORDER BY hdrtime;"


def generate_param_reset_sql(engineering_table_name, column_name, query_start_date, query_end_date):
    """
    Create an update string for SQL engineering table for temperature and salinity which sets all of the temperature/salinity to -99.
    
    :param engineering_table_name: (str) name of engineering table in database
    :param column_name: (str) column in engineering table (either 'sst' or 'sal')
    :param query_start_date: (datetime object) earliest date for search range
    :param query_end_date: (datetime object) latest date for search range
    
    :return: SQL update string
    """
    
    assert column_name == 'sst' or column_name == 'sal'
    
    return f"UPDATE {engineering_table_name} SET {column_name} = -99, {column_name}flag = 11 WHERE hdrtime BETWEEN '{query_start_date.strftime('%Y-%m-%d')}' and '{query_end_date.strftime('%Y-%m-%d %H:%M')}';"


def generate_update_sql(engineering_table_name, column_name):
    """
    Create an update string for SQL engineering table for temperature and salinity.
    
    :param engineering_table_name: (str) name of engineering table in database
    :param column_name: (str) column in engineering table (either 'sst' or 'sal')
    
    :return: SQL update string
    """
    
    assert column_name == 'sst' or column_name == 'sal'
    
    return f"UPDATE {engineering_table_name} SET {column_name} = %s, {column_name}flag = 0 WHERE hdrtime = %s;"


def run_update_sql(cursor, sql_update_str, dtimes, param_data):
    """
    Update database based on SQL string and data provided.
    
    :param cursor: (psychopg2 cursor) cursor connecting to database
    :param sql_update_str: (str) SQL command to update database table.
    :param dtimes: (array-like) datetimes to match to database table.
    :param param_data: parameter data updating table. Must be aligned to dtimes.
    
    :return:
    """
    
    for dtime, value in zip(dtimes, param_data):
        cursor.execute(sql_update_str, (value, dtime.strftime('%Y-%m-%d %H:%M:%S')))

