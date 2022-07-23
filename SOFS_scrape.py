from datetime import datetime, timedelta
import time
import re
import numpy as np
import pandas as pd
import netCDF4
import requests
from bs4 import BeautifulSoup


base_filename_url = r"https://notarealurl/basepathtofilenames/Real-time/"
base_data_extraction_url = r"https://notarealurl/basepathtonetcdfs/Real-time/"


def get_full_filename_url(base_url):
    """
    Appends url extension to access names of SOFS files.
    
    :param base_url: (str or path) Base url path to SOFS NRT SBE data.
    
    :return: (str) url with extension to access file names
    """
    
    current_year = str(datetime.now().year)
    return base_url + current_year + r"_daily/catalog.html"
    

def get_full_data_url(base_url):
    """
    Appends url extension to access NetCDF links of SOFS NRT SBE data.
    
    :param base_url: (str or path) Base url path to SOFS NRT SBE data.
    
    :return: (str) url with extension to access SOFS NetCDF files.
    """
    
    current_year = str(datetime.now().year)
    return base_url + current_year + r"_daily/"


def get_SOFS_filenames_and_upload_dates(base_url=base_filename_url):
    """
    Get file names and upload dates from url based on get_full_filename_url.
    
    :param base_url: (str or path) Base url path to SOFS NRT SBE data.
    
    :return: (pandas dataframe) first column is file names and second column is upload date of associated files.
    """
    
    url = get_full_filename_url(base_url)
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    
    # get file names and upload dates from soup
    filenames = SOFS_filenames(soup)
    upload_dates = SOFS_upload_dates(soup)
    
    df = pd.DataFrame([[f, d] for f, d in zip(filenames, upload_dates)], columns=['filenames', 'upload_dates'])
    df['upload_dates'] = pd.to_datetime(df['upload_dates'])
    df.sort_values('upload_dates', inplace=True)
    df.reset_index(inplace=True, drop=True)
    return df


def SOFS_filenames(html_soup):
    """
    Isolate NetCDF file names from BeautifulSoup html output.
    
    :param html_soup: BeautifulSoup html output.
    
    :return: list of file names ending with .nc (NetCDFs).
    """
    
    output = []
    for a in html_soup.body.find_all('a'):
        txt = a.text
        if txt.endswith('.nc'):
            output.append(txt)
    return output


def SOFS_upload_dates(html_soup):
    """
    Isolate NetCDF file upload dates from BeautifulSoup html output.
    
    :param html_soup: BeautifulSoup html output.
    
    :return: list of file upload dates.
    """
    
    date_search = re.compile("[0-9]+-[0-9]+-[0-9]+\w[0-9]+:[0-9]+:[0-9]+\w")
    output = []
    for t in html_soup.body.find_all('tt'):
        d = re.search(date_search, t.text)
        if d:
            output.append(d.group(0))
    return output


def SOFS_realtime_data(file_df, start_date, end_date=None, base_url=base_data_extraction_url):
    """
    Uses file names from file_df to download NetCDF files and extract SST and SSS data. Data is returned in a pandas dataframe.
    
    :param file_df: (pandas dataframe) first column is file names to be downloaded. Second column is the upload dates of those files.
    :param base_url: (str) url from which data can be accessed using the appropriate additional extension.
    :param start_date: (str or datetime) 
    
    
    """
    
    if type(start_date) == str:
        start_date = pd.to_datetime(start_date)
    if end_date:
        if type(end_date) == str:
            end_date = pd.to_datetime(end_date)
    else:
        end_date = max(file_df['upload_dates'])
    unprocessed_files_df = file_df[(file_df['upload_dates'] > start_date) & (file_df['upload_dates'] <= end_date)]

    url = get_full_data_url(base_data_extraction_url)
    output_df = pd.DataFrame([], columns=['time', 'temp', 'temp_2', 'psal', 'psal_2'])
    for filename in unprocessed_files_df['filenames'].to_numpy():
        r = requests.get(url + filename, allow_redirects=True)
        ds = netCDF4.Dataset('temp', memory=r.content)
        output_df = pd.concat([output_df, SOFS_ncdf_to_dataframe(ds)], ignore_index=True)
    output_df.sort_values('time', inplace=True)
    output_df.reset_index(inplace=True, drop=True)
    
    # update last update date to the most recent SOFS uploaded file date.
    # update_last_upload_date(end_date.strftime('%Y-%m-%dT%H:%M:%SZ'))
    return output_df
    
    
def get_last_upload_date():
    """Load last upload date to database from file (same as update_last_upload_date).
    
    :return: (datetime)
    """
    
    with open('SOFS_last_upload_date.txt', 'r') as f:
        return pd.to_datetime(f.read())


def update_last_upload_date(dt):
    """update file (same file as get_last_upload_date) with most recent upload date to database.
    
    :param dt: (datetime) most recent upload date to database
    """
    
    with open('SOFS_last_upload_date.txt', 'w') as f:
        f.write(dt)


def SOFS_ncdf_to_dataframe(nc_dataset):
    """
    Pull temperature and salinity data from NetCDF to pandas dataframe.
    
    :param nc_dataset: (netCDF4 dataset) NetCDF file.
    
    :return: (pandas dataframe) column 1 is datetimes, column 2 is SST, column 3 is second SST, column 4 is SSS,
             column 5 is second SSS.
    """
    
    params = ['TEMP', 'TEMP_2', 'PSAL', 'PSAL_2']
    df = pd.DataFrame(nc_dataset['TIME'][:], columns=['time'])
    for p in params:
        df[p.lower()] = nc_dataset[p][:]
    
    # Supplement missing values in TEMP and PSAL with TEMP_2 and PSAL_2 (respectively)
    df['TEMP'].fillna(df['TEMP_2'], inplace=True)
    df['PSAL'].fillna(df['PSAL_2'], inplace=True)
    df.drop(['TEMP_2', 'PSAL_2'], axis=1, inplace=True)

    # time as days since 1950-01-01 00:00:00Z to datetime
    start_date = datetime.strptime("1950-01-01 00:00:00Z", "%Y-%m-%d %H:%M:%SZ")
    df['time'] = df['time'].apply(lambda x: start_date + timedelta(x))
    return df
