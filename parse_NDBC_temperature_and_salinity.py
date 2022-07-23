from datetime import datetime
import gzip
import os
import re

import numpy as np
import pandas as pd


separator = re.compile(r'[\s\t]+')

def ndbc_file_to_list(file):
    """
    Converts an NDBC ascii file (specifically for temperature or salinity) to a header list (column names) and data list.
    Date and time data is converted to datetime objects and numeric strings converted to floats.
    
    :param file: file path
    :return: header [list] and data[list of lists] 
    """
    data = []
    header = []
    haveHeader = False
    if file.endswith('.gz'):
        generator = gzip_generator(file)
    else:
        generator = text_generator(file)
    for line in generator:
        line = line.lstrip()
        if line.startswith('1') or line.startswith('2'):
            line = re.split(separator, line)
            temp = [parse_ndbc_timestamp(line[:2])]
            for entry in line[2:]:
                if len(entry) > 0:
                    try:
                        entry = float(entry)
                    except ValueError:
                        pass
                    temp.append(entry)
            data.append(temp)
        elif not haveHeader and line.startswith('YYYY'):
            line = re.split(separator, line)
            header = ['datetime'] + [entry.lower() for entry in line[2:] if len(entry) > 0]
            haveHeader = True
    if data and header:
        assert len(header) == len(data[0])
    return header, data


def gzip_generator(file):
    """
    read a gzip file and yield each line.
    
    :param file: (str or file path)
    """
    
    f = gzip.open(file, mode='rt')
    for line in f:
        yield line


def text_generator(file):
    """
    read file and yield each line.
    
    :param file: (str or file path)
    """
    
    f = open(file)
    for line in f:
        yield line


def parse_ndbc_timestamp(timestamp: list):
    """
    Converts an ndbc timesamp of YYYYMMDD HHMMSS to a datetime object
    
    :param timestamp: (array-like) two entry timestamp string consisting of year-month-day encoded as YYYYMMDD
                      and hour-minute-second encoded as HHMMSS
    :return: datetime object of the timestamp strings
    """
    
    # check that we have the correct string lengths for correct parsing
    assert len(timestamp[0]) == 8
    assert len(timestamp[1]) == 6 or len(timestamp[1]) == 4
    
    return datetime.strptime(' '.join(timestamp), '%Y%m%d %H%M%S') if len(timestamp[1]) == 6 else datetime.strptime(' '.join(timestamp), '%Y%m%d %H%M')


def ndbc_file_to_dataframe(file):
    """
    Converts an NDBC ascii file (specifically for temperature or salinity) to a pandas dataframe.
    Date and time data is converted to datetime objects and numeric strings converted to floats.
    
    :param file: file path
    :return: pandas dataframe  
    """
    
    header, data = ndbc_file_to_list(file)
    df = pd.DataFrame(data, columns=header)
    df.drop_duplicates(subset='datetime', keep='first', inplace=True)
    df.index = df['datetime']
    return df
    
    
def metadata_from_ndbc_filename(file):
    """
    Extract parameter name and station name from NDBC ascii file name.
    
    :param file: file path
    :return: paramter name (str) and station name (str)
    """
    
    f = os.path.split(file)[1]
    f = f.split('_')[0]
    parameter_name = f[:3].lower()
    station_name = f[3:]
    return parameter_name, station_name
    