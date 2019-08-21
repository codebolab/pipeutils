import os
import logging
import csv
import pandas as pd

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

DELIMITER = ';'
ESCAPECHAR = '\\'

def import_dataset(filepath_or_buffer, delimiter=DELIMITER,
                   quoting=csv.QUOTE_NONE, escapechar=ESCAPECHAR, **kwargs):

    """
    Args
    ----
        filepath_or_buffer: str, path object or file-like object 
                              any valid string path is acceptable.
        delimiter: str, default {DELIMITER} Delimiter to use.
        escapechar: str, default {ESCAPECHAR} Sscapechar to use.
    Returns
    -------
        DataFrame
    """
    df = pd.read_csv(filepath_or_buffer, delimiter=delimiter,
                     quoting=csv.QUOTE_NONE, escapechar=escapechar, **kwargs)
    return df

def export_dataset(df, filepath_or_buffer, delimiter=DELIMITER,
                   quoting=csv.QUOTE_NONE, escapechar=ESCAPECHAR, **kwargs):
    """
    Store the data
    Args
    ----
        df: dataframe
        filepath_or_buffer: str, path object or file-like object 
                              Any valid string path is acceptable.
        delimiter: str, default {DELIMITER} Delimiter to use.
        escapechar: str, default {ESCAPECHAR} Sscapechar to use.
    Returns
    -------
    Write DataFrame to a (csv)file.
    """
    kwargs.update({
        'sep': delimiter,
        'quoting': csv.QUOTE_NONE,
        'escapechar': escapechar,
        'index': False,
        'encoding': 'utf-8',
    })

    df.to_csv(filepath_or_buffer, **kwargs)
    return filepath_or_buffer
