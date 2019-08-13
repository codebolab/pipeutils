import os
import logging
from datetime import date


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def local(df, output=None, prefix='', sufix='', separator=';'):
    """
    Stores dataset `df` in `output`. Filename will be formatted as
    {prefix}_{today}_{sufix}.csv

    Args:
        - df (pd.Dataframe): dataset.
        - output: directory path to stage the dataset.
        - prefix: prefix for the filename.
        - sufix: sufix for the filename.
    """
    
    if not os.path.exists(output):
        os.makedirs(output)
    
    today = date.today().strftime('_%Y_%m_%d_')
    path = os.path.join(output, prefix + today + str(sufix) + '.csv')
    df.to_csv(path, encoding='utf-8', index=False, sep=separator)
    logger.info("The CSV file %s was saved" % path)
