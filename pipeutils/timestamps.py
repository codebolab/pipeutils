from tzlocal import get_localzone
from datetime import datetime
from pytz import timezone


LOCAL_TZ = get_localzone()
TIMEZONE = 'America/New_York'


def ts_now_utc():
    '''
    Return local time zone, the time now in UTC.
    '''
    return datetime.utcnow()


def ts_now_timezone(ts="America/New_York"):
    '''
    Return current local time in America/New_York
    '''
    return datetime.now(tz=LOCAL_TZ).astimezone(timezone(ts))


def iso_utc_now():
    '''
    Return a string representing the date in ISO 8601 format
    '''
    return ts_now_utc().isoformat()


def iso_utc_timezone(ts="America/New_York"):
    '''
    Return current local time in America/New_York and string representing
    '''
    return ts_now_timezone(ts).strftime('%Y-%m-%d %H:%M:%S')

def today(ts="America/New_York"):
    """
    Returns a date object for today but using the timezone `ts`
    """
    return datetime.now(tz=timezone(ts)).today()


def iso_today(ts="America/New_York"):
    """
    Returns a date string formatted '%Y-%m-%d' for today
    but using the timezone 'ts'
    """
    return today(ts).isoformat(' ')

def str_today(ts="America/New_York", format='%Y-%m-%d'):
    """
    Returns a date string formatted as `format`
    for today date but using the timezone 'ts'
    """
    return today(ts).strftime(format)
