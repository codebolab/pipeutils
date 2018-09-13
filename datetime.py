from tzlocal import get_localzone
from datetime import datetime
from pytz import timezone


LOCAL_TZ = get_localzone()
TIMEZONE = 'America/New_York'


def ts_now_utc():
    return datetime.utcnow()


def ts_now_timezone(ts="America/New_York"):
    return datetime.now(tz=LOCAL_TZ).astimezone(timezone(ts))


def iso_utc_now():
    return ts_now_utc().isoformat()


def iso_utc_timezone(ts="America/New_York"):
    return ts_now_timezone(ts).strftime('%Y-%m-%d %H:%M:%S')
