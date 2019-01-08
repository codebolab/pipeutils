import os
import six
import logging
import unittest
from datetime import datetime
from tzlocal import get_localzone
from pytz import timezone
from pipeutils import logger
from pipeutils.timestamps import ts_now_utc, iso_utc_now, ts_now_timezone 
from pipeutils.timestamps import iso_utc_timezone


class TestTimeStamps(unittest.TestCase):

    def test_get_current_utc(self):
        """
        check local time zone, the time now in UTC.
        """
        dt = ts_now_utc().strftime("%d/%m/%y %H:%M")
        logger.info(dt)
        time = datetime.utcnow()
        self.assertEqual(dt, datetime.utcnow().strftime("%d/%m/%y %H:%M"))

    def test_get_current_utc_iso(self):
        """
        check local time zone and iso format.
        """
        dt = iso_utc_now()
        logger.info(dt[:21])
        time = datetime.utcnow().isoformat()
        logger.info(time[:21])
        self.assertEqual(dt[:21], time[:21])

    def test_get_ts_now_timezone(self):
        """
        Local time zone in America/New_York defined. 
        """
        dt = ts_now_timezone()
        logger.info('ts_now_timezone %s' % dt.strftime("%d/%m/%y %H:%M"))
        ts="America/New_York"
        time = datetime.now(tz=get_localzone()).astimezone(timezone(ts))
        logger.info('time %s' % time.strftime("%d/%m/%y %H:%M"))
        self.assertEqual(dt.strftime("%d/%m/%y %H:%M"), time.strftime("%d/%m/%y %H:%M"))

    def test_get_ts_now_timezone(self):
        """
        Local time zone in America/New_York and string representing
        """
        dt = iso_utc_timezone()
        logger.info('iso_utc_timezone %s ' % dt)

        ts="America/New_York"
        time = datetime.now(tz=get_localzone()).astimezone(timezone(ts))
        self.assertEqual(dt, time.strftime('%Y-%m-%d %H:%M:%S'))

if __name__ == '__main__':
    unittest.main()
