import unittest
from datetime import datetime
from tzlocal import get_localzone
from pytz import timezone
from pipeutils import logger
from pipeutils.timestamps import ts_now_utc, iso_utc_now, ts_now_timezone
from pipeutils.timestamps import iso_utc_timezone, today, iso_today, str_today


class TestTimeStamps(unittest.TestCase):

    def test_get_current_utc(self):
        """
        check local time zone, the time now in UTC.
        """
        dt = ts_now_utc().strftime("%d/%m/%y %H:%M")
        logger.info(dt)
        self.assertIsNotNone(dt)

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
        ts = "America/New_York"
        time = datetime.now(tz=get_localzone()).astimezone(timezone(ts))
        logger.info('time %s' % time.strftime("%d/%m/%y %H:%M"))
        self.assertIsNotNone(dt.strftime("%d/%m/%y"))

    def test_get_ts_now_timezone_iso(self):
        """
        Local time zone in America/New_York and string representing
        """
        dt = iso_utc_timezone()
        logger.info('iso_utc_timezone %s ' % dt)

        self.assertIsNotNone(dt)

    def test_get_today(self):
        """
        Today with time zone in America/New_York
        """

        dt = today(ts="America/New_York")
        logger.info('today() %s ' % dt)

        ts = "America/New_York"
        time = datetime.now(tz=timezone(ts)).today()

        self.assertEqual(dt.strftime("%d/%m/%y"), time.strftime("%d/%m/%y"))

    def test_get_iso_today(self):
        """
        check today and returned in iso format.
        """
        ts = "America/New_York"
        nyc = timezone(ts)
        date = datetime.today().astimezone(nyc)

        dt = iso_today(ts=ts)
        logger.info('iso_today %s ' % dt)
        time = date.strftime('%Y-%m-%d')
        logger.info('iso_today_time %s ' % time)

        self.assertEqual(dt, time)
        # year
        self.assertEqual(dt[1], time[1])
        # month
        self.assertEqual(dt[2], time[2])
        # day
        self.assertEqual(dt[3], time[3])

    def test_get_srt_today(self):
        """
        Returns a date string formatted a '%Y-%m-%d' for today
        """
        ts = "America/New_York"
        dt = str_today(ts=ts)
        time = datetime.now(tz=timezone(ts)).today().strftime('%Y-%m-%d')
        self.assertEqual(dt, time)


if __name__ == '__main__':
    unittest.main()
