# 2020 4th Whales Marketing all rights reserved ©
import datetime

from dateutil import rrule
from typing import List, Optional


def date_range_report(
        year:int,
        month_nbr:int,
        day: Optional[int] = 1 ) -> List:
    """
    Creates a list of dates from the day (day) of the month number (month_nbr)
    and year (year) of your choice.
    """
    time_to_get = datetime.date(year=year, month=month_nbr, day=day)
    today = datetime.datetime.today().date()
    first_date = today.replace(day=1)
    date_list = []
    for dt in rrule.rrule(rrule.MONTHLY, dtstart=time_to_get, until=first_date):
        date_list.append(dt.strftime("%Y-%m-%d"))
    return date_list