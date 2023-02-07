from datetime import datetime, timedelta
from pytz import timezone, utc
import db

def get_time_periods_that_should_be_in_data(start_timestamp):
    tz_amsterdam = timezone('Europe/Amsterdam')
    start_timestamp = datetime(2022, 1, 1, 0, 0, 0, tzinfo=utc)
    end_timestamp = start_timestamp + timedelta(hours = 4)
    timestamps = []
    # only start calculating stats 1 hour after passing timestamp.
    while end_timestamp <= datetime.now(tz=utc) - timedelta(hours=1):
        timestamps.append((start_timestamp.astimezone(tz_amsterdam), end_timestamp.astimezone(tz_amsterdam)))
       
        start_timestamp = start_timestamp + timedelta(hours = 4)
        end_timestamp = end_timestamp + timedelta(hours = 4)
        # timecorrection for DST.
        hour_modulus_start = (start_timestamp.astimezone(tz_amsterdam).hour - 2) % 4
        if hour_modulus_start == 1:
            start_timestamp = start_timestamp - timedelta(hours = 1)
        elif hour_modulus_start == 3:
            start_timestamp = start_timestamp + timedelta(hours = 1)
        hour_modulus_end = (end_timestamp.astimezone(tz_amsterdam).hour - 2) % 4
        if hour_modulus_end == 1:
            end_timestamp = end_timestamp - timedelta(hours = 1)
        elif hour_modulus_end == 3:
            end_timestamp = end_timestamp + timedelta(hours = 1)
        
    return timestamps

def get_missing_time_periods():
    start_timestamp = datetime(2022, 9, 1, 0, 0, 0, tzinfo=utc)
    time_periods_that_should_be_in_data = get_time_periods_that_should_be_in_data(start_timestamp)
    time_periods_in_data = db.query_all_aggregation_periods_since(start_timestamp.astimezone(timezone('Europe/Amsterdam')))
    
    start_in_data = {}
    end_in_data = {}
    missing_time_periods = []
    for time_period in time_periods_in_data:
        start_in_data[time_period["start_time_period"]] = True
        end_in_data[time_period["end_time_period"]] = True

    for time_period in time_periods_that_should_be_in_data:
        if time_period[0] not in start_in_data or time_period[1] not in end_in_data:
            missing_time_periods.append(time_period)
    return missing_time_periods

