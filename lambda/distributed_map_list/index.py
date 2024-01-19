import datetime
import json
from datetime import timedelta, datetime


def get_dates_in_range(duration, date_format):
    start_date = datetime.now() - timedelta(days=duration)
    dates = []
    for n in range(duration):
        date = start_date + timedelta(days=n)
        dates.append(date.strftime(date_format))
    return dates

def lambda_handler(event, context):
    print(event)
    
    dates = get_dates_in_range(event["duration"], event["date_format"])
    s3_locations = []
    for date in dates:
        print(f"Selected {date} of {len(dates)}")
        location = {
             "src": json.dumps(event["s3_source_uri"] + str(date)),
             "dest": json.dumps(event["s3_destination_uri"] + str(date))
             }
        s3_locations.append(location)
    print("Prefix list complete!")
    return {
        "s3_locations": s3_locations
    }