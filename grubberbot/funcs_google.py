from __future__ import print_function

import datetime
import os.path
import urllib
import urllib.request
from pprint import pprint

from google.auth.transport.requests import Request
from google.cloud import storage
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build as gc_build

SCOPES = [
    "https://www.googleapis.com/auth/calendar",
    "https://www.googleapis.com/auth/calendar.events",
    "https://www.googleapis.com/auth/drive",
]

SERVICE_ACCOUNT_FILE = "credentials/google_credentials.json"
CALENDAR_ID = "sk73kniat254fng6gn59u0pebc@group.calendar.google.com"
TIME_ZONE_WEBSITE = "https://en.wikipedia.org/wiki/List_of_tz_database_time_zones"
DB_PATH = "data/rapid_league.sqlite3"

if os.path.exists(SERVICE_ACCOUNT_FILE):
    CREDENTIALS = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES,
    )

    service = gc_build(
        "calendar",
        "v3",
        credentials=CREDENTIALS,
    )


def add_event(event_name, start, end, time_zone):
    event = {
        "summary": event_name,
        "start": {
            "dateTime": str(start).replace(" ", "T"),
            "timeZone": time_zone,
        },
        "end": {
            "dateTime": str(end).replace(" ", "T"),
            "timeZone": time_zone,
        },
    }

    try:
        event = service.events().insert(calendarId=CALENDAR_ID, body=event).execute()
        event_id = event["id"]
        url = event["htmlLink"]
    except Exception as e:
        if "Invalid time zone" in str(e):
            message = (
                f"Invalid time zone `{time_zone}`, please use a time zone "
                f"in the TZ database name section of this page:\n"
                f"{TIME_ZONE_WEBSITE}"
            )
            return message, None, None
    return None, event_id, url


def get_next_10_events():
    # Call the Calendar API
    now = datetime.datetime.utcnow().isoformat() + "Z"  # 'Z' indicates UTC time
    print("Getting the upcoming 10 events")
    events_result = (
        service.events()
        .list(
            calendarId="sk73kniat254fng6gn59u0pebc@group.calendar.google.com",
            # calendarId='primary',
            timeMin=now,
            maxResults=10,
            singleEvents=True,
            orderBy="startTime",
        )
        .execute()
    )
    events = events_result.get("items", [])
    return events


def print_events(events):
    if not events:
        print("No upcoming events found.")
    for event in events:
        start = event["start"].get("dateTime", event["start"].get("date"))
        print(start, event["summary"])


def delete_events(events):
    for event in events:
        service.events().delete(calendarId=CALENDAR_ID, eventId=event["id"]).execute()


def delete_event_by_id(event_id):
    service.events().delete(calendarId=CALENDAR_ID, eventId=event_id).execute()


def upload_to_bucket(blob_filename, local_filename, bucket_name):
    """Upload data to a bucket"""

    # Explicitly use service account credentials by specifying the private key file
    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_filename)
    blob.upload_from_filename(local_filename)

    # returns a public url
    return blob.public_url


def backup_db():
    if not os.path.exists(DB_PATH):
        return

    timestamp = datetime.datetime.now(datetime.timezone.utc)
    timestamp = timestamp.isoformat(" ", "seconds")
    timestamp = timestamp.replace(" ", "_")

    upload_to_bucket(
        f"sqlite_backup/{timestamp}_UTC.sqlite3",
        DB_PATH,
        "grubberbot_backup",
    )

    upload_to_bucket(
        "rapid_league.sqlite3",
        DB_PATH,
        "grubberbot_backup",
    )


def download_db():
    if os.path.exists(DB_PATH):
        return

    db_name = "https://storage.googleapis.com/grubberbot_backup/rapid_league.sqlite3"
    urllib.request.urlretrieve(db_name, "data/rapid_league.sqlite3")

    # bucket_name = "grubberbot_backup"
    # storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
    # bucket = storage_client.get_bucket(bucket_name)
    # blob = bucket.blob("rapid_league.sqlite3")
    # blob.download_to_filename(DB_PATH)
    # print("downloaded DB")


def main():
    """
    now = datetime.datetime.now()
    start = now + datetime.timedelta(1)
    end = now + datetime.timedelta(1, 1)
    time_zone = "Africa/Abidjan"

    error, event_id, url = add_event("test_event", start, end, time_zone)
    print(error)

    events = get_next_10_events()
    print()
    print_events(events)
    delete_events(events)

    events = get_next_10_events()
    print()
    print_events(events)
    """


if __name__ == "__main__":
    main()
