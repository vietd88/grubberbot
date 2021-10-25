from __future__ import print_function
import datetime
import os.path
from googleapiclient.discovery import build as gc_build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from pprint import pprint

SCOPES = [
    "https://www.googleapis.com/auth/calendar",
    "https://www.googleapis.com/auth/calendar.events",
    "https://www.googleapis.com/auth/drive",
]

SERVICE_ACCOUNT_FILE = "credentials/grubberbot-2f9a174696fa.json"
CALENDAR_ID = "sk73kniat254fng6gn59u0pebc@group.calendar.google.com"
TIME_ZONE_WEBSITE = "https://en.wikipedia.org/wiki/List_of_tz_database_time_zones"

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


def main():
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


if __name__ == "__main__":
    main()
