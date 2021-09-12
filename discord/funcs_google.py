from __future__ import print_function
import datetime
import os.path
from googleapiclient.discovery import build as gc_build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account

SCOPES = [
    'https://www.googleapis.com/auth/calendar',
    'https://www.googleapis.com/auth/calendar.events',
    'https://www.googleapis.com/auth/drive',
]

SERVICE_ACCOUNT_FILE = 'data/grubberbot-2f9a174696fa.json'
CALENDAR_ID ='sk73kniat254fng6gn59u0pebc@group.calendar.google.com'

CREDENTIALS = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=SCOPES,
)

service = gc_build(
    'calendar',
    'v3',
    credentials=CREDENTIALS,
)

def add_event(event_name, start, end):
    event = {
      'summary': event_name,
      'start': {
        'dateTime': '2021-09-28T09:00:00-07:00',
        'timeZone': 'America/Los_Angeles',
      },
      'end': {
        'dateTime': '2021-09-28T17:00:00-07:00',
        'timeZone': 'America/Los_Angeles',
      },
    }
    event = service.events().insert(calendarId=CALENDAR_ID, body=event).execute()

def get_next_10_events():
    # Call the Calendar API
    now = datetime.datetime.utcnow().isoformat() + 'Z' # 'Z' indicates UTC time
    print('Getting the upcoming 10 events')
    events_result = service.events().list(
        calendarId='sk73kniat254fng6gn59u0pebc@group.calendar.google.com',
        #calendarId='primary',
        timeMin=now,
        maxResults=10,
        singleEvents=True,
        orderBy='startTime',
    ).execute()
    events = events_result.get('items', [])
    return events

def print_events(events):
    if not events:
        print('No upcoming events found.')
    for event in events:
        start = event['start'].get('dateTime', event['start'].get('date'))
        print(start, event['summary'])

def delete_events(events):
    for event in events:
        service.events().delete(calendarId=CALENDAR_ID, eventId=event['id']).execute()

def main():
    add_event('test_event', 1, 2)

    events = get_next_10_events()
    print()
    print_events(events)
    delete_events(events)

    events = get_next_10_events()
    print()
    print_events(events)

if __name__ == '__main__':
    main()
