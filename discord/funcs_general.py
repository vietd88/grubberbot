import datetime
import gspread
import numpy as np
import pandas as pd

GOOGLE_TOKEN = 'data/grubberbot-2f9a174696fa.json'
GOOGLE_SHEET_NAME = '1SFH7ntDpDW7blX_xBU_m_kSmvY2tLXkuwq-TOM9wyew'

REFRESH_MESSAGE = (
    'Hi! Looks like '
)

def get_month(month_delta=0, to_str=True):
    date = datetime.datetime.now()
    date = date.replace(day=1)

    for i in range(abs(month_delta)):
        if month_delta > 0:
            delta = datetime.timedelta(31)
        elif month_delta < 0:
            delta = - datetime.timedelta(2)
        date = date + delta
        date = date.replace(day=1)

    if to_str:
        date = date.strftime('%Y%B')
    return date

def gen_substitute_thread_name(seed_id):
    name = f's{seed_id} Substitute Request'
    return name

def gen_pairing_thread_name(game_id, season_name, week_num, white_name, black_name):
    thread_name = (
        f'g{game_id} {season_name} Week{week_num} | {white_name} vs {black_name}'
    )
    return thread_name

def arr_to_sheet(arr, sheet=0):
    gc = gspread.service_account(filename=GOOGLE_TOKEN)
    sh = gc.open_by_key(GOOGLE_SHEET_NAME)
    sheet = sh.get_worksheet(sheet)

    sheet_array = arr
    sheet_array = [row + ['' for _ in range(10)] for row in sheet_array]
    sheet_array = sheet_array + [
        ['' for _ in range(len(sheet_array[0]))]
        for _ in range(100)
    ]
    sheet.update(sheet_array)

def df_to_sheet(df, sheet=0, title=None):
    for col in df.columns:
        df[col] = [str(e) for e in np.array(df[col])]

    if title is None:
        sheet_array = []
    else:
        sheet_array = [[title]]
    sheet_array = sheet_array + [df.columns.values.tolist()]
    sheet_array = sheet_array + df.values.tolist()
    arr_to_sheet(sheet_array, sheet=sheet)
