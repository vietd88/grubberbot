import datetime

import gspread
import numpy as np
import pandas as pd

GOOGLE_TOKEN = "credentials/google_credentials.json"
GOOGLE_SHEET_NAME = "1SFH7ntDpDW7blX_xBU_m_kSmvY2tLXkuwq-TOM9wyew"

REFRESH_MESSAGE = "Hi! Looks like "


def get_month(month_delta=0, to_str=True):
    date = datetime.datetime.now()
    date = date.replace(day=1)

    for _ in range(abs(month_delta)):
        if month_delta > 0:
            delta = datetime.timedelta(31)
        elif month_delta < 0:
            delta = -datetime.timedelta(2)
        date = date + delta
        date = date.replace(day=1)

    if to_str:
        date = date.strftime("%Y%B")
    return date


def gen_substitute_thread_name(seed_id):
    name = f"s{seed_id} Substitute Request"
    return name


def gen_pairing_thread_name(game_id, season_name, week_num, white_name, black_name):
    thread_name = (
        f"g{game_id} {season_name} Week{week_num} | {white_name} vs {black_name}"
    )
    return thread_name


def arr_to_sheet(arr, sheet=0):
    gc = gspread.service_account(filename=GOOGLE_TOKEN)
    sh = gc.open_by_key(GOOGLE_SHEET_NAME)
    sheet = sh.get_worksheet(sheet)

    if len(arr):
        row_length = len(arr[0])
    else:
        row_length = 0

    sheet_array = arr
    sheet_array = sheet_array + [["" for _ in range(row_length)] for _ in range(100)]
    sheet_array = [row + ["" for _ in range(100)] for row in sheet_array]
    sheet.update(sheet_array)


def gen_df_to_sheet(df, title=None):
    for col in df.columns:
        df[col] = [str(e) for e in np.array(df[col])]

    if title is None:
        sheet_array = []
    else:
        sheet_array = [["" for _ in range(len(list(df.columns)))] for _ in title]
        for r, row in enumerate(title):
            for e, elem in enumerate(row):
                sheet_array[r][e] = elem
    sheet_array = sheet_array + [df.columns.values.tolist()]
    sheet_array = sheet_array + df.values.tolist()
    return sheet_array


def df_to_sheet(df, sheet=0, title=None):
    sheet_array = gen_df_to_sheet(df, title=title)
    arr_to_sheet(sheet_array, sheet=sheet)


def dfs_to_sheet(dfs, sheet=0):
    arrs = [gen_df_to_sheet(df, title=title) for title, df in dfs.items()]
    arrs = sorted(arrs, key=lambda x: len(x), reverse=True)
    if len(dfs):
        longest_arr = max(len(a) for a in arrs)
    else:
        longest_arr = 0
    for arr in arrs:
        while len(arr) < longest_arr:
            arr.append(["" for _ in range(len(arr[0]))])

        for row in arr:
            row.append("")

    sheet_array = []
    for i in range(longest_arr):
        row = []
        for j in range(len(arrs)):
            row.extend(arrs[j][i])
        sheet_array.append(row)

    arr_to_sheet(sheet_array, sheet=sheet)
