import datetime
import json
import re
import sqlite3
import time
import urllib
import urllib.error
import urllib.request
from asyncio import gather
from pprint import pformat, pprint

import chessdotcom as cdc
import chessdotcom.aio as cdc_aio
import funcs_general as fgg
import pandas as pd
from chessdotcom.aio import Client as cdc_aio_client

CHESSCOM_DB = "data/chesscom.sqlite3"


def get_game_history_api(chesscom):
    date = fgg.get_month(0, to_str=False)
    url = (
        f"https://api.chess.com/pub/player/{chesscom}/games/"
        f"{str(date.year).zfill(4)}/{str(date.month).zfill(2)}"
    )
    with urllib.request.urlopen(url) as response:
        info = response.read()
    info = json.loads(info)
    games = info["games"]

    if int(date.day) in [1, 2]:
        date = fgg.get_month(-1, to_str=False)
        url = (
            f"https://api.chess.com/pub/player/{chesscom}/games/"
            f"{str(date.year).zfill(4)}/{str(date.month).zfill(2)}"
        )
        with urllib.request.urlopen(url) as response:
            info = response.read()
        info = json.loads(info)
        games = games + info["games"]
    return games


def get_game_history(chesscom):
    cdc.Client.aio = False
    day = datetime.datetime.now().day
    month = fgg.get_month(0, to_str=False)

    info = cdc.get_player_games_by_month(chesscom, datetime_obj=month)
    games = info.json["games"]
    if day in [1, 2]:
        month = fgg.get_month(-1, to_str=False)
        info = cdc.get_player_games_by_month(chesscom, datetime_obj=month)
        games = games + info.json["games"]

    return games


async def get_game_history_async(chesscom):
    day = datetime.datetime.now().day
    month = fgg.get_month(0, to_str=False)

    info = [cdc_aio.get_player_games_by_month(chesscom, datetime_obj=month)]
    responses = cdc_aio_client.loop.run_until_complete(gather(*info))
    info = responses[0]
    games = info.json["games"]
    if day in [1, 2]:
        month = fgg.get_month(-1, to_str=False)
        info = [cdc_aio.get_player_games_by_month(chesscom, datetime_obj=month)]
        responses = cdc_aio_client.loop.run_until_complete(gather(*info))
        info = responses[0]
        games = games + info.json["games"]

    return games


def game_id_from_url(url):
    integers = [int(i[0]) for i in re.finditer(r"[\d]+", url)]
    if len(integers) == 0:
        return None
    cc_game_id = max(integers)
    return cc_game_id


class ChesscomDatabase:
    def __init__(self, path=CHESSCOM_DB, wait_time=(30 * 60)):
        self.path = path
        self.conn = sqlite3.connect(self.path)
        self.cur = self.conn.cursor()
        self.conn.execute("PRAGMA foreign_keys = 1")
        self.conn.commit()
        self.init_tables()
        self.wait_time = wait_time

    def quit(self):
        self.conn.close()

    def get_all_tables(self):
        sql = "SELECT name FROM sqlite_master WHERE type='table';"
        output = self.conn.execute(sql)
        df_dict = {}
        for table in output.fetchall():
            df = pd.read_sql_query(f"SELECT * FROM {table[0]}", self.conn)
            df_dict[table[0]] = df
        return df_dict

    def init_tables(self):

        # Users, id is their discord id
        chess_tbl_sql = """
        CREATE TABLE IF NOT EXISTS chess(
            id integer PRIMARY KEY,
            chesscom text NOT NULL UNIQUE,

            exists_user integer,
            exists_time integer,

            rapid integer,
            rapid_last integer,
            blitz integer,
            blitz_last integer,
            bullet integer,
            bullet_last integer,
            rating_time integer,

            total_count integer,
            rapid_count integer,
            blitz_count integer,
            bullet_count integer,
            count_time integer
        );"""
        # TODO: Force users in a game to also be in the season

        queries = [
            chess_tbl_sql,
        ]
        for query in queries:
            self.cur.execute(query)
        self.conn.commit()

    def _set_exists(self, chesscom, return_message=False):
        url = f"https://api.chess.com/pub/player/{chesscom}"
        try:
            with urllib.request.urlopen(url) as response:
                info = response.read()
        except urllib.error.HTTPError:
            return False
        info = json.loads(info)

        if return_message:
            return info

        if "error" in info:
            if "Not Found" in info["error"]:
                return False

        return True

    def set_exists(self, chesscom):
        exists_user = self._set_exists(chesscom)
        exists_time = time.time()
        sql = """
        INSERT INTO chess(chesscom, exists_user, exists_time) VALUES(?, ?, ?)
        ON CONFLICT(chesscom) DO UPDATE
        SET exists_user = ?, exists_time = ? WHERE chess.chesscom = ?
        ;"""
        params = (
            chesscom,
            exists_user,
            exists_time,
            exists_user,
            exists_time,
            chesscom,
        )
        self.cur.execute(sql, params)
        self.conn.commit()
        return exists_user

    def get_exists(self, chesscom):
        sql = """
        SELECT c.exists_user, c.exists_time FROM chess AS c
        WHERE c.chesscom = ?
        ;"""
        params = (chesscom,)
        df = pd.read_sql_query(sql, self.conn, params=params)

        if len(df) == 0 or (time.time() - df["exists_time"][0] > self.wait_time):
            return self.set_exists(chesscom)
        else:
            return df["exists_user"][0]

    def _set_count(self, chesscom):
        exists = self.get_exists(chesscom)
        if exists is None or not exists:
            return None

        url = f"https://api.chess.com/pub/player/{chesscom}/stats"
        with urllib.request.urlopen(url) as response:
            info = response.read()
        info = json.loads(info)

        categories = [
            "chess_rapid",
            # 'lessons',
            # 'tactics',
            "chess960_daily",
            "chess_blitz",
            # 'puzzle_rush',
            "chess_bullet",
            "chess_daily",
            # 'fide',
        ]
        wdl_list = ["win", "draw", "loss"]

        output = {}
        for category in categories:
            try:
                wdl = info[category]["record"]
            except KeyError:
                wdl = {"win": 0, "draw": 0, "loss": 0}
            val = sum([v for k, v in wdl.items() if k in wdl_list])
            output[category] = val

        new_output = {}
        new_output["rapid_count"] = output["chess_rapid"]
        new_output["blitz_count"] = output["chess_blitz"]
        new_output["bullet_count"] = output["chess_bullet"]
        new_output["total_count"] = sum([v for k, v in output.items()])
        new_output["count_time"] = time.time()
        output = new_output
        return output

    def set_count(self, chesscom):
        exists = self.get_exists(chesscom)
        if exists is None or not exists:
            return None

        sql = """
        UPDATE chess SET
            rapid_count = ?,
            blitz_count = ?,
            bullet_count = ?,
            total_count = ?,
            count_time = ?
        WHERE chess.chesscom = ?
        ;"""

        info = self._set_count(chesscom)
        params = (
            info["rapid_count"],
            info["blitz_count"],
            info["bullet_count"],
            info["total_count"],
            info["count_time"],
            chesscom,
        )
        self.cur.execute(sql, params)
        self.conn.commit()
        return info

    def get_count(self, chesscom):
        exists = self.get_exists(chesscom)
        if exists is None or not exists:
            return None

        sql = """
        SELECT
            c.rapid_count,
            c.blitz_count,
            c.bullet_count,
            c.total_count,
            c.count_time
        FROM chess AS c
        WHERE c.chesscom = ? AND c.count_time IS NOT NULL
        ;"""
        params = (chesscom,)
        df = pd.read_sql_query(sql, self.conn, params=params)

        if len(df) == 0 or (time.time() - df["count_time"][0] > self.wait_time):
            return self.set_count(chesscom)
        else:
            info = {str(c): df[c][0] for c in df.columns}
            return info

    def _set_rating(self, chesscom):
        exists = self.get_exists(chesscom)
        if exists is None or not exists:
            return None

        url = f"https://api.chess.com/pub/player/{chesscom}/stats"
        with urllib.request.urlopen(url) as response:
            info = response.read()
        info = json.loads(info)
        names = {
            "rapid": ["chess_rapid", "last", "rating"],
            "blitz": ["chess_blitz", "last", "rating"],
            "bullet": ["chess_bullet", "last", "rating"],
            "rapid_last": ["chess_rapid", "last", "date"],
            "blitz_last": ["chess_blitz", "last", "date"],
            "bullet_last": ["chess_bullet", "last", "date"],
        }
        output = {}
        for k, v in names.items():
            try:
                val = info[v[0]][v[1]][v[2]]  # if not KeyError else None
            except KeyError:
                val = None
            output[k] = val
        output["rating_time"] = time.time()
        return output

    def set_rating(self, chesscom):
        exists = self.get_exists(chesscom)
        if exists is None or not exists:
            return None

        sql = """
        UPDATE chess SET
            rapid = ?,
            blitz = ?,
            bullet = ?,
            rapid_last = ?,
            blitz_last = ?,
            bullet_last = ?,
            rating_time = ?
        WHERE chess.chesscom = ?
        ;"""
        info = self._set_rating(chesscom)
        params = (
            info["rapid"],
            info["blitz"],
            info["bullet"],
            info["rapid_last"],
            info["blitz_last"],
            info["bullet_last"],
            info["rating_time"],
            chesscom,
        )
        self.cur.execute(sql, params)
        self.conn.commit()
        return info

    def get_rating(self, chesscom):
        exists = self.get_exists(chesscom)
        if exists is None or not exists:
            return None

        sql = """
        SELECT
            c.rapid,
            c.blitz,
            c.bullet,
            c.rapid_last,
            c.blitz_last,
            c.bullet_last,
            c.rating_time
        FROM chess AS c
        WHERE c.chesscom = ? AND c.rating_time IS NOT NULL
        ;"""
        params = (chesscom,)
        df = pd.read_sql_query(sql, self.conn, params=params)

        if len(df) == 0 or (time.time() - df["rating_time"][0] > self.wait_time):
            return self.set_rating(chesscom)
        else:
            info = {str(c): df[c][0] for c in df.columns}
            return info


if __name__ == "__main__":
    foo = get_game_history_api("pawngrubber")
    pprint(foo)
