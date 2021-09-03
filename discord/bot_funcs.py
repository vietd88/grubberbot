import urllib
import urllib.error
import urllib.request
import json
import os
import datetime
from pprint import pprint
import sqlite3
import numpy as np
import string
import random
import pandas as pd
from tqdm import tqdm
import time
import random
import gspread

# TODO: set iter time to be higher

GENERAL_ERROR_MESSAGE = '{} `!{}` error, get help with `!help {}`'

TOKEN = 'data/grubberbot-2f9a174696fa.json'
SHEET_NAME = '1SFH7ntDpDW7blX_xBU_m_kSmvY2tLXkuwq-TOM9wyew'

# TODO: Change this to 1
NEXT_MONTH = 0
CHESSCOM_DB = 'data/chesscom.sqlite3'
LEAGUE_DB = 'data/rapid_league.sqlite3'
SIGNUP_TEAM = 'signup'

def df_to_sheet(df, sheet=1):
    gc = gspread.service_account(filename=TOKEN)
    sh = gc.open_by_key(SHEET_NAME)
    sheet = sh.get_worksheet(sheet-1)

    to_update = []
    to_update = to_update + [df.columns.values.tolist()]
    to_update = to_update + df.values.tolist()
    to_update = to_update + [
        ['' for _ in range(len(df.columns))]
        for _ in range(100)
    ]
    sheet.update(to_update)

class ChesscomDatabase:

    def __init__(self, path=CHESSCOM_DB, wait_time=(30*60)):
        self.path = path
        self.conn = sqlite3.connect(self.path)
        self.cur = self.conn.cursor()
        self.conn.execute('PRAGMA foreign_keys = 1')
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
            df = pd.read_sql_query(f'SELECT * FROM {table[0]}', self.conn)
            df_dict[table[0]] = df
        return df_dict

    def init_tables(self):

        # Users, id is their discord id
        chess_tbl_sql = '''
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
        );'''
        # TODO: Force users in a game to also be in the season

        queries = [
            chess_tbl_sql,
        ]
        for query in queries:
            self.cur.execute(query)
        self.conn.commit()

    def _set_exists(self, chesscom, return_message=False):
        url = f'https://api.chess.com/pub/player/{chesscom}'
        try:
            with urllib.request.urlopen(url) as response:
                info = response.read()
        except urllib.error.HTTPError:
            return False
        info = json.loads(info)

        if return_message:
            return info

        if 'error' in info:
            if 'Not Found' in info['error']:
                return False

        return True

    def set_exists(self, chesscom):
        exists_user = self._set_exists(chesscom)
        exists_time = time.time()
        sql = '''
        INSERT INTO chess(chesscom, exists_user, exists_time) VALUES(?, ?, ?)
        ON CONFLICT(chesscom) DO UPDATE
        SET exists_user = ?, exists_time = ? WHERE chess.chesscom = ?
        ;'''
        params = (
            chesscom, exists_user, exists_time,
            exists_user, exists_time, chesscom,
        )
        self.cur.execute(sql, params)
        self.conn.commit()
        return exists_user

    def get_exists(self, chesscom):
        sql = '''
        SELECT c.exists_user, c.exists_time FROM chess AS c
        WHERE c.chesscom = ?
        ;'''
        params = (chesscom,)
        df = pd.read_sql_query(sql, self.conn, params=params)

        if len(df) == 0 or (time.time()-df['exists_time'][0] > self.wait_time):
            return self.set_exists(chesscom)
        else:
            return df['exists_user'][0]

    def _set_count(self, chesscom):
        exists = self.get_exists(chesscom)
        if exists is None or not exists:
            return None

        url = f'https://api.chess.com/pub/player/{chesscom}/stats'
        with urllib.request.urlopen(url) as response:
            info = response.read()
        info = json.loads(info)

        categories = [
            'chess_rapid',
            #'lessons',
            #'tactics',
            'chess960_daily',
            'chess_blitz',
            #'puzzle_rush',
            'chess_bullet',
            'chess_daily',
            #'fide',
        ]
        wdl_list = ['win', 'loss', 'draw']

        output = {}
        for category in categories:
            num_games = 0
            try:
                wdl = info[category]['record']
            except KeyError:
                wdl = {'win': 0, 'draw': 0, 'loss': 0}
            val = sum([v for k, v in wdl.items() if k in wdl_list])
            output[category] = val

        new_output = {}
        new_output['rapid_count'] = output['chess_rapid']
        new_output['blitz_count'] = output['chess_blitz']
        new_output['bullet_count'] = output['chess_bullet']
        new_output['total_count'] = sum([v for k, v in output.items()])
        new_output['count_time'] = time.time()
        output = new_output
        return output

    def set_count(self, chesscom):
        exists = self.get_exists(chesscom)
        if exists is None or not exists:
            return None

        sql = '''
        UPDATE chess SET
            rapid_count = ?,
            blitz_count = ?,
            bullet_count = ?,
            total_count = ?,
            count_time = ?
        WHERE chess.chesscom = ?
        ;'''

        info = self._set_count(chesscom)
        params = (
            info['rapid_count'],
            info['blitz_count'],
            info['bullet_count'],
            info['total_count'],
            info['count_time'],
            chesscom,
        )
        self.cur.execute(sql, params)
        self.conn.commit()
        return info

    def get_count(self, chesscom):
        exists = self.get_exists(chesscom)
        if exists is None or not exists:
            return None

        sql = '''
        SELECT
            c.rapid_count,
            c.blitz_count,
            c.bullet_count,
            c.total_count,
            c.count_time
        FROM chess AS c
        WHERE c.chesscom = ? AND c.count_time IS NOT NULL
        ;'''
        params = (chesscom,)
        df = pd.read_sql_query(sql, self.conn, params=params)

        if len(df) == 0 or (time.time()-df['count_time'][0] > self.wait_time):
            return self.set_count(chesscom)
        else:
            info = {str(c): df[c][0] for c in df.columns}
            return info

    def _set_rating(self, chesscom):
        exists = self.get_exists(chesscom)
        if exists is None or not exists:
            return None

        url = f'https://api.chess.com/pub/player/{chesscom}/stats'
        with urllib.request.urlopen(url) as response:
            info = response.read()
        info = json.loads(info)
        names = {
            'rapid': ['chess_rapid', 'last', 'rating'],
            'blitz': ['chess_blitz', 'last', 'rating'],
            'bullet': ['chess_bullet', 'last', 'rating'],
            'rapid_last': ['chess_rapid', 'last', 'date'],
            'blitz_last': ['chess_blitz', 'last', 'date'],
            'bullet_last': ['chess_bullet', 'last', 'date'],
        }
        output = {}
        for k, v in names.items():
            try:
                val = info[v[0]][v[1]][v[2]]# if not KeyError else None
            except KeyError as e:
                val = None
            output[k] = val
        output['rating_time'] = time.time()
        return output

    def set_rating(self, chesscom):
        exists = self.get_exists(chesscom)
        if exists is None or not exists:
            return None

        sql = '''
        UPDATE chess SET
            rapid = ?,
            blitz = ?,
            bullet = ?,
            rapid_last = ?,
            blitz_last = ?,
            bullet_last = ?,
            rating_time = ?
        WHERE chess.chesscom = ?
        ;'''
        info = self._set_rating(chesscom)
        params = (
            info['rapid'],
            info['blitz'],
            info['bullet'],
            info['rapid_last'],
            info['blitz_last'],
            info['bullet_last'],
            info['rating_time'],
            chesscom,
        )
        self.cur.execute(sql, params)
        self.conn.commit()
        return info

    def get_rating(self, chesscom):
        exists = self.get_exists(chesscom)
        if exists is None or not exists:
            return None

        sql = '''
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
        ;'''
        params = (chesscom,)
        df = pd.read_sql_query(sql, self.conn, params=params)

        if len(df) == 0 or (time.time()-df['rating_time'][0] > self.wait_time):
            return self.set_rating(chesscom)
        else:
            info = {str(c): df[c][0] for c in df.columns}
            return info

class LeagueDatabase:

    def __init__(self, path=LEAGUE_DB):
        self.path = path
        self.conn = sqlite3.connect(self.path)
        self.cur = self.conn.cursor()
        self.conn.execute('PRAGMA foreign_keys = 1')
        self.conn.commit()
        self.init_tables()
        self.init_season()
        self.chess_db = ChesscomDatabase()

    def quit(self):
        self.conn.close()

    def get_all_tables(self):
        sql = "SELECT name FROM sqlite_master WHERE type='table';"
        output = self.conn.execute(sql)
        df_dict = {}
        for table in output.fetchall():
            df = pd.read_sql_query(f'SELECT * FROM {table[0]}', self.conn)
            df_dict[table[0]] = df
        return df_dict

    def init_tables(self):

        # Users, id is their discord id
        user_tbl_sql = '''
        CREATE TABLE IF NOT EXISTS user(
            id integer PRIMARY KEY,
            discord_id integer NOT NULL UNIQUE,
            discord_name text NOT NULL,
            chesscom text
        );'''

        # Seasons, name is the name of the season
        season_tbl_sql = '''
        CREATE TABLE IF NOT EXISTS season(
            id integer PRIMARY KEY,
            name text NOT NULL UNIQUE
        );'''

        # Teams, name is the name of the team. team 'signup' is not yet assigned
        team_tbl_sql = '''
        CREATE TABLE IF NOT EXISTS team(
            id integer PRIMARY KEY,
            season_id integer NOT NULL REFERENCES season(id),
            name text NOT NULL,
            UNIQUE(season_id, name)
        );'''

        # Membership, bridge table from users to teams (many to many)
        member_tbl_sql = '''
        CREATE TABLE IF NOT EXISTS member(
            id integer PRIMARY KEY,
            user_id integer NOT NULL REFERENCES user(id),
            team_id integer NOT NULL REFERENCES team(id) ON UPDATE CASCADE,
            is_player integer NOT NULL,
            UNIQUE(user_id, team_id)
        );'''

        # Weeks per-league
        week_tbl_sql = '''
        CREATE TABLE IF NOT EXISTS week(
            id integer PRIMARY KEY,
            season_id text NOT NULL REFERENCES season(id),
            num integer NOT NULL,
            UNIQUE(season_id, num)
        );'''

        # Seed per-player
        seed_tbl_sql = '''
        CREATE TABLE IF NOT EXISTS seed(
            id integer NOT NULL PRIMARY KEY,
            week_id integer NOT NULL REFERENCES week(id),
            member_id integer NOT NULL REFERENCES member(id) ON DELETE CASCADE,
            sub_member_id integer NOT NULL REFERENCES member(id),
            request integer NOT NULL DEFAULT 0,
            note text,
            UNIQUE(week_id, member_id)
        );'''

        # Games per-week
        game_tbl_sql = '''
        CREATE TABLE IF NOT EXISTS game(
            id integer NOT NULL PRIMARY KEY,
            white_seed_id integer NOT NULL REFERENCES seed(id),
            black_seed_id integer NOT NULL REFERENCES seed(id),
            schedule text,
            result integer,
            url text,
            UNIQUE(white_seed_id, black_seed_id)
        );'''

        # TODO: Force users in a game to also be in the season

        queries = [
            user_tbl_sql,
            season_tbl_sql,
            team_tbl_sql,
            member_tbl_sql,
            week_tbl_sql,
            seed_tbl_sql,
            game_tbl_sql,
        ]
        for query in queries:
            self.cur.execute(query)
        self.conn.commit()

    def init_season(self):
        season_sql = '''
        INSERT OR IGNORE INTO season(name) VALUES(?)
        ;'''

        week_sql = '''
        INSERT OR IGNORE INTO week(season_id, num)
        VALUES((SELECT id FROM season WHERE name = ?), ?)
        ;'''

        signup_sql = '''
        INSERT OR IGNORE INTO team(season_id, name)
        VALUES((SELECT id FROM season WHERE name = ?), ?)
        ;'''

        months = [self.get_month(i) for i in range(12)]
        for month in months:
            self.cur.execute(season_sql, (month,))
            for i in range(1, 5):
                self.cur.execute(week_sql, (month, i))

            self.cur.execute(signup_sql, (month, SIGNUP_TEAM))

        self.conn.commit()

    def get_league_info(self, discord_id):
        season_name = self.get_month(0)
        sql = '''
        WITH season_ids AS (SELECT s.id FROM season AS s WHERE s.name = ?),
        user_subset AS (SELECT u.* FROM user AS u WHERE u.discord_id = ?)
        SELECT u.discord_name, u.chesscom, mb.is_player, wk.num, sd.request
        FROM seed AS sd
        LEFT JOIN week AS wk ON sd.week_id = wk.id
        LEFT JOIN member AS mb ON sd.member_id = mb.id
        LEFT JOIN user_subset AS u ON mb.user_id = u.id
        WHERE u.id IS NOT NULL
        ;'''
        params = (season_name, discord_id)
        df = pd.read_sql_query(sql, self.conn, params=params)
        df = df.pivot(
            index=['discord_name', 'chesscom', 'is_player'],
            columns='num',
            values='request',
        )
        df.columns = df.columns.get_level_values(0)
        yes_no_dict = {0: 'No', 1: 'Yes'}
        substitute_dict = {0: 'Substitute', 1: 'Player'}
        df_dict = {
            'Rapid Rating': [self.chess_db.get_rating(row[1])['rapid'] for row in df.index],
            'Discord Name': [row[0] for row in df.index],
            'Chesscom Name': [row[1] for row in df.index],
            'Role': [substitute_dict[row[2]] for row in df.index],
            'Requested Sub Week 1': [
                yes_no_dict[row._1] for row in df.itertuples()],
            'Requested Sub Week 2': [
                yes_no_dict[row._2] for row in df.itertuples()],
            'Requested Sub Week 3': [
                yes_no_dict[row._3] for row in df.itertuples()],
            'Requested Sub Week 4': [
                yes_no_dict[row._4] for row in df.itertuples()],
        }
        df = pd.DataFrame(df_dict)
        cols = [f'Requested Sub Week {i}' for i in range(1, 5)]
        for col in cols:
            df[col] = [
                c if r == 'Player' else '-'
                for c, r in zip(df[col], df['Role'])
            ]
        df = df.sort_values(
            by=['Role', 'Rapid Rating', 'Discord Name', 'Chesscom Name'],
            ignore_index=True,
        )
        return df

    def set_chesscom(self, discord_id, discord_name, chesscom):
        sql = '''
        INSERT INTO user(discord_id, discord_name, chesscom)
        VALUES(?, ?, ?)
        ON CONFLICT(discord_id) DO
        UPDATE SET discord_name = ?, chesscom = ?
        ;'''
        params = (discord_id, discord_name, chesscom, discord_name, chesscom)
        self.cur.execute(sql, params)
        self.conn.commit()

    def get_user_data(self, discord_id):
        sql = '''
        SELECT * FROM user WHERE discord_id = ?
        ;'''
        params = (discord_id,)
        df = pd.read_sql_query(sql, self.conn, params=params)
        return df

    def request_sub(self, season_name, week_num, discord_id):
        week_num = int(week_num)
        assert week_num in [1, 2, 3, 4]
        sql = '''
        WITH season_ids AS (SELECT s.id FROM season AS s WHERE s.name = ?),
        team_ids AS (SELECT t.id FROM team AS t WHERE t.season_id IN season_ids),
        user_ids AS (SELECT u.id FROM user AS u WHERE u.discord_id = ?),
        member_ids AS (
            SELECT m.id FROM member AS m
            WHERE m.team_id IN team_ids AND m.user_id IN user_ids
        ),
        week_ids AS (
            SELECT w.id FROM week AS w
            WHERE w.num = ? AND w.season_id IN season_ids
        )

        UPDATE seed SET request = 1
        WHERE seed.member_id IN member_ids
        AND seed.week_id IN week_ids
        ;'''
        params = (season_name, discord_id, week_num)
        self.cur.execute(sql, params)
        self.conn.commit()

    def set_game(self, season_name, week_num, w_discord_id, b_discord_id):
        sql = '''
        WITH season_ids AS (SELECT s.id FROM season AS s WHERE s.name = ?),
        team_ids AS (SELECT t.id FROM team AS t WHERE t.season_id IN season_ids),
        white_user_ids AS (SELECT u.id FROM user AS u WHERE u.discord_id = ?),
        black_user_ids AS (SELECT u.id FROM user AS u WHERE u.discord_id = ?),
        white_member_ids AS (
            SELECT m.id FROM member AS m
            WHERE m.team_id IN team_ids AND m.user_id IN white_user_ids
        ),
        black_member_ids AS (
            SELECT m.id FROM member AS m
            WHERE m.team_id IN team_ids AND m.user_id IN black_user_ids
        ),
        week_ids AS (
            SELECT w.id FROM week AS w
            WHERE w.num = ? AND w.season_id IN season_ids
        ),
        seed_subset AS (
            SELECT s.id, s.member_id FROM seed AS s
            WHERE s.week_id IN week_ids
        ),

        INSERT INTO game(white_seed_id, black_seed_id)
        VALUES(
            (SELECT s.id FROM seed_subset AS s WHERE s.member_id IN white_member_ids),
            (SELECT s.id FROM seed_subset AS s WHERE s.member_id IN black_member_ids),
        )
        ;'''

        params = (season_name, w_discord_id, b_discord_id, week_num)
        self.cur.execute(sql, params)
        self.conn.commit()

    def get_all_games(self, season_name, discord_id):
        sql = '''
        WITH season_ids AS (SELECT s.id FROM season AS s WHERE s.name = ?),
        team_ids AS (SELECT t.id FROM team AS t WHERE t.season_id IN season_ids),
        user_ids AS (SELECT u.id FROM user AS u WHERE u.discord_id = ?),
        member_ids AS (
            SELECT m.id FROM member AS m
            WHERE m.team_id IN team_ids AND m.user_id IN user_ids
        ),
        week_ids AS (SELECT w.id FROM week AS w WHERE w.season_id IN season_ids),
        seed_ids AS (
            SELECT s.id FROM seed AS s
            WHERE s.week_id IN week_ids AND s.user_id IN user_ids
        )
        SELECT g.* FROM game AS g
        WHERE g.black_seed_id IN seed_ids
        OR g.white_seed_id IN seed_ids
        ;'''
        params = (season_name, discord_id)
        df = pd.read_sql_query(sql, self.conn, params=params)
        return df

    def league_player_to_sub(self, season_name, discord_id):
        sql = '''
        WITH season_ids AS (SELECT s.id FROM season AS s WHERE s.name = ?),
        team_ids AS (SELECT t.id FROM team AS t WHERE t.season_id IN season_ids),
        user_ids AS (SELECT u.id FROM user AS u WHERE u.discord_id = ?)

        UPDATE member SET is_player = 0
        WHERE member.user_id IN user_ids
        AND member.team_id IN team_ids
        ;'''
        params = (season_name, discord_id)
        self.cur.execute(sql, params)
        self.conn.commit()

    def league_leave(self, season_name, discord_id):
        sql = '''
        WITH season_ids AS (SELECT s.id FROM season AS s WHERE s.name = ?),
        team_ids AS (SELECT t.id FROM team AS t WHERE t.season_id IN season_ids),
        user_ids AS (SELECT u.id FROM user AS u WHERE u.discord_id = ?)

        DELETE FROM member AS m
        WHERE m.user_id IN user_ids
        AND m.team_id in team_ids
        ;'''

        # If user has a game, just make them a sub
        params = (season_name, discord_id)
        try:
            self.cur.execute(sql, params)
            self.conn.commit()
        except sqlite3.IntegrityError as e:
            print(e)
            self.league_player_to_sub(season_name, discord_id)

    def league_join(
        self, season_name, discord_id, is_player, team=SIGNUP_TEAM, sub_week=None):

        # Default team is signup
        sql = '''
        WITH season_ids AS (SELECT s.id FROM season AS s WHERE s.name = ?),
        team_ids AS (
            SELECT t.id FROM team AS t
            WHERE t.season_id IN season_ids AND t.name = ?
        )

        INSERT OR REPLACE INTO member(user_id, team_id, is_player)
        VALUES(
            (SELECT id FROM user WHERE discord_id = ?),
            (SELECT * FROM team_ids),
            ?)
        ;'''
        params = (season_name, team, discord_id, is_player)
        try:
            self.cur.execute(sql, params)
        except sqlite3.IntegrityError as e:
            return

        # Create seeds for the player
        sql = '''
        WITH season_ids AS (SELECT s.id FROM season AS s WHERE s.name = ?),
        team_ids AS (
            SELECT t.id FROM team AS t
            WHERE t.season_id IN season_ids AND t.name = ?
        ),
        user_ids AS (SELECT id FROM user WHERE discord_id = ?),
        member_ids AS (
            SELECT m.id FROM member AS m
            WHERE m.team_id IN team_ids AND m.user_id IN user_ids
        ),
        week_ids AS (
            SELECT id FROM week WHERE num = ? AND season_id IN season_ids
        )

        INSERT INTO seed(week_id, member_id, sub_member_id)
        VALUES(
            (SELECT * FROM week_ids),
            (SELECT * FROM member_ids),
            (SELECT * FROM member_ids)
        )
        ON CONFLICT(week_id, member_id) DO UPDATE SET request = 0
        WHERE member_id IN member_ids AND week_id IN week_ids
        ;'''
        for i in range(1, 5):
            params = (season_name, team, discord_id, i)
            self.cur.execute(sql, params)
        self.conn.commit()

        if sub_week is not None:
            self.request_sub(season_name, sub_week, discord_id)

    def get_month(self, next=0):
        date = datetime.datetime.now()
        for i in range(next):
            delta = datetime.timedelta(32 - date.day)
            date = date + delta
            date = date.replace(day=1)

        date_string = date.strftime('%Y%B')
        return date_string

    def get_team_names(self, season_name):
        # Grab team names
        sql = '''
        WITH season_ids AS (SELECT s.id FROM season AS s WHERE s.name = ?)
        SELECT t.name FROM team AS t WHERE t.season_id IN season_ids
        ;'''
        params = (season_name,)
        df = pd.read_sql_query(sql, self.conn, params=params)
        team_names = [n for n in df['name'] if n != SIGNUP_TEAM]
        return team_names

    def get_team_members(self, season_name, team, get_subs=False):
        # Then grab users to split into teams
        sql = f'''
        WITH season_ids AS (SELECT s.id FROM season AS s WHERE s.name = ?),
        team_ids AS (
            SELECT t.id FROM team AS t
            WHERE t.season_id IN season_ids
            AND t.name = ?
        ),
        user_ids AS (
            SELECT m.user_id FROM member AS m
            WHERE m.team_id IN team_ids
            AND m.is_player = {int(not get_subs)}
        )

        SELECT u.id, u.discord_name, u.chesscom FROM user AS u WHERE u.id IN user_ids
        ;'''
        params = (season_name, team)
        df = pd.read_sql_query(sql, self.conn, params=params)
        return df

    def reset_teams(self, season_name, assign_sub=False):
        # Then grab users to split into teams
        sql = f'''
        WITH season_ids AS (SELECT s.id FROM season AS s WHERE s.name = ?),
        team_ids AS (
            SELECT t.id FROM team AS t
            WHERE t.season_id IN season_ids
        ),
        user_ids AS (
            SELECT m.user_id FROM member AS m
            WHERE m.team_id IN team_ids
            AND m.is_player = {int(not assign_sub)}
        )

        SELECT u.id, u.chesscom FROM user AS u WHERE u.id IN user_ids
        ;'''
        params = (season_name,)
        df = pd.read_sql_query(sql, self.conn, params=params)

        # Update sql
        sql = '''
        UPDATE member
        SET team_id = (SELECT t.id FROM team AS t WHERE t.name = ?)
        WHERE member.user_id = ?
        ;'''
        dfs = {SIGNUP_TEAM: df}
        for team_name, df in dfs.items():
            for row in df.itertuples():
                params = (team_name, row.id)
                self.cur.execute(sql, params)
        self.conn.commit()

    def assign_teams(self, season_name, assign_sub=False):
        team_names = self.get_team_names(season_name)
        df = self.get_team_members(season_name, SIGNUP_TEAM, assign_sub)

        # Assign users to teams
        ratings = [self.chess_db.get_rating(c)['rapid'] for c in df['chesscom']]
        df['rating'] = ratings
        dfs = even_split(df, team_names, verbose=True)

        # Update sql
        sql = '''
        UPDATE member
        SET team_id = (SELECT t.id FROM team AS t WHERE t.name = ?)
        WHERE member.user_id = ?
        ;'''
        for team_name, df in dfs.items():
            for row in df.itertuples():
                params = (team_name, row.id)
                self.cur.execute(sql, params)
        self.conn.commit()

    def seed_games(self, season_name, week_num):
        rant_df = self.get_team_members(season_name, 'Team Rants')
        rant_df['rating'] = [
            self.chess_db.get_rating(row.chesscom)['rapid']
            for row in rant_df.itertuples()
        ]
        nort_df = self.get_team_members(season_name, 'Team No Rants')
        nort_df['rating'] = [
            self.chess_db.get_rating(row.chesscom)['rapid']
            for row in nort_df.itertuples()
        ]
        rant_df = rant_df.sort_values(by=['rating'], ignore_index=True)
        nort_df = nort_df.sort_values(by=['rating'], ignore_index=True)
        print(rant_df)
        print(nort_df)

        sql = '''
        WITH season_ids AS (SELECT s.id FROM season AS s WHERE s.name = ?),
        team_ids AS (SELECT t.id FROM team AS t WHERE t.season_id IN season_ids),
        white_member_ids AS (
            SELECT m.id FROM member AS m
            WHERE m.team_id IN team_ids AND m.user_id = ?
        ),
        black_member_ids AS (
            SELECT m.id FROM member AS m
            WHERE m.team_id IN team_ids AND m.user_id = ?
        ),
        week_ids AS (
            SELECT w.id FROM week AS w
            WHERE w.num = ? AND w.season_id IN season_ids
        )

        INSERT INTO game(white_seed_id, black_seed_id)
        VALUES(
            (
                SELECT s.id FROM seed AS s
                WHERE s.member_id IN white_member_ids
                AND s.week_id in week_ids
            ),
            (
                SELECT s.id FROM seed AS s
                WHERE s.member_id IN black_member_ids
                AND s.week_id in week_ids
            )
        )
        ;'''

        for i in range(len(nort_df)):
            both_ids = [rant_df['id'][i], nort_df['id'][i]]
            np.random.shuffle(both_ids)
            white_id, black_id = int(both_ids[0]), int(both_ids[1])
            params = (season_name, white_id, black_id, 1)
            print(params)
            self.cur.execute(sql, params)
        self.conn.commit()

    def get_week_games(self, season_name, week_num):
        sql = '''
        WITH season_ids AS (SELECT s.id FROM season AS s WHERE s.name = ?),
        team_ids AS (SELECT t.id FROM team AS t WHERE t.season_id IN season_ids),
        user_ids AS (SELECT u.id FROM user AS u),
        member_ids AS (
            SELECT m.id FROM member AS m
            WHERE m.team_id IN team_ids AND m.user_id IN user_ids
        ),
        week_ids AS (
            SELECT w.id FROM week AS w
            WHERE w.season_id IN season_ids AND w.num = ?
        ),
        seed_ids AS (
            SELECT s.id FROM seed AS s
            WHERE s.week_id IN week_ids AND s.sub_member_id IN member_ids
        )
        SELECT
            g.schedule,
            g.result,
            g.url,
            wu.discord_id,
            wu.discord_name,
            wu.chesscom,
            bu.discord_id,
            bu.discord_name,
            bu.chesscom
        FROM game AS g
        LEFT JOIN seed AS ws ON g.white_seed_id = ws.id
        LEFT JOIN member AS wm ON ws.member_id = wm.id
        LEFT JOIN user AS wu ON wm.user_id = wu.id
        LEFT JOIN seed AS bs ON g.black_seed_id = bs.id
        LEFT JOIN member AS bm ON bs.member_id = bm.id
        LEFT JOIN user AS bu ON bm.user_id = bu.id
        WHERE g.black_seed_id IN seed_ids OR g.white_seed_id IN seed_ids
        ;'''
        params = (season_name, week_num)
        df = pd.read_sql_query(sql, self.conn, params=params)
        df.columns = [
            'schedule',
            'result',
            'url',
            'white_discord_id',
            'white_discord_name',
            'white_chesscom',
            'black_discord_id',
            'black_discord_name',
            'black_chesscom',
        ]
        df['white_rapid_rating'] = [
            self.chess_db.get_rating(c)['rapid']
            for c in df['white_chesscom']
        ]
        df['black_rapid_rating'] = [
            self.chess_db.get_rating(c)['rapid']
            for c in df['black_chesscom']
        ]
        to_sheet = df[[
            'white_discord_name',
            'white_chesscom',
            'white_rapid_rating',
            'black_rapid_rating',
            'black_chesscom',
            'black_discord_name',
            'schedule',
            'result',
            'url',
        ]]
        df_to_sheet(to_sheet, sheet=2)
        return df

    def set_team_names(self, season_name, team_names):
        sql = '''
        INSERT OR IGNORE INTO team(season_id, name)
        VALUES(
            (SELECT s.id FROM season AS s WHERE s.name = ?),
            ?
        );'''
        for team_name in team_names:
            params = (season_name, team_name)
            self.cur.execute(sql, params)
        self.conn.commit()

    def update_signup_info(self, season_name):
        sql = '''
        WITH season_ids AS (SELECT s.id FROM season AS s WHERE s.name = ?)
        SELECT u.discord_name, u.chesscom, mb.is_player, wk.num, sd.request
        FROM seed AS sd
        LEFT JOIN week AS wk ON sd.week_id = wk.id
        LEFT JOIN member AS mb ON sd.member_id = mb.id
        LEFT JOIN user AS u ON mb.user_id = u.id
        ;'''
        params = (season_name,)
        df = pd.read_sql_query(sql, self.conn, params=params)
        df = df.pivot(
            index=['discord_name', 'chesscom', 'is_player'],
            columns='num',
            values='request',
        )
        df.columns = df.columns.get_level_values(0)
        yes_no_dict = {0: 'No', 1: 'Yes'}
        substitute_dict = {0: 'Substitute', 1: 'Player'}
        df_dict = {
            'Rapid Rating': [self.chess_db.get_rating(row[1])['rapid'] for row in df.index],
            'Discord Name': [row[0] for row in df.index],
            'Chesscom Name': [row[1] for row in df.index],
            'Role': [substitute_dict[row[2]] for row in df.index],
            'Requested Sub Week 1': [
                yes_no_dict[row._1] for row in df.itertuples()],
            'Requested Sub Week 2': [
                yes_no_dict[row._2] for row in df.itertuples()],
            'Requested Sub Week 3': [
                yes_no_dict[row._3] for row in df.itertuples()],
            'Requested Sub Week 4': [
                yes_no_dict[row._4] for row in df.itertuples()],
        }
        df = pd.DataFrame(df_dict)
        cols = [f'Requested Sub Week {i}' for i in range(1, 5)]
        for col in cols:
            df[col] = [
                c if r == 'Player' else '-'
                for c, r in zip(df[col], df['Role'])
            ]
        df = df.sort_values(
            by=['Role', 'Rapid Rating', 'Discord Name', 'Chesscom Name'],
            ignore_index=True,
        )

        df_to_sheet(df)
        return df

def gen_split_score(df, splits):
    df_splits = [df.iloc[s] for s in splits]
    mean_score = np.mean([
        np.linalg.norm(d['rating'].mean() - df['rating'].mean())
        for d in df_splits
    ])
    std_score = np.mean([
        np.linalg.norm(d['rating'].std() - df['rating'].std())
        for d in df_splits
    ])
    score = mean_score + std_score
    return score

def even_split(df, team_names, iter_time=10, verbose=False):
    num_teams = len(team_names)
    df = df.reset_index(drop=True)
    inds = list(range(len(df)))
    np.random.shuffle(inds)
    splits = [[] for _ in range(num_teams)]
    for r, i in enumerate(inds):
        splits[r % num_teams].append(i)
    np.random.shuffle(splits)

    score = gen_split_score(df, splits)
    start = time.time()
    while time.time() - start < iter_time:

        # Shuffle splits, preserve old splits if things go wrong
        old_splits = [[i for i in j] for j in splits]
        old_score = score
        for _ in range(101):
            a, b = [int(i) for i in random.sample(range(num_teams), 2)]
            a_ind = int(random.sample(range(len(splits[a])), 1)[0])
            b_ind = int(random.sample(range(len(splits[b])), 1)[0])
            splits[a][a_ind], splits[b][b_ind] = splits[b][b_ind], splits[a][a_ind]

            new_score = gen_split_score(df, splits)
            if new_score < score:
                if verbose:
                    print(score, new_score)
                score = new_score
                for s in splits:
                    if verbose:
                        print(list(df.iloc[s].sort_values(
                            by='rating', ignore_index=True)['rating']))
                if verbose:
                    print()
                new_splits = [[i for i in j] for j in splits]
        if score < old_score:
            splits = new_splits
        else:
            splits = old_splits

    if score < old_score:
        splits = new_splits
    else:
        splits = old_splits
    dfs = {t: df.iloc[s] for t, s in zip(team_names, splits)}
    return dfs

'''
    history = 'data/discord_history.parquet'
    df = pd.read_parquet(history)
    df = df[[row.text.startswith('!') for row in df.itertuples()]]
    df = df.sort_values(by='date', ignore_index=True)
    df['date'] = [d.tz_convert(None) for d in df['date']]
    df = df[[not t.startswith('!help') for t in df['text']]]
    df = df[[not t.startswith('!league_info') for t in df['text']]]
    df = df[df['name'] != 'pawngrubber#1621']
    df['text'] = [' '.join([t.split(' ')[0].lower()] + t.split(' ')[1:]) for t in df['text']]
    df = df.reset_index(drop=True)

    skip_prefixes = [
        '!rapid',
        '!sunglasses',
        '!calendar',
        '!hello',
        '!!',
        '!list',
        '!league_info',

        '!set_chesscom',
        '!link_chesscom',
        '!mod_set_chesscom', # revisit?
        '!join_rapid_league',
        '!league_join',
        '!join',
        '!lhelp',
        '!leave',
        '!league_leave',
        '!league_request_substitute',
    ]
    unique = list(np.unique(df['text']))
    for s in skip_prefixes:
        unique = [u for u in unique if not u.startswith(s)]

    e_text = ''
    LDB = LeagueDatabase()

    for row in tqdm(df.itertuples(), total=len(df)):
        split = row.text.split(' ')
        if row.text.startswith('!set_chesscom') or row.text.startswith('!link_chesscom'):
            if len(split) > 1:
                if exists_chesscom(split[1]):
                    LDB.set_chesscom(row.id, row.name, split[1])

        split = row.text.split(' ')
        ps = ['player', 'substitute']
        if split[0].startswith('!join_rapid_league'):
            if len(split) > 1 and split[1] in ps:
                LDB.league_join(LDB.get_month(0), row.id, (split[1] == 'player'))
                if len(split) > 2 and split[2] in ['1', '2', '3', '4']:
                    LDB.league_join(LDB.get_month(0), row.id,
                        (split[1] == 'player'), sub_week=int(split[2]),
                    )

        if split[0].startswith('!league_join'):
            if len(split) > 1 and split[1] in ps:
                LDB.league_join(LDB.get_month(0), row.id, (split[1] == 'player'))

        if split[0].startswith('!leave') or split[0].startswith('!league_leave'):
            LDB.league_leave(LDB.get_month(0), row.id)

        if split[0].startswith('!league_request_substitute'):
            if len(split) > 1 and split[1] in list('1234'):
                LDB.request_sub(LDB.get_month(0), int(split[1]), row.id)


    pprint(unique)
    print()
    LDB.set_team_names(LDB.get_month(0), ['Team Rants', 'Team No Rants'])
    #LDB.assign_teams(LDB.get_month(0))
    #LDB.league_leave(LDB.get_month(0), 80632841108463616)
    df_dict = LDB.get_all_tables()
    for name, df in df_dict.items():
        print(df)
        print(name)
        print()
    LDB.update_signup_info(LDB.get_month(0))
    print(LDB.get_user_data(841679471789473803))
    print(LDB.get_user_data(1234))
    raise Exception
'''

if __name__ == '__main__':
    LDB = LeagueDatabase()
    #LDB.reset_teams(LDB.get_month(NEXT_MONTH), assign_sub=True)
    #LDB.reset_teams(LDB.get_month(NEXT_MONTH), assign_sub=False)
    #LDB.assign_teams(LDB.get_month(NEXT_MONTH), assign_sub=False)
    #LDB.assign_teams(LDB.get_month(NEXT_MONTH), assign_sub=True)
    #LDB.seed_games(LDB.get_month(NEXT_MONTH), 1)
    '''
    print(LDB.get_week_games(LDB.get_month(NEXT_MONTH), 1))
    '''
    season_name = LDB.get_month(NEXT_MONTH)
    rant_df = LDB.get_team_members(season_name, 'Team Rants', get_subs=False)
    nort_df = LDB.get_team_members(season_name, 'Team No Rants', get_subs=False)
    rant_sub_df = LDB.get_team_members(season_name, 'Team Rants', get_subs=True)
    nort_sub_df = LDB.get_team_members(season_name, 'Team No Rants', get_subs=True)
    print(rant_df)
    print()
    print(nort_df)
    print()
    print(rant_sub_df)
    print()
    print(nort_sub_df)
    print()
    '''
    df_dict = LDB.get_all_tables()
    for name, df in df_dict.items():
        print(df)
        print(name)
        print()
    '''
