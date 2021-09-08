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
import chessdotcom as cdc
import chess
import chess.pgn
import re

import funcs_chesscom as fcc
import funcs_general as fgg

# TODO: set iter time to be higher

GENERAL_ERROR_MESSAGE = '{} `!{}` error, get help with `!help {}`'

# TODO: Change this to 1
NEXT_MONTH = 0
LEAGUE_DB = 'data/rapid_league.sqlite3'
SIGNUP_TEAM = 'signup'

class LeagueDatabase:

    def __init__(self, path=LEAGUE_DB):
        self.path = path
        self.conn = sqlite3.connect(self.path)
        self.cur = self.conn.cursor()
        self.conn.execute('PRAGMA foreign_keys = 1')
        self.conn.commit()
        self.init_tables()
        self.init_season()
        self.chess_db = fcc.ChesscomDatabase()

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

        months = [fgg.get_month(i) for i in range(12)]
        for month in months:
            self.cur.execute(season_sql, (month,))
            for i in range(1, 5):
                self.cur.execute(week_sql, (month, i))

            self.cur.execute(signup_sql, (month, SIGNUP_TEAM))

        self.conn.commit()

    def get_league_info(self, season_name, discord_id):
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

    def get_games_by_week(self, season_name, week_num):
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
            g.id,
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
            'game_id',
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

        fgg.df_to_sheet(df, sheet=0)
        return df

    def set_result(self, game_id, result, url=None):
        sql = '''
        UPDATE game SET result = ?, url = ? WHERE game.id = ?
        ;'''
        params = (result, url, game_id)
        self.cur.execute(sql, params)
        self.conn.commit()

    def get_game_by_id(self, game_id):
        sql = '''
        SELECT
            g.id,
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
        WHERE g.id = ?
        ;'''
        params = (game_id,)
        df = pd.read_sql_query(sql, self.conn, params=params)
        df.columns = [
            'game_id',
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

def read_history():
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

    def title_to_game_id(title):
        season_name = fgg.get_month(NEXT_MONTH)
        df = LDB.get_games_by_week(season_name, 1)

        id_dict = {
            (
                row.white_discord_name.replace('#', ''),
                row.black_discord_name.replace('#', ''),
            ): row.game_id for row in df.itertuples()
        }

        split = title.split(' ')
        white_name = split[2]
        black_name = split[4]
        key = (white_name, black_name)
        game_id = id_dict[key]
        return game_id

    import funcs_discord as fdd
    class User:
        def __init__(self, mention, id_):
            self.id = id_
            self.mention = mention
    for row in tqdm(df.itertuples(), total=len(df)):
        split = row.text.split(' ')
        if split[0].startswith('!league_set_result') and len(split) > 1:
            try:
                game_id = title_to_game_id(row.channel)
            except KeyError:
                continue
            except IndexError:
                continue
            try:
                elem = {
                    'mention': 'me',
                    'user': User('you', row.discord_id),
                    'game_id': game_id,
                    'url': split[1]
                }
            except IndexError as e:
                continue
            pprint(elem)
            msg = fdd.league_set_result(**elem)
            print(msg)
            print()
            #LDB.set_result(**elem)
        '''
        if row.text.startswith('!set_chesscom') or row.text.startswith('!link_chesscom'):
            if len(split) > 1:
                if exists_chesscom(split[1]):
                    LDB.set_chesscom(row.id, row.name, split[1])

        split = row.text.split(' ')
        ps = ['player', 'substitute']
        if split[0].startswith('!join_rapid_league'):
            if len(split) > 1 and split[1] in ps:
                LDB.league_join(fgg.get_month(0), row.id, (split[1] == 'player'))
                if len(split) > 2 and split[2] in ['1', '2', '3', '4']:
                    LDB.league_join(fgg.get_month(0), row.id,
                        (split[1] == 'player'), sub_week=int(split[2]),
                    )

        if split[0].startswith('!league_join'):
            if len(split) > 1 and split[1] in ps:
                LDB.league_join(fgg.get_month(0), row.id, (split[1] == 'player'))

        if split[0].startswith('!leave') or split[0].startswith('!league_leave'):
            LDB.league_leave(fgg.get_month(0), row.id)

        if split[0].startswith('!league_request_substitute'):
            if len(split) > 1 and split[1] in list('1234'):
                LDB.request_sub(fgg.get_month(0), int(split[1]), row.id)
        '''

    pprint(unique)
    print()
    df_dict = LDB.get_all_tables()
    for name, df in df_dict.items():
        print(df)
        print(name)
        print()
    raise Exception
    #LDB.set_team_names(fgg.get_month(0), ['Team Rants', 'Team No Rants'])
    #LDB.assign_teams(fgg.get_month(0))
    #LDB.league_leave(fgg.get_month(0), 80632841108463616)

def backup_databases():
    pass

if __name__ == '__main__':
    #read_history()
    LDB = LeagueDatabase()
    season_name = fgg.get_month(NEXT_MONTH)
    print(LDB.get_games_by_week(season_name, 1))
    #raise Exception
    '''
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
    LDB.league_leave(season_name, 878672511783567422)
    '''
    df_dict = LDB.get_all_tables()
    for name, df in df_dict.items():
        print(df)
        print(name)
        print()

    '''
    week_num = 1
    discord_id = 490529572908171284
    url = 'https://www.chess.com/analysis/game/live/24460856009?tab=review'
    LDB.league_set_url(season_name, week_num, discord_id, url)
    '''
    #fcc.get_game_history('pawngrubber')
