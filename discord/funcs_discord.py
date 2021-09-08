from discord.ext import commands
from typing import Optional
from pprint import pformat, pprint
from dotenv import load_dotenv
from tqdm import tqdm
import discord
import pandas as pd
import logging
import datetime
import os
import numpy as np
import re

import funcs_general as fgg
import funcs_chesscom as fcc
import funcs_league as flg

pd.options.mode.chained_assignment = None

DISCORD_USERS_PARQUET = 'data/discord_users.parquet'
DISCORD_HISTORY_PARQUET = 'data/discord_history.parquet'
LOG_FILE = 'data/grubberbot.log'
GRUBBER_MENTION = '<@490529572908171284>'
GUILD_NAME = "pawngrubber's server"

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
)

GENERAL_ERROR_MESSAGE = '{mention} `!{name}` error, get help with `!help {name}`'
TIME_CONTROL = '900+10'
MODERATOR_ROLES = [
    'The Grubber',
    'Mods',
    'Cool People',
]
WHITE_RESULTS_CODES = {
    'win': 1,
    'checkmated': -1,
    'agreed': 0,
    'repetition': 0,
    'timeout': -1,
    'resigned': -1,
    'stalemate': 0,
    'lose': -1,
    'insufficient': 0,
    '50move': 0,
    'abandoned': -1,
    'kingofthehill': -1,
    'threecheck': -1,
    'timevsinsufficient': 0,
    'bughousepartnerlose': -1
}
DISPLAY_RESULT = {
    1: '1-0',
    0: '1/2-1/2',
    -1: '0-1',
}

LDB = flg.LeagueDatabase()

def gen_chesscom_username_error(mention, user_mention, mod=False):
    if mod:
        error_command = '!mod_set_chesscom'
    else:
        error_command = '!set_chesscom'
    message = (
        f'{mention} User {user_mention} has not yet linked to chesscom, '
        f' use {error_command}'
    )
    return message

def on_command_error(ctx, exception):
    user = ctx.message.author
    mention = user.mention

    # If user doesn't have permissions
    if isinstance(exception, commands.errors.CheckFailure):
        message = 'You do not have the correct role for this command'
        logging_message = f'{user} || {ctx.message.clean_content} || {message}'
        print(logging_message)
        logging.error(logging_message)
        return message

    # Parse out the command from the message
    command_name = ctx.message.content.split(' ')[0][1:]
    message = f'Error, use `!help {command_name}`'

    logging_message = (
        f'{datetime.datetime.now()} || {user} '
        f'|| {ctx.message.clean_content} || {message} || {exception}'
    )
    print(logging_message)
    logging.error(logging_message)

    # TODO: give user list of commands
    return message

    # Send help message and log things
    message = GENERAL_ERROR_MESSAGE.format_map({
        'mention': ctx.message.author.mention,
        'name': command_name
    })
    logging_message = '\n'.join([
        f'MESSAGE: {ctx.message.clean_content}',
        str(ctx.message),
        str(exception),
        ''
    ])
    logging.error(logging_message)
    print(datetime.datetime.now(), 'Exception found')
    print(logging_message)
    return message

def request_substitutes():
    pass

async def test_create_thread(bot, guild):
    channel = discord.utils.get(guild.channels, name='grubberbot-debug')
    thread = await channel.create_thread(
        name='grubberbot-test-thread',
        #message='test123',
        type=discord.ChannelType.public_thread,
        reason='testing-purposes',
    )
    await thread.send('test message')
    print('thread created')

async def announce_pairing(bot, guild):
    df = LDB.get_games_by_week(fgg.get_month(flg.NEXT_MONTH), 1)
    channel = discord.utils.get(guild.channels, name='league-scheduling')

    rows = [row for row in df.itertuples()]
    for row in rows[-1:]:
        title = f'Sep2021 Week1 {row.white_discord_name} vs {row.black_discord_name}'
        thread = await channel.create_thread(
            name=title,
            #message=f'{GRUBBER_MENTION} test123',
            type=discord.ChannelType.public_thread,
            reason='testing-purposes',
        )
        message = (
            'Hi! September Rapid League Week 1 has started, please use '
            'this thread so I can help you.  This thread is for:\n'
            '* Scheduling your rapid game.  Any conversation outside of this '
            'thread cannot be regulated by moderators.  You have until '
            'September 9th 11:59pm ET to schedule your game.\n'
            '* Posting your result.  When your game is done please use '
            '`!league_set_result <url>` (in this thread) where `<url>` is a link to the '
            'chess.com game.\n\n'
            'If you need a substitute please ask in the #league-moderation room.\n\n'
            f'<@{row.white_discord_id}> will play white\n'
            f'<@{row.black_discord_id}> will play black\n'
            'See the pairings online: https://docs.google.com/spreadsheets/d/1SFH7ntDpDW7blX_xBU_m_kSmvY2tLXkuwq-TOM9wyew/edit#gid=2039965781'
        )
        print(message)
        await thread.send(message)
        print(title)

async def set_chesscom(mention, user, chesscom):
    LDB.update_signup_info(fgg.get_month(flg.NEXT_MONTH))
    if not LDB.chess_db.get_exists(chesscom):
        message = f'{mention} Chess.com username not found: `{chesscom}`'
        return message

    user_data = LDB.get_user_data(user.id)
    LDB.set_chesscom(user.id, str(user), chesscom)
    if len(user_data) < 1:
        message = (
            f'{mention} successfully linked '
            f'{user.mention} to Chess.com username `{chesscom}`'
        )
    else:
        message = (
            f'{mention} user {user.mention} was linked to Chess.com username '
            f'`{user_data["chesscom"][0]}` but is now linked to `{chesscom}`'
        )
    LDB.update_signup_info(fgg.get_month(flg.NEXT_MONTH))
    return message

@commands.command(name='set_chesscom')
async def user_set_chesscom(ctx, chesscom: str):
    '''Link your chess.com account to your discord account'''
    user = ctx.message.author
    mention = user.mention
    message = await set_chesscom(mention, user, chesscom)
    await ctx.send(message)

@commands.command(name='mod_set_chesscom')
@commands.has_any_role(*MODERATOR_ROLES)
async def mod_set_chesscom(ctx, discord_mention: discord.Member, chesscom: str):
    '''Link a chess.com account to your discord account'''
    mention = ctx.message.author.mention
    message = await set_chesscom(mention, discord_mention, chesscom)
    await ctx.send(message)

async def league_join(mention, user, season_name, join_type, mod=False):
    if mod:
        fn_name = 'mod_league_join'
    else:
        fn_name = 'league_join'
    general_error_message = flg.GENERAL_ERROR_MESSAGE.format(
        mention, fn_name, fn_name)

    user_data = LDB.get_user_data(user.id)
    player_args = ['player', 'substitute']

    errors = []
    if len(user_data) < 1:
        if mod:
            errors.append(f'Chess.com not yet linked, use `!mod_set_chesscom`')
        else:
            errors.append(f'Chess.com not yet linked, use `!set_chesscom`')
    else:
        chesscom = user_data['chesscom'][0]
        count_info = LDB.chess_db.get_count(chesscom)
        num_games = count_info['total_count']
        num_rapid_games = count_info['rapid_count']
        if not join_type in player_args:
            errors.append(f'Expected `{player_args}`, instead found: `{join_type}`')
        if num_rapid_games < 10:
            errors.append(
                f'Minimum 10 rapid games required, `{chesscom}` '
                f'has played only `{num_rapid_games}` rapid games'
            )
        if num_games < 50:
            errors.append(
                f'Minimum 50 games of any time control required, `{chesscom}`'
                f' has played only `{num_games}` games'
            )

    if errors:
        errors = ['* ' + e for e in errors]
        errors = [general_error_message] + errors
        message = '\n'.join(errors)
    else:
        LDB.league_join(
            season_name,
            user.id,
            join_type=='player',
        )
        message = '\n'.join([
            f'{mention} added {user.mention} to the league `{season_name}` season',
            f'* Chess.com username: `{chesscom}`',
            f'* Signed up as a: `{join_type}`',
        ])
    return message

@commands.command(name='league_join_current')
async def user_league_join_current(ctx):
    '''Join the current season of the rapid league as a substitute'''
    user = ctx.message.author
    mention = user.mention
    season_name = fgg.get_month(0)
    message = await league_join(mention, user, season_name, 'substitute')
    await ctx.send(message)

@commands.command(name='mod_league_join_current')
@commands.has_any_role(*MODERATOR_ROLES)
async def mod_league_join_current(ctx, user: discord.Member, join_type: str):
    '''Add someone to the current season of the rapid league
        <user> Ping someone with @user'
        <join_type>: either "player" or "substitute"'''
    mention = ctx.message.author.mention
    season_name = fgg.get_month(0)
    message = await league_join(mention, user, season_name, join_type, mod=True)
    await ctx.send(message)

@commands.command(name='league_join')
async def user_league_join(ctx, join_type: str):
    '''Join the next season of the rapid league as a player or a substitute,
        <join_type>: either "player" or "substitute"'''
    user = ctx.message.author
    mention = user.mention
    season_name = fgg.get_month(1)
    message = await league_join(mention, user, season_name, join_type)
    await ctx.send(message)

@commands.command(name='mod_league_join')
@commands.has_any_role(*MODERATOR_ROLES)
async def mod_league_join(ctx, user: discord.Member, join_type: str):
    '''Add someone to the next season of the rapid league
        <user> Ping someone with @user'
        <join_type>: either "player" or "substitute"'''
    mention = ctx.message.author.mention
    season_name = fgg.get_month(1)
    message = await league_join(mention, user, season_name, join_type, mod=True)
    await ctx.send(message)

async def league_leave(mention, user, season_name):
    df = LDB.get_league_info(season_name, user.id)
    if len(df) == 0:
        message = (
            f'{mention} user {user.mention} is not currently signed up '
            f'for the rapid league `{season_name}` season'
        )
    else:
        LDB.league_leave(season_name, user.id)
        message = (
            f'{mention} user {user.mention} has left the '
            f'rapid league `{season_name}` season'
        )
    return message

@commands.command(name='league_leave')
async def user_league_leave(ctx):
    '''To leave the upcoming rapid league season'''
    user = ctx.message.author
    mention = user.mention
    season_name = fgg.get_month(1)
    message = await league_leave(mention, user, season_name)
    await ctx.send(message)

@commands.command(name='mod_league_leave')
@commands.has_any_role(*MODERATOR_ROLES)
async def mod_league_leave(ctx, discord_mention: discord.Member):
    '''Remove user from upcoming league'''
    mention = ctx.message.author.mention
    season_name = fgg.get_month(1)
    message = await league_leave(mention, discord_mention, season_name)
    await ctx.send(message)

@commands.command(name='league_leave_current')
async def user_league_leave_current(ctx):
    '''To leave the current rapid league season'''
    user = ctx.message.author
    mention = user.mention
    season_name = fgg.get_month(0)
    message = await league_leave(mention, user, season_name)
    await ctx.send(message)

@commands.command(name='mod_league_leave_current')
@commands.has_any_role(*MODERATOR_ROLES)
async def mod_league_leave_current(ctx, discord_mention: discord.Member):
    '''Remove user from current league'''
    mention = ctx.message.author.mention
    season_name = fgg.get_month(0)
    message = await league_leave(mention, discord_mention, season_name)
    await ctx.send(message)

async def league_request_substitute(
        mention,
        user,
        is_next_season,
        sub_week,
        mod=False,
    ):

    # Verify that sub week is in 1-4
    sub_weeks = list(range(1, 5))
    if sub_week not in sub_weeks:
        message = (
            f'{mention} Expected one of `{sub_weeks}`, instead got `{sub_week}`'
        )
        return message

    # Get user chesscom
    user_data = LDB.get_user_data(user.id)
    if len(user_data) == 0:
        message = gen_chesscom_username_error(mention, user_mention, mod=False)
        return message
    chesscom = user_data['chesscom'][0]

    # Verify that the user is signed up for the season
    season_name = fgg.get_month(int(is_next_season))
    df = LDB.get_league_info(season_name, user.id)
    if len(df) == 0:
        message = (
            f'{mention} User {user.mention} is not signed up for '
            f'the Rapid League `{season_name}` season'
        )
        return message

    LDB.request_sub(season_name, sub_week, user.id)
    message = (
        f'{mention} user {user.mention} has requested a substitute on '
        f'week {sub_week} of the rapid league `{next_month}` season'
    )
    return message

@commands.command(name='league_request_sub_this_month')
async def user_league_request_sub_this_month(ctx, sub_week: int):
    '''
        <sub_week> one of `1, 2, 3, 4` for the week you want a substitute
    '''
    user = ctx.message.author
    mention = user.mention
    message = await league_request_substitute(mention, user, False, sub_week)
    await ctx.send(message)

help_text = '\n'.join([
])
@commands.command(name='mod_league_request_sub_this_month', help=help_text)
@commands.has_any_role(*MODERATOR_ROLES)
async def mod_league_request_sub_this_month(
        ctx,
        user: discord.Member,
        sub_week: int,
    ):
    '''
        <user> use @someone
        <sub_week> one of `1, 2, 3, 4` for the week you want a substitute
    '''
    mention = ctx.message.author.mention
    message = await league_request_substitute(
        mention, user, False, sub_week, mod=True)
    await ctx.send(message)

@commands.command(name='league_request_sub_next_month')
async def user_league_request_sub_next_month(ctx, sub_week: int):
    '''
        <sub_week> one of `1, 2, 3, 4` for the week you want a substitute
    '''
    user = ctx.message.author
    mention = user.mention
    message = await league_request_substitute(mention, user, True, sub_week)
    await ctx.send(message)

help_text = '\n'.join([
])
@commands.command(name='mod_league_request_sub_next_month', help=help_text)
@commands.has_any_role(*MODERATOR_ROLES)
async def mod_league_request_sub_next_month(
        ctx,
        user: discord.Member,
        sub_week: int,
        ):
    '''
        <user> use @someone
        <sub_week> one of `1, 2, 3, 4` for the week you want a substitute
    '''
    mention = ctx.message.author.mention
    message = await league_request_substitute(
        mention, user, True, sub_week, mod=True)
    await ctx.send(message)

@commands.command(name='league_info')
async def league_info(ctx, discord_mention: Optional[discord.Member]=None):
    '''Get league info, optionally @mention someone to get info about them'''
    df = LDB.update_signup_info(fgg.get_month(flg.NEXT_MONTH))
    mention = ctx.message.author.mention
    user = discord_mention

    next_month = fgg.get_month(flg.NEXT_MONTH)
    if discord_mention is None:
        message = '\n'.join([
            f'{mention} ',
            f'* Number of members in the {next_month} season: {len(df)}',
        ])
    else:
        df = LDB.get_league_info(fgg.get_month(1), user.id)
        if len(df) == 0:
            message = f'{mention} user {user.mention} is not signed up for the league {next_month} season'
        else:
            info = {str(c): df[c][0] for c in df.columns}
            message = '\n'.join([
                f'{mention} the user {user.mention} has:',
                pformat(info),
            ])

    await ctx.send(message)

def league_set_result(mention, user, game_id, url=None, mod=False, result=None):

    if mod and url is None:
        if result not in [-1, 0, 1]:
            message = (f'Result must be in `{[-1, 0, 1]}`')
        else:
            LDB.set_result(game_id, result, url)
            display_result = DISPLAY_RESULT[result]
            message = f'{mention} Set result for game `{game_id}` to `{display_result}`'
        return message

    # Pull game info from the database
    game_df = LDB.get_game_by_id(game_id)
    if len(game_df) == 0:
        message = f'{mention} Game ID not found, pinging {GRUBBER_MENTION}'
        return message
    game_dict = {c: game_df[c][0] for c in game_df.columns}

    # Verify the user has a real chess.com username
    user_data = LDB.get_user_data(user.id)
    if len(user_data) < 1:
        message = gen_chesscom_username_error(mention, user.mention)
        return message
    chesscom = user_data['chesscom'][0]

    # Get cc_game_id from url
    cc_game_id = fcc.game_id_from_url(url)
    if cc_game_id is None:
        message = (f'{mention} No chesscom game_id found in url')
        return message

    # Get games and index by cc_game_id
    games = fcc.get_game_history_api(chesscom)
    cc_game_id_dict = {fcc.game_id_from_url(game['url']): game for game in games}

    # Make sure the user has played the game in question
    if cc_game_id not in cc_game_id_dict:
        message = f'{mention} Game not found in user game history: {url}'
        return message

    # Log errors about the game
    errors = []
    game = cc_game_id_dict[cc_game_id]
    chesscoms = {
        'white': game['white']['username'].lower(),
        'black': game['black']['username'].lower(),
    }
    if (
        game_dict['white_chesscom'].lower() != chesscoms['white'].lower()
        or game_dict['black_chesscom'].lower() != chesscoms['black'].lower()
    ):
        message = (
            f'Incorrect players, expected: '
            f'White: `{game_dict["white_chesscom"]}` '
            f'Black: `{game_dict["black_chesscom"]}` \n'
            f'Instead found:'
            f'White: `{chesscoms["white"]}` Black: `{chesscoms["black"]}`'
        )
        errors.append(message)
    if game['time_control'] != TIME_CONTROL:
        message = (
            f'Expected time control: `{TIME_CONTROL}` '
            f'Instead got time control: `{game["time_control"]}` '
        )
        errors.append(message)
    if not game['rated']:
        message = f'Game is unrated, all games must be rated.'
        errors.append(message)
    if (
        chesscom.lower() != chesscoms['white'].lower()
        and chesscom.lower() != chesscoms['black'].lower()
    ):
        message = (
            f'Incorrect permissions, user `{chesscom}` must be one '
            f'of the players, instead found `{chesscoms}`'
        )
        errors.append(message)

    if not mod and errors:
        errors = ['* ' + e for e in errors]
        errors = [f'{mention} Errors in setting the result:'] + errors
        message = '\n'.join(errors)
    else:
        if result is None:
            game_result = cc_game_id_dict[cc_game_id]['white']['result']
            result = WHITE_RESULTS_CODES[game_result]
        LDB.set_result(game_id, result, url)
        display_result = DISPLAY_RESULT[result]
        message = f'{mention} Set result for game `{game_id}` to `{display_result}`'
    return message

def title_to_game_id(title):
    season_name = fgg.get_month(flg.NEXT_MONTH)
    df = LDB.get_games_by_week(season_name, 1)

    id_dict = {
        (
            row.white_discord_name.replace('#', ''),
            row.black_discord_name.replace('#', ''),
        ): row.game_id for row in df.itertuples()
    }

    split = title.replace('Sep2021 Week1 ', '').split(' vs ')

    white_name = split[0]
    black_name = split[1]
    key = (white_name, black_name)
    game_id = id_dict[key]
    return game_id

@commands.command(name='user_league_set_result')
async def user_league_set_result(ctx, url):
    '''Use with the game url in a game thread to assign a result to the game'''
    user = ctx.message.author
    mention = ctx.message.author.mention

    if not isinstance(ctx.message.channel, discord.Thread):
        message = 'This command must be used in a game thread'
    else:
        game_id = title_to_game_id(ctx.message.channel.name)
        message = league_set_result(mention, user, game_id, url)
    await ctx.send(message)

@commands.command(name='mod_league_set_result')
@commands.has_any_role(*MODERATOR_ROLES)
async def mod_league_set_result(ctx, user: discord.Member, url: str):
    '''Use with the game url in a game thread to assign a result to the game'''
    mention = ctx.message.author.mention

    if not isinstance(ctx.message.channel, discord.Thread):
        message = 'This command must be used in a game thread'
    else:
        game_id = title_to_game_id(ctx.message.channel.name)
        message = league_set_result(mention, user, game_id, url, mod=True)
    await ctx.send(message)

@commands.command(name='mod_league_custom_result')
@commands.has_any_role(*MODERATOR_ROLES)
async def mod_league_custom_result(
        ctx,
        user: discord.Member,
        result: int,
        url: Optional[str]=None,
    ):
    '''
    result in `[-1, 0, 1]`, optionally include url (after result) for logging purposes
    '''
    mention = ctx.message.author.mention

    if not isinstance(ctx.message.channel, discord.Thread):
        message = 'This command must be used in a game thread'
    else:
        game_id = title_to_game_id(ctx.message.channel.name)
        message = league_set_result(mention, user, game_id, result=result, mod=True, url=url)
    await ctx.send(message)

# For testing
@commands.command(name='test')
async def test(ctx):
    '''A test message to make sure GrubberBot is working'''
    await ctx.send('test successful')

def save_user_data(guild):
    members = list(guild.members)
    df_dict = {
        'discord_id': [member.id for member in tqdm(members)],
        'discord_name': [str(member) for member in tqdm(members)],
    }
    df = pd.DataFrame(df_dict)
    df.to_parquet(DISCORD_USERS_PARQUET)
    print(f'updated {DISCORD_USERS_PARQUET}')

async def save_discord_history(guild):
    data = {
        'date': [],
        'discord_id': [],
        'name': [],
        'text': [],
        'channel': [],
        'is_thread': [],
    }

    LIMIT = None
    channels = [(False, c) for c in guild.channels]
    threads = [(True, t) for t in guild.threads]
    threads = threads + [
        (True, t)
        for c in guild.channels
        if hasattr(c, 'archived_threads')
        async for t in c.archived_threads()
        if 'league' in c.name
    ]
    combined = channels + threads
    for is_thread, channel in tqdm(combined):
        print()
        print(is_thread, channel.name)
        if hasattr(channel, 'history'):
            msgs = await channel.history(limit=LIMIT).flatten()
            for msg in msgs:
                data['date'].append(msg.created_at)
                data['discord_id'].append(msg.author.id)
                data['name'].append(str(msg.author))
                data['text'].append(msg.content)
                data['channel'].append(channel.name)
                data['is_thread'].append(is_thread)

    df = pd.DataFrame(data)
    df.to_parquet(DISCORD_HISTORY_PARQUET)
    print(f'updated {DISCORD_HISTORY_PARQUET}')

def update_google_sheet():
    LDB.update_signup_info(fgg.get_month(flg.NEXT_MONTH))

    for week_num in [1, 2, 3, 4]:
        season_name = fgg.get_month(0)
        df = LDB.get_games_by_week(season_name, week_num)
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
        to_sheet['result'] = [
            DISPLAY_RESULT[r] if not np.isnan(r) else '-'
            for r in np.array(to_sheet['result'])
        ]
        fgg.df_to_sheet(to_sheet, sheet=week_num+1)

class CustomClient(discord.Client):
    async def on_ready(self):
        guild = discord.utils.get(self.guilds, name=GUILD_NAME)
        print(f'Client {self.user} has connected to {guild.name}')

        save_user_data(guild)
        await save_discord_history(guild)

def main():
    load_dotenv()
    DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
    intents = discord.Intents.all()
    client = CustomClient(intents=intents)
    client.run(DISCORD_TOKEN)

if __name__ == '__main__':
    main()
