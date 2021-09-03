import os
import sys
from dotenv import load_dotenv
import json
import discord
from discord.ext import commands
from pprint import pprint
import logging
import datetime
from typing import Optional

import bot_funcs as bfn
import discord_funcs as dfn

logging.basicConfig(
    filename='grubberbot.log',
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
)
MODERATOR_ROLES = [
    'The Grubber',
    'Mods',
    'Cool People',
]

# TODO: sql injection problem
# TODO: @everyone problem
# TODO: Backup sqlite3 data
# TODO: what if discord username or chesscom username changes?
# TODO: !info command
# TODO: enforce 1:1 discord and chesscom usernames in both directions

'''
!list_commands

# For league membership
!set_chesscom <chesscom> - renaming !link_chesscom.  !league_ commands won't work until this is done, and this command stops working until the season is done.
!league_join <player, substitute> - join next season, request a week off with !league_request_substitute
!league_leave - renaming !leave_rapid_league, to leave next season
!league_info <optional: @someone> - general league info (schedule, points, etc) optionally ask for info on someone such as membership (this season and next), elo, etc
!league_join_current - to join current season as a substitute

# for scheduling a game
!league_game_status <optional: @someone_else> - get info on status of the current game (where to go to talk about it, whether it's even scheduled), optional argument to use on someone else
!league_schedule_game <date and time> - schedule a game, somehow i'll require confirmation from both players
!league_set_result <link_to_chesscom_game> - get the result of a game and record it

# for substitutes
!league_request_substitute <this_season, next_season> <1, 2, 3, 4> - on a given week number request a substitute (resign if no substitute)
!league_claim_substitution - usable only in a thread, replace the player requesting a substitute in the thread

# mod commands to do anything or manually set anything
!mod_set_chesscom <@someone> <chesscom>
!mod_league_join <@someone> <player, substitute>
!mod_league_leave <@someone>
!mod_league_join_current <@someone>
!mod_league_leave_current <@someone> - kick someone out this season, cannot rejoin as substitute
!mod_league_schedule_game <@someone> <date and time> - schedule a game without requiring confirmation
!mod_league_set_result <@someone> <1, 2, 3, 4> <win/draw/loss> - set the result of a game
!mod_league_request_substitute <@someone> <1, 2, 3, 4>
!mod_league_claim_substitution <@someone>
'''

# Declare variables
GUILD_NAME = "pawngrubber's server"

# Read secret information from .env file
load_dotenv()
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')

# The bot
bot = commands.Bot(command_prefix='!')

# Database stuff
LDB = bfn.LeagueDatabase()
def set_league(discord_id, join_type):
    LDB.league_join(
        LDB.get_month(bfn.NEXT_MONTH),
        discord_id,
        join_type=='player',
    )

def del_league(discord_id):
    possible_error = False
    try:
        LDB.league_leave(
            LDB.get_month(bfn.NEXT_MONTH),
            discord_id,
        )
    except Exception as e:
        possible_error = e
        logging.error(f'DELETED {discord_id} and raised an error:')
        logging.error(str(possible_error))
    return possible_error

def set_sub_week(discord_id, user, sub_week):
    df = LDB.get_league_info(user.id)
    if len(df) == 0:
        return None
    possible_error = False
    try:
        LDB.request_sub(
            LDB.get_month(bfn.NEXT_MONTH), sub_week, discord_id)
    except Exception as e:
        possible_error = None
        logging.error(f'DELETED {discord_id} and raised an error:')
        logging.error(str(possible_error))
    return possible_error

# Generic error message
# TODO: replace with generic event handling
def generic_error(fn, fn_name):
    @fn.error
    async def error_lambda(ctx, exception):
        if isinstance(exception, commands.errors.CheckFailure):
            await ctx.send('You do not have the correct role for this command.')
            return
        general_error_message = bfn.GENERAL_ERROR_MESSAGE.format(
            ctx.message.author.mention,
            fn_name,
            fn_name,
        )
        await ctx.send(general_error_message)
        logging_message = '\n'.join([
            f'MESSAGE: {ctx.message.clean_content}',
            str(ctx.message),
            str(exception),
            ''
        ])
        logging.error(logging_message)
        print(datetime.datetime.now(), 'Exception logged')
        print(logging_message)

'''
# For league membership
!set_chesscom <chesscom> - renaming !link_chesscom.  !league_ commands won't work until this is done, and this command stops working until the season is done.
!league_join <player, substitute> - join next season, request a week off with !league_request_substitute
!league_leave - renaming !leave_rapid_league, to leave next season
!league_join_current - to join current season as a substitute
!league_info <optional: @someone> - general league info (schedule, points, etc) optionally ask for info on someone such as membership (this season and next), elo, etc
'''

# League membership
async def set_chesscom(mention, user, chesscom):
    LDB.update_signup_info(LDB.get_month(bfn.NEXT_MONTH))
    if not LDB.chess_db.exists_chesscom(chesscom):
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
    LDB.update_signup_info(LDB.get_month(bfn.NEXT_MONTH))
    return message

help_text = 'Link your chess.com account to your discord account'
@bot.command(name='set_chesscom', help=help_text)
async def user_set_chesscom(ctx, chesscom: str):
    user = ctx.message.author
    mention = user.mention
    message = await set_chesscom(mention, user, chesscom)
    await ctx.send(message)
generic_error(user_set_chesscom, 'set_chesscom')

help_text = 'Link a chess.com account to a discord account'
@bot.command(name='mod_set_chesscom', help=help_text)
@commands.has_any_role(*MODERATOR_ROLES)
async def mod_set_chesscom(ctx, discord_mention: discord.Member, chesscom: str):
    mention = ctx.message.author.mention
    message = await set_chesscom(mention, discord_mention, chesscom)
    await ctx.send(message)
generic_error(mod_set_chesscom, 'mod_set_chesscom')

async def league_join(mention, user, join_type, mod=False):
    LDB.update_signup_info(LDB.get_month(bfn.NEXT_MONTH))
    if mod:
        fn_name = 'mod_league_join'
    else:
        fn_name = 'league_join'
    general_error_message = bfn.GENERAL_ERROR_MESSAGE.format(
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

        next_month = LDB.get_month(bfn.NEXT_MONTH)

    if errors:
        errors = ['* ' + e for e in errors]
        errors = [general_error_message] + errors
        message = '\n'.join(errors)
    else:
        set_league(user.id, join_type)
        message = '\n'.join([
            f'{mention} added {user.mention} to the league `{next_month}` season',
            f'* Chess.com username: `{chesscom}`',
            f'* Signed up as a: `{join_type}`',
        ])
    LDB.update_signup_info(LDB.get_month(bfn.NEXT_MONTH))
    return message

help_text = '\n'.join([
    'Join the next season of the rapid league as a player or a substitute',
    '    <join_type>: either "player" or "substitute"',
])
@bot.command(name='league_join', help=help_text)
async def user_league_join(ctx, join_type: str):
    user = ctx.message.author
    mention = user.mention
    message = await league_join(mention, user, join_type)
    await ctx.send(message)
generic_error(user_league_join, 'league_join')

help_text = '\n'.join([
    'Add someone to the next season of the rapid league',
    '    <discord_mention> Ping someone with @user',
    '    <join_type>: either "player" or "substitute"',
])
@bot.command(name='mod_league_join', help=help_text)
@commands.has_any_role(*MODERATOR_ROLES)
async def mod_league_join(ctx, discord_mention: discord.Member, join_type: str):
    mention = ctx.message.author.mention
    message = await league_join(mention, discord_mention, join_type, mod=True)
    await ctx.send(message)
generic_error(mod_league_join, 'mod_league_join')

async def league_leave(mention, user):
    LDB.update_signup_info(LDB.get_month(bfn.NEXT_MONTH))
    possible_error = del_league(user.id)
    next_month = LDB.get_month(bfn.NEXT_MONTH)

    if possible_error:
        message = (
            f'{mention} user {user.mention} is not currently signed up '
            f'for the rapid league `{next_month}` season'
        )
    else:
        message = (
            f'{mention} user {user.mention} has left the '
            f'rapid league `{next_month}` season'
        )
    LDB.update_signup_info(LDB.get_month(bfn.NEXT_MONTH))
    return message

@bot.command(name='league_leave', help='To leave the upcoming rapid league')
async def user_league_leave(ctx):
    user = ctx.message.author
    mention = user.mention
    message = await league_leave(mention, user)
    await ctx.send(message)
generic_error(user_league_leave, 'league_leave')

@bot.command(name='mod_league_leave', help='Remove user from upcoming league')
@commands.has_any_role(*MODERATOR_ROLES)
async def mod_league_leave(ctx, discord_mention: discord.Member):
    mention = ctx.message.author.mention
    message = await league_leave(mention, discord_mention)
    await ctx.send(message)
generic_error(mod_league_leave, 'mod_league_leave')

async def league_request_substitute(mention, user, season, sub_week, mod=False):
    LDB.update_signup_info(LDB.get_month(bfn.NEXT_MONTH))
    sub_weeks = ['1', '2', '3', '4']
    if sub_week not in sub_weeks:
        message = (
            f'{mention} Expected one of `{sub_weeks}`, instead got `{sub_week}`'
        )
        return message

    '''
    seasons = ['this_season, next_season']
    if season not in seasons:
        message = (
            f'{mention} Expected one of `{seasons}`, instead got `{season}`'
        )
        return message
    '''

    user_data = LDB.get_user_data(user.id)

    if len(user_data) < 1:
        if mod:
            error_command = '!mod_set_chesscom'
        else:
            error_command = '!set_chesscom'
        message = (
            f'{mention} User {user.mention} has not yet linked to chesscom, '
            f' use !{error_command}'
        )
        return message
    chesscom = user_data['chesscom'][0]

    next_month = LDB.get_month(bfn.NEXT_MONTH)
    possible_error = set_sub_week(user.id, user, sub_week)

    if possible_error is None:
        message = (
            f'{mention} user {user.mention} is not currently signed up '
            f'for the rapid league `{next_month}` season'
        )
    else:
        message = (
            f'{mention} user {user.mention} has requested a substitute on '
            f'week {sub_week} of the rapid league `{next_month}` season'
        )
    LDB.update_signup_info(LDB.get_month(bfn.NEXT_MONTH))
    return message

help_text = '\n'.join([
    '    <sub_week> one of `1, 2, 3, 4` for the week you want a substitute'
])
@bot.command(name='league_request_substitute', help=help_text)
async def user_league_request_substitute(ctx, sub_week):
    user = ctx.message.author
    mention = user.mention
    season = LDB.get_month(bfn.NEXT_MONTH)
    message = await league_request_substitute(mention, user, season, sub_week)
    await ctx.send(message)
generic_error(user_league_request_substitute, 'league_request_substitute')

help_text = '\n'.join([
    '    <discord_mention> use @someone'
    '    <sub_week> one of `1, 2, 3, 4` for the week you want a substitute'
])
@bot.command(name='mod_league_request_substitute', help=help_text)
@commands.has_any_role(*MODERATOR_ROLES)
async def mod_league_request_substitute(ctx, discord_mention: discord.Member, sub_week):
    mention = ctx.message.author.mention
    season = LDB.get_month(bfn.NEXT_MONTH)
    message = await league_request_substitute(mention, discord_mention, season, sub_week, mod=True)
    await ctx.send(message)
generic_error(mod_league_request_substitute, 'mod_league_request_substitute')

@bot.event
async def on_ready():
    guild = discord.utils.get(bot.guilds, name=GUILD_NAME)
    print(f'Bot {bot.user.name} has connected to {guild.name}')
    LDB.update_signup_info(LDB.get_month(bfn.NEXT_MONTH))
    await dfn.announce_pairing(bot, guild)

@bot.event
async def on_command_error(ctx, exception):
    message = dfn.on_command_error(ctx, exception)
    if message is not None:
        await ctx.send(message)

def main():

    bot.add_command(dfn.test)

    bot.add_command(dfn.league_info)

    bot.run(DISCORD_TOKEN)

if __name__ == '__main__':
    main()
