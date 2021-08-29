import os
import sys
from dotenv import load_dotenv
import json
import discord
from discord.ext import commands
import bot_funcs as bfn
import discord_funcs as dfn
from pprint import pprint
import logging
import datetime
from typing import Optional

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

# TODO: Backup json data
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
!league_request_substitute <1, 2, 3, 4> - on a given week number request a substitute (resign if no substitute)
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

# Build files if they don't already exist
bfn.gen_files()

# Read secret information from .env file
load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')

# The bot
bot = commands.Bot(command_prefix='!')

# Generic error message
# TODO: replac with generic event handling
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
    if not bfn.exists_chesscom(chesscom):
        message = f'{mention} Chess.com username not found: `{chesscom}`'
        return message

    name_data = bfn.get_chesscom(user.id)
    bfn.set_chesscom(user.id, str(user), chesscom)
    if name_data is None:
        message = (
            f'{mention} successfully linked '
            f'{user.mention} to Chess.com username `{chesscom}`'
        )
    else:
        message = (
            f'{mention} user {user.mention} was linked to Chess.com username '
            f'`{name_data}` but is now linked to `{chesscom}`'
        )
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
    if mod:
        fn_name = 'mod_league_join'
    else:
        fn_name = 'league_join'
    general_error_message = bfn.GENERAL_ERROR_MESSAGE.format(
        mention, fn_name, fn_name)

    chesscom = bfn.get_chesscom(user.id)
    num_games = bfn.count_games(chesscom)
    num_rapid_games = bfn.count_games(chesscom, rapid=True)
    player_args = ['player', 'substitute']

    errors = []
    if chesscom is None:
        if mod:
            errors.append(f'Chess.com not yet linked, use `!mod_link_chesscom`')
        else:
            errors.append(f'Chess.com not yet linked, use `!link_chesscom`')
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

    league_data = bfn.get_league(user.id)
    next_month = bfn.get_month(next=True)

    if errors:
        errors = ['* ' + e for e in errors]
        errors = [general_error_message] + errors
        message = '\n'.join(errors)
    elif league_data is None:
        bfn.set_league(user.id, str(user), join_type)
        message = '\n'.join([
            f'{mention} added {user.mention} to the league `{next_month}` season',
            f'* Chess.com username: `{chesscom}`',
            f'* Signed up as a: `{join_type}`',
        ])
    else:
        bfn.set_league(user.id, str(user), join_type)
        message = '\n'.join([
            f'{mention}: {user.mention} already in the `{next_month}` season',
            f'* Chess.com username: `{league_data["chesscom"]}`',
            f'* Signed up as a: `{league_data["join_type"]}`',
            '',
            f'Changed {user.mention} info for the `{next_month}` season to',
            f'* Chess.com username: `{chesscom}`',
            f'* Signed up as a: `{join_type}`',
        ])
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
    message = await league_join(mention, discord_mention, join_type)
    await ctx.send(message)
generic_error(mod_league_join, 'mod_league_join')

async def league_leave(mention, user):
    league_data = bfn.del_league(user.id)
    next_month = bfn.get_month(next=True)

    if league_data is None:
        message = (
            f'{mention} user {user.mention} is not currently signed up '
            f'for the rapid league `{next_month}` season'
        )
    else:
        message = (
            f'{mention} user {user.mention} has left the '
            f'rapid league `{next_month}` season'
        )
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

# !league_info <optional: @someone> - general league info (schedule, points, etc) optionally ask for info on someone such as membership (this season and next), elo, etc
help_text = 'Get league info, optionally @mention someone to get info about them'
@bot.command(name='league_info', help=help_text)
async def league_info(ctx, discord_mention: Optional[discord.Member]):
    mention = ctx.message.author.mention
    user = discord_mention

    next_month = bfn.get_month(next=True)
    if discord_mention is None:
        data = bfn.get_league_info()
        message = '\n'.join([
            f'{mention} ',
            f'* Number of members in the {next_month} season: {len(data)}',
        ])
    else:
        league_data = bfn.get_league(user.id)
        if league_data is None:
            message = f'{mention} user {user.mention} is not signed up for the league {next_month} season'
        else:
            chesscom = bfn.get_chesscom(user.id)
            rapid_rating = bfn.get_rating(chesscom)
            #num_games = bfn.count_games(chesscom)
            #num_rapid_games = bfn.count_games(chesscom, rapid=True)
            message = '\n'.join([
                f'{mention} the user {user.mention} has:',
                f'* the Chess.com username: {chesscom}',
                f'* the Rapid rating: {rapid_rating}',
                f'* signed up for the {next_month} season',
            ])

    await ctx.send(message)
generic_error(league_info, 'league_info')

@bot.event
async def on_ready():
    guild = discord.utils.get(bot.guilds, name=GUILD_NAME)
    print(f'Bot {bot.user.name} has connected to {guild.name}')

    '''
    channel = discord.utils.get(guild.channels, name='grubberbot-debug')
    thread = await channel.create_thread(
        name='grubberbot-test-thread',
        #message='test123',
        type=discord.ChannelType.public_thread,
        reason='testing-purposes',
    )
    await thread.send('test message')
    print('thread created')
    '''

@bot.command(name='test')
async def test(ctx):
    #print(type(ctx))
    #await ctx.message.create_thread('autogeneratedthread')
    #await thread.send('test thread')
    await ctx.send('test successful')

'''
# General error handling
@bot.event
async def on_command_error(ctx, exception):
    if isinstance(exception, commands.errors.CheckFailure):
        await ctx.send('You do not have the correct role for this command.')
'''

class CustomClient(discord.Client):
    async def on_ready(self):
        guild = discord.utils.get(client.guilds, name=GUILD_NAME)
        print(f'Client {client.user} has connected to {guild.name}')
        print(guild.members)

        data = {
            str(member): member.id for member in guild.members
        }
        with open('mapping.json', 'w') as f:
            json.dump(data, f, sort_keys=True, indent=4)

'''
        channel = discord.utils.get(guild.channels, name='grubberbot-debug')
        print(channel)
        thread = await channel.create_thread(
            name='grubberbot-test-thread',
            #message='test123',
            type=discord.ChannelType.public_thread,
            reason='testing-purposes',
        )
        await thread.send('test message')
'''

if __name__ == '__main__':
    '''
    intents = discord.Intents.default()
    intents.members = True
    client = CustomClient(intents=intents)
    client.run(TOKEN)
    '''

    bot.run(TOKEN)
