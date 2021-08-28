import os
import sys
from dotenv import load_dotenv
import json
import discord
from discord.ext import commands
import bot_funcs as bfn
from pprint import pprint

# TODO: what if discord username or chesscom username changes?
# TODO: !info command

'''
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
def generic_error(fn, fn_name):
    @fn.error
    async def error_lambda(ctx, exception):
        general_error_message = bfn.GENERAL_ERROR_MESSAGE.format(fn_name, fn_name)
        await ctx.send(general_error_message)
        raise exception

'''
# For league membership
!set_chesscom <chesscom> - renaming !link_chesscom.  !league_ commands won't work until this is done, and this command stops working until the season is done.
!league_join <player, substitute> - join next season, request a week off with !league_request_substitute
!league_leave - renaming !leave_rapid_league, to leave next season
!league_info <optional: @someone> - general league info (schedule, points, etc) optionally ask for info on someone such as membership (this season and next), elo, etc
!league_join_current - to join current season as a substitute
'''

# League membership
help_text = '\n'.join([
    (
        'Link your chess.com and discord accounts '
        'by replacing "<chesscom_name>" with your chess.com username'
    ),
])
@bot.command(name='set_chesscom', help=help_text)
async def set_chesscom(ctx, chesscom_name):
    discord_user = ctx.message.author
    discord_name = str(discord_user)

    if not bfn.exists_chesscom_name(chesscom_name):
        message = f'Username not found on chess.com: `{chesscom_name}`'
        await ctx.send(message)
        return

    name_data = bfn.get_chesscom_name(discord_name)
    bfn.set_chesscom_name(discord_name, chesscom_name)
    if name_data is None:
        message = (
            f'Discord username `{discord_name}` '
            f'successfully linked to Chess.com username `{chesscom_name}`'
        )
    else:
        message = (
            f'Discord username `{discord_name}` '
            f'previously linked to Chess.com username `{chesscom_name}`'
            f'is now linked to Chess.com username `{chesscom_name}`'
        )
    await ctx.send(message)

generic_error(set_chesscom, 'set_chesscom')
'''
@set_chesscom.error
async def error_set_chesscom(ctx, exception):
    fn_name = 'set_chesscom'
    general_error_message = bfn.GENERAL_ERROR_MESSAGE.format(fn_name, fn_name)
    await ctx.send(general_error_message)
    raise exception
'''

@bot.event
async def on_ready():
    guild = discord.utils.get(bot.guilds, name=GUILD_NAME)
    print(f'Bot {bot.user.name} has connected to {guild.name}')

    channel = discord.utils.get(guild.channels, name='grubberbot-debug')
    thread = await channel.create_thread(
        name='grubberbot-test-thread',
        #message='test123',
        type=discord.ChannelType.public_thread,
        reason='testing-purposes',
    )
    await thread.send('test message')
    print('thread created')

@bot.command(name='test')
async def test(ctx):
    print(type(ctx))
    await ctx.message.create_thread('autogeneratedthread')
    #await thread.send('test thread')
    await ctx.send('test successful')

@bot.command(name='leave_rapid_league', help='To leave the upcoming rapid league')
async def leave_rapid_league(ctx):
    discord_user = ctx.message.author
    discord_name = str(discord_user)
    league_data = bfn.del_league(discord_name)
    next_month = bfn.get_month(next=True)

    if league_data is None:
        message = (
            f'user `{discord_name}` is not currently signed up for the '
            f'rapid league `{next_month}` season'
        )
    else:
        message = (
            f'user `{discord_name}` has left the '
            f'rapid league `{next_month}` season'
        )
    await ctx.send(message)

help_text = '\n'.join([
    '    <player>: either "player" or "substitute"',
    '    <sub_week>: "None" if you will not need a substitute, '
        'or a number 1, 2, 3, or 4 for the week you will need a substitute.',
])
@bot.command(name='join_rapid_league', help=help_text)
async def join_rapid_league(ctx, player, sub_week):
    fn_name = 'join_rapid_league'
    general_error_message = bfn.GENERAL_ERROR_MESSAGE.format(fn_name, fn_name)
    discord_user = ctx.message.author
    discord_name = str(discord_user)

    chesscom_name = bfn.get_chesscom_name(discord_name)
    player_args = ['player', 'substitute']
    sub_weeks = ['None'] + [f'{i}' for i in range(1, 5)]

    num_games = bfn.count_games(chesscom_name)
    num_rapid_games = bfn.count_games(chesscom_name, rapid=True)
    errors = []
    if chesscom_name is None:
        errors.append(f'Chess.com account not yet linked, use `!link_chesscom`')
    if not player in player_args:
        errors.append(f'Expected `{player_args}`, instead found: `{player}`')
    if not sub_week in sub_weeks:
        errors.append(f'Expected `{sub_weeks}`, instead found: `{sub_week}`')
    if num_rapid_games < 10:
        errors.append(
            f'Minimum 10 rapid games required, `{chesscom_name}` '
            f'has played only `{num_rapid_games}` rapid games'
        )
    if num_games < 50:
        errors.append(
            f'Minimum 50 games of any time control required, `{chesscom_name}`'
            f' has played only `{num_games}` games'
        )

    league_data = bfn.get_league(discord_name)
    next_month = bfn.get_month(next=True)

    if errors:
        errors = ['* ' + e for e in errors]
        errors = [general_error_message] + errors
        message = '\n'.join(errors)
    elif league_data is None:
        bfn.set_league(discord_name, player, sub_week)
        message = '\n'.join([
            f'Successfully joined the rapid league `{next_month}` season',
            f'* Chess.com username: `{chesscom_name}`',
            f'* Signed up as a: `{player}`',
            f'* Requested a substite on week: `{sub_week}`',
        ])
    else:
        bfn.set_league(discord_name, player, sub_week)
        message = '\n'.join([
            f'Previously joined the rapid league `{next_month}` season with',
            f'* Chess.com username: `{league_data["chesscom_name"]}`',
            f'* Signed up as a: `{league_data["player"]}`',
            f'* Requested a substite on week: `{league_data["sub_week"]}`',
            '',
            f'Changed rapid league info for the `{next_month}` season to',
            f'* Chess.com username: `{chesscom_name}`',
            f'* Signed up as a: `{player}`',
            f'* Requested a substite on week: `{sub_week}`',
        ])
    await ctx.send(message)

@join_rapid_league.error
async def join_rapid_league_error(ctx, exception):
    fn_name = 'join_rapid_league'
    general_error_message = bfn.GENERAL_ERROR_MESSAGE.format(fn_name, fn_name)
    await ctx.send(general_error_message)
    raise exception

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
    client = CustomClient()
    client.run(TOKEN)
    '''

    bot.run(TOKEN)