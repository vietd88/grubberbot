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
import pandas as pd

import funcs_league as flg
import funcs_discord as fdd
import funcs_general as fgg

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
# TODO: !info command

'''
!list_commands

# for scheduling a game
!league_game_status <optional: @someone_else> - get info on status of the current game (where to go to talk about it, whether it's even scheduled), optional argument to use on someone else

!league_schedule_game <date and time> - schedule a game, somehow i'll require confirmation from both players
!mod_league_schedule_game <@someone> <date and time> - schedule a game without requiring confirmation
'''

# Declare variables
GUILD_NAME = "pawngrubber's server"

# Read secret information from .env file
load_dotenv()
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')

# The bot
bot = commands.Bot(command_prefix='!')

# Database stuff
LDB = flg.LeagueDatabase()
def set_league(discord_id, join_type):
    LDB.league_join(
        fgg.get_month(1),
        discord_id,
        join_type=='player',
    )

@bot.event
async def on_ready():
    guild = discord.utils.get(bot.guilds, name=GUILD_NAME)
    print(f'Bot {bot.user.name} has connected to {guild.name}')
    LDB.update_signup_info(fgg.get_month(flg.NEXT_MONTH))
    #await fdd.announce_pairing(bot, guild)

@bot.event
async def on_command_error(ctx, error):
    message = fdd.on_command_error(ctx, error)
    if message is not None:
        await ctx.send(message)

@bot.event
async def on_command_completion(ctx):
    fdd.update_google_sheet()

def main():

    # Testing
    bot.add_command(fdd.test)

    # General commands
    bot.add_command(fdd.league_info)

    # League membership
    bot.add_command(fdd.user_set_chesscom)
    bot.add_command(fdd.mod_set_chesscom)
    bot.add_command(fdd.user_league_join)
    bot.add_command(fdd.mod_league_join)
    bot.add_command(fdd.user_league_join_current)
    bot.add_command(fdd.mod_league_join_current)
    bot.add_command(fdd.user_league_leave)
    bot.add_command(fdd.mod_league_leave)
    #bot.add_command(fdd.user_league_leave_current)
    bot.add_command(fdd.mod_league_leave_current)

    # Setting results
    bot.add_command(fdd.user_league_set_result)
    bot.add_command(fdd.mod_league_set_result)
    bot.add_command(fdd.mod_league_custom_result)

    # Requesting substitutes
    bot.add_command(fdd.user_league_request_sub_next_month)
    bot.add_command(fdd.user_league_request_sub_this_month)
    bot.add_command(fdd.mod_league_request_sub_next_month)
    bot.add_command(fdd.mod_league_request_sub_this_month)
    bot.add_command(fdd.user_league_claim_substitute)
    bot.add_command(fdd.mod_league_claim_substitute)

    bot.run(DISCORD_TOKEN)

if __name__ == '__main__':
    main()
