import datetime
import json
import logging
import os
import sys
from pprint import pprint
from typing import Optional

import discord
import funcs_discord as fdd
import funcs_general as fgg
import funcs_league as flg
import pandas as pd
import yaml
from discord.ext import commands

logging.basicConfig(
    filename="grubberbot.log",
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S %p",
)
MODERATOR_ROLES = [
    "The Grubber",
    "Mods",
    "Cool People",
]

# TODO: sql injection problem
# TODO: @everyone problem
# TODO: Backup sqlite3 data
# TODO: !info command
# TODO: ping users before a season starts to make sure they're active on discord

# !list_commands
# for scheduling a game
# !league_game_status <optional: @someone_else> - get info on status of the
# current game (where to go to talk about it, whether it's even scheduled),
# optional argument to use on someone else

# !league_schedule_game <date and time> - schedule a game, somehow i'll
# require confirmation from both players

# !mod_league_schedule_game <@someone> <date and time> - schedule a game
# without requiring confirmation

# Declare variables
GUILD_NAME = "pawngrubber's server"

# Read secret information from yaml file
DISCORD_TOKEN_LOCATION = "credentials/discord.yml"
with open(DISCORD_TOKEN_LOCATION, "r") as f:
    data = yaml.safe_load(f)
DISCORD_TOKEN = data["DISCORD_TOKEN"]

# The bot
bot = commands.Bot(command_prefix="!")

# Database stuff
LDB = flg.LeagueDatabase()


def set_league(discord_id, join_type):
    LDB.league_join(
        fgg.get_month(1),
        discord_id,
        join_type == "player",
    )


@bot.event
async def on_ready():
    guild = discord.utils.get(bot.guilds, name=GUILD_NAME)
    message = f"{bot.user.mention} has connected to {guild.name}"
    print(message)
    channel = discord.utils.get(guild.channels, name="grubberbot-logs")
    await channel.send(message)
    fdd.update_google_sheet()
    # await fdd.announce_pairing(bot, guild)


@bot.event
async def on_command_error(ctx, error):
    message = fdd.on_command_error(ctx, error)
    if message is not None:
        await ctx.send(message)


@bot.event
async def on_command_completion(ctx):
    fdd.update_google_sheet()


@commands.command(name="commands")
async def user_commands(ctx):
    """List all user commands available to GrubberBot"""
    message = [
        f"`!{command}`"
        for command in bot.commands
        if not str(command).startswith("mod")
    ]
    message = sorted(message)
    message = "\n".join(message)
    await ctx.send(message)


@commands.command(name="mod_commands")
async def mod_commands(ctx):
    """List all mod commands available to GrubberBot"""
    message = [
        f"`!{command}`" for command in bot.commands if str(command).startswith("mod")
    ]
    message = sorted(message)
    message = "\n".join(message)
    await ctx.send(message)


def main():

    # Testing
    bot.add_command(fdd.test)
    bot.add_command(fdd.reboot)
    bot.add_command(user_commands)
    bot.add_command(mod_commands)

    # General commands
    bot.add_command(fdd.league_info)

    # League membership
    bot.add_command(fdd.user_set_chesscom)
    bot.add_command(fdd.user_join_player)
    bot.add_command(fdd.user_join_substitute)
    bot.add_command(fdd.user_join_current)
    bot.add_command(fdd.user_leave_next)
    # bot.add_command(fdd.user_leave_current)

    bot.add_command(fdd.mod_set_chesscom)
    bot.add_command(fdd.mod_join_player)
    bot.add_command(fdd.mod_join_substitute)
    bot.add_command(fdd.mod_join_current)
    bot.add_command(fdd.mod_leave_next)
    bot.add_command(fdd.mod_leave_current)

    # Setting results
    bot.add_command(fdd.user_schedule)
    bot.add_command(fdd.mod_schedule)
    bot.add_command(fdd.user_set_result)
    bot.add_command(fdd.mod_set_result)
    bot.add_command(fdd.mod_custom_result)

    # Requesting substitutes
    bot.add_command(fdd.user_request_substitute_next)
    bot.add_command(fdd.user_request_substitute_current)
    bot.add_command(fdd.user_claim_substitute)

    bot.add_command(fdd.mod_request_substitute_next)
    bot.add_command(fdd.mod_request_substitute_current)
    bot.add_command(fdd.mod_claim_substitute)

    bot.run(DISCORD_TOKEN)


if __name__ == "__main__":
    main()
