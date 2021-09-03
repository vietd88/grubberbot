import discord
from pprint import pformat, pprint
from discord.ext import commands
import pandas as pd
from tqdm import tqdm
import logging
from typing import Optional
import datetime

import bot_funcs as bfn

DISCORD_USERS_PARQUET = 'data/discord_users.parquet'
DISCORD_HISTORY_PARQUET = 'data/discord_history.parquet'
LOG_FILE = 'data/grubberbot.log'
GRUBBER_MENTION = '<@490529572908171284>'

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
)

GENERAL_ERROR_MESSAGE = '{mention} `!{name}` error, get help with `!help {name}`'

LDB = bfn.LeagueDatabase()

HELP_DICT = {
    'test': {'name': 'test', 'help_text': (
        'A test message to make sure GrubberBot is working'
    )},
    'league_info': {'name': 'league_info', 'help_text': (
        'Get league info, optionally @mention someone to get info about them'
    )},
}

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
    command_name = ctx.message.content.split(' ')[0]
    command_name = command_name[1:]
    help_text = [
        v['help_text'] for k, v in HELP_DICT.items()
        if v['name'] == command_name
    ]
    if len(help_text) == 0:
        return None

        # TODO: give user list of commands
        message = f'Unknown error, pinging {GRUBBER_MENTION}'
        logging_message = (
            f'{datetime.datetime.now()} || {user} '
            f'|| {ctx.message.clean_content} || {message}'
        )
        print(logging_message)
        logging.error(logging_message)
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
    df = LDB.get_week_games(LDB.get_month(bfn.NEXT_MONTH), 1)
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
            'Hi! September Rapid League Week 1 has started, please use'
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

@commands.command(**HELP_DICT['league_info'])
async def league_info(ctx, discord_mention: Optional[discord.Member]=None):
    df = LDB.update_signup_info(LDB.get_month(bfn.NEXT_MONTH))
    mention = ctx.message.author.mention
    user = discord_mention

    next_month = LDB.get_month(bfn.NEXT_MONTH)
    if discord_mention is None:
        message = '\n'.join([
            f'{mention} ',
            f'* Number of members in the {next_month} season: {len(df)}',
        ])
    else:
        df = LDB.get_league_info(user.id)
        if len(df) == 0:
            message = f'{mention} user {user.mention} is not signed up for the league {next_month} season'
        else:
            info = {str(c): df[c][0] for c in df.columns}
            message = '\n'.join([
                f'{mention} the user {user.mention} has:',
                pformat(info),
            ])

    await ctx.send(message)

# For testing
@commands.command(**HELP_DICT['test'])
async def test(ctx):
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
    }

    LIMIT = None
    for channel in tqdm(list(guild.channels)):
        print(channel.name)
        if hasattr(channel, 'history'):
            msgs = await channel.history(limit=LIMIT).flatten()
            for msg in msgs:
                data['date'].append(msg.created_at)
                data['id'].append(msg.author.id)
                data['name'].append(str(msg.author))
                data['text'].append(msg.content)
                data['channel'].append(channel.name)

    df = pd.DataFrame(data)
    df.to_parquet(DISCORD_HISTORY_PARQUET)
    print(f'updated {DISCORD_HISTORY_PARQUET}')

class CustomClient(discord.Client):
    async def on_ready(self):
        guild = discord.utils.get(client.guilds, name=GUILD_NAME)
        print(f'Client {client.user} has connected to {guild.name}')

        save_user_data(guild)
        save_discord_history(guild)

if __name__ == '__main__':
    intents = discord.Intents.all()
    client = CustomClient(intents=intents)
    client.run(TOKEN)
