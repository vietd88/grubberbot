import urllib
import urllib.error
import urllib.request
import json
import os
import datetime
from pprint import pprint

DISCORD_CHESSCOM_JSON = 'data/discord_chesscom.json'
LEAGUE_DIR = 'data/{}'
LEAGUE_JSON = f'{LEAGUE_DIR}/rapid_league.json'

GENERAL_ERROR_MESSAGE = '{} `!{}` error, get help with `!help {}`'

def get_month(next=False):
    date = datetime.datetime.now()
    if next:
        month = (date.month + 1 - 1) % 12 + 1 # Wrap December
        next_date = datetime.date(date.year, month, 1)
        date_string = next_date.strftime('%B%Y')
    else:
        date_string = date.strftime('%B%Y')

    return date_string

THIS_LEAGUE_JSON = LEAGUE_JSON.format(get_month())
NEXT_LEAGUE_JSON = LEAGUE_JSON.format(get_month(next=True))

def gen_files():
    folders = [
        LEAGUE_DIR.format(m)
        for m in [get_month(), get_month(next=True)]
    ]
    for folder in folders:
        if not os.path.exists(folder):
            os.makedirs(folder)

    jsons_to_gen = [
        DISCORD_CHESSCOM_JSON,
        THIS_LEAGUE_JSON,
        NEXT_LEAGUE_JSON,
    ]

    for filename in jsons_to_gen:
        if not os.path.exists(filename):
            data = {}
            with open(filename, 'w') as f:
                json.dump(data, f, sort_keys=True, indent=4)

def exists_chesscom(username, return_message=False):
    url = f'https://api.chess.com/pub/player/{username}'
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

def set_chesscom(discord_id, discord_name, chesscom):
    discord_id = str(discord_id)
    with open(DISCORD_CHESSCOM_JSON, 'r') as f:
        data = json.load(f)
    data[discord_id] = {
        'discord': discord_name,
        'chesscom': chesscom,
    }
    with open(DISCORD_CHESSCOM_JSON, 'w') as f:
        json.dump(data, f, sort_keys=True, indent=4)

def get_chesscom(discord_id):
    discord_id = str(discord_id)
    with open(DISCORD_CHESSCOM_JSON, 'r') as f:
        data = json.load(f)
    if discord_id in data:
        return data[discord_id]['chesscom']
    else:
        return None

def set_league(discord_id, discord_name, join_type):
    discord_id = str(discord_id)
    with open(NEXT_LEAGUE_JSON, 'r') as f:
        data = json.load(f)
    chesscom = get_chesscom(discord_id)
    if chesscom is None:
        raise Exception
    if discord_id in data:
        data[discord_id] = {
            'discord': discord_name,
            'chesscom': chesscom,
            'join_type': join_type,
            'sub_week': data[discord_id]['sub_week'],
        }
    else:
        data[discord_id] = {
            'discord': discord_name,
            'chesscom': chesscom,
            'join_type': join_type,
            'sub_week': {i: False for i in range(1, 5)},
        }
    with open(NEXT_LEAGUE_JSON, 'w') as f:
        json.dump(data, f, sort_keys=True, indent=4)

def get_league(discord_id):
    discord_id = str(discord_id)
    with open(NEXT_LEAGUE_JSON, 'r') as f:
        data = json.load(f)
    if discord_id in data:
        return data[discord_id]
    else:
        return None

def get_league_info():
    with open(NEXT_LEAGUE_JSON, 'r') as f:
        data = json.load(f)
    return data

def del_league(discord_id):
    discord_id = str(discord_id)
    with open(NEXT_LEAGUE_JSON, 'r') as f:
        data = json.load(f)
    if discord_id not in data:
        return None
    output = data[discord_id]
    del data[discord_id]
    with open(NEXT_LEAGUE_JSON, 'w') as f:
        json.dump(data, f, sort_keys=True, indent=4)
    return output

def count_games(chesscom, rapid=False):
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
    if rapid:
        categories = ['chess_rapid']
    wdl_list = ['win', 'loss', 'draw']

    num_games = 0
    for category in categories:
        try:
             wdl = info[category]['record']
             num_sum = sum([v for k, v in wdl.items() if k in wdl_list])
             num_games += num_sum
        except KeyError:
            pass

    return num_games

def get_rating(chesscom):
    if chesscom is None:
        return None
    url = f'https://api.chess.com/pub/player/{chesscom}/stats'
    with urllib.request.urlopen(url) as response:
        info = response.read()
    info = json.loads(info)
    rapid_rating = info['chess_rapid']['last']['rating']
    rapid_date = info['chess_rapid']['last']['date']
    return rapid_rating

def split_into_teams():
    with open(THIS_LEAGUE_JSON, 'r') as f:
        data = json.load(f)
    #print(data)
    data = {
        k: {kv: vv for kv, vv in v.items()}.update(**{'rating': get_rating(v['chesscom'])})
        for k, v in data.items()
    }
    #print(data)

if __name__ == '__main__':
    with open('mapping.json', 'r') as f:
        id_dict = json.load(f)
    print(id_dict)

    '''
    with open(NEXT_LEAGUE_JSON, 'r') as f:
        data = json.load(f)
    data = {
        k: {
            'discord': v['discord'],
            'chesscom': v['chesscom'],
        } for k, v in data.items()
    }
    with open(DISCORD_CHESSCOM_JSON, 'w') as f:
        json.dump(data, f, sort_keys=True, indent=4)
    '''

    '''
    username = 'pawngrubber'
    gen_files()
    split_into_teams()
    print(get_rating(username))
    print(get_month())
    print(get_month(next=True))
    '''
