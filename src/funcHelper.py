from HTTPHelper import send_request
import env
import json
env.load()
token = env.get("SPORTMONKS_API_TOKEN")



base = "https://api.sportmonks.com/v3/football/"


# build_player_season_stats_url(player_id, season_id=None, resource=None, include=None, filters=None) -> str (url)
    # Builds the SportMonks API URL for a player's season stats; default include=statistics.details.type; used by all season-stat calls.

# get_player_season_row(player_id, season_id, token) -> (team_id: int, row: list)
#     # Calls API for one (player_id, season_id), parses JSON into team_id + ordered stats list matching PLAYER_SEASON_INSERT (minus player_id).

# get_player_season_row_detail(player_id, season_id, token) -> dict (data)
#     # Debug helper: returns the full raw JSON for a single (player_id, season_id) request without flattening into a stats row.

# build_url(base, resource, token, include=None, filters=None) -> str (url)
#     # Low-level URL builder used by build_player_season_stats_url; attaches api_token, include, and filters query parameters.

# build_season_stat_list(player_id, season_list, token) -> list[(team_id, row)]
#     # For one player_id and a list of season_ids: loops seasons, calls get_player_season_row for each, and collects all (team_id, row) pairs.

# build_season_list(data) -> list[season_id]
#     # Given raw JSON response containing statistics[]: extracts all season_id values into a flat Python list.

# get_player_season_list(player_id, token) -> list[season_id]
#     # High-level season discovery: calls API once (no season filter), then uses build_season_list to return all seasons where this player has stats.

###--------------URL's

def build_player_season_stats_url(player_id, season_id = None,resource = None, include = None, filters = None):
    if resource == None: resource = f"players/{player_id}"
    if include == None: include = "statistics.details.type"
    if filters is None and season_id is not None:
        filters = f"playerStatisticSeasons:{season_id}"
    return build_url(base, resource, token, include, filters)

def build_player_description_url(player_id):
    resource = f"players/{player_id}"
    return build_url(base, resource, token, None, None)

###---------------------------------
###------ API Caller's -------------
def get_player_season_row(player_id, season_id, token):
    url = build_player_season_stats_url(player_id, season_id)
    response = send_request(url)
    data = response
    print(url)
    team_id = response['data']['statistics'][0]['team_id']
    details = data["data"]["statistics"][0]["details"]
    stats = {}
    for d in details:
        code = d["type"]["code"]
        value = d["value"]

        
                
        if isinstance(value, dict) and "total" in value:
            value = value["total"]

        stats[code] = value

    subs = stats.get("substitutions") or {}
    sub_in = subs.get("in")
    sub_out = subs.get("out")

    apg = stats.get("average-points-per-game") or {}
    avg_points = apg.get("average")

    cb = stats.get("crosses-blocked") or {}
    crosses_blocked = cb.get("crosses_blocked")
    row = [
        season_id,
        stats.get("appearances"),
        stats.get("minutes_played"),
        stats.get("goals"),
        stats.get("shots-total"),
        stats.get("shots-on-target"),
        stats.get("passes"),
        stats.get("accurate-passes"),
        stats.get("accurate-passes-percentage"),
        stats.get("key-passes"),
        stats.get("dribble-attempts"),
        stats.get("successful-dribbles"),
        stats.get("tackles"),
        stats.get("interceptions"),
        stats.get("clearances"),
        stats.get("total-duels"),
        stats.get("duels-won"),
        stats.get("aeriels-won"),
        stats.get("fouls"),
        stats.get("dispossessed"),
        stats.get("total-crosses"),
        stats.get("accurate-crosses"),
        stats.get("shots-blocked"),
        sub_in,
        sub_out,
        stats.get("hit-woodwork"),
        stats.get("redcards"),
        stats.get("goals-conceded"),
        stats.get("fouls-drawn"),
        stats.get("dribbled-past"),
        stats.get("cleansheets"),
        stats.get("team-wins"),
        stats.get("team-draws"),
        stats.get("team-lost"),
        stats.get("lineups"),
        stats.get("bench"),
        avg_points,
        crosses_blocked
    ]
    return team_id, row



def get_player_description_row(player_id, token):
    url = build_player_description_url(player_id)
    response = send_request(url)
    data = response["data"]

    player_id   = data["id"]
    first_name  = data.get("firstname")
    last_name   = data.get("lastname")
    display_name = data.get("display_name") or data.get("name")

    country = None
    country_data = data.get("country")
    if isinstance(country_data, dict):
        country = country_data.get("name")

    return [
        player_id,
        first_name,
        last_name,
        display_name,
        country,
    ]






##--------Inserts--------------------

def insert_player(cur, player_id, token):
    row = get_player_description_row(player_id, token)
    cur.execute(PLAYER_INSERT, row)








##----------Checks-------------------





#Mainly A Debugging function
def get_player_season_row_detail(player_id, season_id, token):
    url = build_player_season_stats_url(player_id, season_id, None, None, f"playerStatisticSeasons:{season_id}")
    response = send_request(url)
    data = response
    return data



def build_url(base, resource, token, include=None, filters=None):
    url = f"{base}{resource}?api_token={token}"
    print(url)
    if include:
        url += f"&include={include}"
    if filters:
        url += f"&filters={filters}"
    return url


def build_season_stat_list(player_id, season_list,token):
    player_career_stats = []
    
    for season in season_list:
        player_career_stats.append(get_player_season_row(player_id, season,token))

    return player_career_stats



def build_season_list(data):
    # data = json.loads(data.text)
    num_seasons = len(data['data']['statistics'])
    season_list = []


    for x in range(num_seasons):
        season_list.append(data['data']['statistics'][x]['season_id'])

    return season_list



def get_player_season_list(player_id, token):
    url = build_player_season_stats_url(player_id)
    response = send_request(url)
    data = response
    return build_season_list(data)
