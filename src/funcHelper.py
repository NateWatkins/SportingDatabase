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

###--------------URL's---------------------------------------------------

def build_player_season_stats_url(player_id, season_id = None,resource = None, include = None, filters = None):
    if resource == None: resource = f"players/{player_id}"
    if include == None: include = "statistics.details.type"
    if filters is None and season_id is not None:
        filters = f"playerStatisticSeasons:{season_id}"
    return build_url(base, resource, token, include, filters)

def build_player_description_url(player_id):
    resource = f"players/{player_id}"
    return build_url(base, resource, token, None, None)

def build_league_seasons_url(league_id):
    resource = f"leagues/{league_id}"
    include = "seasons"
    filters = None
    return build_url(base, resource, token, include, filters)

def build_teams_by_season_url(season_id):
    resource = f"teams/seasons/{season_id}"
    include = None
    filters = None
    return build_url(base, resource, token, include, filters)

def build_team_squad_url(team_id, season_id):
    resource = f"squads/seasons/{season_id}/teams/{team_id}"
    include = None
    filters = None
    return build_url(base, resource, token, include, filters)

###---------------------------------
###------ API Caller's -------------


def get_player_season_row(player_id, season_id, token):
    url = build_player_season_stats_url(player_id, season_id)
    response = send_request(url)
    data = response
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
        stats.get("minutes-played"),
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

#League -> most recent season_id
def get_most_recent_season(league_id, token):

    url = build_league_seasons_url(league_id)
    response = send_request(url)
    data = response["data"]

    seasons = data.get("seasons", []) or []
    if not seasons:
        raise ValueError(f"No seasons found for league {league_id}")

    # try end_date, fall back to ending_at if needed
    def season_key(s):
        return s.get("end_date") or s.get("ending_at") or ""

    most_recent = max(seasons, key=season_key)
    return most_recent["id"]

#Season -> list of team dicts
def get_teams_for_season(season_id, token):
    """
    Season -> list of team dicts
    """
    url = build_teams_by_season_url(season_id)
    response = send_request(url)
    return response.get("data", [])  # list of teams

#Team + Season -> list of player_ids in that squad
def get_team_squad_player_ids(team_id, season_id, token):

    url = build_team_squad_url(team_id, season_id)
    response = send_request(url)
    rows = response.get("data", []) or []
    return [row["player_id"] for row in rows]

#League -> (current season) -> (all teams) -> (all squad player_ids)
def get_league_season_players(league_id, token):

    season_id = get_most_recent_season(league_id, token)
    teams = get_teams_for_season(season_id, token)

    player_ids = set()

    for team in teams:
        team_id = team["id"]
        squad_players = get_team_squad_player_ids(team_id, season_id, token)
        for pid in squad_players:
            player_ids.add(pid)

    return list(player_ids)


###---------------------------------
##Insert Statements
LEAGUE_INSERT = """
    INSERT INTO leagues (
        league_id,
        name,
        country_id
    ) VALUES (%s, %s, %s)
    ON CONFLICT (league_id) DO UPDATE
    SET name       = EXCLUDED.name,
        country_id = EXCLUDED.country_id;
"""
SEASON_INSERT = """
    INSERT INTO seasons (
        season_id,
        league_id,
        name,
        start_date,
        end_date
    ) VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (season_id) DO UPDATE
    SET league_id  = EXCLUDED.league_id,
        name       = EXCLUDED.name,
        start_date = EXCLUDED.start_date,
        end_date   = EXCLUDED.end_date;
"""
PLAYER_INSERT = """
    INSERT INTO players (
        player_id,
        first_name,
        last_name,
        display_name,
        country
    ) VALUES (
        %s, %s, %s, %s, %s
    )
    ON CONFLICT (player_id)
    DO NOTHING;
"""
TEAM_INSERT = """
    INSERT INTO teams (
        team_id,
        name,
        league_id
    ) VALUES (
        %s, %s, %s
    )
    ON CONFLICT (team_id) DO NOTHING;
"""



##--------Inserts--------------------
##----------------------
##Player Description Table
def insert_player(cur, player_id, token):
    row = get_player_description_row(player_id, token)
    cur.execute(PLAYER_INSERT, row)
    print("executed")
def get_player_description_row(player_id, token):
    url = build_player_description_url(player_id)
    response = send_request(url)
    data = response["data"]
    
    player_id   = data["id"]
    first_name  = data.get("firstname")
    last_name   = data.get("lastname")
    display_name = data.get("display_name") or data.get("name")
    
    country_data = data.get("nationality_id")
    print("____----------______-----___---__---_---_--__")
    print(data)
    print("_------__----_--__--_--_______---__--_--_--_-")
    if isinstance(country_data, dict):
        country_data = country_data.get("name")

    return [
        player_id,
        first_name,
        last_name,
        display_name,
        country_data,
    ]
##Player Desription table
##--------------------
##League Description Table
def build_league_description_url(league_id):
    resource = f"leagues/{league_id}"
    include = None
    filters = None
    return build_url(base, resource, token, include, filters)
def get_league_description_row(league_id, token):
    url = build_league_description_url(league_id)
    response = send_request(url)
    data = response["data"]

    league_id  = data["id"]
    name       = data.get("name")
    country_id = data.get("country_id")  # raw ID, may be None

    return [
        league_id,
        name,
        country_id,
    ]
def insert_league(cur, league_id, token):
    row = get_league_description_row(league_id, token)
    cur.execute(LEAGUE_INSERT, row)
    print("inserted league", league_id)
##League Description Table
##-----------------------
##Season Description Table
def build_season_description_url(season_id):
    # e.g. GET /seasons/{season_id}
    resource = f"seasons/{season_id}"
    include = None
    filters = None
    return build_url(base, resource, token, include, filters)
def get_season_description_row(season_id, token):
    url = build_season_description_url(season_id)
    response = send_request(url)
    data = response["data"]

    season_id  = data["id"]
    league_id  = data.get("league_id")
    name       = data.get("name")
    # Sportmonks uses starting_at / ending_at timestamps – you can store as DATE or TIMESTAMP
    start_date = data.get("starting_at")
    end_date   = data.get("ending_at")

    return [
        season_id,
        league_id,
        name,
        start_date,
        end_date,
    ]
def insert_season(cur, season_id, token):
    row = get_season_description_row(season_id, token)
    cur.execute(SEASON_INSERT, row)
    print("inserted season", season_id)
##Season Description Table
##---------------------
##Team Description Table
def build_team_description_url(team_id):
    resource = f"teams/{team_id}"
    include = None
    filters = None
    return build_url(base, resource, token, include, filters)
def get_team_description_row(team_id, league_id, token):
    url = build_team_description_url(team_id)
    response = send_request(url)
    data = response["data"]

    team_id   = data["id"]
    name      = data.get("name")

    # league_id comes from the caller (you already know what league you're loading)
    return [
        team_id,
        name,
        league_id,
    ]
def insert_team(cur, team_id, league_id, token):
    row = get_team_description_row(team_id, league_id, token)
    cur.execute(TEAM_INSERT, row)
    print("inserted team", team_id)

##Team Description Table
##--------------------





##----------Checks-------------------





#Mainly A Debugging function
def get_player_season_row_detail(player_id, season_id, token):
    url = build_player_season_stats_url(player_id, season_id, None, None, f"playerStatisticSeasons:{season_id}")
    response = send_request(url)
    data = response
    return data



def build_url(base, resource, token, include=None, filters=None):
    url = f"{base}{resource}?api_token={token}"
    # print(url)
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

def get_league_for_season(season_id, token):
    url = build_url(
        base="https://api.sportmonks.com/v3/football/",
        resource=f"seasons/{season_id}",
        token=token
    )
    resp = send_request(url)
    return resp["data"]["league_id"]



def get_player_season_list(player_id, token):
    url = build_player_season_stats_url(player_id)
    response = send_request(url)
    data = response
    return build_season_list(data)


if __name__ == "__main__":
    token = env.get("SPORTMONKS_API_TOKEN")  # or however you load it
    league_id = 8   # Premier League for test

    print("\n=== TEST 1: get_most_recent_season ===")
    try:
        season_id = get_most_recent_season(league_id, token)
        print("Most recent season_id:", season_id)
    except Exception as e:
        print("ERROR:", e)

    print("\n=== TEST 2: get_teams_for_season ===")
    try:
        teams = get_teams_for_season(season_id, token)
        print("Team count:", len(teams))
        print("First team sample:", teams[0] if teams else None)
    except Exception as e:
        print("ERROR:", e)

    print("\n=== TEST 3: get_team_squad_player_ids ===")
    if teams:
        sample_team_id = teams[0]["id"]
        print("Testing with team_id:", sample_team_id)
        try:
            squad_players = get_team_squad_player_ids(sample_team_id, season_id, token)
            print("Squad player count:", len(squad_players))
            print("Sample player_id(s):", squad_players[:10])
        except Exception as e:
            print("ERROR:", e)
    else:
        print("No teams available; skipping TEST 3")

    print("\n=== TEST 4: get_league_season_players ===")
    try:
        player_ids = get_league_season_players(league_id, token)
        print("Total unique players:", len(player_ids))
        print("Sample player_ids:", player_ids[:20])
    except Exception as e:
        print("ERROR:", e)

    print("\n=== ALL TESTS COMPLETED ===")




