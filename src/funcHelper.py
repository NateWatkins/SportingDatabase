from HTTPHelper import send_request
import env
import json
env.load()
token = env.get("SPORTMONKS_API_TOKEN")



base = "https://api.sportmonks.com/v3/football/"



def build_player_season_stats_url(player_id, season_id,resource = None, include = None, filters = None):
    if resource == None: resource = f"players/{player_id}"
    if include == None: include = "statistics.details.type"
    if filters == None: filters = f"playerStatisticSeasons:{season_id}"
    return build_url(base, resource, token, include, filters)


def get_player_season_row(player_id, season_id, token):
    url = build_player_season_stats_url(player_id, season_id)
    response = send_request(url)
    
    data = response
    details = data["data"]["statistics"][0]["details"]
    stats = {}
    for d in details:
        code = d["type"]["code"]
        value = d["value"]

        
                
        if isinstance(value, dict) and "total" in value:
            value = value["total"]

        stats[code] = value
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
        stats.get("substitutions"),
        stats.get("hit-woodwork"),
        stats.get("redcards"),
        stats.get("goals-conceded"),
        stats.get("fouls-drawn"),
        stats.get("dribbled-past"),
        stats.get("rating"),
        stats.get("cleansheets"),
        stats.get("team-wins"),
        stats.get("team-draws"),
        stats.get("team-lost"),
        stats.get("lineups"),
        stats.get("bench"),
        stats.get("average-points-per-game"),
        stats.get("crosses-blocked"),
    ]
    return row

#Mainly A Debugging function
def get_player_season_row_detail(player_id, season_id, token):
    url = build_player_season_stats_url(player_id, season_id, None, None, f"playerStatisticSeasons:{season_id}")
    response = send_request(url)
    data = response
    return data


def build_url(base, resource, token, include=None, filters=None):
    url = f"{base}{resource}?api_token={token}"
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


