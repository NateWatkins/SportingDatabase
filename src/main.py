import env
from HTTPHelper import send_request
from funcHelper import build_player_season_stats_url,build_url, get_player_season_row, build_season_list,get_player_season_row_detail, insert_player, insert_league, insert_season,get_league_season_players,get_teams_for_season,insert_team,get_most_recent_season
env.load()
import json
from dbhelper import upload_player_seasons_stats,insert_player_season
from dbhelper import connect_db




token = env.get("SPORTMONKS_API_TOKEN")

player_id = 52296
season_id = 16036
teamId = 20

base = "https://api.sportmonks.com/v3/football/"
resource = f"players/{player_id}"
include = "statistics.season"
filters = f"playerStatistics"
filters = None

# print(get_player_season_row(player_id, season_id, token))

#Functions:
#build_url                                  - Given base resource include filter strings            ->  returns HTTP String ready to send                                   seasonPlayerRow = get_player_season_row(player_id, season_id, token)
#get_player_season_row                      - Given Season_id, player_id, token                     ->  returns player/Season row of statistics
#build_season_list                          - Given Response Body of player-season                  ->  returns list of seasons    
#Has_values                                 - Given Response Body looks for has_value               ->  returns boolean of has_value
#build_season_stat_list                     - Given player_id, season_list                          -> returns 2D list of statistics per player_season 
#get_player_season_row_detail(player_id, season_id,token)                       -> returns the details of the player-season-row



ul = build_url(base, resource, token, include, filters)
data = send_request(ul) 



# player_seasons = build_season_list(data)
# print(player_seasons)


# # print(get_player_season_row_detail(player_id, season_id,token))
def build_season_stat_list(player_id, season_list,token):
    player_career_stats = []   
    for season in season_list:
        rowVal = get_player_season_row_detail(player_id,season, token)
        print(rowVal)
        rowVal['data']['statistics'][0]['has_values']
        if(rowVal['data']['statistics'][0]['has_values'] == True):
            player_career_stats.append(get_player_season_row(player_id, season,token))
        else:
            print("nah son")
    return player_career_stats
# print(build_season_stat_list(player_id, player_seasons, token))


# conn = connect_db("postgres", "natwat", "")
conn = connect_db(
    db_name="postgres",  # or your actual DB name
    user="eval_master",
    password="appl3pid11",
    host="player-eval-dev.cp6si6q8kkrb.us-west-1.rds.amazonaws.com",
    port="5432"
)
cur = conn.cursor()


league_ids = [5]


def build_all_description_tables(conn,cur, league_ids, token):
    seen_team_ids = set()
    for league_id in league_ids:
        #league description
        insert_league(cur, league_id, token)

        #Get most recent season_id to build lineup -> Add to description table now
        season_id = get_most_recent_season(league_id, token)
        insert_season(cur, season_id, token)

        #Get most recent 
        
        teams = get_teams_for_season(season_id, token)
        print("Teams in season", season_id, ":", len(teams))

        #Description Table for all teams in the league
        for team in teams:
            team_id = team["id"]
            if team_id not in seen_team_ids:
                insert_team(cur, team_id, league_id, token)
                seen_team_ids.add(team_id)

        player_ids = get_league_season_players(league_id, token)
        print("Players in league", league_id, ":", len(player_ids))

        #Description table for all players
        for player_id in player_ids:
            insert_player(cur,player_id,token)
            upload_player_seasons_stats(cur,player_id,token)
            print(f"_____-----__--__--_--_--_--_--__-__-__- Finished Player: {player_id}")
            conn.commit()
        print("Finished league:", league_id)



# print(get_player_season_row_detail(player_id,season_id, token))

# upload_player_seasons_stats(cur, 52296, token)
build_all_description_tables(conn,cur,league_ids,token)
# insert_player_season(cur,player_id,season_id,token, 8)


conn.commit()



conn.close()
