import env
from HTTPHelper import send_request
from funcHelper import build_url, get_player_season_row, build_season_list,get_player_season_row_detail, insert_player, insert_league, insert_season,get_league_season_players,get_teams_for_season,insert_team,get_most_recent_season
env.load()
import json
from dbhelper import upload_player_seasons_stats
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
# # print(get_player_season_row_detail(player_id, season_id,token))
# def build_season_stat_list(player_id, season_list,token):
#     player_career_stats = []
    
#     for season in season_list:
#         rowVal = get_player_season_row_detail(player_id,season, token)
#         rowVal['data']['statistics'][0]['has_values']
#         if(rowVal['data']['statistics'][0]['has_values'] == True):
#             player_career_stats.append(get_player_season_row(player_id, season,token))
#         else:
#             print("nah son")
#     return player_career_stats





conn = connect_db("postgres", "natwat", "")
cur = conn.cursor()


league_ids = [486]


def build_all_description_tables(cur, league_ids, token):
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
            upload_player_seasons_stats(cur,conn,player_id,token)
            print(f"_____-----__--__--_--_--_--_--__-__-__- Finished Player: {player_id}")
        print("Finished league:", league_id)



print(get_player_season_row_detail(player_id,season_id, token))

# upload_player_seasons_stats(cur, 52296, token)
# build_all_description_tables(cur,league_ids,token)


conn.commit()



conn.close()
