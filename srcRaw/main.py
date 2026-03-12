import env
from HTTPHelper import send_request
from funcHelper import build_player_season_stats_url,build_url, get_player_season_row, build_season_list,get_player_season_row_detail, insert_player, insert_league, insert_season,get_league_season_players,get_teams_for_season,insert_team,get_most_recent_season
env.load()
import json
from dbhelper import upload_player_seasons_stats,insert_player_season
from dbhelper import connect_db
import time 
token = env.get("SPORTMONKS_API_TOKEN")
# envPassword= env.get("DB_PASSWORD")
# envHost=env.get("DB_HOST") 
# envPort=env.get("DB_PORT")
# envDBName=env.get("DB_NAME")

envPassword= env.get("T_DB_PASSWORD")
envHost=env.get("T_DB_HOST") 
envPort=env.get("T_DB_PORT")
envDBName=env.get("T_DB_NAME")



###-----------------------------------------------------------
###-----------------------------------------------------------
###-----------------------------------------------------------


[8,564,301,384,82,208,72,74,779,462]
league_ids = [648]

##Ten Leagues In each pick one and plug into league_id's
[8,564,301,384,82,208,72,74,779,462]
[648,651,636,968,791,1607,27,304,310,85]
[9,573,444,944,181,387,453,262,244,672,675]


###-----------------------------------------------------------
###-----------------------------------------------------------
###-----------------------------------------------------------










# conn = connect_db("postgres", "natwat", "")
conn = connect_db(
    db_name=envDBName,  # or your actual DB name
    user="postgres",
    password=envPassword,
    host=envHost,
    port=envPort
)
cur = conn.cursor()



def make_caches():
    return {
        "player_desc": {},
        "league_desc": {},
        "season_desc": {},
        "team_desc": {},

        "most_recent_season": {},
        "teams_for_season": {},
        "team_squad_players": {},
        "league_season_players": {},

        "player_season_list": {},
        "league_for_season": {},
        "player_season_row": {},
        "seen_player_season_ids": set()
    }




def build_all_description_tables(conn, cur, league_ids, token):
    caches = make_caches()

    seen_league_ids = set()
    seen_season_ids = set()
    seen_team_ids = set()
    seen_player_ids = set()
    uploaded_player_stats = set()

    total_processed = 0
    BATCHSIZE = 10

    for league_id in league_ids:
        print(f"\n===== LEAGUE START {league_id} =====")
        league_processed = 0

        try:
            if league_id not in seen_league_ids:
                insert_league(cur, league_id, token, caches)
                seen_league_ids.add(league_id)

            season_id = get_most_recent_season(league_id, token, caches)

            if season_id not in seen_season_ids:
                insert_season(cur, season_id, token, caches)
                seen_season_ids.add(season_id)

            teams = get_teams_for_season(season_id, token, caches)
            print(f"[LEAGUE {league_id}] Teams in season {season_id}: {len(teams)}")

            for team in teams:
                team_id = team["id"]
                if team_id not in seen_team_ids:
                    insert_team(cur, team_id, league_id, token, caches)
                    seen_team_ids.add(team_id)

            player_ids = get_league_season_players(league_id, token, caches)
            print(f"[LEAGUE {league_id}] Players: {len(player_ids)}")

            for player_id in player_ids:
                try:
                    if player_id not in seen_player_ids:
                        insert_player(cur, player_id, token, caches)
                        seen_player_ids.add(player_id)

                    if player_id not in uploaded_player_stats:
                        upload_player_seasons_stats(cur, player_id, token, caches)
                        uploaded_player_stats.add(player_id)

                    total_processed += 1
                    league_processed += 1

                    print(f"[LEAGUE {league_id}] [{league_processed}/{len(player_ids)}] Finished player {player_id}")

                    if total_processed % BATCHSIZE == 0:
                        conn.commit()
                        print(f"[COMMIT] total_processed={total_processed}")
                    time.sleep(0.1)
                except Exception as e:
                    conn.rollback()
                    print(f"[ERROR] league_id={league_id} player_id={player_id} error={e}")

            print(f"===== LEAGUE COMPLETE {league_id} =====")

        except Exception as e:
            conn.rollback()
            print(f"[LEAGUE ERROR] league_id={league_id} error={e}")

    conn.commit()
    print(f"\nRUN COMPLETE | total players processed: {total_processed}")



# print(get_player_season_row_detail(player_id,season_id, token))

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


#upload_player_seasons_stats(cur, 52296, token)
build_all_description_tables(conn,cur,league_ids,token)
# insert_player_season(cur,player_id,season_id,token, 8)

conn.commit()
conn.close()



# player_id = 52296
# season_id = 16036
# teamId = 20
