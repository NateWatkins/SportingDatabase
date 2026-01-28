# DBHelper.py

import psycopg2
import sys
from funcHelper import get_player_season_list, get_player_season_row,insert_season,get_league_for_season,insert_league,insert_team


# connect_db(db_name, user, password, host="localhost", port="5432") -> conn
#     # Opens a PostgreSQL connection; called once at startup before any DB work.

# insert_player_season(cur, player_id, season_id, token) -> None
#     # For one (player_id, season_id): calls get_player_season_row, builds params, inserts 1 row into player_season_stats.

# upload_player_seasons_stats(cur, player_id, token) -> None
#     # For one player_id: gets all season_ids via get_player_season_list, loops them, and calls insert_player_season for each.

# get_player_season_list(player_id, token) -> list[season_id]
#     # From funcHelper: hits API (or cached data) to return all seasons where this player has stats; drives the season loop.

# get_player_season_row(player_id, season_id, token) -> (team_id, row)
#     # From funcHelper: for a single (player_id, season_id) gets raw API data, extracts team_id + ordered stats list matching PLAYER_SEASON_INSERT.

def connect_db(db_name, user, password, host="localhost", port="5432"):
    """
    Open a connection to PostgreSQL and return the connection object.
    Kill the script on failure (brute-force simple).
    """
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=user,
            password=password,
            host=host,
            port=port
        )
        print("PostgreSQL connection established.")
        return conn

    except psycopg2.Error as e:
        print(f"[DB ERROR] {e}")
        sys.exit(1)




PLAYER_SEASON_INSERT = """
    INSERT INTO player_season_stats (
        player_id, season_id, team_id,
        appearances, minutes_played, goals, shots_total, shots_on_target,
        passes, accurate_passes, accurate_passes_percentage, key_passes,
        dribble_attempts, successful_dribbles, tackles, interceptions,
        clearances, total_duels, duels_won, aerials_won, fouls, dispossessed,
        total_crosses, accurate_crosses, shots_blocked, sub_in, sub_out,
        hit_woodwork, redcards, goals_conceded, fouls_drawn, dribbled_past,
        cleansheets, team_wins, team_draws, team_lost, lineups, bench,
        avg_points, crosses_blocked
    ) VALUES (
        %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s,
        %s, %s
    )
    ON CONFLICT (player_id, season_id, team_id)
    DO NOTHING;
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



def insert_player_season(cur, player_id, season_id, token,league_id):
    team_id, row = get_player_season_row(player_id, season_id, token)
    insert_team(cur,team_id,league_id,token)
    params = [player_id, season_id, team_id] + row[1:]
    cur.execute(PLAYER_SEASON_INSERT, params)

def upload_player_seasons_stats(cur,conn, player_id, token):
    season_list = get_player_season_list(player_id, token)
    for season_id in season_list:
        league_id = get_league_for_season(season_id, token)
        insert_league(cur, league_id, token)  
        insert_season(cur,season_id,token)
        insert_player_season(cur, player_id, season_id, token,league_id)
        print(f"__--__--_- Finished Player - {player_id} ---- Season - {season_id}")
        






if __name__ == "__main__":
    conn = connect_db("postgres", "natwat", "")  #    conn = connect_db("your_db_name", "your_user", "your_password")
    cur = conn.cursor()
    cur.execute("SELECT 1;")
    print("Test query result:", cur.fetchone())
    cur.close()
    conn.close()



