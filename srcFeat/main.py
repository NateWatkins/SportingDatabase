import sys
import os
import psycopg2
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from srcRaw import env
env.load()

from queries import pull_leagues, pull_seasons, pull_teams, pull_player_season_stats

LEAGUE_ID = 648

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
            port=port,
            sslmode="require",  # required for AWS RDS
            connect_timeout=5
        )
        print("Cloud connection established.")
        return conn

    except psycopg2.Error as e:
        print(f"[DB ERROR] {e}")
        sys.exit(1)


conn = connect_db(
    db_name=env.get("DB_NAME"),
    user="postgres",
    password=env.get("DB_PASSWORD"),
    host=env.get("DB_HOST"),
    port=env.get("DB_PORT")
)

# Pull reference tables filtered to league 648
leagues = pull_leagues(conn)
league = leagues[leagues["league_id"] == LEAGUE_ID]
print(f"League:\n{league}\n")

seasons = pull_seasons(conn)
league_seasons = seasons[seasons["league_id"] == LEAGUE_ID]
season_ids = league_seasons["season_id"].tolist()
print(f"Seasons ({len(season_ids)}):\n{league_seasons}\n")

teams = pull_teams(conn)
league_teams = teams[teams["league_id"] == LEAGUE_ID]
print(f"Teams ({len(league_teams)}):\n{league_teams}\n")

# Pull stats for all seasons in this league
stats = pull_player_season_stats(conn, season_ids=season_ids)
print(f"player_season_stats shape: {stats.shape}")
print(stats.head())

conn.close()
