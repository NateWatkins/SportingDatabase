# create_tables.py
from dbhelper import connect_db



def reset_tables(conn):
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS player_season_stats CASCADE;")
    cur.execute("DROP TABLE IF EXISTS players CASCADE;")
    cur.execute("DROP TABLE IF EXISTS teams CASCADE;")
    cur.execute("DROP TABLE IF EXISTS seasons CASCADE;")
    cur.execute("DROP TABLE IF EXISTS leagues CASCADE;")

    conn.commit()
    cur.close()


def create_tables(conn):
    cur = conn.cursor()


    # 1) leagues
    cur.execute("""
        CREATE TABLE IF NOT EXISTS leagues (
            league_id   BIGINT PRIMARY KEY,
            name        TEXT
        );
    """)

    # 2) seasons
    cur.execute("""
        CREATE TABLE IF NOT EXISTS seasons (
            season_id   BIGINT PRIMARY KEY,
            league_id   BIGINT REFERENCES leagues(league_id),
            name        TEXT,
            start_date  DATE,
            end_date    DATE
        );
    """)

    # 3) teams
    cur.execute("""
        CREATE TABLE IF NOT EXISTS teams (
            team_id     BIGINT PRIMARY KEY,
            name        TEXT,
            league_id   BIGINT REFERENCES leagues(league_id)
        );
    """)



    # 4) players
    cur.execute("""
        CREATE TABLE IF NOT EXISTS players (
            player_id     BIGINT PRIMARY KEY,
            first_name    TEXT,
            last_name     TEXT,
            display_name  TEXT,
            country       TEXT
        );
    """)

    # 5) player_season_stats (wide table)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS player_season_stats (
            player_id   BIGINT REFERENCES players(player_id),
            season_id   BIGINT REFERENCES seasons(season_id),
            team_id     BIGINT REFERENCES teams(team_id),

            -- 1:1 with get_player_season_row (after the keys)
            appearances                 INT,
            minutes_played              INT,
            goals                       INT,
            shots_total                 INT,
            shots_on_target             INT,
            passes                      INT,
            accurate_passes             INT,
            accurate_passes_percentage  NUMERIC,
            key_passes                  INT,
            dribble_attempts            INT,
            successful_dribbles         INT,
            tackles                     INT,
            interceptions               INT,
            clearances                  INT,
            total_duels                 INT,
            duels_won                   INT,
            aerials_won                 INT,
            fouls                       INT,
            dispossessed                INT,
            total_crosses               INT,
            accurate_crosses            INT,
            shots_blocked               INT,
            sub_in                      INT,
            sub_out                     INT,
            hit_woodwork                INT,
            redcards                    INT,
            goals_conceded              INT,
            fouls_drawn                 INT,
            dribbled_past               INT,
            cleansheets                 INT,
            team_wins                   INT,
            team_draws                  INT,
            team_lost                   INT,
            lineups                     INT,
            bench                       INT,
            avg_points                  NUMERIC,
            crosses_blocked             INT,

            PRIMARY KEY (player_id, season_id, team_id)
        );
    """)


    conn.commit()
    cur.close()
    print("Tables created (or already existed).")

if __name__ == "__main__":
    conn = connect_db("postgres", "natwat", "")
    reset_tables(conn)
    create_tables(conn)
    conn.close()
