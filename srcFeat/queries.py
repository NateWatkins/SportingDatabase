import sys
from main import connect_db
import pandas as pd



def pull_df(conn, sql, params=None):
    """
    Run a SELECT query and return a pandas DataFrame.
    """
    try:
        return pd.read_sql(sql, conn, params=params)
    except Exception as e:
        print(f"[QUERY ERROR] {e}")
        print("SQL:\n", sql)
        print("Params:", params)
        sys.exit(1)


# Pull functions


def pull_players(conn):
    sql = """
        SELECT
            player_id,
            name,
            nationality,
            position,
            height,
            weight
        FROM players;
    """
    return pull_df(conn, sql)


def pull_teams(conn):
    sql = """
        SELECT
            team_id,
            name,
            league_id
        FROM teams;
    """
    return pull_df(conn, sql)


def pull_seasons(conn):
    sql = """
        SELECT
            season_id,
            name,
            league_id
        FROM seasons;
    """
    return pull_df(conn, sql)


def pull_leagues(conn):
    sql = """
        SELECT
            league_id,
            name,
            country_id
        FROM leagues;
    """
    return pull_df(conn, sql)


def pull_player_season_stats(conn, season_ids=None, player_ids=None):
    """
    Pull from player_season_stats with optional filters.
    season_ids / player_ids should be Python lists like [21644, 21645]
    """
    where = []
    params = []

    if season_ids:
        where.append("season_id = ANY(%s)")
        params.append(season_ids)

    if player_ids:
        where.append("player_id = ANY(%s)")
        params.append(player_ids)

    where_sql = ""
    if where:
        where_sql = "WHERE " + " AND ".join(where)

    sql = f"""
        SELECT
            player_id,
            season_id,
            team_id,
            minutes_played,
            goals,
            assists
            -- add more columns here (avoid SELECT *)
        FROM player_season_stats
        {where_sql};
    """

    return pull_df(conn, sql, params=params if params else None)
