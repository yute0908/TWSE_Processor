from datetime import datetime


def get_time_lines(since=None, to=None):
    if since is None and to is None:
        print("since and to should not both be None")
        return

    now = datetime.now()
    if since is not None:
        since_year = since.get('year')
        if since_year is None:
            print("since year should not be None")
            return
        since_season = since.get('season', 1)
        if to is not None:
            to_year = to.get('year', now.year)
            to_season = to.get('season', (now.month - 1) / 3 if to_year == now.year else 4)
        else:
            to_year = now.year
            to_season = (now.month - 1) / 3 + 1

    else:
        since_year = to_year = to.get('year')
        since_season = to.get('season')
        if since_season is None:
            since_season = 1
            to_season = to.get('season', (now.month - 1) / 3 + 1)
        else:
            to_season = since_season

    year_count = to_year - since_year - 1
    season_count = 4 - since_season + to_season + year_count * 4 + 1
    time_lines = []
    print("get seasons")
    for i in range(int(season_count)):
        mod_season = (since_season + i) % 4
        year = since_year + int((since_season + i) / 4) - (1 if mod_season == 0 else 0)
        season = mod_season if mod_season > 0 else 4
        print("(", year, ",", season, ")")
        time_lines.append({'year': year, 'season': season})
    print("\n")
    return time_lines


def get_recent_seasons(count=0):
    time_lines = []
    now = datetime.now()
    year = now.year
    season = int((now.month - 1) / 3) + 1
    for i in range(count):
        # print("(", year, ",", season, ")")
        time_lines.insert(0, {'year': year, 'season': season})
        season = season - 1
        if season == 0:
            season = 4
            year = year - 1
    print(time_lines)
    return time_lines
