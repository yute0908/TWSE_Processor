from datetime import datetime


def normalize_params(stock_id, since_year, to_year=None):
    if stock_id is None:
        return None
    if since_year is None:
        return None
    if to_year is not None and since_year > to_year:
        return None

    get_recent_four_seasons = False
    if to_year is None:
        get_recent_four_seasons = True
        to_year = datetime.now().year

    return {'stock_id': stock_id, 'since_year': since_year, 'to_year': to_year,
            'get_recent_four_seasons': get_recent_four_seasons}