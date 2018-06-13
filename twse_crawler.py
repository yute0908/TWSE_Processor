from bs4 import BeautifulSoup
import requests
import time
import pandas as pd


def get_type_code(r):
    res = r.content
    soup = BeautifulSoup(res, 'html.parser')
    typeks = [[x['value'], x.string] for x in soup.select('select[name="TYPEK"] > option')]
    codes = [[x['value'], x.string] for x in soup.select('select[name="code"] > option') if x.string]
    return typeks, codes


def get_payload(typek, code):
    payload = {'encodeURIComponent': '1',
               'step': '1',
               'firstin': '1',
               'TYPEK': typek,
               'code': code}
    return payload


def get_table(s, url, payload):
    # use payload
    response = s.post(url, data=payload)
    res = response.content
    soup = BeautifulSoup(res, 'html.parser')
    table = soup.find(id='table01')
    return table


def get_df(table):
    raws = table.find_all('table')[1].find_all('tr')
    # get the header
    header = raws[0].find_all('th')
    header_of_table = [x.get_text() for x in header]
    # get the cell
    list_of_talbe = []
    for raw in raws:
        r = [x.get_text() for x in raw.find_all('td')]
        if len(r) > 0:
            list_of_talbe.append(r)

    df = pd.DataFrame(list_of_talbe, columns=header_of_table)
    return df


import traceback


def main():
    # get the session
    url = 'http://mops.twse.com.tw/mops/web/t51sb01'
    s = requests.Session()
    r = s.get(url)
    typeks, codes = get_type_code(r)

    if r.ok:
        for typek in typeks:
            for code in codes:
                try:
                    payload = get_payload(typek[0], code[0])
                    print(payload)
                    table = get_table(s, url, payload)
                    df = get_df(table)
                    df.to_excel('data/twse_%s_%s.xlsx' % (typek[1], code[1]), index=False, encoding='UTF-8')
                except:
                    print('%s, %s faild' % (typek[1], code[1]))
                    traceback.print_exc()
                time.sleep(10)


if __name__ == "__main__":
    # execute only if run as a script
    main()
