import requests, zipfile, datetime, sqlite3, pandas

def download_bhavcopy(day):
    dd=day.strftime('%d')
    mmm=day.strftime('%b').upper()
    yyyy=day.strftime('%Y')
    url = 'https://www.nseindia.com/content/historical/EQUITIES/'+yyyy+'/'+mmm+'/cm'+dd+mmm+yyyy+'bhav.csv.zip'
    zfile = requests.get(url, allow_redirects=True)
    open('cm'+dd+mmm+yyyy+'bhav.csv.zip', 'wb').write(zfile.content)
    zip_ref = zipfile.ZipFile('cm'+dd+mmm+yyyy+'bhav.csv.zip', 'r')
    zip_ref.extractall()
    zip_ref.close()
    con = sqlite3.connect('NSE.db')
    cur = con.cursor()
    df = pandas.read_csv('cm'+dd+mmm+yyyy+'bhav.csv')
    # df.to_sql(table_name, conn, if_exists='append', index=False)
    df.to_sql('NSE', con, if_exists='append', index=False, index_label='SYMBOL')
    con.commit()
    con.close()
    return url

download_bhavcopy(datetime.date(2018,1,16))