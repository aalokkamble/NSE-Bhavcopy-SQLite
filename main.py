import requests, zipfile, datetime, sqlite3, pandas, os
from datetime import date, timedelta

def bhavcopy_download(day):
    #Assign date
    dd=day.strftime('%d')
    mmm=day.strftime('%b').upper()
    yyyy=day.strftime('%Y')

    #Define download URL
    url = 'https://www.nseindia.com/content/historical/EQUITIES/'+yyyy+'/'+mmm+'/cm'+dd+mmm+yyyy+'bhav.csv.zip'

    #Downlaod bhavcompy file and extract csv file
    zfile = requests.get(url, allow_redirects=True)
    if(zfile.status_code!=404):
        open('cm'+dd+mmm+yyyy+'bhav.csv.zip', 'wb').write(zfile.content)
        zip_ref = zipfile.ZipFile('cm'+dd+mmm+yyyy+'bhav.csv.zip', 'r')
        zip_ref.extractall()
        zip_ref.close()

        #Insert the data from CSV file to NSE.db database
        con = sqlite3.connect('NSE.db')
        df = pandas.read_csv('cm'+dd+mmm+yyyy+'bhav.csv')
        # df.to_sql(table_name, conn, if_exists='append', index=False)
        df.to_sql('NSE', con, if_exists='append', index=False, index_label='SYMBOL')
        con.commit()
        con.close()
        print("Data for " + str(day.strftime('%d-%b-%Y')).upper() + " added")

def bhavcopy(day):
    delta = date.today() - day
    for i in range (delta.days +1):
        daycheck = day+timedelta(days=i)

        #check if db alreay exists
        if not os.path.isfile("NSE.db"):
            bhavcopy_download(daycheck)
            continue

        #check if data for the date already exists in db
        con = sqlite3.connect('NSE.db')
        cur = con.execute("Select TIMESTAMP from NSE where TIMESTAMP =?",(str(daycheck.strftime('%d-%b-%Y')).upper(),))
        NSEdata = cur.fetchone()
        if NSEdata is None:
            #add data
            bhavcopy_download(daycheck)
        else:
            print("Data for "+str(daycheck.strftime('%d-%b-%Y')).upper()+" already exists.")
        con.close()

bhavcopy(date(2018,1,17))
