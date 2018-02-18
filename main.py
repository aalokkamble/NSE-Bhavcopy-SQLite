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
    print(url)
    if not os.path.exists('Downloads'):
        os.makedirs('Downloads')
    if(zfile.status_code!=404):
        open('Downloads\cm'+dd+mmm+yyyy+'bhav.csv.zip', 'wb').write(zfile.content)
        zip_ref = zipfile.ZipFile('Downloads\cm'+dd+mmm+yyyy+'bhav.csv.zip', 'r')
        zip_ref.extractall('Downloads')
        zip_ref.close()

        #Insert the data from CSV file to NSE.db database
        con = sqlite3.connect('NSE.db')
        df = pandas.read_csv('Downloads\cm'+dd+mmm+yyyy+'bhav.csv')
        # df.to_sql(table_name, conn, if_exists='append', index=False)
        df.to_sql('NSE', con, if_exists='append', index=False, index_label='SYMBOL')
        con.commit()
        con.close()
        format_date()
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
        cur2 = con.execute("Select TIMESTAMP from NSE where TIMESTAMP =?",(str(daycheck.strftime('%Y-%m-%d')),))
        if cur.fetchone() is None and cur2.fetchone() is None:
            #add data
            bhavcopy_download(daycheck)
        else:
            print("Data for "+str(daycheck.strftime('%d-%b-%Y')).upper()+" already exists.")
        con.close()

def keep_series_EQ():
    con = sqlite3.connect('NSE.db')
    con.execute("delete from NSE where SERIES <> 'EQ'")
    con.execute("delete from NSE where SERIES is null")
    con.commit()
    con.close()

def keep_only_FO():
    con = sqlite3.connect('NSE.db')
    #List of FO stocks can be modified here:
    con.execute('delete from NSE where SYMBOL not in ("ACC","ADANIENT","ADANIPORTS","ADANIPOWER","AJANTPHARM","ALBK","AMARAJABAT","AMBUJACEM","ANDHRABANK","APOLLOHOSP","APOLLOTYRE","ARVIND","ASHOKLEY","ASIANPAINT","AUROPHARMA","AXISBANK","BAJAJ-AUTO","BAJAJFINSV","BAJFINANCE","BALKRISIND","BALRAMCHIN","BANKBARODA","BANKINDIA","BATAINDIA","BEL","BEML","BERGEPAINT","BHARATFIN","BHARATFORG","BHARTIARTL","BHEL","BIOCON","BOSCHLTD","BPCL","BRITANNIA","CADILAHC","CANBK","CANFINHOME","CAPF","CASTROLIND","CEATLTD","CENTURYTEX","CESC","CGPOWER","CHENNPETRO","CHOLAFIN","CIPLA","COALINDIA","COLPAL","CONCOR","CUMMINSIND","DABUR","DALMIABHA","DCBBANK","DHFL","DISHTV","DIVISLAB","DLF","DRREDDY","EICHERMOT","ENGINERSIN","EQUITAS","ESCORTS","EXIDEIND","FEDERALBNK","FORTIS","GAIL","GLENMARK","GMRINFRA","GODFRYPHLP","GODREJCP","GODREJIND","GRANULES","GRASIM","GSFC","HAVELLS","HCC","HCLTECH","HDFC","HDFCBANK","HDIL","HEROMOTOCO","HEXAWARE","HINDALCO","HINDPETRO","HINDUNILVR","HINDZINC","IBULHSGFIN","ICICIBANK","ICICIPRULI","ICIL","IDBI","IDEA","IDFC","IDFCBANK","IFCI","IGL","INDIACEM","INDIANB","INDIGO","INDUSINDBK","INFIBEAM","INFRATEL","INFY","IOC","IRB","ITC","JETAIRWAYS","JINDALSTEL","JISLJALEQS","JPASSOCIAT","JSWENERGY","JSWSTEEL","JUBLFOOD","JUSTDIAL","KAJARIACER","KOTAKBANK","KPIT","KSCL","KTKBANK","L&TFH","LICHSGFIN","LT","LUPIN","M&M","M&MFIN","MANAPPURAM","MARICO","MARUTI","MCDOWELL-N","MCX","MFSL","MGL","MINDTREE","MOTHERSUMI","MRF","MRPL","MUTHOOTFIN","NATIONALUM","NBCC","NCC","NESTLEIND","NHPC","NIITTECH","NMDC","NTPC","OFSS","OIL","ONGC","ORIENTBANK","PAGEIND","PCJEWELLER","PEL","PETRONET","PFC","PIDILITIND","PNB","POWERGRID","PTC","PVR","RAMCOCEM","RAYMOND","RBLBANK","RCOM","RECLTD","RELCAPITAL","RELIANCE","RELINFRA","REPCOHOME","RNAVAL","RPOWER","SAIL","SBIN","SHREECEM","SIEMENS","SOUTHBANK","SREINFRA","SRF","SRTRANSFIN","STAR","SUNPHARMA","SUNTV","SUZLON","SYNDIBANK","TATACHEM","TATACOMM","TATAELXSI","TATAGLOBAL","TATAMOTORS","TATAMTRDVR","TATAPOWER","TATASTEEL","TCS","TECHM","TITAN","TORNTPHARM","TORNTPOWER","TV18BRDCST","TVSMOTOR","UBL","UJJIVAN","ULTRACEMCO","UNIONBANK","UPL","VEDL","VGUARD","VOLTAS","WIPRO","WOCKPHARMA","YESBANK","ZEEL")')
    con.commit()
    con.close()

def format_date():
    con = sqlite3.connect('NSE.db')
    #Update date format:
    con.execute("UPDATE NSE SET TIMESTAMP = replace( TIMESTAMP, 'JAN', '01' ) WHERE TIMESTAMP LIKE '%JAN%';")
    con.execute("UPDATE NSE SET TIMESTAMP = replace( TIMESTAMP, 'FEB', '02' ) WHERE TIMESTAMP LIKE '%FEB%';")
    con.execute("UPDATE NSE SET TIMESTAMP = replace( TIMESTAMP, 'MAR', '03' ) WHERE TIMESTAMP LIKE '%MAR%';")
    con.execute("UPDATE NSE SET TIMESTAMP = replace( TIMESTAMP, 'APR', '04' ) WHERE TIMESTAMP LIKE '%APR%';")
    con.execute("UPDATE NSE SET TIMESTAMP = replace( TIMESTAMP, 'MAY', '05' ) WHERE TIMESTAMP LIKE '%MAY%';")
    con.execute("UPDATE NSE SET TIMESTAMP = replace( TIMESTAMP, 'JUN', '06' ) WHERE TIMESTAMP LIKE '%JUN%';")
    con.execute("UPDATE NSE SET TIMESTAMP = replace( TIMESTAMP, 'JUL', '07' ) WHERE TIMESTAMP LIKE '%JUL%';")
    con.execute("UPDATE NSE SET TIMESTAMP = replace( TIMESTAMP, 'AUG', '08' ) WHERE TIMESTAMP LIKE '%AUG%';")
    con.execute("UPDATE NSE SET TIMESTAMP = replace( TIMESTAMP, 'SEP', '09' ) WHERE TIMESTAMP LIKE '%SEP%';")
    con.execute("UPDATE NSE SET TIMESTAMP = replace( TIMESTAMP, 'OCT', '10' ) WHERE TIMESTAMP LIKE '%OCT%';")
    con.execute("UPDATE NSE SET TIMESTAMP = replace( TIMESTAMP, 'NOV', '11' ) WHERE TIMESTAMP LIKE '%NOV%';")
    con.execute("UPDATE NSE SET TIMESTAMP = replace( TIMESTAMP, 'DEC', '12' ) WHERE TIMESTAMP LIKE '%DEC%';")
    con.execute("update NSE set timestamp = strftime('%Y-%m-%d',datetime(substr(TIMESTAMP, 7, 4) || '-' || substr(TIMESTAMP, 4, 2) || '-' || substr(TIMESTAMP, 1, 2))) where substr(TIMESTAMP, 3, 1)='-' and substr(TIMESTAMP, 6, 1)='-';")
    con.commit()
    con.close() 

if __name__ == "__main__":
    print("Enter the date from which you want to add the data to NSE.db")
    day = input('Enter a date dd-mm-yyyy (i.e. 23-12-2017):')
    dd,mm,yyyy = map(int, day.split('-'))
    bhavcopy(date(yyyy,mm,dd))

    EQ = input('Do you wish to keep only Series-EQ data and remove the rest? (y/n):')
    if(EQ=='y'):
        keep_series_EQ()
        print("Removed data")
    else:
        print("No Changes")

    FO = input('Do you wish to keep only FO stocks data and remove the rest? (y/n):')
    if (FO == 'y'):
        keep_only_FO()
        print("Removed data")
    else:
        print("No Changes")
