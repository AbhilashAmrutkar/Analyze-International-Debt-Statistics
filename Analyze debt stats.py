import mysql.connector as conn

mydb = conn.connect(host = "localhost", user = "root", password = "root")
print(mydb)
cursor = mydb.cursor()
cursor.execute("show databases")
print(cursor.fetchall())
#cursor.execute("create database analyze_debt_statistics")
print(cursor.fetchall())
cursor.execute("use analyze_debt_statistics")
print(cursor.fetchall())

# WORLD BANK INTERNATIONAL DEBT DATA
import pandas as pd

df = pd.read_csv('C:\ABHILASH\Data Science\Project\Project 3\IDS_CSV\IDS_ALLCountries_Data.csv')
print(df)
print(df.columns)

cursor.execute("show tables")
print(cursor.fetchall())

#cursor.execute("select * from ids_allcountries_data ")
#print(cursor.fetchall())
#cursor.execute("select * from ids_countrymetadatar")
#print(cursor.fetchall())
#cursor.execute("select * from ids_countryseriesmetadatar")
#print(cursor.fetchall())
#cursor.execute("select * from ids_footnotesr")
#print(cursor.fetchall())
#cursor.execute("select * from ids_seriesmetadatar")
#print(cursor.fetchall())




## NUMBER OF DISTINCT COUNTRIES
print("NUMBER OF DISTINCT COUNTRIES WITH NAME :")
cursor.execute("select `Country Name`, count(*) from IDS_ALLCountries_Data group by `Country Name`")
print(cursor.fetchall())

print("NUMBER OF DISTINCT COUNTRIES :")
cursor.execute("SELECT count(distinct`Country Name`) FROM IDS_ALLCountries_Data ORDER BY `Country Name`")
print(cursor.fetchall())


## DISTINCT DEBT INDICATORS
df1 = pd.read_csv(r'C:\ABHILASH\Data Science\Project\Project 3\IDS_CSV\Uploaded data\IDS_SeriesMetaDataR.csv')
print(df1)
print(df1.columns)

print("DISTINCT DEBT INDICATORS :")
cursor.execute("select count(`Indicator Name`),`Indicator Name` from IDS_SeriesMetaDataR group by `Indicator Name`")
print(cursor.fetchall())


cursor.execute("select count(distinct`Indicator Name`) from IDS_SeriesMetaDataR order by `Indicator Name`")
print(cursor.fetchall())


## TOTALING AMOUNT OF DEBT OWED BY THE COUNTRIES
a = "select `Country Name`, sum(`1970`+`1971`+`1972`+ `1973`+ `1974`+ `1975`+ `1976`+ `1977`+ `1978`+ `1979`+ `1980`+ `1981`+ `1982`+ `1983`+ `1984`+ `1985`+ `1986`+ `1987`+ `1988`+ `1989`+ `1990`+ `1991`+ `1992`+ `1993`+ `1994`+ `1995`+ `1996`+ `1997`+ `1998`+ `1999`+ `2000`+ `2001`+ `2002`+ `2003`+ `2004`+ `2005`+ `2006`+ `2007`+ `2008`+ `2009`+ `2010`+ `2011`+ `2012`+ `2013`+ `2014`+ `2015`+ `2016`+ `2017`+ `2018`+ `2019`+ `2020`+ `2021`+ `2022`+ `2023`+ `2024`+ `2025`+ `2026`+ `2027`+ `2028`+ `2029`) as total from ids_allcountries_data group by `Country Name`"

print("DEBT OWED BY INDIVIDUAL COUNTRY :")
cursor.execute(a)
print(cursor.fetchall())
df3 = pd.read_sql_query(a, mydb)
print(df3)
df3.to_csv("C:\ABHILASH\Data Science\Project\Project 3\Amount_of_debt_owed_by_countries.csv")

b = "select sum(`1970`+`1971`+`1972`+ `1973`+ `1974`+ `1975`+ `1976`+ `1977`+ `1978`+ `1979`+ `1980`+ `1981`+ `1982`+ `1983`+ `1984`+ `1985`+ `1986`+ `1987`+ `1988`+ `1989`+ `1990`+ `1991`+ `1992`+ `1993`+ `1994`+ `1995`+ `1996`+ `1997`+ `1998`+ `1999`+ `2000`+ `2001`+ `2002`+ `2003`+ `2004`+ `2005`+ `2006`+ `2007`+ `2008`+ `2009`+ `2010`+ `2011`+ `2012`+ `2013`+ `2014`+ `2015`+ `2016`+ `2017`+ `2018`+ `2019`+ `2020`+ `2021`+ `2022`+ `2023`+ `2024`+ `2025`+ `2026`+ `2027`+ `2028`+ `2029`) as total from ids_allcountries_data"
print("TOTAL AMOUNT OF DEBT OWED BY THE COUNTRIES :")
cursor.execute(b)
print(cursor.fetchall())



## COUNTRY WITH HIGHEST DEBT

c =  "select `Country Name`, sum(`1970`+`1971`+`1972`+ `1973`+ `1974`+ `1975`+ `1976`+ `1977`+ `1978`+ `1979`+ `1980`+ `1981`+ `1982`+ `1983`+ `1984`+ `1985`+ `1986`+ `1987`+ `1988`+ `1989`+ `1990`+ `1991`+ `1992`+ `1993`+ `1994`+ `1995`+ `1996`+ `1997`+ `1998`+ `1999`+ `2000`+ `2001`+ `2002`+ `2003`+ `2004`+ `2005`+ `2006`+ `2007`+ `2008`+ `2009`+ `2010`+ `2011`+ `2012`+ `2013`+ `2014`+ `2015`+ `2016`+ `2017`+ `2018`+ `2019`+ `2020`+ `2021`+ `2022`+ `2023`+ `2024`+ `2025`+ `2026`+ `2027`+ `2028`+ `2029`) as total from ids_allcountries_data group by `Country Name` order by total desc limit 10"

print("COUNTRY WITH HIGHEST DEBT :")
cursor.execute(c)
print(cursor.fetchall())


## AVERAGE AMOUNT OF DEBT ACROSS INDICATORS

print(df1.columns)
view_query = "create view indicator as select `Code`, `Indicator Name` from ids_seriesmetadatar  "

#cursor.execute(view_query)
mydb.commit()
print(cursor.fetchall())

#cursor.execute("select * from indicator")
#print(cursor.fetchall())


df4 = pd.read_sql_query("select * from indicator", mydb)
print(df4)

print(df)

# Rename the column in df4
df4.rename(columns={'Code': 'Series Code'}, inplace=True)
print(df4)

df5 = pd.merge(df, df4, on= 'Series Code')
print(df5)

#df5.to_csv("C:\ABHILASH\Data Science\Project\Project 3\Debt_across_indicators.csv")

cursor.execute("show tables")
print(cursor.fetchall())

print(df5.columns)

d = "select `Indicator Name`, sum(`1970`+`1971`+`1972`+ `1973`+ `1974`+ `1975`+ `1976`+ `1977`+ `1978`+ `1979`+ `1980`+ `1981`+ `1982`+ `1983`+ `1984`+ `1985`+ `1986`+ `1987`+ `1988`+ `1989`+ `1990`+ `1991`+ `1992`+ `1993`+ `1994`+ `1995`+ `1996`+ `1997`+ `1998`+ `1999`+ `2000`+ `2001`+ `2002`+ `2003`+ `2004`+ `2005`+ `2006`+ `2007`+ `2008`+ `2009`+ `2010`+ `2011`+ `2012`+ `2013`+ `2014`+ `2015`+ `2016`+ `2017`+ `2018`+ `2019`+ `2020`+ `2021`+ `2022`+ `2023`+ `2024`+ `2025`+ `2026`+ `2027`+ `2028`+ `2029`) as total from debt_across_indicatorsr group by `Indicator Name` order by total desc"

print("Debt Across Indicators :")
cursor.execute(d)
print(cursor.fetchall())
df6 = pd.read_sql_query(d, mydb)
#df6.to_csv("C:\ABHILASH\Data Science\Project\Project 3\Debt_across_indicators_distribution.csv")


## MOST FREQUENT DEBT INDICATORS
e = "select `Indicator Name`, sum(`1970`+`1971`+`1972`+ `1973`+ `1974`+ `1975`+ `1976`+ `1977`+ `1978`+ `1979`+ `1980`+ `1981`+ `1982`+ `1983`+ `1984`+ `1985`+ `1986`+ `1987`+ `1988`+ `1989`+ `1990`+ `1991`+ `1992`+ `1993`+ `1994`+ `1995`+ `1996`+ `1997`+ `1998`+ `1999`+ `2000`+ `2001`+ `2002`+ `2003`+ `2004`+ `2005`+ `2006`+ `2007`+ `2008`+ `2009`+ `2010`+ `2011`+ `2012`+ `2013`+ `2014`+ `2015`+ `2016`+ `2017`+ `2018`+ `2019`+ `2020`+ `2021`+ `2022`+ `2023`+ `2024`+ `2025`+ `2026`+ `2027`+ `2028`+ `2029`) as total from debt_across_indicatorsr group by `Indicator Name` order by total desc limit 10"
print('Most Common Debt Indicator :')
cursor.execute(e)
print(cursor.fetchall())

## HIGHEST AMOUNT OF PRINCIPAL REPAYMENT
f = "select `Country Name`, sum(`1970`+`1971`+`1972`+ `1973`+ `1974`+ `1975`+ `1976`+ `1977`+ `1978`+ `1979`+ `1980`+ `1981`+ `1982`+ `1983`+ `1984`+ `1985`+ `1986`+ `1987`+ `1988`+ `1989`+ `1990`+ `1991`+ `1992`+ `1993`+ `1994`+ `1995`+ `1996`+ `1997`+ `1998`+ `1999`+ `2000`+ `2001`+ `2002`+ `2003`+ `2004`+ `2005`+ `2006`+ `2007`+ `2008`+ `2009`+ `2010`+ `2011`+ `2012`+ `2013`+ `2014`+ `2015`+ `2016`+ `2017`+ `2018`+ `2019`+ `2020`+ `2021`+ `2022`+ `2023`+ `2024`+ `2025`+ `2026`+ `2027`+ `2028`+ `2029`) as total from ids_allcountries_data group by `Country Name` order by total desc"
print("HIGHEST AMOUNT OF PRINCIPAL REPAYMENTS : ")
cursor.execute(f)
print(cursor.fetchall())



















