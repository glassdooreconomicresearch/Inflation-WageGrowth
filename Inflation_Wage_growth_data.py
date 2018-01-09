###############################################################
##### Are Wages Keeping up with Inflation?
#####
##### by Patrick Wong
##### Data Scientist, Glassdoor
#####
##### The following code was written to pull data from
##### (1) The BLS Consumer Price Index and
##### (2) Glassdoor Local Pay Reports Data
##### (3) Merge and clean together to use for analysis
#####
##### January 2018
#####
##### Contact:
#####   Web: www.glassdoor.com/research
#####   Email: economics@glassdoor.com
###############################################################

import pandas as pd
import datetime
from datetime import date, timedelta, datetime
import requests
import json

#############################
# Pull BLS CPI DATA
#############################

headers = {'Content-type': 'application/json'}
data = json.dumps({"seriesid": ['CUUR0000SA0','CUURA319SA0','CUURA103SA0','CUURA207SA0','CUURA318SA0','CUURA421SA0','CUURA101SA0','CUURA102SA0','CUURA423SA0','CUURA422SA0','CUURA311SA0'],"startyear":"2011", "endyear":"2017"})
p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
json_data = json.loads(p.text)
df=pd.DataFrame(columns=["series id","year","period","value"])
for series in json_data['Results']['series']:
    seriesId = series['seriesID']
    for item in series['data']:
        year = item['year']
        period = item['period']
        value = item['value']
        if 'M01' <= period <= 'M12':
            df = df.append({'series id':seriesId,\
                           'year':year,\
                           'period':period,\
                           'value':value},ignore_index=True)

# These are the metro locations to match with Glassdoor LPR data
cities = {'CUUR0000SA0':'National','CUURA319SA0':'Atlanta','CUURA103SA0':'Boston','CUURA207SA0':'Chicago','CUURA318SA0':'Houston',\
           'CUURA421SA0':'Los Angeles','CUURA101SA0':'New York City' ,'CUURA102SA0':'Philadelphia',\
           'CUURA423SA0':'Seattle','CUURA422SA0':'San Francisco','CUURA311SA0':'Washington DC'}

df['metro'] = df['series id'].map(lambda x: cities[x])
df['date'] = df.apply(lambda x: str(x['year'])+"-"+str(x['period']).split('M')[1], axis=1)

# Date Object to match with Glassdoor data

def parser(x):
    return datetime.strptime(x, '%Y-%m')

df['date2'] = df.apply(lambda x: parser(x['date']), axis=1)

# Interpolate Missing Data
# Most Metro CPI data are only available in 2-month increments, while Glassdoor LPR data comes in monthly increments.

cpi = pd.DataFrame()

for m in pd.unique(df.metro):
    dfm = pd.to_numeric(df.loc[df['metro'] == m,'value'])
    dfm.index = df[df['metro']==m]['date2']
    upsampled = dfm.resample('MS')
    interpolated = upsampled.interpolate(method='linear')
    dfi = pd.DataFrame(interpolated)
    dfi['metro'] = m # add metro name
    dfi['Inflation'] = dfi.value.pct_change(periods=12) # add yoy calc
    cpi = cpi.append(dfi)

cpi.reset_index(inplace=True)

#############################
# Pull Glassdoor LPR Data
#############################

# This is Glassdoor's LPR archive file from the December 2017 release
salaries = "https://www.glassdoor.com/research/app/uploads/sites/2/2017/12/LPR_data-2017-11.xlsx" 
resp = requests.get(salaries)

output = open('salaries.xls','wb') # Save the file
output.write(resp.content)
output.close()

salaries = pd.read_excel('salaries.xls')
salaries = salaries[salaries['Dimension Type'] == 'Timeseries']
salaries.rename(columns={'Value':'Pay'},inplace=True)

# Calculate YoY Wage Growth
salaries2 = pd.DataFrame()

for m in pd.unique(salaries.Metro):
    s = salaries[salaries['Metro'] == m]
    s = s.sort_values('Month', ascending=True)
    s['Wage_Growth'] = s.Pay.pct_change(periods=12) # add yoy calc
    salaries2 = salaries2.append(s)

#############################
# Merge and Export
#############################

cpi['Month'] = cpi.apply(lambda x: str(x['date2'])[:7], axis=1)
cpi.rename(columns={'value':'CPI','metro':'Metro','date2':'Date'},inplace=True)

df = cpi.merge(salaries2[['Metro','Month','Pay','Wage_Growth']], on=['Metro','Month'], how='left')

# Export to CSV for analysis
df.to_csv('inflationWageGrowthData.csv',index=False) 