from google.cloud import bigquery
import os
import pandas as pd

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/root/credentials'

client = bigquery.Client()

query = "SELECT DATE, SOURCECOMMONNAME, DOCUMENTIDENTIFIER from `gdelt-bq.gdeltv2.gkg` WHERE (THEMES Like '%CLIMATE_CHANGE%') AND SOURCECOLLECTIONIDENTIFIER = 1"

QUERY = (query)
query_job = client.query(QUERY)
rows = query_job.result()  # Waits for query to finish

outfile = '/root/gdelt/climate_change.csv'

def process(obj):
    if type(obj) == str:
        new = obj.encode('utf-8')
    elif type(obj) == int:
        new = str(obj).encode('utf-8')
    elif obj is None:
        new = "".encode('utf-8')
    return new

f = open(outfile, 'wb')
for row in rows:
    line = b'\t'.join([process(x) for x in row])
    f.write(line + b'\n')

f.close()


dat = pd.read_table(outfile, names=['date', 'source', 'url'], header=None)
#From https://blog.gdeltproject.org/mapping-the-media-a-geographic-lookup-of-gdelts-sources/
#And http://data.gdeltproject.org/blog/2018-news-outlets-by-country-may2018-update/MASTER-GDELTDOMAINSBYCOUNTRY-MAY2018.TXT
source = pd.read_table('/root/gdelt/MASTER-GDELTDOMAINSBYCOUNTRY-MAY2018.TXT', names=['URL', 'CTY', 'country'], header=None)

us_source = source[source.country=='United States']
us_dat = dat[dat.source.isin(us_source.URL)]
us_dat.to_csv('/root/gdelt/climate_change_us.csv', index=False)

