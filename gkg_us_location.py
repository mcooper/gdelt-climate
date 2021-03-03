import pandas as pd
import numpy as np
import re
import time
import ast
outfile = '/datadrive/liuz/local_news_us_loca.csv'
#outfile = './local_debugging_climate_change_news_us_loca.csv'
errfile = '/datadrive/liuz/local_news_us_errfile.txt'
#errfile = './local_debugging_climate_change_news_us_err.csv'
#assign fields name
fields = ['GKGRECORDID','DATE','SOURCECOLLECTIONIDENTIFIER','SOURCECOMMONNAME','DOCUMENTIDENTIFIER',
          'V1COUNTS','V2COUNTS','THEMES','V2THEMES','LOCATIONS','V2LOCATIONS',
          'PERSONS','V2PERSONS','ORGANIZATIONS','V2ORGANIZATIONS','TONE','V2DATES',
          'GCAM','SHARINGIMAGE','RELATEDIMAGES','SOCIALIMAGEEMBEDS','SOCIALVIDEOEMBEDS',
          'QUOTATIONS','ALLNAMES','AMOUNTS','TRANSLATIONINFO','EXTRASXML']
print('v0203')
def extract_us_locations(full_loc_str):
    loc_str_list = full_loc_str.split(';')
    loc_us_list = []
    loc_us_dict = {}
    for loc_str in loc_str_list:
        if int(loc_str.split('#')[0]) == 2 or int(loc_str.split('#')[0]) == 3:
            #loc_us_list.append((loc_str.split('#')[0],loc_str.split('#')[1],loc_str.split('#')[5],loc_str.split('#')[6]))
            loc_us_dict[loc_str.split('#')[1]] = [loc_str.split('#')[0],loc_str.split('#')[1],loc_str.split('#')[5],loc_str.split('#')[6]]
    return loc_us_dict

def extract_theme_by_key(full_theme_str,key):
    theme_str_list = full_theme_str.split(';')
    flag = False
    match_themes = []
    for theme_str in theme_str_list:
        res = re.findall(f'(.*?{key}.*?),',theme_str)
        if len(res) > 0:
            match_themes.append(res[0])
            flag=True
    return flag,match_themes

#get all gkg file paths and download latest 500
gkg_file_list = []
with open('masterfilelist.txt') as fo:
    lines = fo.readlines()
for line in lines:
    url = line.split(' ')[-1].rstrip('\n')
    res = re.match('.*.gkg..*',url)
    if res:
        #print(url)
        gkg_file_list.append(url)

# extract geoinformation for climate related news

error_fpaths = []
t0 = time.time()
count = 0
total = len(gkg_file_list)
for fpath in gkg_file_list:
    results = []
    count += 1
    try:
        gkg_pull_one = pd.read_csv(fpath,sep='\t',engine='python',header=None,encoding='latin_1')
    except:
        print(f'error met {fpath}')
        error_fpaths.append(fpath)
        continue
    gkg_pull_one.columns = fields
    #test performance for one file
    for record_tuple in gkg_pull_one[gkg_pull_one.SOURCECOLLECTIONIDENTIFIER == 1].itertuples():
        full_theme_str = record_tuple[9]
        #print(full_theme_str)
        if not full_theme_str or full_theme_str is np.nan:
            #no theme
            pass
        else:
            #print(len(full_theme_str))
            flag,match_themes = extract_theme_by_key(full_theme_str,'LOCAL')
            if flag:
                full_loc_str = record_tuple[11]
                if not full_loc_str or full_loc_str is np.nan:
                    pass
                else:
                    us_locations = extract_us_locations(full_loc_str)
                    if (len(us_locations) > 0):
                        results.append({'nid':record_tuple[1],'domain':record_tuple[4],'url':record_tuple[5],'locations':us_locations})
    t1 = time.time()
    
    print(f'{count}/{total} done with valid news count {len(results)}. {len(error_fpaths)} error loading. {fpath} accumulated run time {t1-t0}s ')

    pd.DataFrame(results).to_csv(outfile,mode = 'a',index=False,header=False)
    with open(errfile, 'w') as f:
        for item in error_fpaths:
            f.write(f"{item}\n")