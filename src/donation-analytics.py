#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 10 15:52:31 2018

@author: YuZhang
"""
from pandas.core.frame import DataFrame
import numpy as np
import datetime

#reading each line of text data 
def get_data(address):
    data=[]
    with open(address,'r') as f:  
        lines=f.readlines()
        for line in lines:  
            data.append(line.split('|'))
    f.close()
    return data

#getting percentile
def get_pc(address):
    try:
        with open(address,'r') as f:  
            lines=f.readline()
        f.close()
        return int(lines)
    except ValueError:
        return False

#validating date format
def validate_date(date_str):
    try:
        datetime.datetime.strptime(date_str, '%m%d%Y')
        return True
    except ValueError:
        return False

#validating name format
def validate_name(name_str):
    try:
        return name_str.replace(',',' ').replace(' ','').isalpha()
    except ValueError:
        return False

#preparing data for calculating
def process_data(data):
    drop_list=[] #the list of lines we need to drop
    new_zip=[]
    new_AMT=[]
    new_DT=[]
    #make a dataframe
    header=['CMTE_ID','AMNDT_IND','RPT_TP','TRANSACTION_PGI','IMAGE_NUM','TRANSACTION_TP',\
            'ENTITY_TP','NAME','CITY','STATE','ZIP_CODE','EMPLOYER','OCCUPATION','TRANSACTION_DT',\
            'TRANSACTION_AMT','OTHER_ID','TRAN_ID','FILE_NUM','MEMO_CD','MEMO_TEXT','SUB_ID']
    df=DataFrame(data,columns=header)
    #make new dataframe with the information we need
    df=df[['CMTE_ID','NAME','ZIP_CODE','TRANSACTION_DT','TRANSACTION_AMT','OTHER_ID']]
    #transfer string in TRANSACTION_AMT into int
    for i in df['TRANSACTION_AMT']:
        new_AMT.append(int(i))
    df['TRANSACTION_AMT']=new_AMT    
    #remove invalid data
    for i in list(df.index.values):
        if df.loc[i]['CMTE_ID']=='' or validate_name(df.loc[i]['NAME'])==False \
        or (len(df.loc[i]['ZIP_CODE'])>=5)==False or validate_date(df.loc[i]['TRANSACTION_DT'])!=True \
        or df.loc[i]['TRANSACTION_AMT']=='' or df.loc[i]['OTHER_ID']!='':
            drop_list.append(i)    
        else:
            new_zip.append(df.loc[i]['ZIP_CODE'][0:5])
            new_DT.append(int(df.loc[i]['TRANSACTION_DT'][4:]))
    #remove invalid line in drop list
    df.drop(drop_list,inplace=True)
    #Add number of 
    df['ZIP_CODE']=new_zip
    df['TRANSACTION_DT']=new_DT
    return df

#sort datafrane by year and and remove data doesn't use
def sort_by_year(df):
    #order data by year
    df=df.sort_values(by=['TRANSACTION_DT'])
    df=df[['CMTE_ID','NAME','ZIP_CODE','TRANSACTION_DT','TRANSACTION_AMT']]
    return df

#calculate precentile contribution
def calculate_pc(df,p):
    final_list=[]
    repeat_list=[]
    percentile_cal={}
    for i in list(df.index.values):
        repeater_id=df.loc[i]['NAME']+' '+df.loc[i]['ZIP_CODE']
        if repeater_id in repeat_list:
            percentile_id=str(df.loc[i]['CMTE_ID'])+'|'+str(df.loc[i]['ZIP_CODE'])+'|'+str(df.loc[i]['TRANSACTION_DT'])
            if percentile_id not in percentile_cal.keys():
                percentile_cal[percentile_id]=[df.loc[i]['TRANSACTION_AMT']]
                a=np.array(percentile_cal[percentile_id])
                pc=round(np.percentile(a,p/100))
                final_list.append(percentile_id+'|'+str(int(pc))+'|'+str(a.sum())+'|'+str(len(percentile_cal[percentile_id])))
            else:
                percentile_cal[percentile_id].append(df.loc[i]['TRANSACTION_AMT'])
                a=np.array(percentile_cal[percentile_id])
                pc=round(np.percentile(a,p/100))
                final_list.append(percentile_id+'|'+str(int(pc))+'|'+str(a.sum())+'|'+str(len(percentile_cal[percentile_id])))
        else:
            repeat_list.append(repeater_id)
    return final_list

if __name__ == "__main__":
    data=get_data('input/itcont.txt')
    p=get_pc('input/percentile.txt')
    if p==False:
        print "wrong input on percentile.txt"
    else:
        df=process_data(data)
        #print pc
        df=sort_by_year(df)
        a=calculate_pc(df,p)
        file = open('output/repeat_donors.txt', 'w')
        for item in a:
            file.write("%s\n" % item)
        file.close()
