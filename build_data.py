import pandas as pd
import numpy as np
from pandas import ExcelWriter
from pandas import ExcelFile
from time import strptime
import datetime as dt


##SET PATH
import os
os.chdir('/Users/christina.caddel/Desktop/bp_reporting/weekly_reporting_pulls')

##FIX COMMAS IN NUMBER IMPORT ISSUE
valid = '1234567890.' #valid characters for a float
def sanitize(data):
    return float(''.join(filter(lambda char: char in valid, data)))

##BRING IN FACEBOOK FILE AND TRANSFORM IT
Facebook = pd.read_csv('Bluprint_Weekly_Reporting_Template_Facebook.csv')
Facebook.drop(columns=['Ad Set ID', 'Ad ID', 'Reporting Starts', 'Reporting Ends'], axis=1, inplace=True)
Facebook.columns = ['Placement', 'Creative', 'Date', 'Cost', 'Impressions', 'Clicks', 'Views', 'Video Completion', 'Conversions']
Facebook.to_csv('Facebook_Weekly.csv')

##BRING IN PINTEREST FILE AND TRANSFORM IT
Pinterest = pd.read_csv('Bluprint_Weekly_Reporting_Template_Pinterest.csv')
Pinterest.drop(columns=['Ad holdout experiment', 'Promoted Pin ID', 'Pin Promotion campaign id', 'Campaign name', 'Pin','Promoted Pin description','Destination URL','Total clickthrough rate','Checkout - cost per action','Checkout - return on ad spend',
                        'Checkout - number of conversions','Checkout - conversion value','Checkout - click conversions','Checkout - value from click conversions','Checkout - engagement conversions','Checkout - value from engagement conversions',
                        'Checkout - view conversions','Checkout - value from view conversions','Signup - cost per action','Signup - return on ad spend','Signup - number of conversions', 'Signup - conversion value','Signup - click conversions',
                        'Signup - value from click conversions','Signup - engagement conversions','Signup - value from engagement conversions','Signup - view conversions','Signup - value from view conversions','Lead - cost per action','Lead - return on ad spend',
                        'Lead - number of conversions','Lead - conversion value','Lead - click conversions','Lead - value from click conversions','Lead - engagement conversions','Lead - value from engagement conversions','Lead - view conversions','Lead - value from view conversions',
                        'Add to cart - cost per action','Add to cart - return on ad spend','Add to cart - number of conversions','Add to cart - conversion value','Add to cart - click conversions','Add to cart - value from click conversions','Add to cart - engagement conversions',
                        'Add to cart - value from engagement conversions','Add to cart - view conversions','Add to cart - value from view conversions'], axis=1, inplace=True)
Pinterest.columns = ['Placement', 'Creative', 'Cost', 'Impressions', 'Clicks', 'Conversions', 'Date']
Pinterest['Views'] = 0
Pinterest['Video Completion'] = 0
Pinterest = Pinterest[['Placement', 'Creative', 'Date', 'Cost', 'Impressions', 'Clicks', 'Views', 'Video Completion', 'Conversions']]
Pinterest.to_csv('Pinterest_Weekly.csv')

##BRING IN GOOGLE FILE AND TRANSFORM IT
Google = pd.read_csv('Bluprint_Weekly_Reporting_Template_Google.csv')
Google.drop(Google.index[:1], inplace=True)
new_header = Google.iloc[0]
Google = Google[1:]
Google.columns = new_header
Google.drop(columns=['Headline', 'Headline 1', 'Headline 2','Expanded Text Ad Headline 3', 'Short headline', 'Long headline', 'Description','Expanded Text Ad Description 2', 'Description line 1', 'Description line 2', 'Display URL', 'Path 1', 'Path 2', 'Business name'], axis=1, inplace=True)
Google.columns = ['Placement', 'Creative', 'Date', 'Cost', 'Impressions', 'Clicks', 'Views', 'Video Completion', 'Conversions']
Google['Cost'] = Google['Cost'].apply(sanitize)
Google['Impressions'] = Google['Impressions'].apply(sanitize)
Google['Clicks'] = Google['Clicks'].apply(sanitize)
Google['Views'] = Google['Views'].apply(sanitize)
Google['Video Completion'] = Google['Video Completion'].apply(sanitize)
Google['Conversions'] = Google['Conversions'].apply(sanitize)
Google.to_csv('Google_Weekly.csv')

##BRING IN GOOGLE ANALYTICS FILE AND TRANSFORM IT
GoogleAnalytics = pd.read_csv('Bluprint_Weekly_Reporting_Template_Google_Analytics_Raw.csv')
GoogleAnalytics.drop(GoogleAnalytics.index[:5], inplace=True)
new_header = GoogleAnalytics.iloc[0]
GoogleAnalytics = GoogleAnalytics[1:]
GoogleAnalytics.columns = new_header
GoogleAnalytics.columns = ['MAID', 'Date', 'Sessions', 'New Users', 'Bounces', 'Registrations', 'Subscriptions']
GoogleAnalytics['Date'] =  pd.to_datetime(GoogleAnalytics['Date'], format='%Y/%m/%d')
GoogleAnalytics['Sessions'] = GoogleAnalytics['Sessions'].apply(sanitize)
GoogleAnalytics['Sessions'] = pd.to_numeric(GoogleAnalytics['Sessions'], errors='coerce')
GoogleAnalytics['New Users'] = GoogleAnalytics['New Users'].apply(sanitize)
GoogleAnalytics['New Users'] = pd.to_numeric(GoogleAnalytics['New Users'], errors='coerce')
GoogleAnalytics['Bounces'] = GoogleAnalytics['Bounces'].apply(sanitize)
GoogleAnalytics['Bounces'] = pd.to_numeric(GoogleAnalytics['Bounces'], errors='coerce')
GoogleAnalytics['Registrations'] = GoogleAnalytics['Registrations'].apply(sanitize)
GoogleAnalytics['Registrations'] = pd.to_numeric(GoogleAnalytics['Registrations'], errors='coerce')
GoogleAnalytics['Subscriptions'] = GoogleAnalytics['Subscriptions'].apply(sanitize)
GoogleAnalytics['Subscriptions'] = pd.to_numeric(GoogleAnalytics['Subscriptions'], errors='coerce')
GoogleAnalytics = GoogleAnalytics.groupby(['MAID','Date']).agg({'Sessions': 'sum', 'New Users': 'sum', 'Bounces': 'sum', 'Registrations': 'sum', 'Subscriptions': 'sum'})
GoogleAnalytics.to_csv('GoogleAnalytics_Weekly.csv')

##MERGE DATA SETS
ALL_DATA = pd.concat([Google, Facebook, Pinterest], ignore_index=True)
ALL_DATA['Cost'] = pd.to_numeric(ALL_DATA['Cost'], errors='coerce')
ALL_DATA['Impressions'] = pd.to_numeric(ALL_DATA['Impressions'], errors='coerce')
ALL_DATA['Clicks'] = pd.to_numeric(ALL_DATA['Clicks'], errors='coerce')
ALL_DATA['Views'] = pd.to_numeric(ALL_DATA['Views'], errors='coerce')
ALL_DATA['Video Completion'] = pd.to_numeric(ALL_DATA['Video Completion'], errors='coerce')
ALL_DATA['Conversions'] = pd.to_numeric(ALL_DATA['Conversions'], errors='coerce')
ALL_DATA.to_csv('ALL_DATA_RAW_Weekly.csv')

##EXTRACT COLUMNS FOR CATEGORIZATION
ALL_DATA['MAID'] = ALL_DATA['Placement'].str.extract('(_1\d\d\d\d\d)', expand=True)
ALL_DATA['MAID'] = ALL_DATA['MAID'].str.strip('_')
ALL_DATA['Funnel'] = ALL_DATA['Placement'].str.split('_').str[0]
ALL_DATA['Tactic'] = ALL_DATA['Placement'].str.split('_').str[1]
ALL_DATA['Publisher'] = ALL_DATA['Placement'].str.split('_').str[2]
ALL_DATA['Hook'] = ALL_DATA['Placement'].str.split('_').str[3]
ALL_DATA['Mega'] = ALL_DATA['Placement'].str.split('_').str[4]
ALL_DATA['Micro'] = ALL_DATA['Placement'].str.split('_').str[5]
ALL_DATA['Campaign'] = ALL_DATA['Placement'].str.split('_').str[6]
ALL_DATA['Targ/aud/pin'] = ALL_DATA['Placement'].str.split('_').str[7]
ALL_DATA['Keyword'] = ALL_DATA['Placement'].str.split('_').str[8]

##REPLACE CODE WITH FRIENDLY NAMES
ALL_DATA['Funnel'] = ALL_DATA['Funnel'].replace({'AW' : 'Awareness', 'Cons' : 'Consideration', 'CONS' : 'Consideration', 'Conv' : 'Conversion', 'CONV' : 'Conversion'})
ALL_DATA['Publisher'] = ALL_DATA['Publisher'].replace({'G' : 'Google Search', 'GDN' : 'Google Display Network', 'YT' : 'YouTube', 'FB' : 'Facebook', 'Pin' : 'Pinterest'})

##WRITE PROCESSED FILE TO CSV
ALL_DATA.to_csv('ALL_DATA_PROCESSED_Weekly.csv')

##CALCULATE DAILY IMPRESSIONS BY MAID
IMP_DATA_BY_MAID = ALL_DATA.groupby(by=['MAID', 'Date'])['Impressions'].sum().reset_index()
IMP_DATA_BY_MAID.to_csv('MAID_DATE_Weekly.csv')

##JOIN IMPRESSIONS TO GOOGLE ANALYTICS
IMP_DATA_BY_MAID['Date'] = pd.to_datetime(IMP_DATA_BY_MAID['Date'])
GoogleAnalytics = pd.merge(GoogleAnalytics, IMP_DATA_BY_MAID, how='inner', on=['MAID', 'Date'])
##GoogleAnalytics = GoogleAnalytics.merge(IMP_DATA_BY_MAID, on=['MAID', 'Date'])
##GoogleAnalytics = pd.DataFrame(GoogleAnalytics[GoogleAnalytics.index_x==GoogleAnalytics.index_y]['MAID', 'Date'], columns=['MAID', 'DATE']).reset_index(drop=True)

##CALCULATE COLUMNS TO BE JOINED INTO OVERALL DATASET
GoogleAnalytics['Session Rate'] = GoogleAnalytics['Sessions'].div(GoogleAnalytics['Impressions'], fill_value=0).replace({np.inf: 0})
GoogleAnalytics['New Session Rate'] = GoogleAnalytics['New Users'].div(GoogleAnalytics['Sessions'], fill_value=0).replace({np.inf: 0})
GoogleAnalytics['Bounce Rate'] = GoogleAnalytics['Bounces'].div(GoogleAnalytics['Sessions'], fill_value=0).replace({np.inf: 0})
GoogleAnalytics['Reg Rate'] = GoogleAnalytics['Registrations'].div(GoogleAnalytics['Impressions'], fill_value=0).replace({np.inf: 0})
GoogleAnalytics['Sub Rate'] = GoogleAnalytics['Subscriptions'].div(GoogleAnalytics['Impressions'], fill_value=0).replace({np.inf: 0})
GoogleAnalytics.to_csv('GoogleAnalytics_Weekly_ImpressionJoin.csv')
GoogleAnalyticsRates = GoogleAnalytics
GoogleAnalyticsRates.drop(columns=['Sessions', 'New Users', 'Bounces', 'Registrations', 'Subscriptions', 'Impressions'], axis=1, inplace=True)

##JOIN NEW GOOGLE ANALYTICS TO ALL_DATA_MERGE
ALL_DATA['Date'] = pd.to_datetime(ALL_DATA['Date'])
FINAL_DATA_MERGE = pd.merge(ALL_DATA, GoogleAnalyticsRates, how='inner', on=['MAID', 'Date'])
##FINAL_DATA_MERGE = ALL_DATA.merge(GoogleAnalyticsRates, on=['MAID', 'Date'])
##FINAL_DATA_MERGE = pd.DataFrame(FINAL_DATA_MERGE[FINAL_DATA_MERGE.index_x==FINAL_DATA_MERGE.index_y]['MAID', 'Date'], columns=['MAID', 'DATE']).reset_index(drop=True)
FINAL_DATA_MERGE['Sessions'] = FINAL_DATA_MERGE['Impressions'].mul(FINAL_DATA_MERGE['Session Rate'])
FINAL_DATA_MERGE['New Sessions'] = FINAL_DATA_MERGE['Sessions'].mul(FINAL_DATA_MERGE['New Session Rate'])
FINAL_DATA_MERGE['Bounces'] = FINAL_DATA_MERGE['Sessions'].mul(FINAL_DATA_MERGE['Bounce Rate'])
FINAL_DATA_MERGE['Registrations'] = FINAL_DATA_MERGE['Impressions'].mul(FINAL_DATA_MERGE['Reg Rate'])
FINAL_DATA_MERGE['Subscriptions'] = FINAL_DATA_MERGE['Impressions'].mul(FINAL_DATA_MERGE['Sub Rate'])
FINAL_DATA_MERGE.drop(columns=['Session Rate', 'New Session Rate', 'Bounce Rate', 'Reg Rate', 'Sub Rate'], axis=1, inplace=True)

##ADD REMAINING COLUMNS
FINAL_DATA_MERGE['Start Date'] = FINAL_DATA_MERGE.groupby(['Placement'])['Date'].transform(min)
FINAL_DATA_MERGE['Video v Static'] = np.where(FINAL_DATA_MERGE['Views']>=1, 'Video', 'Static')
FINAL_DATA_MERGE['Impressions with Video View'] = np.where(FINAL_DATA_MERGE['Views']>=1, FINAL_DATA_MERGE['Impressions'], 0)
FINAL_DATA_MERGE['Reporting Month'] = FINAL_DATA_MERGE['Date'].dt.strftime('%B')
FINAL_DATA_MERGE['Week Start'] = FINAL_DATA_MERGE['Date'].dt.to_period('W').apply(lambda r: r.start_time)
FINAL_DATA_MERGE['Reporting Period'] = FINAL_DATA_MERGE['Date'].dt.to_period('W').apply(lambda r: r.start_time)

##REORGANIZE REMAINING COLUMNS
FINAL_DATA_MERGE = FINAL_DATA_MERGE[['Funnel', 'Tactic', 'Publisher', 'Hook', 'Mega', 'Micro', 'Campaign', 'Targ/aud/pin', 'Keyword', 'Start Date', 'MAID', 'Video v Static', 'Impressions with Video View', 'Week Start', 'Reporting Month', 'Reporting Period', 'Placement', 'Creative', 'Date', 'Cost', 'Impressions', 'Clicks', 'Views', 'Video Completion', 'Conversions', 'Sessions', 'New Sessions', 'Bounces', 'Registrations', 'Subscriptions']]

FINAL_DATA_MERGE.to_csv('WEEKLY DATA MERGE.csv')
