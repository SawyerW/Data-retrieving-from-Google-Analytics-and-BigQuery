import pandas as pd
from read_credentials import GetService_analytics
import sys
reload(sys)
sys.setdefaultencoding('utf8')


custom_dimensions =[
    ['x','USER',False],
    ['x','USER',False],
    ['x','USER',False],
    ['x','USER',False],
    ['x','SESSION',False]]
config = GetService_analytics()

#UPDATE WHICH ACCOUNTS AND PROPERTIES NEED TO BE UPDATED
accountList = ['xxxxxxx']
propertyList = ['UA-xxxxxx-2']

#add custom dimension function
def add_custom_dimension(accountId, propId, name, scope, active):
    config.management().customDimensions().insert(
        accountId=accountId,
        webPropertyId=propId,
        body={
            'name': name,
            'scope': scope,
            'active': active
        }
    ).execute()

#update custom dimension function
def update_custom_dimension(accountId, propId, dimensionId, name, scope, active):
    config.management().customDimensions().update(
        accountId=accountId,
        webPropertyId=propId,
        customDimensionId=dimensionId,
        body={
            'name': name,
            'scope': scope,
            'active': active
        }
    ).execute()


#Get all Google Analytics accounts
#accounts = config.analytics.management().accounts().list().execute()
for accountId in accountList:
    count=0
    propertyId = propertyList[accountList.index(accountId)]
    for i in range(len(custom_dimensions)):
        tmp = 'ga:dimension' + str(i+1)
        print tmp
        row = custom_dimensions[i]
        try:

            dimensions = config.management().customDimensions().get(
                accountId=accountId,
                webPropertyId=propertyId,
                customDimensionId=tmp
            ).execute()
            print ('custom dimension exists, updating')

            update_custom_dimension(accountId, propertyId, tmp, row[0], row[1], row[2])
            count+=1
        except:
            # Handle errors in constructing a query.
            print ("updating finishes, inserting")
            # print('There was an error in constructing your query : %s' % error)


            add_custom_dimension(accountId, propertyId, row[0], row[1], row[2])

