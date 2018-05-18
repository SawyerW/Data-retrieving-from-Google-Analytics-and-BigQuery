import pandas as pd
from StringIO import StringIO

def read_sheet(service):
    id = '1tZIPXXXXXXXXXXXXXXXXXXXXXXXXXXXC_mUYo'
    rangeName = '2017102'
    result = service.spreadsheets().values().get(
        spreadsheetId=id, range=rangeName).execute()

    # retrieve data by rows
    values = result.get('values', [])

    #retrieve profile id list which should be used
    headers = values[0]
    index_shouldbeused=headers.index('shouldBeUsed')
    index_profileid=headers.index('profileId')
    profile_id_list=[]
    for i in range(len(values)):
        if values[i][index_shouldbeused] == 'TRUE':
            profile_id_list.append(values[i][index_profileid])
    return profile_id_list
