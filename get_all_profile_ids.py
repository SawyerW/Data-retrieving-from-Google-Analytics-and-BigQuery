import pandas as pd
def get_list(service):
    account_summaries = service.management().accountSummaries().list().execute()

    account_list = []
    property_list = []
    profile_list = []
    # get all accounts within that this credential
    for accounts in account_summaries.get('items'):
        account_list.append(accounts.get('id'))
        # get all properties within this account
        for properties in accounts.get('webProperties'):
            property_list.append(properties.get('id'))
            # get all profiles within this property
            if properties.get('profiles'):
                for profiles in properties.get('profiles'):
                    profile_list.append(profiles.get('id'))

    return profile_list

def get_property_info(service):
    account_summaries = service.management().accountSummaries().list().execute()

    account_id = []
    account_name = []
    property_id = []
    property_name = []
    property_level = []
    # get all accounts within that this credential
    for accounts in account_summaries.get('items'):
        for properties in accounts.get('webProperties'):
            account_id.append(accounts.get('id'))
            account_name.append(accounts.get('name'))
            property_id.append(properties.get('id'))
            property_name.append(properties.get('name'))
            property_level.append(properties.get('level'))

    df = {'accountId': account_id, 'accountName': account_name, 'propertyId': property_id,
          'propertyName': property_name, 'level': property_level}
    df = pd.DataFrame(df)
    df.to_csv('property_info.csv', encoding='utf-8')
        # account_list.append(accounts.get('id'))
        # # get all properties within this account
        # for properties in accounts.get('webProperties'):
        #     property_list.append(properties.get('id'))
        #     # get all profiles within this property
        #     if properties.get('profiles'):
        #         for profiles in properties.get('profiles'):
        #             profile_list.append(profiles.get('id'))

    # return profile_list


def check_property_(service):
    account_summaries = service.management().accountSummaries().list().execute()

    account_list = []
    account_name = []
    property_list = []
    property_name = []
    profile_list = []
    linkedAccount_id_list = []
    status_list = []
    remarketing_list = []
    # get all accounts within that this credential
    for accounts in account_summaries.get('items'):
        accountId = accounts.get('id')
        accountName = accounts.get('name')

        print ('accounts is:' + str(accountId))
        # account_list.append(accounts.get('id'))
        # get all properties within this account
        for properties in accounts.get('webProperties'):

            propertyId =  properties.get("id")
            propertyName = properties.get('name')

            print ('propertyId is:' + str(propertyId))
            remarketing = service.management().remarketingAudience().list(
                accountId=accountId,
                webPropertyId=str(propertyId)).execute()
            adWordsLinks = service.management().webPropertyAdWordsLinks().list(
                accountId=accountId,
                webPropertyId=str(propertyId)).execute()
            print remarketing
            print adWordsLinks
            account_list.append(accountId)
            account_name.append(accountName)
            property_list.append(propertyId)
            property_name.append(propertyName)
            if remarketing.get('items')==[]:
                remarketing_list.append("NA")
                status_list.append("NA")
            else:
                tmp1=[]
                tmp2=[]
                for adaccount in remarketing.get('items'):
                    for linkedaccount in adaccount.get('linkedAdAccounts'):
                        tmp1.append(linkedaccount.get('linkedAccountId'))
                        tmp2.append(linkedaccount.get('status'))
                remarketing_list.append(tmp1)
                status_list.append(tmp2)


    df = {'accountId':account_list,'accountName':account_name,'propertyId':property_list,'propertyName':property_name,'remarketing':remarketing_list,'status':status_list}
    df = pd.DataFrame(df)
    df.to_csv('remarketing_list.csv', encoding='utf-8')
            # property_list.append(properties.get('id'))
            # # get all profiles within this property
            # if properties.get('profiles'):
            #     for profiles in properties.get('profiles'):
            #         profile_list.append(profiles.get('id'))

