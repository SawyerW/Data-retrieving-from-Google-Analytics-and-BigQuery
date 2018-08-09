import datetime
import time
import re
import pandas as pd
def updating_applications_data(IDs,Opcos,Countries,service,last_monday,last_sunday,bigquery_key):
    day = datetime.timedelta(days=1)
    Metrics = ['ga:totalEvents','ga:users']
    Metrics_br = ['ga:pageviews', 'ga:users']
    Dimensions = 'ga:eventCategory,ga:deviceCategory,ga:eventLabel'
    Opco=[]
    date=[]
    country=[]
    deviceType=[]
    profile_IDs=[]
    eventType=[]
    eventNumber=[]
    users=[]
    for i in range(len(IDs)):
        profile_id = IDs[i]
        #Netherlands, Germany, Japan are dealt with differently
        if profile_id != 'AAAA' and profile_id != 'AAAA' and profile_id != 'AAAA':
            data_count = last_monday
            while data_count <= last_sunday:
                for ms in range(len(Metrics)):
                    # specially for US
                    if profile_id == 'AAAA':
                        raw_data = service.data().ga().get(
                            ids='ga:' + profile_id,
                            start_date=str(data_count),
                            end_date=str(data_count),
                            dimensions='ga:eventCategory,ga:deviceCategory,ga:eventAction',
                            metrics=Metrics[ms],
                            max_results='10000',
                            filters='ga:eventCategory==application flow;ga:eventAction==complete'
                        ).execute()
                    #specially for Sweden
                    elif profile_id == 'AAAA':
                            raw_data = service.data().ga().get(
                                ids='ga:' + profile_id,
                                start_date=str(data_count),
                                end_date=str(data_count),
                                dimensions='ga:eventCategory,ga:deviceCategory,ga:eventAction',
                                metrics=Metrics[ms],
                                max_results='10000',
                                filters='ga:eventCategory==ecommerce;ga:eventAction==transaction'
                            ).execute()
                    #specially for Italy
                    elif profile_id == 'AAAA':
                        raw_data = service.data().ga().get(
                            ids='ga:' + profile_id,
                            start_date=str(data_count),
                            end_date=str(data_count),
                            dimensions='ga:eventCategory,ga:deviceCategory,ga:eventAction',
                            metrics=Metrics[ms],
                            max_results='10000',
                            filters='ga:eventCategory=@[H]Candidate;ga:eventAction=@[H]Hard'
                        ).execute()
                    # specially adding for Brazil
                    elif profile_id == 'AAAA':
                        raw_data = service.data().ga().get(
                            ids='ga:' + profile_id,
                            start_date=str(data_count),
                            end_date=str(data_count),
                            dimensions='ga:deviceCategory',
                            metrics=Metrics[ms],
                            max_results='10000',
                            filters='ga:eventLabel=@[H]DJA,ga:eventLabel=@[H]OJA'
                        ).execute()
                        raw_data_redirect = service.data().ga().get(
                            ids='ga:' + 'AAAA',
                            start_date=str(data_count),
                            end_date=str(data_count),
                            dimensions='ga:deviceCategory',
                            metrics=Metrics_br[ms],
                            max_results='10000',
                            segment='sessions::sequence::ga:source==aa.com.br->>ga:pagePath=@aaFeedback',
                            filters='ga:source==aa.com.br;ga:pagePath=@aaFeedback'
                        ).execute()
                    # specially for Norway
                    elif profile_id == 'AAAA':
                        raw_data = service.data().ga().get(
                            ids='ga:' + profile_id,
                            start_date=str(data_count),
                            end_date=str(data_count),
                            dimensions='ga:deviceCategory',
                            metrics=Metrics_br[ms],
                            max_results='10000',
                            segment='sessions::condition::ga:pagePath=@aa-soknaden',
                            filters='ga:pagePath=@aa-soknaden'
                        ).execute()
                    # specially for AR
                    elif profile_id == 'AAAA':
                        raw_data = service.data().ga().get(
                            ids='ga:' + profile_id,
                            start_date=str(data_count),
                            end_date=str(data_count),
                            dimensions='ga:deviceCategory',
                            metrics=Metrics_br[ms],
                            max_results='10000',
                            segment='sessions::condition::ga:pagePath=@aaa/JobApplicationConfirm',
                            filters='ga:pagePath=@aaa/JobApplicationConfirm'
                        ).execute()
                    # specially for CZ
                    elif profile_id == 'AAAA':
                        raw_data = service.data().ga().get(
                            ids='ga:' + profile_id,
                            start_date=str(data_count),
                            end_date=str(data_count),
                            dimensions='ga:deviceCategory',
                            metrics=Metrics[ms],
                            max_results='10000',
                            filters='ga:eventLabel=@[H]DJA,ga:eventLabel=@[H]OJA'
                        ).execute()
                        raw_data_redirect = service.data().ga().get(
                            ids='ga:' + 'AAAA',
                            start_date=str(data_count),
                            end_date=str(data_count),
                            dimensions='ga:deviceCategory',
                            metrics=Metrics_br[ms],
                            max_results='10000',
                            segment='sessions::condition::ga:pagePath=@public/form/thankyou/',
                            filters='ga:pagePath=@public/form/thankyou/'
                        ).execute()
                    else:
                        raw_data = service.data().ga().get(
                            ids='ga:' + profile_id,
                            start_date=str(data_count),
                            end_date=str(data_count),
                            dimensions=Dimensions,
                            metrics=Metrics[ms],
                            max_results='10000',
                            filters='ga:eventCategory=@[H]Candidate;ga:eventAction=@[H]Hard'
                        ).execute()
                    #specially for brazil, UK and CZ
                    if profile_id != 'AAAA' and profile_id != 'AAAA' and profile_id != 'AAAA' and profile_id != 'AAAA':
                        if raw_data.get('rows'):
                            if len(raw_data.get('rows')) > 0 and ms == 0:
                                for data_oneweek in raw_data.get('rows'):
                                    Opco.append(Opcos[i])
                                    country.append(Countries[i])
                                    date.append(str(data_count))
                                    deviceType.append(data_oneweek[1])
                                    profile_IDs.append(profile_id)
                                    eventType.append(data_oneweek[2])
                                    eventNumber.append(int(data_oneweek[3]))
                            if len(raw_data.get('rows')) > 0 and ms == 1:
                                for data_oneweek in raw_data.get('rows'):
                                    users.append(int(data_oneweek[3]))
                    #For Norway
                    if profile_id == 'AAAA' or profile_id == 'AAAA':
                        if raw_data.get('rows'):
                            if len(raw_data.get('rows')) > 0 and ms == 0:
                                for data_oneweek in raw_data.get('rows'):
                                    Opco.append(Opcos[i])
                                    country.append(Countries[i])
                                    date.append(str(data_count))
                                    deviceType.append(data_oneweek[0])
                                    profile_IDs.append(profile_id)
                                    eventType.append('applications')
                                    eventNumber.append(int(data_oneweek[1]))
                            if len(raw_data.get('rows')) > 0 and ms == 1:
                                for data_oneweek in raw_data.get('rows'):
                                    users.append(int(data_oneweek[1]))
                    #For Brazil and CZ
                    if profile_id == 'AAAA' or profile_id == 'AAAA':
                        if raw_data.get('rows'):
                            if len(raw_data.get('rows')) > 0 and ms == 0:
                                for data_oneweek in raw_data.get('rows'):
                                    Opco.append(Opcos[i])
                                    country.append(Countries[i])
                                    date.append(str(data_count))
                                    deviceType.append(data_oneweek[0])
                                    profile_IDs.append(profile_id)
                                    eventType.append('applications')
                                    eventNumber.append(int(data_oneweek[1]))
                            if len(raw_data.get('rows')) > 0 and ms == 1:
                                for data_oneweek in raw_data.get('rows'):
                                    users.append(int(data_oneweek[1]))
                         # get data from another domain
                        if raw_data_redirect.get('rows'):
                            if len(raw_data_redirect.get('rows')) > 0 and ms == 0:
                                for data_oneweek in raw_data_redirect.get('rows'):
                                    Opco.append(Opcos[i])
                                    country.append(Countries[i])
                                    date.append(str(data_count))
                                    deviceType.append(data_oneweek[0])
                                    profile_IDs.append(profile_id)
                                    eventType.append('applications')
                                    eventNumber.append(int(data_oneweek[1]))
                            if len(raw_data_redirect.get('rows')) > 0 and ms == 1:
                                for data_oneweek in raw_data_redirect.get('rows'):
                                    users.append(int(data_oneweek[1]))


                data_count = data_count + day
        #specially extracting data of Japan from bigquery
        if profile_id == 'AAAA':
            query = '''select count(deviceCategory) as application, deviceCategory, actiontimestamp
                        from (
                        SELECT
                          fullVisitorId,
                          DATE(actiontimestamp) AS actiontimestamp,
                          deviceCategory
                        FROM (
                          SELECT
                            fullVisitorId,
                            visitorId,
                            device.deviceCategory AS deviceCategory,
                            hits.page.pagePath,
                            STRFTIME_UTC_USEC(SEC_TO_TIMESTAMP(visitStartTime+ hits.time/1000),"%Y-%m-%d %H:%M:%S") AS actiontimestamp
                          FROM
                            TABLE_DATE_RANGE([AAAA.ga_sessions_],
                              DATE_ADD(CURRENT_TIMESTAMP(), -7, 'DAY'), DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY'))
                          WHERE
                            hits.type = 'PAGE'
                            AND (hits.page.pagePath CONTAINS 'registration/OSTSTF0042.do' or hits.page.pagePath CONTAINS 'mypage/OWMSTF0142.do' or hits.page.pagePath CONTAINS 'mypage/OWMSTF0193.do')
                          GROUP BY
                            fullVisitorId,
                            visitorId,
                            deviceCategory,
                            hits.page.pagePath,
                            actiontimestamp))
                        group by deviceCategory, actiontimestamp
                        order by actiontimestamp,deviceCategory'''
            #DATE_ADD(CURRENT_TIMESTAMP(), -7, 'DAY'), DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY')
            #DATE_ADD(CURRENT_TIMESTAMP(), -7, 'DAY'), DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY')
            df = pd.read_gbq(query, project_id="japan", private_key=bigquery_key,
                             dialect='legacy')
            df = df.values.tolist()
            for i_japan in range(len(df)):
                date.append(df[i_japan][2])
                country.append(Countries[i])
                profile_IDs.append(profile_id)
                Opco.append(Opcos[i])
                eventType.append('applications')
                eventNumber.append(df[i_japan][0])
                deviceType.append(df[i_japan][1])
            query = '''select COUNT (distinct fullvisitorid) as application, deviceCategory, actiontimestamp
                        from (
                        SELECT
                          fullVisitorId,
                          DATE(actiontimestamp) AS actiontimestamp,
                          deviceCategory
                        FROM (
                          SELECT
                            fullVisitorId,
                            visitorId,
                            device.deviceCategory AS deviceCategory,
                            hits.page.pagePath,
                            STRFTIME_UTC_USEC(SEC_TO_TIMESTAMP(visitStartTime+ hits.time/1000),"%Y-%m-%d %H:%M:%S") AS actiontimestamp
                          FROM
                            TABLE_DATE_RANGE([AAAA.ga_sessions_],
                              DATE_ADD(CURRENT_TIMESTAMP(), -7, 'DAY'), DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY'))
                          WHERE
                            hits.type = 'PAGE'
                            AND (hits.page.pagePath CONTAINS 'registration/OSTSTF0042.do' or hits.page.pagePath CONTAINS 'mypage/OWMSTF0142.do' or hits.page.pagePath CONTAINS 'mypage/OWMSTF0193.do')
                          GROUP BY
                            fullVisitorId,
                            visitorId,
                            deviceCategory,
                            hits.page.pagePath,
                            actiontimestamp)
                            )
                        group by deviceCategory, actiontimestamp
                        order by actiontimestamp,deviceCategory '''
            # DATE_ADD(CURRENT_TIMESTAMP(), -7, 'DAY'), DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY')
            # DATE_ADD(CURRENT_TIMESTAMP(), -7, 'DAY'), DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY')
            df = pd.read_gbq(query, project_id="japan", private_key=bigquery_key,
                             dialect='legacy')
            df = df.values.tolist()
            for i_japan in range(len(df)):
                users.append(df[i_japan][0])
        # specially extracting data of Netherlands from bigquery
        if profile_id == 'AAAA':
            query = '''select count(deviceCategory) as application, deviceCategory, actiontimestamp
                        from (
                        SELECT
                          fullVisitorId,
                          DATE(actiontimestamp) AS actiontimestamp,
                          deviceCategory
                        FROM (
                          SELECT
                            fullVisitorId,
                            visitorId,
                            device.deviceCategory AS deviceCategory,
                            hits.page.pagePath,
                            STRFTIME_UTC_USEC(SEC_TO_TIMESTAMP(visitStartTime+ hits.time/1000),"%Y-%m-%d %H:%M:%S") AS actiontimestamp
                          FROM
                            TABLE_DATE_RANGE([AAAA.ga_sessions_],
                              DATE_ADD(CURRENT_TIMESTAMP(), -7, 'DAY'), DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY'))
                          WHERE
                            hits.type = 'PAGE'
                            AND hits.eCommerceAction.action_type = '6'
                          GROUP BY
                            fullVisitorId,
                            visitorId,
                            deviceCategory,
                            hits.page.pagePath,
                            actiontimestamp))
                        group by deviceCategory, actiontimestamp
                        order by actiontimestamp,deviceCategory'''
            # DATE_ADD(CURRENT_TIMESTAMP(), -7, 'DAY'), DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY')
            # DATE_ADD(CURRENT_TIMESTAMP(), -7, 'DAY'), DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY')
            df = pd.read_gbq(query, project_id="AAAA", private_key=bigquery_key,
                             dialect='legacy')
            df = df.values.tolist()
            for i_japan in range(len(df)):
                date.append(df[i_japan][2])
                country.append(Countries[i])
                profile_IDs.append(profile_id)
                Opco.append(Opcos[i])
                eventType.append('applications')
                eventNumber.append(df[i_japan][0])
                deviceType.append(df[i_japan][1])
            query = '''select COUNT (distinct fullvisitorid) as application, deviceCategory, actiontimestamp
                        from (
                        SELECT
                          fullVisitorId,
                          DATE(actiontimestamp) AS actiontimestamp,
                          deviceCategory
                        FROM (
                          SELECT
                            fullVisitorId,
                            visitorId,
                            device.deviceCategory AS deviceCategory,
                            hits.page.pagePath,
                            STRFTIME_UTC_USEC(SEC_TO_TIMESTAMP(visitStartTime+ hits.time/1000),"%Y-%m-%d %H:%M:%S") AS actiontimestamp
                          FROM
                            TABLE_DATE_RANGE([AAAA.ga_sessions_],
                              DATE_ADD(CURRENT_TIMESTAMP(), -7, 'DAY'), DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY'))
                          WHERE
                            hits.type = 'PAGE'
                            AND hits.eCommerceAction.action_type = '6'
                          GROUP BY
                            fullVisitorId,
                            visitorId,
                            deviceCategory,
                            hits.page.pagePath,
                            actiontimestamp)
                            )
                        group by deviceCategory, actiontimestamp
                        order by actiontimestamp,deviceCategory '''
            # DATE_ADD(CURRENT_TIMESTAMP(), -7, 'DAY'), DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY')
            # DATE_ADD(CURRENT_TIMESTAMP(), -7, 'DAY'), DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY')
            df = pd.read_gbq(query, project_id="AAAA", private_key=bigquery_key,
                             dialect='legacy')
            df = df.values.tolist()
            for i_japan in range(len(df)):
                users.append(df[i_japan][0])
        # specially extracting data of Germany from bigquery
        if profile_id == 'AAAA':
            query = '''select count(deviceCategory) as application, deviceCategory, actiontimestamp
                        from (
                        SELECT
                          fullVisitorId,
                          DATE(actiontimestamp) AS actiontimestamp,
                          deviceCategory
                        FROM (
                          SELECT
                            fullVisitorId,
                            visitorId,
                            device.deviceCategory AS deviceCategory,
                            hits.page.pagePath,
                            STRFTIME_UTC_USEC(SEC_TO_TIMESTAMP(visitStartTime+ hits.time/1000),"%Y-%m-%d %H:%M:%S") AS actiontimestamp
                          FROM
                            TABLE_DATE_RANGE([AAAA.ga_sessions_],
                              DATE_ADD(CURRENT_TIMESTAMP(), -7, 'DAY'), DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY'))
                          WHERE
                            hits.type = 'PAGE'
                            AND hits.eCommerceAction.action_type = '6'
                          GROUP BY
                            fullVisitorId,
                            visitorId,
                            deviceCategory,
                            hits.page.pagePath,
                            actiontimestamp))
                        group by deviceCategory, actiontimestamp
                        order by actiontimestamp,deviceCategory'''
            # DATE_ADD(CURRENT_TIMESTAMP(), -7, 'DAY'), DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY')
            # DATE_ADD(CURRENT_TIMESTAMP(), -7, 'DAY'), DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY')
            df = pd.read_gbq(query, project_id="germany", private_key=bigquery_key,
                             dialect='legacy')
            df = df.values.tolist()
            for i_japan in range(len(df)):
                date.append(df[i_japan][2])
                country.append(Countries[i])
                profile_IDs.append(profile_id)
                Opco.append(Opcos[i])
                eventType.append('applications')
                eventNumber.append(df[i_japan][0])
                deviceType.append(df[i_japan][1])
            query = '''select COUNT (distinct fullvisitorid) as application, deviceCategory, actiontimestamp
                        from (
                        SELECT
                          fullVisitorId,
                          DATE(actiontimestamp) AS actiontimestamp,
                          deviceCategory
                        FROM (
                          SELECT
                            fullVisitorId,
                            visitorId,
                            device.deviceCategory AS deviceCategory,
                            hits.page.pagePath,
                            STRFTIME_UTC_USEC(SEC_TO_TIMESTAMP(visitStartTime+ hits.time/1000),"%Y-%m-%d %H:%M:%S") AS actiontimestamp
                          FROM
                            TABLE_DATE_RANGE([AAAA.ga_sessions_],
                              DATE_ADD(CURRENT_TIMESTAMP(), -7, 'DAY'), DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY'))
                          WHERE
                            hits.type = 'PAGE'
                            AND hits.eCommerceAction.action_type = '6'
                          GROUP BY
                            fullVisitorId,
                            visitorId,
                            deviceCategory,
                            hits.page.pagePath,
                            actiontimestamp)
                            )
                        group by deviceCategory, actiontimestamp
                        order by actiontimestamp,deviceCategory '''
            # DATE_ADD(CURRENT_TIMESTAMP(), -7, 'DAY'), DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY')
            # DATE_ADD(CURRENT_TIMESTAMP(), -7, 'DAY'), DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY')
            df = pd.read_gbq(query, project_id="germany", private_key=bigquery_key,
                             dialect='legacy')
            df = df.values.tolist()
            for i_japan in range(len(df)):
                users.append(df[i_japan][0])

    print('length')
    print(len(users))
    print(len(country))
    #process event labels of UK for data cleaning
    for i in range(len(eventType)):
        # if Opco[i] == 'aa Italy':
        #     eventType[i] = re.sub(r'^.*DJA/[\d]+.*','[H]DJA (Professionals)',eventType[i])
        #     eventType[i] = re.sub(r'^.*DJA/SE[\d]+-SEDE-.*', '[H]DJA (staffing)', eventType[i])
        if Opco[i] == 'aa UK':
            eventType[i] = re.sub(r'^.*DJA.+(non-staffing).*', '[H]DJA (non-staffing)', eventType[i])
            eventType[i] = re.sub(r'^.*DJA.+(staffing).*', '[H]DJA (staffing)', eventType[i])
            eventType[i] = re.sub(r'^.*OJA.*', '[H]OJA', eventType[i])

    Opco_new=[]
    date_new=[]
    country_new=[]
    users_new=[]
    profileId_new=[]
    deviceType_new=[]
    eventType_new=[]
    eventNumber_new=[]

    for i in range(len(Opco)):
        if i == 0:
            Opco_new.append(Opco[i])
            country_new.append(country[i])
            date_new.append(date[i])
            profileId_new.append(profile_IDs[i])
            deviceType_new.append(deviceType[i])
            eventNumber_new.append(int(eventNumber[i]))
            eventType_new.append(eventType[i])
            users_new.append(users[i])
        else:
            for j in range(len(Opco_new)):
                if (Opco[i] == Opco_new[j]) and (country[i] == country_new[j]) and (date[i] == date_new[j]) and (profile_IDs[i] == profileId_new[j]) and (deviceType[i] == deviceType_new[j]) and (eventType[i] == eventType_new[j]):
                    eventNumber_new[j] = int(eventNumber_new[j]) + int(eventNumber[i])
                    users_new[j] = int(users_new[j]) + int(users[i])
                    break

                if (Opco[i] != Opco_new[j]) or (date[i] != date_new[j]) or (profile_IDs[i] != profileId_new[j]) or (
                        deviceType[i] != deviceType_new[j]) or (eventType[i] != eventType_new[j]) or (country[i] != country_new[j]):

                    if j == len(eventNumber_new)-1:
                        Opco_new.append(str(Opco[i]))
                        country_new.append(str(country[i]))
                        date_new.append(str(date[i]))
                        profileId_new.append(str(profile_IDs[i]))
                        deviceType_new.append(str(deviceType[i]))
                        eventNumber_new.append(int(eventNumber[i]))
                        eventType_new.append(str(eventType[i]))
                        users_new.append(int(users[i]))
                        break
    eventType_general=[]
    for i in range(len(eventType_new)):
        if 'DJA' in eventType_new[i]:
            eventType_general.append('DJA')
        elif 'OJA' in eventType_new[i]:
            eventType_general.append('OJA')
        else:
            eventType_general.append(eventType_new[i])
        print(Opco_new[i],eventType_new[i],deviceType_new[i],date_new[i],eventNumber_new[i])
    df={'Date':date_new,'profileId':profileId_new,'Opco':Opco_new,'Country':country_new,'deviceType':deviceType_new,'eventType':eventType_new,'eventNumber':eventNumber_new,'eventType_general':eventType_general,'users':users_new}
    df = pd.DataFrame(df)
    pd.io.gbq.to_gbq(df, 'AAAA', 'AAAA',
                     if_exists='replace',
                     private_key=bigquery_key)
