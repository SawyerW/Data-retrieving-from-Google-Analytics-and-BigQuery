import datetime
import time
import re
import pandas as pd
def updating_csr_data(IDs,Opcos,Countries,service,bigquery_key):
    start_date='2018-01-01'
    # If you need data for Q3 or Q4, just need to add corresponding date and quater name to list end_date and Qs
    #Before running, iit's better to check if there are some changes to be made, incase they changed way to measure application or even property.
    end_date=['2018-03-31','2018-06-30']
    Qs = ['Q1','Q2']
    Metrics = 'ga:users'
    country=[]
    profile_IDs=[]
    users=[]
    Quater=[]
    applicants=[]
    for i in range(len(IDs)):
        for q in range(len(Qs)):
            profile_id = IDs[i]

            #Netherlands, Germany, Japan are dealt with differently
            if profile_id != 'AAAA' and profile_id != 'AAAA' and profile_id != 'AAAA':
                # specially for US
                if profile_id == 'AAAA':
                    raw_data = service.data().ga().get(
                        ids='ga:' + profile_id,
                        start_date=start_date,
                        end_date=end_date[q],
                        metrics=Metrics,
                        max_results='10000',
                        filters='ga:eventCategory==application flow;ga:eventAction==complete'
                    ).execute()

                #specially for Sweden
                elif profile_id == 'AAAA':
                    raw_data = service.data().ga().get(
                        ids='ga:' + profile_id,
                        start_date=start_date,
                        end_date=end_date[q],
                        metrics=Metrics,
                        max_results='10000',
                        filters='ga:eventCategory==ecommerce;ga:eventAction==transaction'
                    ).execute()
                # specially adding for Brazil
                elif profile_id == 'AAAA':
                    raw_data = service.data().ga().get(
                        ids='ga:' + profile_id,
                        start_date=start_date,
                        end_date=end_date[q],
                        metrics=Metrics,
                        max_results='10000',
                        filters='ga:eventCategory=@[H]Candidate;ga:eventAction=@[H]Hard'
                    ).execute()
                    raw_data_redirect = service.data().ga().get(
                        ids='ga:' + 'AAAA',
                        start_date=start_date,
                        end_date=end_date[q],
                        metrics=Metrics,
                        max_results='10000',
                        segment='sessions::sequence::ga:source==randstad.com.br,ga:source==randstad.com->>ga:pagePath=@InviteAcceptedFeedback',
                        filters='ga:source==randstad.com.br,ga:source==randstad.com;ga:pagePath=@InviteAcceptedFeedback'
                    ).execute()
                # specially for Norway
                elif profile_id == 'AAAA':
                    raw_data = service.data().ga().get(
                        ids='ga:' + profile_id,
                        start_date=start_date,
                        end_date=end_date[q],
                        metrics=Metrics,
                        max_results='10000',
                        segment='sessions::condition::ga:pagePath=@takk-for-soknaden',
                        filters='ga:pagePath=@takk-for-soknaden'
                    ).execute()
                # specially for CZ
                elif profile_id == 'AAAA':
                    raw_data = service.data().ga().get(
                        ids='ga:' + profile_id,
                        start_date=start_date,
                        end_date=end_date[q],
                        metrics=Metrics,
                        max_results='10000',
                        filters='ga:eventCategory=@[H]Candidate;ga:eventAction=@[H]Hard'
                    ).execute()
                    raw_data_redirect = service.data().ga().get(
                        ids='ga:' + 'AAAA',
                        start_date=start_date,
                        end_date=end_date[q],
                        metrics=Metrics,
                        max_results='10000',
                        segment='sessions::condition::ga:pagePath=@public/form/thankyou/',
                        filters='ga:pagePath=@public/form/thankyou/'
                    ).execute()
                # specially for AR
                elif profile_id == 'AAAA':
                    raw_data = service.data().ga().get(
                        ids='ga:' + profile_id,
                        start_date=start_date,
                        end_date=end_date[q],
                        metrics=Metrics,
                        max_results='10000',
                        segment='sessions::condition::ga:pagePath=@MiRandstad/JobApplicationConfirm',
                        filters='ga:pagePath=@MiRandstad/JobApplicationConfirm'
                    ).execute()
                else:
                    raw_data = service.data().ga().get(
                        ids='ga:' + profile_id,
                        start_date=start_date,
                        end_date=end_date[q],
                        metrics=Metrics,
                        max_results='10000',
                        filters='ga:eventCategory=@[H]Candidate;ga:eventAction=@[H]Hard'
                    ).execute()
                # all user
                raw_data_all = service.data().ga().get(
                    ids='ga:' + profile_id,
                    start_date=start_date,
                    end_date=end_date[q],
                    metrics=Metrics,
                    max_results='10000',
                ).execute()
                #specially for brazil and CZ
                if profile_id != 'AAAA' and profile_id != 'AAAA':
                    if raw_data.get('rows'):
                        print(Countries[i],Qs[q],raw_data.get('rows'))
                        if len(raw_data.get('rows')) > 0:
                            for data_oneweek in raw_data.get('rows'):
                                country.append(Countries[i])
                                profile_IDs.append(profile_id)
                                applicants.append(data_oneweek[0])
                                Quater.append(Qs[q])
                            for data_oneweek in raw_data_all.get('rows'):
                                users.append(data_oneweek[0])

                #For Brazil and CZ
                if profile_id == 'AAAA' or profile_id == 'AAAA':
                    if raw_data.get('rows'):
                        print(Countries[i], Qs[q], raw_data.get('rows'))
                        if len(raw_data.get('rows')) > 0:
                            for data_oneweek in raw_data.get('rows'):
                                country.append(Countries[i])
                                profile_IDs.append(profile_id)
                                Quater.append(Qs[q])
                                applicants.append(data_oneweek[0])
                            for data_oneweek in raw_data_all.get('rows'):
                                users.append(data_oneweek[0])
                    if raw_data_redirect.get('rows'):
                        print(Countries[i], Qs[q], raw_data_redirect.get('rows'))
                        if len(raw_data_redirect.get('rows')) > 0:
                            for data_oneweek in raw_data_redirect.get('rows'):
                                country.append(Countries[i])
                                profile_IDs.append(profile_id)
                                applicants.append(data_oneweek[0])
                                Quater.append(Qs[q])
                                users.append(0)


        #specially extracting data of Japan from bigquery
        if profile_id == 'AAAA':
            # for Q1
            query_conversion = '''select count(fullvisitorid) as count
                                            from(
                                            select fullvisitorid 
                                            FROM TABLE_DATE_RANGE([AAAA.ga_sessions_], TIMESTAMP('2018-01-01'), TIMESTAMP('2018-03-31'))
                                            where hits.type = 'PAGE' and (hits.page.pagePath CONTAINS 'registration/OSTSTF0042.do' or hits.page.pagePath CONTAINS 'mypage/OWMSTF0142.do' or hits.page.pagePath CONTAINS 'mypage/OWMSTF0193.do')
                                            group by fullvisitorid)'''

            df_conversion = pd.read_gbq(query_conversion, project_id="AAAA",
                                        private_key=bigquery_key,
                                        dialect='legacy')
            df_conversion = df_conversion.values.tolist()

            query = '''select count(fullvisitorid) as count
                                            from(
                                            select fullvisitorid 
                                            FROM TABLE_DATE_RANGE([AAAA.ga_sessions_], TIMESTAMP('2018-01-01'), TIMESTAMP('2018-03-31'))
                                            group by fullvisitorid)'''
            df = pd.read_gbq(query, project_id="AAAA", private_key=bigquery_key,
                             dialect='legacy')
            df = df.values.tolist()
            country.append(Countries[i])
            profile_IDs.append(profile_id)
            Quater.append('Q1')
            applicants.append(df_conversion[0][0])
            users.append(df[0][0])

            # for Q2
            query_conversion = '''select count(fullvisitorid) as count
                                                        from(
                                                        select fullvisitorid 
                                                        FROM TABLE_DATE_RANGE([AAAA.ga_sessions_], TIMESTAMP('2018-01-01'), TIMESTAMP('2018-06-30'))
                                                        where hits.type = 'PAGE' and (hits.page.pagePath CONTAINS 'registration/OSTSTF0042.do' or hits.page.pagePath CONTAINS 'mypage/OWMSTF0142.do' or hits.page.pagePath CONTAINS 'mypage/OWMSTF0193.do')
                                                        group by fullvisitorid)'''

            df_conversion = pd.read_gbq(query_conversion, project_id="AAAA",
                                        private_key=bigquery_key,
                                        dialect='legacy')
            df_conversion = df_conversion.values.tolist()

            query = '''select count(fullvisitorid) as count
                                                        from(
                                                        select fullvisitorid 
                                                        FROM TABLE_DATE_RANGE([AAAA.ga_sessions_], TIMESTAMP('2018-01-01'), TIMESTAMP('2018-06-30'))
                                                        group by fullvisitorid)'''
            df = pd.read_gbq(query, project_id="AAAA", private_key=bigquery_key,
                             dialect='legacy')
            df = df.values.tolist()
            country.append(Countries[i])
            profile_IDs.append(profile_id)
            Quater.append('Q2')
            applicants.append(df_conversion[0][0])
            users.append(df[0][0])


        # specially extracting data of Netherlands from bigquery
        if profile_id == 'AAAA':
            query_conversion = '''select count(fullvisitorid) as count
                                                from(
                                                select fullvisitorid 
                                                FROM TABLE_DATE_RANGE([AAAA.ga_sessions_], TIMESTAMP('2018-01-01'), TIMESTAMP('2018-03-31'))
                                                where hits.eCommerceAction.action_type = '6'
                                                group by fullvisitorid)'''

            df_conversion = pd.read_gbq(query_conversion, project_id="AAAAA",
                                        private_key=bigquery_key,
                                        dialect='legacy')
            df_conversion = df_conversion.values.tolist()
            query = '''select count(fullvisitorid) as count
                                                from(
                                                select fullvisitorid 
                                                FROM TABLE_DATE_RANGE([AAAA.ga_sessions_], TIMESTAMP('2018-01-01'), TIMESTAMP('2018-03-31'))
                                                group by fullvisitorid)'''
            df = pd.read_gbq(query, project_id="AAAA", private_key=bigquery_key,
                             dialect='legacy')
            df = df.values.tolist()
            country.append(Countries[i])
            profile_IDs.append(profile_id)
            Quater.append('Q1')
            applicants.append(df_conversion[0][0])
            users.append(df[0][0])

            query_conversion = '''select count(fullvisitorid) as count
                                                            from(
                                                            select fullvisitorid 
                                                            FROM TABLE_DATE_RANGE([AAAA.ga_sessions_], TIMESTAMP('2018-01-01'), TIMESTAMP('2018-06-30'))
                                                            where hits.eCommerceAction.action_type = '6'
                                                            group by fullvisitorid)'''

            df_conversion = pd.read_gbq(query_conversion, project_id="AAAA",
                                        private_key=bigquery_key,
                                        dialect='legacy')
            df_conversion = df_conversion.values.tolist()
            query = '''select count(fullvisitorid) as count
                                                            from(
                                                            select fullvisitorid 
                                                            FROM TABLE_DATE_RANGE([AAAA.ga_sessions_], TIMESTAMP('2018-01-01'), TIMESTAMP('2018-06-30'))
                                                            group by fullvisitorid)'''
            df = pd.read_gbq(query, project_id="AAAA", private_key=bigquery_key,
                             dialect='legacy')
            df = df.values.tolist()
            country.append(Countries[i])
            profile_IDs.append(profile_id)
            Quater.append('Q2')
            applicants.append(df_conversion[0][0])
            users.append(df[0][0])
        # specially extracting data of Germany from bigquery
        if profile_id == 'AAAAA':
            query_conversion = '''select count(fullvisitorid) as count
                                                       from(
                                                       select fullvisitorid 
                                                       FROM TABLE_DATE_RANGE([AAAA.ga_sessions_], TIMESTAMP('2018-01-01'), TIMESTAMP('2018-03-31'))
                                                       where hits.eCommerceAction.action_type = '6'
                                                       group by fullvisitorid)'''

            df_conversion = pd.read_gbq(query_conversion, project_id="AAAA",
                                        private_key=bigquery_key,
                                        dialect='legacy')
            df_conversion = df_conversion.values.tolist()
            query = '''select count(fullvisitorid) as count
                                                       from(
                                                       select fullvisitorid 
                                                       FROM TABLE_DATE_RANGE([AAAA.ga_sessions_], TIMESTAMP('2018-01-01'), TIMESTAMP('2018-03-31'))
                                                       group by fullvisitorid)'''
            df = pd.read_gbq(query, project_id="AAAA", private_key=bigquery_key,
                             dialect='legacy')
            df = df.values.tolist()
            country.append(Countries[i])
            profile_IDs.append(profile_id)
            Quater.append('Q1')
            applicants.append(df_conversion[0][0])
            users.append(df[0][0])
            query_conversion = '''select count(fullvisitorid) as count
                                                                   from(
                                                                   select fullvisitorid 
                                                                   FROM TABLE_DATE_RANGE([AAAA.ga_sessions_], TIMESTAMP('2018-01-01'), TIMESTAMP('2018-06-30'))
                                                                   where hits.eCommerceAction.action_type = '6'
                                                                   group by fullvisitorid)'''

            df_conversion = pd.read_gbq(query_conversion, project_id="AAAA",
                                        private_key=bigquery_key,
                                        dialect='legacy')
            df_conversion = df_conversion.values.tolist()
            query = '''select count(fullvisitorid) as count
                                                                   from(
                                                                   select fullvisitorid 
                                                                   FROM TABLE_DATE_RANGE([AAAA.ga_sessions_], TIMESTAMP('2018-01-01'), TIMESTAMP('2018-06-30'))
                                                                   group by fullvisitorid)'''
            df = pd.read_gbq(query, project_id="AAAA", private_key=bigquery_key,
                             dialect='legacy')
            df = df.values.tolist()
            country.append(Countries[i])
            profile_IDs.append(profile_id)
            Quater.append('Q2')
            applicants.append(df_conversion[0][0])
            users.append(df[0][0])


    df={'profileId':profile_IDs,'Country':country,'applicants':applicants,'users':users,'Quater':Quater}
    df = pd.DataFrame(df)
    pd.io.gbq.to_gbq(df, 'AAAA', 'AAAA',
                     if_exists='replace',
                     private_key=bigquery_key)
