def get_results(service, profile_id):
    # Use the Analytics Service Object to query the Core Reporting API
    # for the number of sessions within the past seven days.

    Metrics = 'ga:users,ga:newUsers,ga:sessionsPerUser'
    Dimensions = 'ga:userType,ga:source'
    return service.data().ga().get(
            ids='ga:' + profile_id,
            start_date='7daysAgo',
            end_date='today',
            # dimensions=Dimensions,
            metrics=Metrics
                    ).execute()


# res = service.data().ga().get(ids='ga:' + profile_id, start_date='2014-01-01', end_date=t, metrics='ga:sessions',
#                               dimensions='ga:browser',sort='-ga:sessions' , max_results='5' ).execute()