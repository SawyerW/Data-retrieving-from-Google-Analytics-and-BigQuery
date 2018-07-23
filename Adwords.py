import datetime
import time
import pandas as pd
from currency_converter import CurrencyConverter
def updating_adwords_data(IDs,Opcos,Countries,service,last_monday,last_sunday,bigquery_key):
    c=CurrencyConverter()
    day = datetime.timedelta(days=1)
    Metrics = 'ga:adClicks,ga:adCost,ga:CPC,ga:CTR'
    Dimensions = 'ga:deviceCategory,ga:date'
    currency_converter_list=[]
    currency=['USD','DKK','NOK','SEK','USD','GBP','USD','AUD','USD','USD','CAD','MXN','CAD','CHF','USD']
    Opco=[]
    date=[]
    country=[]
    profile_IDs=[]
    deviceType=[]
    adClicks=[]
    adCost=[]
    CPC=[]
    CTR=[]

    for i in range(len(IDs)):
        data_count = last_monday
        # skip Argentina
        profile_id = IDs[i]

        if profile_id != 'AAAA':
            if profile_id in currency_converter_list:
                currency_id = currency[currency_converter_list.index(profile_id)]
                while data_count <= last_sunday:
                    raw_data = service.data().ga().get(
                        ids='ga:' + profile_id,
                        start_date=str(data_count),
                        end_date=str(data_count),
                        dimensions=Dimensions,
                        metrics=Metrics,
                        max_results='10000',
                    ).execute()
                    date_used_for_currency = data_count

                    for correct_cc in range(0, 100):
                        try:
                            cc = c.convert(1, 'EUR', currency_id, date=date_used_for_currency)
                            break
                        except:
                            date_used_for_currency = date_used_for_currency - day
                    if raw_data.get('rows'):
                        print(Countries[i], raw_data)
                        if len(raw_data.get('rows')) > 0:
                            for data_oneweek in raw_data.get('rows'):
                                Opco.append(str(Opcos[i]))
                                country.append(str(Countries[i]))
                                date.append(str(str(data_count)))
                                deviceType.append(str(data_oneweek[0]))
                                adClicks.append(int(data_oneweek[2]))
                                adCost.append(float(data_oneweek[3])/cc)
                                CPC.append(float(data_oneweek[4])/cc)
                                CTR.append(float(data_oneweek[5]))
                                profile_IDs.append(str(profile_id))
                    data_count = data_count + day

            elif profile_id not in currency_converter_list:
                while data_count <= last_sunday:
                    raw_data = service.data().ga().get(
                        ids='ga:' + profile_id,
                        start_date=str(data_count),
                        end_date=str(data_count),
                        dimensions=Dimensions,
                        metrics=Metrics,
                        max_results='10000',
                    ).execute()
                    if raw_data.get('rows'):
                        print(Countries[i],raw_data)
                        if len(raw_data.get('rows')) > 0:
                            for data_oneweek in raw_data.get('rows'):
                                Opco.append(str(Opcos[i]))
                                country.append(str(Countries[i]))
                                date.append(str(str(data_count)))
                                deviceType.append(str(data_oneweek[0]))
                                adClicks.append(int(data_oneweek[2]))
                                adCost.append(float(data_oneweek[3]))
                                CPC.append(float(data_oneweek[4]))
                                CTR.append(float(data_oneweek[5]))
                                profile_IDs.append(str(profile_id))
                    data_count = data_count + day


    df={'Date':date,'profileId':profile_IDs,'Opco':Opco,'Country':country,'deviceType':deviceType,'adClicks':adClicks,'adCost':adCost,'CPC':CPC,'CTR':CTR}
    df = pd.DataFrame(df)
    pd.io.gbq.to_gbq(df, 'AAAA', 'AAAA',
                     if_exists='replace',
                     private_key=bigquery_key)
