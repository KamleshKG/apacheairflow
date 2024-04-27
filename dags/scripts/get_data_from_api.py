from sqlalchemy import create_engine
import requests
import json
SQLALCHEMY_SILENCE_UBER_WARNING=1

def get_aqi_data():
    decode_data_london_aqi_data = requests.get('https://api.erg.ic.ac.uk/AirQuality/Annual/MonitoringObjective/GroupName=All/Json').content.decode('utf-8-sig')
    london_aqi_data = json.loads(decode_data_london_aqi_data)
    engine = create_engine(url="mysql+pymysql://root:example@localhost:3306/test")
    for data in london_aqi_data['SiteObjectives']['Site']:
        for objective in data['Objective']:
            engine.execute(f'INSERT INTO AQIDATA VALUES("{objective['@Year']}","{data['@SiteCode']}","{data['@SiteName']}","{str(data['@Latitude'])}","{str(data['@Longitude'])}","{str(objective['@SpeciesCode'])}","{objective['@SpeciesDescription']}","{str(objective['@Value'])}")')
