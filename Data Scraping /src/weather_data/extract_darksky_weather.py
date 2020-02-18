import datetime
import sys
#sys.path.append('./src')

from configparser import ConfigParser
import Weather
import City
import boto3
from utils import locate_config
import os


#############################INPUTS#############################
city_names = ['Napa','Solano']
state_name = 'California'

start_date = datetime.datetime(2015, 1, 1)
end_date = datetime.datetime(2019, 12, 31)

to_omnisci = False


csv_path = './data/darksky_weather.csv'
################################################################


if __name__ == "__main__":

    config_path = locate_config(sys.argv)
    config = ConfigParser()
    config.read(config_path)

    api_key = config.get('Darksky', 'key')



    for c in city_names:
        city = City.City(c, state_name)

        forecast_handle = Weather.Weather(api_key)
        weather_forecast = forecast_handle.get_hourly_weather(city, start_date, end_date, True)
        if not os.path.isfile(
                r'/Users/sravichandran/Desktop/Sindu/Traffic/Data Scraping /output/weather_data_2015/darksky_2015.csv'):
            weather_forecast.to_csv(
                r'/Users/sravichandran/Desktop/Sindu/Traffic/Data Scraping /output/weather_data_2015/darksky_2015.csv',
                index=None, header=True)
        else:
            weather_forecast.to_csv(
                r'/Users/sravichandran/Desktop/Sindu/Traffic/Data Scraping /output/weather_data_2015/darksky_2015.csv',
                mode='a', header=False)

    # Upload to s3
    s3 = boto3.client('s3', aws_access_key_id='',
                      aws_secret_access_key='')
    print("S3 upload for weather data")
    try:
        s3.upload_file(r'/Users/sravichandran/Desktop/Sindu/Traffic/Data Scraping /output/weather_data_2015/darksky_2015.csv', 'traffic--data', '2015_Weather_data.csv')
        print("Upload Successful")
    except FileNotFoundError:
        print("The file was not found")
    print("Exiting Main Thread")
