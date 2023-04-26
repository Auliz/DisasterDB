import os
import glob
import sqlite3
import pandas as pd
import geopandas as gpd

# ---Merge SHAPE with CROP---
shapefile = gpd.read_file('data/cb_2018_us_county_20m.shp')
crop_dec = pd.read_excel('data/crop-year-2014-disaster-declarations-1.xls')
crop_dec = crop_dec.rename(columns = {
                          'FIPS': 'fips'
                          ,'Designation Number': 'designation number'
                          ,'DROUGHT': 'drought'
                          ,'FLOOD, Flash flooding': 'flash flooding'
                          ,'Excessive rain, moisture, humidity': 'rain'
                          ,'Severe Storms, thunderstorms': 'severe storms'
                          ,'Ground Saturation\nStanding Water': 'waterlogged'
                          ,'Hail': 'hail'
                          ,'Wind, High Winds': 'high wind'
                          ,'Fire, Wildfire': 'fire'
                          ,'Heat, Excessive heat\nHigh temp. (incl. low humidity)': 'heatwave'
                          ,'Winter Storms, Ice Storms, Snow, Blizzard': 'snow'
                          ,'Frost, FREEZE': 'frost'
                          ,'Hurricanes, Typhoons, Tropical Storms': 'hurricane'
                          ,'Tornadoes': 'tornado'
                          ,'Volcano': 'volcano'
                          ,'Mudslides, Debris Flows, Landslides': 'landslide'
                          ,'Heavy Surf': 'heavy surf'
                          ,'Ice Jams': 'ice jam'
                          ,'Insects': 'insects'
                          ,'Tidal Surges': 'tidal surge'
                          ,'Cold, wet weather': 'cold and wet'
                          ,'Cool/Cold, Below-normal Temperatures': 'cold'
                          ,'Lightning': 'lightning'
                          ,'Disease': 'disease'})

shapefile['GEOID'] = shapefile['GEOID'].astype(str)
crop_dec['fips'] = crop_dec['fips'].astype(str)
crop_dec['fips'] = [x if len(x) == 5 else "0"+x for x in crop_dec['fips']]

shapeJoin = crop_dec.merge(shapefile, right_on = 'GEOID', left_on = 'fips')

# ---BEGIN DATABASE---
  # Clean up data a little by forcing the column names to lower case and strip whitespace
shapeJoin.columns = shapeJoin.columns.str.lower()
shapeJoin.columns = shapeJoin.columns.str.strip()
shapeJoin['begin date'] = pd.to_datetime(shapeJoin['begin date']).astype(str)

  # Create a database connection
connection = sqlite3.connect('disaster_database.sqlite')

# Set the chunk size for reading in the CSVs
chunksize = 1000

# Loop through all CSVs in the data folder
for filename in glob.glob('data/*.csv'):
    # Read in the CSV in chunks
    for i, chunk in enumerate(pd.read_csv(filename, chunksize=chunksize)):
        # Clean up column names by removing whitespace and converting to lowercase
        chunk.columns = chunk.columns.str.lower().str.replace(' ', '_')
        try:
            # Write the chunk to the database
            # chunk.to_sql('disasters', connection, if_exists='append', index=False)
            # chunk.to_sql(filename[:-4], connection, if_exists='append', index=False)
            chunk.to_sql(os.path.basename(filename)[:-4], connection, if_exists='append', index=False)
        except Exception as e:
            print(f"Error inserting chunk {i} of {filename}: {e}")

# Close the database connection
connection.close()
