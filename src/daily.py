import csv
import json
import os
import sys
import time
from datetime import datetime, timedelta
from glob import glob

import pandas as pd
from google.cloud import storage
from tqdm import tqdm

STORAGE_BUCKET = 'at-network.alexk.nz'

VEHICLES_FIELDS = [
    'file_time',
    'id',
    'is_deleted',
    'trip_id',
    'route_id',
    'start_time',
    'schedule_relationship',
    'vehicle_id',
    'latitude',
    'longitude',
    'timestamp',
    'occupancy_status'
]

TRIP_UPDATES_FIELDS = [
    'file_time',
    'id',
    'trip_id',
    'route_id',
    'start_time',
    'trip_schedule_relationship',
    'vehicle_id',
    'stop_sequence',
    'stop_id',
    'stop_schedule_relationship',
    'departure_delay',
    'departure_time',
    'timestamp'
]


def get_files_in_day(date):
    """Get a list of all files relevant to a given date"""

    paths = glob('data/realtime_combined_feed_*.json')
    current_paths = []

    for path in paths:
        time_int = int(path.split('_')[-1][:-5])
        file_time = datetime.fromtimestamp(time_int)

        if file_time > date and file_time <= date + timedelta(1):
            current_paths.append(path)
    
    return current_paths


def extract_info(file_path):
    """Extract a list of dictionaries that are actually data points of interest"""
    trip_updates = []
    vehicles = []

    file_time = file_path.split('_')[-1][:-5]
    
    with open(file_path) as response:
        data = json.load(response)

    for entry in data['response']['entity']:
        if type(entry) != dict:
            continue

        if 'vehicle' in entry.keys():
            processed_entry = {
                'file_time': file_time,
                'id': entry.get('id'),
                'is_deleted': entry.get('is_deleted'),
                'trip_id': entry['vehicle']['trip'].get('trip_id'),
                'route_id': entry['vehicle']['trip'].get('route_id'),
                'start_time': entry['vehicle']['trip'].get('start_time'),
                'schedule_relationship': entry['vehicle']['trip'].get('schedule_relationship'),
                'vehicle_id': entry['vehicle']['vehicle'].get('id'),
                'latitude': entry['vehicle']['position'].get('latitude'),
                'longitude': entry['vehicle']['position'].get('longitude'),
                'timestamp': entry['vehicle'].get('timestamp'),
                'occupancy_status': entry['vehicle'].get('occupancy_status')
            }
            vehicles.append(processed_entry)

        elif 'trip_update' in entry.keys():
            # account for departure (sometimes not present)
            departure = entry['trip_update']['stop_time_update'].get('departure')
            if departure:
                departure_delay = departure.get('delay')
                departure_time = departure.get('time')
            else:
                departure_delay = None
                departure_time = None

            processed_entry = {
                'file_time': file_time,
                'id': entry.get('id'),
                'trip_id': entry['trip_update']['trip'].get('trip_id'),
                'route_id': entry['trip_update']['trip'].get('route_id'),
                'start_time': entry['trip_update']['trip'].get('start_time'),
                'trip_schedule_relationship': entry['trip_update']['trip'].get('schedule_relationship'),
                'vehicle_id': entry['trip_update']['vehicle'].get('id'),
                'stop_sequence': entry['trip_update']['stop_time_update'].get('stop_sequence'),
                'stop_id': entry['trip_update']['stop_time_update'].get('stop_id'),
                'stop_schedule_relationship': entry['trip_update']['stop_time_update'].get('schedule_relationship'),
                'departure_delay': departure_delay,
                'departure_time': departure_time,
                'timestamp': entry['trip_update'].get('timestamp')
            }
            trip_updates.append(processed_entry)

    return vehicles, trip_updates


def remove_duplicates(date): 
    """Removed duplicates from processed files"""
    file_date = date.strftime('%Y-%m-%d')

    vehicles_name = 'processed/vehicles_{}.csv'.format(file_date)
    trip_updates_name = 'processed/trip_updates_{}.csv'.format(file_date)

    df = pd.read_csv(vehicles_name, names=VEHICLES_FIELDS)
    df.drop_duplicates(subset=['id']).to_csv(vehicles_name, index=False, header=False)

    df = pd.read_csv(trip_updates_name, names=TRIP_UPDATES_FIELDS)
    df.drop_duplicates(subset=['id']).to_csv(trip_updates_name, index=False, header=False)


def upload_processed_files(date):
    """Upload files to the Google Cloud Storage"""
    file_date = date.strftime('%Y-%m-%d')

    vehicles_name = 'processed/vehicles_{}.csv'.format(file_date)
    trip_updates_name = 'processed/trip_updates_{}.csv'.format(file_date)

    client = storage.Client(project='solar-system')
    bucket = client.bucket(STORAGE_BUCKET)
    assert bucket.exists()

    blob = bucket.blob(vehicles_name.split('/')[-1])
    blob.upload_from_filename(vehicles_name)
    
    blob = bucket.blob(trip_updates_name.split('/')[-1])
    blob.upload_from_filename(trip_updates_name)


def clean_up(date, file_paths):
    """Clean up the old files later"""
    for path in file_paths:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

    file_date = date.strftime('%Y-%m-%d')

    try: 
        os.remove('processed/vehicles_{}.csv'.format(file_date))
    except FileNotFoundError:
        pass

    try:
        os.remove('processed/trip_updates_{}.csv'.format(file_date))
    except FileNotFoundError:
        pass


def secondly_to_daily(date):
    file_paths = get_files_in_day(date)

    suffix = '_{}.csv'.format(date.strftime('%Y-%m-%d'))
    
    with open('processed/vehicles'+suffix, 'w') as vehicles_file, \
         open('processed/trip_updates'+suffix, 'w') as trip_updates_file:
        
        vehicles = csv.DictWriter(vehicles_file, VEHICLES_FIELDS, lineterminator='\n')
        trip_updates = csv.DictWriter(trip_updates_file, TRIP_UPDATES_FIELDS, lineterminator='\n')

        for path in tqdm(file_paths):
            vehicles_data, trip_updates_data = extract_info(path)

            vehicles.writerows(vehicles_data)
            trip_updates.writerows(trip_updates_data)

    remove_duplicates(date)
    upload_processed_files(date)
    clean_up(date, file_paths)


if __name__ == '__main__':
    try:
        date = datetime(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]))
    except IndexError:
        raise Exception('Not enough arguments passed! Should pass year, month and day')
    
    secondly_to_daily(date)
