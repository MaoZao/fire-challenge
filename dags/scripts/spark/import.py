import csv
import datetime
import os
from io import StringIO

import requests

current_date = datetime.datetime.now().strftime("%Y-%m-%d")


def fetch_data(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.text
    else:
        raise Exception(
            (
                f"Failed to fetch data from API. "
                f"Status code: {response.status_code}"
            )
        )


def save_to_csv(csv_data, csv_filename, columns_to_exclude):
    csv_file = StringIO(csv_data)

    # Ensure the directory exists
    os.makedirs(os.path.dirname(csv_filename), exist_ok=True)

    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csv_file)
        header = next(reader, None)
        filtered_header = [
            col for col in header if col not in columns_to_exclude
        ]
        filtered_data = [
            [
                row[i]
                for i in range(len(row))
                if header[i] not in columns_to_exclude
            ]
            for row in reader
        ]
        writer = csv.writer(csvfile)
        writer.writerow(filtered_header)
        writer.writerows(filtered_data)


if __name__ == "__main__":
    api_url = (
        "https://data.sfgov.org/resource/wr8u-xric.csv?"
        "$order=incident_date DESC&$limit=2000"
    )
    # csv_filename = './data/Fire_SF.csv'
    csv_filename = '/opt/airflow/data/Fire_SF.csv'
    columns_to_exclude = [
        'exposure_number',
        'estimated_property_loss',
        'box',
        'estimated_contents_loss',
        'mutual_aid',
        'action_taken_secondary',
        'action_taken_other',
        'detector_alerted_occupants',
        'ignition_factor_secondary',
        'structure_type',
        'structure_status',
        'fire_spread',
        'no_flame_spead',
        'number_of_floors_with_minimum_damage',
        'number_of_floors_with_heavy_damage',
        'number_of_floors_with_extreme_damage',
        'detectors_present',
        'detector_type',
        'detector_operation',
        'detector_failure_reason',
        'automatic_extinguishing_system_present',
        'automatic_extinguishing_sytem_type',
        'automatic_extinguishing_sytem_perfomance',
        'automatic_extinguishing_sytem_failure_reason',
        'supervisor_district',
        'number_of_sprinkler_heads_operating',
        'ignition_cause',
        'heat_source',
        'item_first_ignited',
        'number_of_floors_with_significant_damage',
        'detector_effectiveness',
        'floor_of_fire_origin',
        'first_unit_on_scene',
        'other_units',
        'other_personnel',
        'station_area',
    ]
    csv_data = fetch_data(api_url)
    save_to_csv(csv_data, csv_filename, columns_to_exclude)
    print(f"CSV data has been successfully saved to {csv_filename}")
