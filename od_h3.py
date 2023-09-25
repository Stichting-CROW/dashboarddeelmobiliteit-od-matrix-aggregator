import db
import h3

def process(trips, aggregation_period_id):
    od_matrix = {**calculate_od_h3(trips, 7), **calculate_od_h3(trips, 8)}
    if len(od_matrix) == 0:
        return
    store_od_h3(od_matrix, aggregation_period_id)

def calculate_od_h3(trips, h3_level):
    od_matrix = {}
    for trip in trips:
        if not trip["end_location_latitude"]:
            continue
        start_h3_index = h3.geo_to_h3(trip["start_location_latitude"], trip["start_location_longitude"], h3_level)
        end_h3_index = h3.geo_to_h3(trip["end_location_latitude"], trip["end_location_longitude"], h3_level)
        key = start_h3_index + ":" + end_h3_index + ":" + str(h3_level) + ":" + trip["form_factor"]
        if key in od_matrix:
            od_matrix[key] += 1
        else: 
            od_matrix[key] = 1
    return od_matrix

def store_od_h3(od_matrix, aggregation_period_id):
    query = get_od_h3_prepare_insert(aggregation_period_id, od_matrix)
    if query:
        db.execute(query)

def get_od_h3_prepare_insert(aggregation_period_id, od_matrix):
    insert_values = []

    for key, number_of_trips in od_matrix.items():
        origin_cell_str, destination_cell_str, h3_level, modality = key.split(":")

        origin_cell = h3.h3.string_to_h3(origin_cell_str)
        destination_cell = h3.h3.string_to_h3(destination_cell_str)
        insert_values.append(
            f"('{aggregation_period_id}', {origin_cell}, {destination_cell}, '{h3_level}', '{modality}', { number_of_trips})"
        )

    if len(insert_values) == 0:
        return None
    query = f"""INSERT INTO od_h3
      VALUES {','.join(insert_values)}
    """
    return query