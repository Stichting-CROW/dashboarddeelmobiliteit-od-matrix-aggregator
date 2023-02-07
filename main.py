import db
import h3
import time
import missing_data
import logging

logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

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

def store_od_h3(start_time_period, end_time_period, od_matrix, aggregation_period_id = None):
    if aggregation_period_id == None:
        aggregation_period_id = db.insert_aggregation_period(start_time_period=start_time_period, end_time_period=end_time_period)
    else:
        db.new_calculation_iteration_aggregation_period(aggregation_period_id)
        db.delete_od_aggregation_period(aggregation_period_id)

    query = get_od_h3_prepare_insert(aggregation_period_id, od_matrix)
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

    query = f"""INSERT INTO od_h3
      VALUES {','.join(insert_values)}
    """
    return query

# This method inserts data for all time periods that are not in the db and should be.
def insert_data():
    logger.info("insert new aggregation periods")
    missing_time_periods = missing_data.get_missing_time_periods()
    for missing_time_period in missing_time_periods:
        start_timestamp = missing_time_period[0]
        end_timestamp = missing_time_period[1]
        logger.info(str(start_timestamp) + " - " + str(end_timestamp))

        trips = db.query_trips_per_time_block(start_timestamp, end_timestamp)
        od_matrix = {**calculate_od_h3(trips, 7), **calculate_od_h3(trips, 8)}
        store_od_h3(start_timestamp, end_timestamp, od_matrix)
        
    logger.info("finished inserting new aggregation periods.")

# This method makes sure that data is updated after 1, 3, 7 and 30 days.
def update_data():
    logger.info("recalculate od matrices")
    to_update = db.query_to_update_periods()
    for to_update_time_period in to_update:
        start_timestamp = to_update_time_period["start_time_period"]
        end_timestamp = to_update_time_period["end_time_period"]
        logger.info(str(start_timestamp) + " - " + str(end_timestamp))

        trips = db.query_trips_per_time_block(start_timestamp, end_timestamp)
        od_matrix = {**calculate_od_h3(trips, 7), **calculate_od_h3(trips, 8)}
        store_od_h3(start_timestamp, end_timestamp, od_matrix, to_update_time_period["aggregation_period_id"])
    logger.info("finish recalculating od matrices")

def main():
    start = time.time()
    insert_data()
    update_data()
    logger.info("import took {:.2f}s".format(time.time() - start))

main()