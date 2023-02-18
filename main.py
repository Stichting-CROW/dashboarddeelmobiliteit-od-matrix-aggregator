import db
import h3
import time
import od_aggregation_periods
import logging
import json
from shapely.geometry import shape
import od_h3
import od_residential
import get_residential_area

logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# This method inserts data for all time periods that are not in the db and should be.
def insert_data(geometries):
    logger.info("insert new aggregation periods")
    missing_time_periods = od_aggregation_periods.get_missing_time_periods()
    for missing_time_period in missing_time_periods:
        start_time_period = missing_time_period[0]
        end_time_period = missing_time_period[1]
        logger.info(str(start_time_period) + " - " + str(end_time_period))

        trips = db.query_trips_per_time_block(start_time_period, end_time_period)
        aggregation_period_id = db.insert_aggregation_period(start_time_period=start_time_period, end_time_period=end_time_period)
        od_h3.process(trips, aggregation_period_id)
        od_residential.process(trips, geometries, aggregation_period_id)
        
    logger.info("finished inserting new aggregation periods.")

# This method makes sure that data is updated after 1, 3, 7 and 30 days.
def update_data(geometries):
    logger.info("recalculate od matrices")
    to_update = db.query_to_update_periods()
    for to_update_time_period in to_update:
        start_time_period = to_update_time_period["start_time_period"]
        end_time_period = to_update_time_period["end_time_period"]
        aggregation_period_id = to_update_time_period["aggregation_period_id"]
        logger.info(str(start_time_period) + " - " + str( end_time_period))

        trips = db.query_trips_per_time_block(start_time_period,  end_time_period)
        od_aggregation_periods.prepare_new_calculation_iteration(aggregation_period_id)
        od_h3.process(trips, aggregation_period_id)
        od_residential.process(trips, geometries, aggregation_period_id)
    logger.info("finish recalculating od matrices")


def get_h3s():
    res = db.get_municipalities()
    for row in res:
        s = shape(json.loads(row["area"]))
        for polygon in list(s.geoms):
            xx, yy = polygon.exterior.coords.xy
            print(h3.polyfill_polygon(list(zip(yy, xx)), 8))

def main():
    start = time.time()
    # get_h3s()
    residential_area = get_residential_area.get_residential_areas()
    insert_data(residential_area)
    update_data(residential_area)
    logger.info("import took {:.2f}s".format(time.time() - start))

main()