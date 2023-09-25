
import logging
from shapely.geometry import Point
import db

logger = logging.getLogger(__name__)

def process(trips, geometries, aggregation_period_id):
    od_matrix = calculate_od_geometry(trips, geometries)
    if len(od_matrix) == 0:
        return
    store_od(od_matrix, aggregation_period_id)

def calculate_od_geometry(trips, geometries):
    od_matrix = {}
    for trip in trips:
        if not trip["end_location_latitude"]:
            continue
        start_ref = get_residential_area_stats_ref(trip["start_location_latitude"], trip["start_location_longitude"], geometries)
        if start_ref == None:
            start_ref = "unknown"
        end_ref = get_residential_area_stats_ref(trip["end_location_latitude"], trip["end_location_longitude"], geometries)
        if end_ref == None:
            end_ref = "unknown"
        
        key = start_ref + "::" + end_ref + "::" + trip["form_factor"]
        if key in od_matrix:
            od_matrix[key] += 1
        else: 
            od_matrix[key] = 1
    return od_matrix

def get_residential_area_stats_ref(latitude, longitude, geometries):
    res = [geometries.stats_ref[idx] for idx in geometries.tree.query(Point(longitude, latitude), predicate="intersects")]
    if len(res) > 1:
        logger.info("More then 1 intersecting point")
    elif len(res) == 0:
        return
    return res[0]

def store_od(od_matrix, aggregation_period_id):
    query = get_od_geometry_insert(aggregation_period_id, od_matrix)
    db.execute(query)

def get_od_geometry_insert(aggregation_period_id, od_matrix):
    insert_values = []

    for key, number_of_trips in od_matrix.items():
        origin_ref, destination_ref, modality = key.split("::")
        insert_values.append(
            f"('{aggregation_period_id}', '{origin_ref}', '{destination_ref}', '{modality}', { number_of_trips})"
        )

    query = f"""INSERT INTO od_geometry
      VALUES {','.join(insert_values)}
    """
    return query

