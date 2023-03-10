from db_helper import db_helper
from datetime import timedelta
from psycopg2.extras import execute_values

def query_all_aggregation_periods_since(timestamp):
    stmt = """
        SELECT aggregation_period_id, start_time_period, end_time_period
        FROM od_aggregation_period
        WHERE start_time_period >= %(start_period)s
        ORDER BY start_time_period;
    """
    with db_helper.get_resource() as (cur, conn):
        try:
            cur.execute(stmt, {"start_period": timestamp})
            return cur.fetchall()
        except Exception as e:
            conn.rollback()
            print(e)

def query_to_update_periods():
    stmt = """SELECT aggregation_period_id, start_time_period, end_time_period 
    FROM od_aggregation_period 
    WHERE (NOW() - updated_at > '1 DAY' and updated_at - start_time_period < '1 DAY')
    OR (NOW() - updated_at > '3 DAYS' and updated_at - start_time_period < '3 DAYS')
    OR (NOW() - updated_at > '1 WEEK' and updated_at - start_time_period < '1 WEEK')
    OR (NOW() - updated_at > '30 DAYS' and updated_at - start_time_period < '30 DAYS');
    """
    with db_helper.get_resource() as (cur, conn):
        try:
            cur.execute(stmt)
            return cur.fetchall()
        except Exception as e:
            conn.rollback()
            print(e)

def query_trips_per_time_block(start_time_period, end_time_period):
    stmt = """
        SELECT trips.system_id, 
        ST_Y(trips.start_location) as start_location_latitude,
        ST_X(trips.start_location) as start_location_longitude,
        ST_Y(trips.end_location) as end_location_latitude,
        ST_X(trips.end_location) as end_location_longitude,
        form_factor 
        FROM trips
        JOIN vehicle_type 
        USING (vehicle_type_id) 
        WHERE start_time >= %(start_period)s  
        AND start_time <= %(end_period)s ;
    """
    with db_helper.get_resource() as (cur, conn):
        try:
            cur.execute(stmt, {"start_period": start_time_period, "end_period": end_time_period})
            return cur.fetchall()
        except Exception as e:
            conn.rollback()
            print(e)

def check_if_aggregation_period_exists(start_time_period, end_time_period):
    stmt = """
        SELECT aggregation_period_id
        FROM od_aggregation_period
        WHERE start_time_period = %s
        AND end_time_period = %s
    """
    with db_helper.get_resource() as (cur, conn):
        try:
            cur.execute(stmt, (start_time_period, end_time_period))
            result = cur.fetchone()
            if result != None:
                result["aggregation_period_id"]
            return None
        except Exception as e:
            conn.rollback()
            print(e)

def insert_aggregation_period(start_time_period, end_time_period):
    stmt = """
        INSERT INTO od_aggregation_period
        (start_time_period, end_time_period, calculation_iteration, created_at, updated_at)
        VALUES (%s, %s, 1, NOW(), NOW())
        RETURNING aggregation_period_id
    """
    with db_helper.get_resource() as (cur, conn):
        try:
            cur.execute(stmt, (start_time_period, end_time_period))
            result = cur.fetchone()
            conn.commit()
            return result["aggregation_period_id"]
        except Exception as e:
            conn.rollback()
            print(e)

def new_calculation_iteration_aggregation_period(aggregation_period_id):
    stmt = """
        UPDATE od_aggregation_period
        SET calculation_iteration = calculation_iteration + 1,
        updated_at = NOW()
        WHERE aggregation_period_id = %s 
    """
    with db_helper.get_resource() as (cur, conn):
        try:
            cur.execute(stmt, (aggregation_period_id,))
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(e)

def delete_od_h3_aggregation_period(aggregation_period_id):
    stmt = """
        DELETE 
        FROM od_h3
        WHERE aggregation_period_id = %s
    """
    with db_helper.get_resource() as (cur, conn):
        try:
            cur.execute(stmt, (aggregation_period_id,))
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(e)

def delete_od_geometry_aggregation_period(aggregation_period_id):
    stmt = """
        DELETE 
        FROM od_geometry
        WHERE aggregation_period_id = %s
    """
    with db_helper.get_resource() as (cur, conn):
        try:
            cur.execute(stmt, (aggregation_period_id,))
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(e)

def get_municipalities():
    stmt = """
        SELECT name, st_asgeojson(area) as area, municipality
        FROM zones 
        WHERE zone_type = 'municipality';
    """
    with db_helper.get_resource() as (cur, conn):
        try:
            cur.execute(stmt)
            result = cur.fetchall()
            conn.commit()
            return result
        except Exception as e:
            conn.rollback()
            print(e)

def get_residential_areas():
    stmt = """
        SELECT name, st_asgeojson(area) as area, stats_ref
        FROM zones 
        WHERE zone_type = 'residential_area';
    """
    with db_helper.get_resource() as (cur, conn):
        try:
            cur.execute(stmt)
            result = cur.fetchall()
            conn.commit()
            return result
        except Exception as e:
            conn.rollback()
            print(e)

def check_h3_cache_filled():
    stmt = """
        SELECT count(*) != 0 as is_filled 
        FROM od_h3_acl;
    """
    with db_helper.get_resource() as (cur, conn):
        try:
            cur.execute(stmt) 
            return cur.fetchone()["is_filled"]
        except Exception as e:
            conn.rollback()
            print(e)
            return False

def insert_h3_cache_acl(rows):
    stmt = """
    INSERT INTO od_h3_acl
    (municipality_code, municipality_name, h3_level, cells)
    VALUES
    %s
    """
    with db_helper.get_resource() as (cur, conn):
        try:
            execute_values (
                cur, stmt, rows, "(%s, %s, %s, %s)", page_size=100
            )    
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(e)

def is_admin(user_id):
    stmt = """
    INSERT INTO od_h3_acl
    (municipality_code, municipality_name, h3_level, cells)
    VALUES
    %s
    """
    with db_helper.get_resource() as (cur, conn):
        try:
            execute_values (
                cur, stmt, rows, "(%s, %s, %s, %s)", page_size=100
            )    
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(e)

def insert_h3_cache_acl(rows):
    stmt = """
    INSERT INTO od_h3_acl
    (municipality_code, municipality_name, h3_level, cells)
    VALUES
    %s
    """
    with db_helper.get_resource() as (cur, conn):
        try:
            execute_values (
                cur, stmt, rows, "(%s, %s, %s, %s)", page_size=100
            )    
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(e)
    

def execute(stmt):
    with db_helper.get_resource() as (cur, conn):
        try:
            cur.execute(stmt)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(e)

