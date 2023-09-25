import db
import h3
import json
from shapely.geometry import mapping, shape, Point, multipolygon
import shapely

def cache_h3_acl():
    if db.check_h3_cache_filled():
        return
    rows = get_h3_cells_per_municipality()
    db.insert_h3_cache_acl(rows=rows)

def get_h3_cells_per_municipality():
    rows = db.get_municipalities()
    insert_rows = []
    for row in rows:
        h3_cells_level_7 = get_h3_cells(row["area"], 7)
        insert_rows.append((row["municipality"], row["name"], 7, h3_cells_level_7))
        h3_cells_level_8 = get_h3_cells(row["area"], 8)
        insert_rows.append((row["municipality"], row["name"], 8, h3_cells_level_8))
    return insert_rows

def get_h3_cells(geojson, h3_level):
    h3_in_area = set()
    geo_object = shape(json.loads(geojson))
    if geo_object.geom_type == 'MultiPolygon':	    
        for p in geo_object.geoms:
            h3_in_area.update(get_cells_for_polygon(p, h3_level))
    elif geo_object.geom_type == 'Polygon':
        h3_in_area.update(get_cells_for_polygon(geo_object, h3_level))
    return list(h3_in_area)

def get_cells_for_polygon(polygon, h3_level):
    polygon_2d = shapely.transform(polygon, lambda coord: coord)
    geojson_dict = shapely.geometry.mapping(polygon_2d)
    cells = h3.polyfill(geojson_dict, h3_level, geo_json_conformant=True)
    cells = map(lambda h3_cell_str: h3.h3.string_to_h3(h3_cell_str), cells)
    return cells
