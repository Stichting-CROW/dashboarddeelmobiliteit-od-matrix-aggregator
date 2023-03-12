import db
import h3
import json
from shapely.geometry import multipolygon
from shapely.geometry import mapping, shape, Point, multipolygon

def cache_h3_acl():
    if db.check_h3_cache_filled():
        return
    rows = get_h3_cells_per_municipality()
    db.insert_h3_cache_acl(rows=rows)

def get_h3_cells_per_municipality():
    rows = db.get_municipalities()
    insert_rows = []
    for row in rows:
        h3_cells_level_7 = get_h3_cells_for_multipolygon_geojson(row["area"], 7)
        insert_rows.append((row["municipality"], row["name"], 7, h3_cells_level_7))
        h3_cells_level_8 = get_h3_cells_for_multipolygon_geojson(row["area"], 8)
        insert_rows.append((row["municipality"], row["name"], 8, h3_cells_level_8))
    return insert_rows

def get_h3_cells_for_multipolygon_geojson(geojson, h3_level):
    h3_in_area = set()
    multipolygon_object = shape(json.loads(geojson))
    for p in multipolygon_object.geoms:
        cells = h3.polyfill(mapping(p), h3_level, geo_json_conformant=True)
        cells = map(lambda h3_cell_str: h3.h3.string_to_h3(h3_cell_str), cells)
        h3_in_area.update(cells)
    return list(h3_in_area)

    