from shapely.geometry import mapping, shape, Point
from shapely import STRtree
from dataclasses import dataclass
import json
import db

@dataclass
class ResidentialAreas:
    stats_ref: list[int]
    areas: list[shape]
    tree: STRtree

def get_residential_areas():
    res = db.get_residential_areas()
    areas = []
    stats_ref = []
    for row in res:
        areas.append(shape(json.loads(row["area"])))
        stats_ref.append(row["stats_ref"])
    tree = STRtree(areas)
    return ResidentialAreas(
        areas=areas,
        stats_ref=stats_ref,
        tree=tree
    )