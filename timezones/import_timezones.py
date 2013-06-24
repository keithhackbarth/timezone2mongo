"""
Function is used to import the timezone shapefile available here:
    http://efele.net/maps/tz/world/

Into mongo with a GeoJSON format.

Requires the Python Shapefile library https://code.google.com/p/pyshp/

"""

import os
import pymongo
import pytz
import shapefile


def import_shapefile():
    """Converts the shapefile into a python object"""

    timezones = []

    shape_reader = shapefile.Reader("world/tz_world")
    shape_records = shape_reader.shapeRecords()

    for shape_record in shape_records:

        # timezone must be valid, don't bother with 'unihabited'
        try:
            timezone = shape_record.record[0]
            pytz.timezone(shape_record.record[0])
        except pytz.UnknownTimeZoneError:
            continue

        # shapes must be in bson format
        # also pymongo requires list() type instead of object
        coordinates = []
        for coordinate in shape_record.shape.points:
            coordinates.append([coordinate[0], coordinate[1]])

        timezones.append({
            'timezone': timezone,
            'coordinates': [coordinates]
        })

    return timezones


def update_mongo(timezones):
    """Writes the shapefile back into python"""

    # get replica set
    try:
        db = pymongo.Connection(
                host='localhost',
                replicaSet = 'rs_main',
            )['main']
    except pymongo.errors.ConfigurationError:
        db = pymongo.Connection()

    # there's no feasible way to upsert
    # so drop the legacy collection (if exists)
    db['timezone'].remove({})

    # import each row
    for timezone in timezones:
        db['timezone'].insert({
            'timezone': timezone['timezone'],
            'shape': {
                'type': 'Polygon',
                'coordinates': timezone['coordinates']
            }
        })

    # ensure the index
    db['timezone'].ensure_index([('shape', '2dsphere')])

if __name__ == "__main__":
    update_mongo(import_shapefile())
