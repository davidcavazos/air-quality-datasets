from __future__ import annotations

from collections.abc import Iterator
import io
import logging
import random

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.filesystems import FileSystems
import ee
from google.api_core import retry
import google.auth
import numpy as np
import tensorflow as tf

# Default values.
PATCH_SIZE = 128
SCALE = 10  # meters per pixel
TIME_WINDOW_HOURS = 3
MAX_EE_REQUESTS = 20

# Constant values.
QUERY_TEMPLATES = {
    "california": """
        SELECT
            NO2,
            UNIX_SECONDS(TIMESTAMP(Date_Time)) as timestamp,
            Longitude AS longitude,
            Latitude AS latitude,
            ST_GEOHASH(ST_GeogPoint(Longitude, Latitude), 4) AS key,
        FROM `street-view-air-quality.California_20150528_20190607_GoogleAclimaAQ.California_Unified_2015_2019`
        WHERE NO2 IS NOT NULL
            AND EXTRACT(HOUR FROM TIMESTAMP(Date_Time)) - 7 = 10
            AND EXTRACT(YEAR FROM TIMESTAMP(Date_Time)) > 2017
    """,
    "houston": """
        SELECT
            CAST(meas_val AS FLOAT64) AS NO2,
            UNIX_SECONDS(TIMESTAMP(meas_ts_local_tz)) AS timestamp,
            CAST(meas_lon_deg_dec AS FLOAT64) AS longitude,
            CAST(meas_lat_deg_dec AS FLOAT64) AS latitude,
            ST_GEOHASH(ST_GeogPoint(meas_lon_deg_dec, meas_lat_deg_dec), 4) AS key,
        FROM `street-view-air-quality-dev.AQDC_tables.houston_mobile`
        WHERE measrnd_nm = 'NO2'
            AND meas_val IS NOT NULL
            AND EXTRACT(HOUR FROM TIMESTAMP(meas_ts_local_tz)) - 5 = 10
            AND EXTRACT(YEAR FROM TIMESTAMP(meas_ts_local_tz)) > 2017
    """,
    "amsterdam": """
        SELECT
            CalNO2 as NO2,
            UNIX_SECONDS(TIMESTAMP(DateTime)) as timestamp,
            LON as longitude,
            LAT as latitude,
            ST_GEOHASH(ST_GeogPoint(LON, LAT), 4) as key,
        FROM `street-view-air-quality-uu.Validated_Data.AMS_201905_202008`
        WHERE CalNO2 IS NOT NULL
            AND EXTRACT(HOUR FROM TIMESTAMP(DateTime)) + 2 = 10
            AND EXTRACT(YEAR FROM TIMESTAMP(DateTime)) > 2017
    """,
    "copenhagen": """
        SELECT
            CalNO2 as NO2,
            UNIX_SECONDS(TIMESTAMP(Datetimee)) as timestamp,
            LON as longitude,
            LAT as latitude,
            ST_GEOHASH(ST_GeogPoint(LON, LAT), 4) as key,
        FROM `street-view-air-quality-uu.Validated_Data.CPH_201811_202008`
        WHERE CalNO2 IS NOT NULL
            AND EXTRACT(HOUR FROM TIMESTAMP(Datetimee)) + 2 = 10
            AND EXTRACT(YEAR FROM TIMESTAMP(Datetimee)) > 2017
    """,
}

GFS_BAND_SCALES = {
    "temperature_2m_above_ground": [-69.18, 52.25],
    "specific_humidity_2m_above_ground": [0, 0.03],
    "relative_humidity_2m_above_ground": [1, 100],
    "u_component_of_wind_10m_above_ground": [-60, 60],
    "v_component_of_wind_10m_above_ground": [-60, 60],
    "precipitable_water_entire_atmosphere": [0, 100],
}


# Sample points proportional to size.  See:
# https://code.earthengine.google.com/21951376a7d27a7315b1809b0e90e6af
def sample_points(
    rows: Iterator[dict],
    threshold: float = 100,
    offset: float = 1,
) -> Iterator[tuple[list[float], list[dict]]]:
    """Poisson sampling using the threshold and offset, both in ppb."""
    points = list(rows)
    for point in points:
        # The purpose of the offset is to give zeros a non-zero probability of
        # getting in the sample.  Note that all values greater than the
        # threshold get in the sample with probability 1.
        weight = (point["NO2"] + offset) / threshold
        if random.random() < weight:
            coords = [point["longitude"], point["latitude"]]
            yield (coords, points)


class GetPatch(beam.DoFn):
    def __init__(self, patch_size: int = PATCH_SIZE, scale: int = SCALE) -> None:
        self.patch_size = patch_size
        self.scale = scale

    def setup(self) -> None:
        credentials, project = google.auth.default(
            scopes=[
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/earthengine",
            ]
        )
        # Use the Earth Engine High Volume endpoint.
        #   https://developers.google.com/earth-engine/cloud/highvolume
        ee.Initialize(
            credentials.with_quota_project(None),
            project=project,
            opt_url="https://earthengine-highvolume.googleapis.com",
        )

    def process(self, request: tuple[list[float], list[dict]]) -> Iterator[np.ndarray]:
        (coords, points) = request
        image = get_image(points)
        yield download_patch(coords, image, self.scale, self.patch_size)


def get_image(points: list[dict]) -> ee.Image:
    millis = sum(p["timestamp"] for p in points) / len(points) * 1000
    xs = [p["longitude"] for p in points]
    ys = [p["latitude"] for p in points]
    roi = ee.Geometry.Rectangle(min(xs), min(ys), max(xs), max(ys))
    return ee.Image.cat(
        get_no2_image(points),
        get_s2_image(millis),
        # get_s5p_image(millis),
        get_gfs_image(millis),
        get_roads_image(roi.buffer(1000)),
    )


def get_no2_image(points: list[dict]) -> ee.Image:
    def to_feature(row: dict) -> ee.Feature:
        point = ee.Geometry.Point([row["longitude"], row["latitude"]])
        return ee.Feature(point, {"NO2": row["NO2"]})

    return (
        ee.FeatureCollection([to_feature(point) for point in points])
        .reduceToImage(["NO2"], ee.Reducer.mean())
        .float()
        .unmask(-9999)
    )


def get_s2_image(millis: float) -> ee.Image:
    def mask_sentinel2_clouds(image: ee.Image) -> ee.Image:
        CLOUD_BIT = 10
        CIRRUS_CLOUD_BIT = 11
        bit_mask = (1 << CLOUD_BIT) | (1 << CIRRUS_CLOUD_BIT)
        mask = image.select("QA60").bitwiseAnd(bit_mask).eq(0)
        return image.updateMask(mask)

    date = ee.Date(millis)
    start = date.advance(-2, "month")
    end = date.advance(2, "month")

    return (
        ee.ImageCollection("COPERNICUS/S2_HARMONIZED")
        .filterDate(start, end)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 20))
        .map(mask_sentinel2_clouds)
        .select("B.*")
        .median()
        .divide(10000)
        .unmask(0)
        .float()
    )


# Not used.
def get_s5p_image(millis: float) -> ee.Image:
    s5p = ee.ImageCollection("COPERNICUS/S5P/OFFL/L3_NO2")
    n02Band = "tropospheric_NO2_column_number_density"
    cloudBand = "cloud_fraction"

    date = ee.Date(millis)
    start = date.advance(-1, "day")
    end = date.advance(1, "day")
    subset = s5p.select([n02Band, cloudBand]).filterDate(start, end)
    subset = subset.map(
        lambda image: image.select(n02Band)
        .updateMask(image.select(cloudBand).lte(30))
        .resample("bilinear")
    )

    return subset.mean().rename("no2").float()


def get_gfs_image(millis: float) -> ee.Image:
    bands_scales = ee.Dictionary(GFS_BAND_SCALES)
    date = ee.Date(millis)
    start = date.advance(-1.5, "hour")
    end = date.advance(4.5, "hour")

    gfs = ee.ImageCollection("NOAA/GFS0P25")
    gfs = gfs.select(bands_scales.keys())
    gfs = gfs.filter(ee.Filter.eq("forecast_hours", 0))
    gfs = gfs.map(lambda image: image.resample("bilinear"))
    image = (
        gfs.filterDate(start, end)
        .mosaic()
        .addBands(ee.Image([0, 0, 0, 0, 0, 0]).rename(list(GFS_BAND_SCALES.keys())))
    )

    def unit_scale_gfs(band):
        scale = ee.List(bands_scales.get(band))
        return image.select([band]).unitScale(scale.get(0), scale.get(1))

    images = bands_scales.keys().map(unit_scale_gfs)
    image = ee.ImageCollection(images).toBands().rename(bands_scales.keys())
    return image


# https://www.census.gov/library/reference/code-lists/route-type-codes.html
# https://code.earthengine.google.com/4c99ff0116e69fb3f8bf796d301ed4e5
def get_roads_image(roi: ee.Geometry) -> ee.Image:
    roads = ee.FeatureCollection("TIGER/2016/Roads")
    types_dict = ee.Dictionary({"C": 0, "I": 0, "M": 0, "O": 0, "S": 0, "U": 0})

    roads = (
        roads.filterBounds(roi)
        .filter(ee.Filter.And(ee.Filter.neq("rttyp", ""), ee.Filter.neq("rttyp", None)))
        .map(lambda f: f.set(types_dict.set(f.get("rttyp"), 1)))
    )

    return roads.reduceToImage(
        **{
            "properties": types_dict.keys(),
            "reducer": ee.Reducer.sum().forEach(types_dict.keys()),
        }
    ).unmask(0)


@retry.Retry()
def download_patch(
    coords: list[float],
    image: ee.Image,
    scale: int,
    patch_size: int,
) -> np.ndarray:
    """Get a training patch centered on the coordinates."""
    # Make a projection to discover the scale in degrees.
    # NOTE: Pass this in so it doesn't get computed every time.
    proj = ee.Projection("EPSG:4326").atScale(scale).getInfo()

    # Get scales out of the transform.
    [lon, lat] = coords
    scale_x = proj["transform"][0]
    scale_y = -proj["transform"][4]
    offset_x = -scale_x * (patch_size + 1) / 2
    offset_y = -scale_y * patch_size / 2

    # Make a request object.
    request = {
        "expression": image,
        "fileFormat": "NPY",
        "grid": {
            "dimensions": {"width": patch_size, "height": patch_size},
            "crsCode": proj["crs"],
            "affineTransform": {
                "scaleX": scale_x,
                "scaleY": scale_y,
                "shearX": 0,
                "shearY": 0,
                "translateX": lon + offset_x,
                "translateY": lat + offset_y,
            },
        },
    }
    return np.load(io.BytesIO(ee.data.computePixels(request)))


def serialize(patch: np.ndarray) -> bytes:
    features = {
        name: tf.train.Feature(
            float_list=tf.train.FloatList(value=patch[name].flatten())
        )
        for name in patch.dtype.names
    }
    example = tf.train.Example(features=tf.train.Features(feature=features))
    return example.SerializeToString()


def run(
    place: str,
    data_path: str,
    patch_size: int = PATCH_SIZE,
    scale: int = SCALE,
    time_window_hours: float = TIME_WINDOW_HOURS,
    max_ee_requests: int = MAX_EE_REQUESTS,
    max_rows: int | None = None,
    beam_options: PipelineOptions | None = None,
) -> None:
    query = QUERY_TEMPLATES[place]
    if max_rows is not None:
        query += f" LIMIT {max_rows}"

    with beam.Pipeline(options=beam_options) as pipeline:
        time_window = time_window_hours * 60 * 60  # seconds
        _ = (
            pipeline
            | f"Read BQ {place}"
            >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
            | "Bucket by time and space"
            >> beam.GroupBy(
                lambda row: (row["key"], int(row["timestamp"] / time_window))
            )
            | "Drop the keys" >> beam.MapTuple(lambda _, rows: rows)
            | "Get points" >> beam.FlatMap(sample_points)
            | f"Throttle to {max_ee_requests}"
            >> beam.Reshuffle(num_buckets=max_ee_requests)
            | "Get patch" >> beam.ParDo(GetPatch(patch_size, scale))
            | "Unthrottle" >> beam.Reshuffle()
            | "Serialize to TFRecord" >> beam.Map(serialize)
            | "Write TFRecords"
            >> beam.io.tfrecordio.WriteToTFRecord(
                file_path_prefix=FileSystems.join(data_path, place, "part"),
                file_name_suffix=".tfrecord.gz",
            )
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("place", choices=QUERY_TEMPLATES.keys())
    parser.add_argument("data_path")
    parser.add_argument("--patch-size", type=int, default=PATCH_SIZE)
    parser.add_argument("--scale", type=int, default=SCALE)
    parser.add_argument("--time-window-hours", type=float, default=TIME_WINDOW_HOURS)
    parser.add_argument("--max-ee-requests", type=int, default=MAX_EE_REQUESTS)
    parser.add_argument("--max-rows", type=int, default=None)
    args, beam_args = parser.parse_known_args()

    beam_options = PipelineOptions(
        beam_args,
        pickle_library="cloudpickle",
        direct_num_workers=args.max_ee_requests,
        direct_running_mode="multi_threading",
        requirements_file="requirements.txt",
        requirements_cache="skip",
    )

    logging.getLogger().setLevel(logging.INFO)
    run(
        place=args.place,
        data_path=args.data_path,
        patch_size=args.patch_size,
        scale=args.scale,
        time_window_hours=args.time_window_hours,
        max_ee_requests=args.max_ee_requests,
        max_rows=args.max_rows,
        beam_options=beam_options,
    )
