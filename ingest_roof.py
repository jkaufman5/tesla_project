import os
import json
import logging
from typing import Dict, List, NoReturn
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame, functions as F


def get_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.master("local")
        .appName("tesla_roof_ingestion")
        .getOrCreate()
    )

    return spark


def fix_string_nulls(df: DataFrame) -> DataFrame:
    """
    Replace potential str representations of NULL with actual NULL
    for all string columns
    """
    for column_name, data_type in df.dtypes:
        if data_type == "string":
            df = df.withColumn(
                column_name,
                F.when(
                    condition=F.lower(F.col(column_name)).isin(["none", "null"]),
                    value=None,
                ).otherwise(F.col(column_name)),
            )
    return df


def fix_json_files(base_dir: str, logger: logging.Logger) -> List[str]:
    """
    Iterate and fix JSON files, as needed, to be in the expected/consistent structure.
    In a real-world scenario, this should be preferably fixed upstream with the process that produces the JSON.

    Return a list of json file names for those files to be read in
    """
    roof_json_data_files = list()

    for roof_data_file_name in os.listdir(base_dir):
        if (
            roof_data_file_name.endswith(".json")
            and "_fixed" not in roof_data_file_name
        ):
            # For a real-world scenario, I would fail the job with a helpful message and notify the upstream team.
            # However, For the purposes of this project, we will skip the bad file and print a warning message
            try:
                json_path = f"{base_dir}/{roof_data_file_name}"
                json_dict = json.load(open(json_path))
                json_str = json.dumps(json_dict)
            except ValueError as v:
                logger.info(
                    "The file, %s, contains malformed JSON; skipping parsing this file."
                    % roof_data_file_name
                )
                logger.info(v)
                continue

            save_modified_copy = False

            if '"mountingPlaness":' in json_str:
                json_str = json_str.replace('"mountingPlaness":', '"mountingPlanes":')
                save_modified_copy = True

            if '"pitchAngle": "18.39688505962881"' in json_str:
                json_str = json_str.replace(
                    '"pitchAngle": "18.39688505962881"',
                    '"pitchAngle": 18.39688505962881',
                )
                save_modified_copy = True

            if '"azimuthAngle": "357.0"' in json_str:
                json_str = json_str.replace(
                    '"azimuthAngle": "357.0"', '"azimuthAngle": 357.0'
                )
                save_modified_copy = True

            if '"id": "33"' in json_str:
                json_str = json_str.replace('"id": "33"', '"id": 33')
                save_modified_copy = True

            if save_modified_copy:
                new_file_name = f"{roof_data_file_name.split('.')[0]}_fixed.json"

                with open(f"{base_dir}/{new_file_name}", "w") as new_file:
                    json.dump(json.loads(json_str), new_file)

                roof_json_data_files.append(new_file_name)
            else:
                roof_json_data_files.append(roof_data_file_name)


def read_and_parse_json_data(
    spark: SparkSession, base_dir: str, json_files: List[str], logger: logging.Logger
) -> Dict[str, DataFrame]:
    """
    Iterate to read in each JSON file,
    parse/split into separate Spark dataframes,
    and return as a dictionary

    E.g.,
    {
        "table_name1":
        df1
    }
    """
    # In manual review, roof_8.json is a good candidate to determine the expected JSON structure
    roof_data_df = (
        spark.read.json(f"{base_dir}/roof_8.json")
        .limit(0)
        .withColumn("source_file_name", F.lit(""))
    )

    # Iterate to read each JSON file a Spark dataframe and union together
    for iteration, roof_data_file_name in enumerate(iterable=json_files, start=1):
        json_path = f"{base_dir}/{roof_data_file_name}"
        logger.info("%s: Preparing to read %s" % (iteration, roof_data_file_name))

        json_file_df = spark.read.json(
            path=json_path, schema=roof_data_df.schema
        ).withColumn("source_file_name", F.lit(roof_data_file_name))

        # Union/append base df
        roof_data_df = roof_data_df.union(json_file_df)

    # 9 records from 9 non-malformed JSON files
    roof_data_df.cache()

    # Sample output:
    # roof_data_df.sort("id").show(100, False)
    # +--------------------+-------------------------+---+--------------+--------------------+-------+-----------------+
    # |         dateCreated|externalSiteModelSourceId| id|installationId|           siteModel|version| source_file_name|
    # +--------------------+-------------------------+---+--------------+--------------------+-------+-----------------+
    # |2022-10-13T18:52:10Z|                        0|  1|             1|{[{true, [{801.11...|     v3|      roof_1.json|
    # |2022-03-22T17:21:45Z|                        0|  2|             2|{[{true, [{2.4671...|     v3|roof_2_fixed.json|
    # |2021-11-04T20:43:37Z|                        0|  3|             3|{[{true, [{693.04...|     v3|      roof_3.json|
    # |2021-06-08T14:19:06Z|                        0|  4|             4|{[{true, [{407.82...|     v2|      roof_4.json|
    # |2021-08-19T22:06:55Z|                        0|  5|             5|{[{true, [{36.080...|     v3|      roof_5.json|
    # |2021-10-18T14:57:46Z|                        0|  6|             6|{[{true, [{209.93...|     v3|roof_6_fixed.json|
    # |2022-02-12T02:45:51Z|                        0|  7|             7|{[{false, [{99.94...|     v3|      roof_7.json|
    # |2022-11-23T18:09:02Z|                        0|  8|             8|{[{true, [{349.71...|     v3|      roof_8.json|
    # |2021-01-25T04:46:45Z|                        1| 10|            10|{[{true, [{17.420...|     v1|     roof_10.json|
    # +--------------------+-------------------------+---+--------------+--------------------+-------+-----------------+

    roof_df = (
        roof_data_df.withColumnRenamed("id", "roof_id")
        .withColumn("site_model_heading_vector_x", F.col("siteModel.headingVector.x"))
        .withColumn("site_model_heading_vector_y", F.col("siteModel.headingVector.y"))
        .withColumn("site_model_heading_vector_z", F.col("siteModel.headingVector.z"))
        .withColumn("site_model_north_vector_x", F.col("siteModel.northVector.x"))
        .withColumn("site_model_north_vector_y", F.col("siteModel.northVector.y"))
        .withColumn("site_model_north_vector_z", F.col("siteModel.northVector.z"))
        .withColumn("site_model_unit_angle", F.col("siteModel.units.angle"))
        .withColumn("site_model_unit_area", F.col("siteModel.units.area"))
        .withColumn("site_model_unit_length", F.col("siteModel.units.length"))
        .select(
            "roof_id",  # PK
            "externalSiteModelSourceId",
            "installationId",
            "version",
            "dateCreated",
            "site_model_heading_vector_x",
            "site_model_heading_vector_y",
            "site_model_heading_vector_z",
            "site_model_north_vector_x",
            "site_model_north_vector_y",
            "site_model_north_vector_z",
            "site_model_unit_angle",
            "site_model_unit_area",
            "site_model_unit_length",
        )
    )

    building_mounting_plane_df = (
        roof_data_df.withColumnRenamed("id", "roof_id")
        .withColumn("building", F.explode(F.col("siteModel.buildings")))
        .withColumn("mounting_plane", F.explode(F.col("building.mountingPlanes")))
        .withColumn("area", F.col("mounting_plane.area"))
        .withColumn("mounting_plane_id", F.col("mounting_plane.id"))
        .withColumn("azimuth_angle", F.col("mounting_plane.azimuthAngle"))
        .withColumn("azimuth_vector_x", F.col("mounting_plane.azimuthVector.x"))
        .withColumn("azimuth_vector_y", F.col("mounting_plane.azimuthVector.y"))
        .withColumn("azimuth_vector_z", F.col("mounting_plane.azimuthVector.z"))
        .withColumn("centroid_x", F.col("mounting_plane.centroid.x"))
        .withColumn("centroid_y", F.col("mounting_plane.centroid.y"))
        .withColumn("centroid_z", F.col("mounting_plane.centroid.z"))
        .withColumn(
            "coordinate_system_xaxis_x",
            F.col("mounting_plane.coordinateSystem.xAxis.x"),
        )
        .withColumn(
            "coordinate_system_xaxis_y",
            F.col("mounting_plane.coordinateSystem.xAxis.y"),
        )
        .withColumn(
            "coordinate_system_xaxis_z",
            F.col("mounting_plane.coordinateSystem.xAxis.z"),
        )
        .withColumn(
            "coordinate_system_yaxis_x",
            F.col("mounting_plane.coordinateSystem.yAxis.x"),
        )
        .withColumn(
            "coordinate_system_yaxis_y",
            F.col("mounting_plane.coordinateSystem.yAxis.y"),
        )
        .withColumn(
            "coordinate_system_yaxis_z",
            F.col("mounting_plane.coordinateSystem.yAxis.z"),
        )
        .withColumn(
            "coordinate_system_zaxis_x",
            F.col("mounting_plane.coordinateSystem.zAxis.x"),
        )
        .withColumn(
            "coordinate_system_zaxis_y",
            F.col("mounting_plane.coordinateSystem.zAxis.y"),
        )
        .withColumn(
            "coordinate_system_zaxis_z",
            F.col("mounting_plane.coordinateSystem.zAxis.z"),
        )
        .withColumn("pitch_angle", F.col("mounting_plane.pitchAngle"))
        .withColumn("roof_material_type", F.col("mounting_plane.roofMaterialType"))
        .select(
            "mounting_plane_id",  # PK
            "roof_id",  # PK, FK
            "area",
            "azimuth_angle",
            "azimuth_vector_x",
            "azimuth_vector_y",
            "azimuth_vector_z",
            "centroid_x",
            "centroid_y",
            "centroid_z",
            "coordinate_system_xaxis_x",
            "coordinate_system_xaxis_y",
            "coordinate_system_xaxis_z",
            "coordinate_system_yaxis_x",
            "coordinate_system_yaxis_y",
            "coordinate_system_yaxis_z",
            "coordinate_system_zaxis_x",
            "coordinate_system_zaxis_y",
            "coordinate_system_zaxis_z",
            "pitch_angle",
            "roof_material_type",
        )
        # Dedupe
        .distinct()
    )

    building_mounting_plane_penetration_df = (
        roof_data_df.withColumn("building", F.explode(F.col("siteModel.buildings")))
        .withColumn("mounting_plane", F.explode(F.col("building.mountingPlanes")))
        .withColumn("mounting_plane_id", F.col("mounting_plane.id"))
        .withColumn("penetration", F.explode(F.col("mounting_plane.penetrations")))
        .withColumn("penetration_id", F.col("penetration.id"))
        .withColumn("obstruction_id", F.col("penetration.obstructionId"))
        .withColumn(
            "penetration_ring_winding_direction",
            F.col("penetration.ring.windingDirection"),
        )
        .select(
            "penetration_id",  # PK
            "mounting_plane_id",  # PK, FK
            "obstruction_id",
            "penetration_ring_winding_direction",
        )
        # Dedupe
        .distinct()
    )

    building_mounting_plane_penetration_ring_edge_df = (
        roof_data_df.withColumn("building", F.explode(F.col("siteModel.buildings")))
        .withColumn("mounting_plane", F.explode(F.col("building.mountingPlanes")))
        .withColumn("penetration", F.explode(F.col("mounting_plane.penetrations")))
        .withColumn("penetration_id", F.col("penetration.id"))
        .withColumn("ring_edge", F.explode(F.col("penetration.ring.edges")))
        .withColumn(
            "angle_between_bearing_vector_and_right_vector",
            F.col("ring_edge.angleBetweenBearingVectorAndRightVector"),
        )
        .withColumn(
            "angle_between_bearing_vector_and_up_vector",
            F.col("ring_edge.angleBetweenBearingVectorAndUpVector"),
        )
        .withColumn("bearing_vector", F.col("ring_edge.bearingVector"))
        .withColumn("edge_condition", F.col("ring_edge.edgeCondition"))
        .withColumn("end_point_x", F.col("ring_edge.endPoint.x"))
        .withColumn("end_point_y", F.col("ring_edge.endPoint.y"))
        .withColumn("end_point_z", F.col("ring_edge.endPoint.z"))
        .withColumn("ring_edge_id", F.col("ring_edge.id"))
        .withColumn("siding_material", F.col("ring_edge.sidingMaterial"))
        .withColumn("start_point_x", F.col("ring_edge.startPoint.x"))
        .withColumn("start_point_y", F.col("ring_edge.startPoint.y"))
        .withColumn("start_point_z", F.col("ring_edge.startPoint.z"))
        .select(
            "ring_edge_id",
            "penetration_id",  # FK
            "angle_between_bearing_vector_and_right_vector",
            "angle_between_bearing_vector_and_up_vector",
            "bearing_vector",
            "edge_condition",
            "end_point_x",
            "end_point_y",
            "end_point_z",
            "siding_material",
            "start_point_x",
            "start_point_y",
            "start_point_z",
        )
    )
    # Replace String representations of NULL for all String column types
    # (E.g., This is an issue for "edge_condition" and "siding_material")
    building_mounting_plane_penetration_ring_edge_df = fix_string_nulls(
        df=building_mounting_plane_penetration_ring_edge_df
    ).distinct()

    building_mounting_plane_polygon_exterior_ring_df = (
        roof_data_df.withColumn("building", F.explode(F.col("siteModel.buildings")))
        .withColumn("mounting_plane", F.explode(F.col("building.mountingPlanes")))
        .withColumn("mounting_plane_id", F.col("mounting_plane.id"))
        .withColumn(
            "polygon_exterior_ring_edge",
            F.explode(F.col("mounting_plane.polygon.exteriorRing.edges")),
        )
        .withColumn(
            "angle_between_bearing_vector_and_right_vector",
            F.col("polygon_exterior_ring_edge.angleBetweenBearingVectorAndRightVector"),
        )
        .withColumn(
            "angle_between_bearing_vector_and_up_vector",
            F.col("polygon_exterior_ring_edge.angleBetweenBearingVectorAndUpVector"),
        )
        .withColumn("bearing_vector", F.col("polygon_exterior_ring_edge.bearingVector"))
        .withColumn("edge_condition", F.col("polygon_exterior_ring_edge.edgeCondition"))
        .withColumn("end_point_x", F.col("polygon_exterior_ring_edge.endPoint.x"))
        .withColumn("end_point_y", F.col("polygon_exterior_ring_edge.endPoint.y"))
        .withColumn("end_point_z", F.col("polygon_exterior_ring_edge.endPoint.z"))
        .withColumn(
            "polygon_exterior_ring_edge_id", F.col("polygon_exterior_ring_edge.id")
        )
        .withColumn(
            "siding_material", F.col("polygon_exterior_ring_edge.sidingMaterial")
        )
        .withColumn("start_point_x", F.col("polygon_exterior_ring_edge.startPoint.x"))
        .withColumn("start_point_y", F.col("polygon_exterior_ring_edge.startPoint.y"))
        .withColumn("start_point_z", F.col("polygon_exterior_ring_edge.startPoint.z"))
        .withColumn(
            "polygon_exterior_ring_winding_direction",
            F.col("mounting_plane.polygon.exteriorRing.windingDirection"),
        )
        .select(
            "polygon_exterior_ring_edge_id",
            "mounting_plane_id",  # FK
            "angle_between_bearing_vector_and_right_vector",
            "angle_between_bearing_vector_and_up_vector",
            "bearing_vector",
            "edge_condition",
            "end_point_x",
            "end_point_y",
            "end_point_z",
            "siding_material",
            "start_point_x",
            "start_point_y",
            "start_point_z",
            "polygon_exterior_ring_winding_direction",
        )
    )
    # Replace String representations of NULL for all String column types
    building_mounting_plane_polygon_exterior_ring_df = fix_string_nulls(
        df=building_mounting_plane_polygon_exterior_ring_df
    ).distinct()

    building_mounting_plane_polygon_interior_ring_edge_df = (
        roof_data_df.withColumn("building", F.explode(F.col("siteModel.buildings")))
        .withColumn("mounting_plane", F.explode(F.col("building.mountingPlanes")))
        .withColumn("mounting_plane_id", F.col("mounting_plane.id"))
        .withColumn(
            "polygon_interior_ring",
            F.explode(F.col("mounting_plane.polygon.interiorRings")),
        )
        .withColumn(
            "polygon_interior_ring_edge",
            F.explode(F.col("polygon_interior_ring.edges")),
        )
        .withColumn(
            "angle_between_bearing_vector_and_right_vector",
            F.col("polygon_interior_ring_edge.angleBetweenBearingVectorAndRightVector"),
        )
        .withColumn(
            "angle_between_bearing_vector_and_up_vector",
            F.col("polygon_interior_ring_edge.angleBetweenBearingVectorAndUpVector"),
        )
        .withColumn("bearing_vector", F.col("polygon_interior_ring_edge.bearingVector"))
        .withColumn("edge_condition", F.col("polygon_interior_ring_edge.edgeCondition"))
        .withColumn("end_point_x", F.col("polygon_interior_ring_edge.endPoint.x"))
        .withColumn("end_point_y", F.col("polygon_interior_ring_edge.endPoint.y"))
        .withColumn("end_point_z", F.col("polygon_interior_ring_edge.endPoint.z"))
        .withColumn(
            "polygon_interior_ring_edge_id", F.col("polygon_interior_ring_edge.id")
        )
        .withColumn(
            "siding_material", F.col("polygon_interior_ring_edge.sidingMaterial")
        )
        .withColumn("start_point_x", F.col("polygon_interior_ring_edge.startPoint.x"))
        .withColumn("start_point_y", F.col("polygon_interior_ring_edge.startPoint.y"))
        .withColumn("start_point_z", F.col("polygon_interior_ring_edge.startPoint.z"))
        .withColumn(
            "polygon_interior_ring_winding_direction",
            F.col("polygon_interior_ring.windingDirection"),
        )
        .select(
            "polygon_interior_ring_edge_id",  # PK
            "mounting_plane_id",  # FK
            "angle_between_bearing_vector_and_right_vector",
            "angle_between_bearing_vector_and_up_vector",
            "bearing_vector",
            "edge_condition",
            "end_point_x",
            "end_point_y",
            "end_point_z",
            "siding_material",
            "start_point_x",
            "start_point_y",
            "start_point_z",
        )
    )
    # Replace String representations of NULL for all String column types
    building_mounting_plane_polygon_interior_ring_edge_df = fix_string_nulls(
        df=building_mounting_plane_polygon_interior_ring_edge_df
    ).distinct()

    site_model_obstruction_df = (
        roof_data_df.withColumnRenamed("id", "roof_id")
        .withColumn("obstruction", F.explode(F.col("siteModel.obstructions")))
        .withColumn("center_x", F.col("obstruction.center.x"))
        .withColumn("center_y", F.col("obstruction.center.y"))
        .withColumn("center_z", F.col("obstruction.center.z"))
        .withColumn("feature_name", F.col("obstruction.featureName"))
        .withColumn("site_model_obstruction_id", F.col("obstruction.id"))
        .withColumn("radius", F.col("obstruction.radius"))
        .withColumn("shape_type", F.col("obstruction.shapeType"))
        .select(
            "site_model_obstruction_id",  # PK
            "roof_id",  # PK, FK
            "center_x",
            "center_y",
            "center_z",
            "feature_name",
            "radius",
            "shape_type",
        )
        # Dedupe
        .distinct()
    )

    site_model_obstruction_ring_edge_df = (
        roof_data_df.withColumn(
            "obstruction", F.explode(F.col("siteModel.obstructions"))
        )
        .withColumn("site_model_obstruction_id", F.col("obstruction.id"))
        .withColumn("ring_edge", F.explode(F.col("obstruction.ring.edges")))
        .withColumn(
            "angle_between_bearing_vector_and_right_vector",
            F.col("ring_edge.angleBetweenBearingVectorAndRightVector"),
        )
        .withColumn(
            "angle_between_bearing_vector_and_up_vector",
            F.col("ring_edge.angleBetweenBearingVectorAndUpVector"),
        )
        .withColumn("bearing_vector", F.col("ring_edge.bearingVector"))
        .withColumn("edge_condition", F.col("ring_edge.edgeCondition"))
        .withColumn("end_point_x", F.col("ring_edge.endPoint.x"))
        .withColumn("end_point_y", F.col("ring_edge.endPoint.y"))
        .withColumn("end_point_z", F.col("ring_edge.endPoint.z"))
        .withColumn("obstruction_ring_edge_id", F.col("ring_edge.id"))
        .withColumn("siding_material", F.col("ring_edge.sidingMaterial"))
        .withColumn("start_point_x", F.col("ring_edge.startPoint.x"))
        .withColumn("start_point_y", F.col("ring_edge.startPoint.y"))
        .withColumn("start_point_z", F.col("ring_edge.startPoint.z"))
        .withColumn(
            "obstruction_ring_winding_direction",
            F.col("obstruction.ring.windingDirection"),
        )
        .select(
            "obstruction_ring_edge_id",  # Column is always zero
            "site_model_obstruction_id",  # FK
            "angle_between_bearing_vector_and_right_vector",
            "angle_between_bearing_vector_and_up_vector",
            "bearing_vector",
            "edge_condition",
            "end_point_x",
            "end_point_y",
            "end_point_z",
            "siding_material",
            "start_point_x",
            "start_point_y",
            "start_point_z",
            "obstruction_ring_winding_direction",
        )
        # Dedupe
        .distinct()
    )

    roof_dfs = dict(
        roof=roof_df,
        building_mounting_plane=building_mounting_plane_df,
        building_mounting_plane_penetration=building_mounting_plane_penetration_df,
        building_mounting_plane_penetration_ring_edge=building_mounting_plane_penetration_ring_edge_df,
        building_mounting_plane_polygon_exterior_ring=building_mounting_plane_polygon_exterior_ring_df,
        building_mounting_plane_polygon_interior_ring_edge=building_mounting_plane_polygon_interior_ring_edge_df,
        site_model_obstruction=site_model_obstruction_df,
        site_model_obstruction_ring_edge=site_model_obstruction_ring_edge_df,
    )

    return roof_dfs


def write_to_file(table_names_and_dfs: Dict[str, DataFrame]) -> NoReturn:
    """
    Simulate writing each dataframe to a table by instead writing locally as CSV
    """
    # Relative path
    output_dir = "output_data"

    for output_sub_folder, df in table_names_and_dfs.items():
        (
            # One output file per dataframe due to small size
            df.coalesce(1)
            .write.mode("overwrite")
            .option("header", "true")
            .option("delimiter", ",")
            .csv(f"{output_dir}/{output_sub_folder}")
        )


def main():
    start_time = datetime.now()

    spark = get_spark_session()
    logger = logging.getLogger("ingest_roof")
    # Relative path
    base_dir = "roof_data"

    json_file_list = fix_json_files(base_dir=base_dir, logger=logger)
    names_and_dfs = read_and_parse_json_data(
        spark=spark, base_dir=base_dir, json_files=json_file_list
    )
    write_to_file(table_names_and_dfs=names_and_dfs)

    end_time = datetime.now()
    execution_time = round(number=(end_time - start_time).total_seconds(), ndigits=2)
    logger.info("Total execution time: %s" % execution_time)


if __name__ == "__main__":
    main()
