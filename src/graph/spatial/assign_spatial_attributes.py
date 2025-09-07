"""
SpatialRelator Module
=====================

This module provides the `SpatialRelator` class for assigning spatial
relationships between geographic entities such as roads, places,
neighborhoods, and intersections.

It performs spatial joins, proximity calculations, and ensures
proper CRS alignment for accurate spatial relationships.

Dependencies
------------
- geopandas
- shapely

Example
-------
from assign_spatial_attributes import SpatialRelator

relator = SpatialRelator()
roads = relator.assign_roads_to_neighborhood(roads, neighborhoods)
places = relator.assign_places_to_roads(places, roads)
places = relator.assign_places_to_neighborhoods(places, neighborhoods)
places = relator.assign_places_to_intersections(places, intersections)
"""

import geopandas as gpd
from shapely.geometry import Point

class SpatialRelator:
    """
    A class for spatially relating geographic entities such as roads,
    places, and neighborhoods using spatial joins.
    """

    def assign_roads_to_neighborhood(
        self, roads: gpd.GeoDataFrame, neighborhoods: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        """
        Assign each street to the neighborhood it is located within.

        Parameters
        ----------
        roads : gpd.GeoDataFrame
            GeoDataFrame containing street geometries and attributes.
        neighborhoods : gpd.GeoDataFrame
            GeoDataFrame containing neighborhood geometries and attributes.

        Returns
        -------
        gpd.GeoDataFrame
            Updated `roads` GeoDataFrame with added columns:
            - `neighborhood_id`: ID of the neighborhood containing the street.
            - `neighborhood_name`: Name of the neighborhood containing the street.

        Notes
        -----
        - The method performs a spatial join using the `within` predicate.
        - CRS (Coordinate Reference System) of both layers is matched before the join.
        """
        # Ensure both layers have the same CRS
        roads = roads.to_crs(neighborhoods.crs)

        # Spatial join: match roads to the neighborhoods they fall within
        joined = gpd.sjoin(
            roads, 
            neighborhoods[["neighborhood_id", "name", "geometry"]],
            how="left", 
            predicate="within"
        )

        # Join results back to roads by index
        roads = roads.join(
            joined[["neighborhood_id", "name_right"]].rename(columns={"name_right": "neighborhood_name"})
            )
        
        return roads

    def assign_places_to_roads(
        self, places: gpd.GeoDataFrame, roads: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        """
        Assign each place to the nearest road within a 25-meter buffer.

        Parameters
        ----------
        places : gpd.GeoDataFrame
            GeoDataFrame containing place geometries and attributes.
        roads : gpd.GeoDataFrame
            GeoDataFrame containing road geometries and attributes.

        Returns
        -------
        gpd.GeoDataFrame
            Updated `places` GeoDataFrame with added columns:
            - `road_id`: ID of the nearest road within the buffer.
            - `road_name`: Name of the nearest road.

        Notes
        -----
        - Roads are buffered by 25 meters to account for proximity matching.
        - The method uses the `intersects` predicate for spatial matching.
        - CRS is transformed to a projected system for buffering and then reverted.
        """
        # Ensure both layers have the same CRS
        places = places.to_crs(roads.crs)

        # Create a buffer around road geometries (in meters)
        buffered = roads.to_crs(epsg=31983).copy()  # Projected CRS for Brazil
        buffered["geometry"] = buffered.geometry.buffer(25)
        buffered = buffered.to_crs(places.crs)

        # Spatial join: match places to buffered roads
        joined = gpd.sjoin(
            places, 
            buffered[["road_id", "name", "geometry"]],
            how="left", 
            predicate="intersects"
        )

        # Remove duplicate matches, keeping only the first
        joined = joined[~joined.index.duplicated(keep="first")]
        
        # Assign road ID and name to places aligned by index
        places = places.join(
            joined[["road_id", "name_right"]].rename(columns={"name_right": "road_name"})
        )

        return places

    def assign_places_to_neighborhoods(
        self, places: gpd.GeoDataFrame, neighborhoods: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        """
        Assign each place to the neighborhood it is located within.

        Parameters
        ----------
        places : gpd.GeoDataFrame
            GeoDataFrame containing place geometries and attributes.
        neighborhoods : gpd.GeoDataFrame
            GeoDataFrame containing neighborhood geometries and attributes.

        Returns
        -------
        gpd.GeoDataFrame
            Updated `places` GeoDataFrame with added columns:
            - `neighborhood_id_osm`: OSM ID of the containing neighborhood.
            - `neighborhood_name_osm`: OSM name of the containing neighborhood.

        Notes
        -----
        - The method uses the `within` predicate for spatial matching.
        - Duplicates are removed to ensure one-to-one matching.
        - CRS of both layers is matched before the join.
        """
        # Ensure both layers have the same CRS
        places = places.to_crs(neighborhoods.crs)

        # Spatial join: match places to neighborhoods
        joined = gpd.sjoin(
            places, 
            neighborhoods[["neighborhood_id", "name", "geometry"]],
            how="left", 
            predicate="within"
        )

        # Remove duplicate matches, keeping first occurrence for each place
        joined = joined[~joined.index.duplicated(keep="first")]

        # Join neighborhood info back to places, aligned by index
        places = places.join(
            joined[["neighborhood_id", "name_right"]].rename(
                columns={"neighborhood_id": "neighborhood_id_osm", "name_right": "neighborhood_name_osm"}
            )
        )

        return places

    def assign_places_to_intersections(
        self, places: gpd.GeoDataFrame, intersections: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        """
        Assign each place to the nearest intersection, computing the distance in meters.

        Parameters
        ----------
        places : gpd.GeoDataFrame
            GeoDataFrame containing place geometries and attributes.
        intersections : gpd.GeoDataFrame
            GeoDataFrame containing intersection geometries and attributes.

        Returns
        -------
        gpd.GeoDataFrame
            Updated `places` GeoDataFrame with added columns:
            - `nearest_intersection_osmid`: OSM ID of the nearest intersection.
            - `distance_to_nearest_intersection_m`: Distance in meters to the nearest intersection.

        Notes
        -----
        - Both GeoDataFrames are projected to EPSG:31983 for accurate distance calculation.
        - Uses `sjoin_nearest` to find nearest neighbor spatial join.
        - Results are reprojected back to original CRS of `places`.
        """
        # Project to metric CRS for distance calculation
        places_proj = places.to_crs(epsg=31983)
        intersections_proj = intersections.to_crs(epsg=31983)

        # Nearest spatial join with distance column in meters
        joined = gpd.sjoin_nearest(
            places_proj,
            intersections_proj[["osmid", "geometry"]],
            how="left",
            distance_col="distance_to_nearest_intersection_m"
        )

        # Reproject joined result back to original CRS of places
        joined = joined.to_crs(places.crs)

        # Join nearest intersection info back to places by index
        places = places.join(
            joined[["osmid", "distance_to_nearest_intersection_m"]].rename(columns={"osmid": "nearest_intersection_osmid"})
        )

        return places