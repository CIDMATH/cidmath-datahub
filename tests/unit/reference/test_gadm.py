"""Unit tests for the pure helpers in ``cidmath_datahub.reference.gadm`` (ADR 0023).

The IO helpers (download/extract/read_layer) and the lazy geospatial/Spark
factories are exercised by the build jobs, not here. These tests cover the
deterministic seams — ``centroid`` and ``gdf_to_dict_rows`` — using lightweight
stand-ins so the suite needs neither shapely nor geopandas installed.
"""

from __future__ import annotations

import pytest

from cidmath_datahub.reference import gadm


class _FakePoint:
    def __init__(self, x: float, y: float) -> None:
        self.x = x
        self.y = y


class _FakeGeom:
    """Minimal stand-in for a shapely geometry (centroid only needs this)."""

    def __init__(self, lon: float, lat: float, *, empty: bool = False) -> None:
        self._lon = lon
        self._lat = lat
        self.is_empty = empty

    def representative_point(self) -> _FakePoint:
        return _FakePoint(self._lon, self._lat)


@pytest.mark.unit
class TestCentroid:
    def test_returns_lon_lat(self):
        assert gadm.centroid(_FakeGeom(10.5, -3.25)) == (10.5, -3.25)

    def test_none_geometry(self):
        assert gadm.centroid(None) == (None, None)

    def test_empty_geometry(self):
        assert gadm.centroid(_FakeGeom(1.0, 2.0, empty=True)) == (None, None)

    def test_coerces_to_float(self):
        lon, lat = gadm.centroid(_FakeGeom(1, 2))
        assert isinstance(lon, float) and isinstance(lat, float)


class _FakeRow:
    def __init__(self, data: dict, geometry: object) -> None:
        self._data = data
        self.geometry = geometry

    def __getitem__(self, key: str):
        return self._data[key]


class _FakeGeoDataFrame:
    """Stand-in for a GeoDataFrame: ``columns`` + ``iterrows`` is all we use."""

    def __init__(self, columns: list[str], rows: list[_FakeRow]) -> None:
        self.columns = columns
        self._rows = rows

    def iterrows(self):
        yield from enumerate(self._rows)


@pytest.mark.unit
class TestGdfToDictRows:
    def test_carries_columns_and_geometry(self):
        gdf = _FakeGeoDataFrame(
            ["GID_0", "NAME_1", "geometry"],
            [_FakeRow({"GID_0": "USA", "NAME_1": "Georgia"}, geometry="GEOM")],
        )
        rows = gadm.gdf_to_dict_rows(gdf)
        assert rows == [{"GID_0": "USA", "NAME_1": "Georgia", "geometry": "GEOM"}]

    def test_geometry_column_not_duplicated_from_columns(self):
        # "geometry" is excluded from the column copy and re-attached from
        # the row's .geometry accessor, so it appears exactly once.
        gdf = _FakeGeoDataFrame(
            ["GID_0", "geometry"],
            [_FakeRow({"GID_0": "BRA"}, geometry="POLY")],
        )
        rows = gadm.gdf_to_dict_rows(gdf)
        assert rows[0]["geometry"] == "POLY"
        assert list(rows[0].keys()) == ["GID_0", "geometry"]

    def test_empty_frame(self):
        assert gadm.gdf_to_dict_rows(_FakeGeoDataFrame(["GID_0", "geometry"], [])) == []


@pytest.mark.unit
class TestConstants:
    def test_gadm_constants_present(self):
        assert gadm.GADM_ZIP_URL.startswith("https://")
        assert gadm.GADM_GPKG_NAME.endswith(".gpkg")
        assert gadm.GADM_VINTAGE == 2022
        assert "non-commercial" in gadm.GADM_LICENSE
        assert gadm.GENERALIZE_TOLERANCE_DEG == 0.005
