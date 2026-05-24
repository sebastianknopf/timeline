from __future__ import annotations

import unittest

from sqlalchemy.dialects import postgresql

from processor.database import Base


def _compiled_type(column_name: str, table_name: str) -> str:
    dialect = postgresql.dialect()
    return str(Base.metadata.tables[table_name].c[column_name].type.compile(dialect=dialect))


class DatabaseModelsTests(unittest.TestCase):
    def test_metadata_contains_documented_tables(self) -> None:
        self.assertEqual(
            {"dim_stops", "dim_trips", "fact_stop_times"},
            set(Base.metadata.tables.keys()),
        )

    def test_dim_stops_matches_database_documentation(self) -> None:
        table = Base.metadata.tables["dim_stops"]

        expected_columns = {
            "instance_id": ("TEXT", False),
            "stop_id": ("TEXT", False),
            "stop_name": ("TEXT", False),
            "stop_lat": ("DOUBLE PRECISION", False),
            "stop_lon": ("DOUBLE PRECISION", False),
        }

        self.assertEqual(set(expected_columns.keys()), set(table.c.keys()))
        for column_name, (expected_type, expected_nullable) in expected_columns.items():
            self.assertEqual(expected_type, _compiled_type(column_name, "dim_stops"))
            self.assertEqual(expected_nullable, table.c[column_name].nullable)

        self.assertEqual(["instance_id", "stop_id"], list(table.primary_key.columns.keys()))
        self.assertEqual(
            {("instance_id", "stop_name")},
            {tuple(index.columns.keys()) for index in table.indexes},
        )

    def test_dim_trips_matches_database_documentation(self) -> None:
        table = Base.metadata.tables["dim_trips"]

        expected_columns = {
            "instance_id": ("TEXT", False),
            "operation_day_date": ("DATE", False),
            "trip_id": ("TEXT", False),
            "route_id": ("TEXT", False),
            "route_name": ("TEXT", False),
            "concessionaire_id": ("TEXT", False),
            "concessionaire_name": ("TEXT", False),
            "operator_id": ("TEXT", True),
            "operator_name": ("TEXT", True),
            "nom_start_time": ("TIMESTAMP WITH TIME ZONE", False),
            "nom_end_time": ("TIMESTAMP WITH TIME ZONE", False),
            "act_start_time": ("TIMESTAMP WITH TIME ZONE", True),
            "act_end_time": ("TIMESTAMP WITH TIME ZONE", True),
            "nom_start_stop_id": ("TEXT", False),
            "nom_end_stop_id": ("TEXT", False),
            "nom_total_distance": ("DOUBLE PRECISION", False),
            "act_total_distance": ("DOUBLE PRECISION", True),
            "schedule_relationship": ("TEXT", False),
        }

        self.assertEqual(set(expected_columns.keys()), set(table.c.keys()))
        for column_name, (expected_type, expected_nullable) in expected_columns.items():
            self.assertEqual(expected_type, _compiled_type(column_name, "dim_trips"))
            self.assertEqual(expected_nullable, table.c[column_name].nullable)

        self.assertEqual(
            ["instance_id", "operation_day_date", "trip_id"],
            list(table.primary_key.columns.keys()),
        )
        self.assertEqual("'UNKNOWN'", table.c.schedule_relationship.server_default.arg.text)
        self.assertEqual(
            {
                ("instance_id", "operation_day_date", "route_id"),
                ("instance_id", "operation_day_date", "operator_id"),
                ("instance_id", "operation_day_date", "concessionaire_id"),
            },
            {tuple(index.columns.keys()) for index in table.indexes},
        )
        self.assertEqual(
            {
                ("instance_id", "nom_start_stop_id"),
                ("instance_id", "nom_end_stop_id"),
            },
            {tuple(constraint.column_keys) for constraint in table.foreign_key_constraints},
        )

    def test_fact_stop_times_matches_database_documentation(self) -> None:
        table = Base.metadata.tables["fact_stop_times"]

        expected_columns = {
            "instance_id": ("TEXT", False),
            "operation_day_date": ("DATE", False),
            "trip_id": ("TEXT", False),
            "stop_id": ("TEXT", False),
            "distance_from_start": ("DOUBLE PRECISION", False),
            "nom_arrival_time": ("TIMESTAMP WITH TIME ZONE", False),
            "nom_departure_time": ("TIMESTAMP WITH TIME ZONE", False),
            "act_arrival_time": ("TIMESTAMP WITH TIME ZONE", True),
            "act_departure_time": ("TIMESTAMP WITH TIME ZONE", True),
            "schedule_relationship": ("TEXT", False),
        }

        self.assertEqual(set(expected_columns.keys()), set(table.c.keys()))
        for column_name, (expected_type, expected_nullable) in expected_columns.items():
            self.assertEqual(expected_type, _compiled_type(column_name, "fact_stop_times"))
            self.assertEqual(expected_nullable, table.c[column_name].nullable)

        self.assertEqual(
            [
                "instance_id",
                "operation_day_date",
                "trip_id",
                "stop_id",
                "distance_from_start",
            ],
            list(table.primary_key.columns.keys()),
        )
        self.assertEqual("'UNKNOWN'", table.c.schedule_relationship.server_default.arg.text)
        self.assertEqual(
            {
                ("instance_id", "operation_day_date", "stop_id"),
                ("instance_id", "operation_day_date", "trip_id"),
                ("instance_id", "act_arrival_time"),
                ("instance_id", "act_departure_time"),
            },
            {tuple(index.columns.keys()) for index in table.indexes},
        )
        self.assertEqual(
            {
                ("instance_id", "stop_id"),
                ("instance_id", "operation_day_date", "trip_id"),
            },
            {tuple(constraint.column_keys) for constraint in table.foreign_key_constraints},
        )


if __name__ == "__main__":
    unittest.main()
