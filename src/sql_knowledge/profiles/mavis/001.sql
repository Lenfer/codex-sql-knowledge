CREATE TABLE mavis_buildings (
 id TEXT PRIMARY KEY,
 entity_id TEXT NOT NULL UNIQUE REFERENCES entities(id),
 address TEXT,
 native_house_id TEXT,
 native_object_id TEXT,
 latitude REAL,
 longitude REAL,
 import_revision INTEGER NOT NULL,
 provenance_ref TEXT NOT NULL
);
CREATE INDEX mavis_buildings_native_object ON mavis_buildings(native_object_id);
CREATE TABLE mavis_scenes (
 id TEXT PRIMARY KEY,
 entity_id TEXT NOT NULL UNIQUE REFERENCES entities(id),
 scope TEXT,
 pipeline_stage TEXT,
 terrain_parameters_json TEXT CHECK(terrain_parameters_json IS NULL OR json_valid(terrain_parameters_json)),
 import_revision INTEGER NOT NULL,
 provenance_ref TEXT NOT NULL
);
