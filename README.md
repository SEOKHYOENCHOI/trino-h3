# trino-h3

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Build](https://github.com/SEOKHYOENCHOI/trino-h3/actions/workflows/tests.yml/badge.svg)](https://github.com/SEOKHYOENCHOI/trino-h3/actions/workflows/tests.yml)
[![codecov](https://codecov.io/gh/SEOKHYOENCHOI/trino-h3/branch/main/graph/badge.svg)](https://codecov.io/gh/SEOKHYOENCHOI/trino-h3)
[![GitHub Release](https://img.shields.io/github/v/release/SEOKHYOENCHOI/trino-h3)](https://github.com/SEOKHYOENCHOI/trino-h3/releases)
[![H3 Version](https://img.shields.io/badge/h3-v4.3.2-blue.svg)](https://github.com/uber/h3/releases/tag/v4.3.2)
[![Trino Version](https://img.shields.io/badge/trino-v436-blue.svg)](https://trino.io/)
[![Java Version](https://img.shields.io/badge/java-21-blue.svg)](https://openjdk.org/)

This library provides Trino bindings for the [H3 Core Library](https://github.com/uber/h3) via a Trino plugin. For API reference, please see the [H3 Documentation](https://h3geo.org/).

> **Note**: This project is forked from [foursquare/h3-presto](https://github.com/foursquare/h3-presto) and migrated to Trino.

## Installation

### Option 1: Docker Image (Recommended)

Use the pre-built Docker image with H3 plugin already installed:

```bash
docker run -p 8080:8080 seokhyoenchoi/trino-h3:latest
```

Or specify a version:

```bash
docker run -p 8080:8080 seokhyoenchoi/trino-h3:1.0.0
```

### Option 2: Download JAR from Releases

Download the latest JAR from [GitHub Releases](https://github.com/SEOKHYOENCHOI/trino-h3/releases):

```bash
# Download the JAR
wget https://github.com/SEOKHYOENCHOI/trino-h3/releases/latest/download/trino-h3-1.0.0.jar

# Create plugin directory and copy JAR
mkdir -p /usr/lib/trino/plugin/h3/
cp trino-h3-*.jar /usr/lib/trino/plugin/h3/

# Restart Trino server
```

### Option 3: Build from Source

Build the plugin yourself:

```bash
./gradlew shadowJar
```

Copy the JAR from `build/libs/trino-h3-*.jar` to `<trino-server>/plugin/h3/` on all nodes and restart Trino.

## Usage

Once installed, Trino will automatically load the `H3Plugin` at startup and the functions will then be available from SQL:

```sql
SELECT h3_latlng_to_cell(lat, lng, 9) AS hex FROM my_table;
```

## Available Functions

### Indexing
| Function | Return Type | Description |
|----------|-------------|-------------|
| `h3_latlng_to_cell(lat DOUBLE, lng DOUBLE, resolution INTEGER)` | `BIGINT` | Convert lat/lng to H3 cell |
| `h3_latlng_to_cell(point GEOMETRY, resolution INTEGER)` | `BIGINT` | Convert geometry point to H3 cell |
| `h3_cell_to_latlng(cell BIGINT)` | `GEOMETRY` | Get cell center as point |
| `h3_cell_to_boundary(cell BIGINT)` | `GEOMETRY` | Get cell boundary as polygon |

### Inspection
| Function | Return Type | Description |
|----------|-------------|-------------|
| `h3_get_resolution(cell BIGINT)` | `INTEGER` | Get cell resolution (0-15) |
| `h3_get_base_cell_number(cell BIGINT)` | `INTEGER` | Get base cell number (0-121) |
| `h3_string_to_h3(str VARCHAR)` | `BIGINT` | Convert hex string to H3 index |
| `h3_h3_to_string(cell BIGINT)` | `VARCHAR` | Convert H3 index to hex string |
| `h3_is_valid_cell(cell BIGINT)` | `BOOLEAN` | Check if cell is valid |
| `h3_is_res_class_iii(cell BIGINT)` | `BOOLEAN` | Check if resolution is Class III |
| `h3_is_pentagon(cell BIGINT)` | `BOOLEAN` | Check if cell is a pentagon |
| `h3_get_icosahedron_faces(cell BIGINT)` | `ARRAY(INTEGER)` | Get icosahedron face IDs |

### Hierarchy
| Function | Return Type | Description |
|----------|-------------|-------------|
| `h3_cell_to_parent(cell BIGINT, resolution INTEGER)` | `BIGINT` | Get parent cell |
| `h3_cell_to_children(cell BIGINT, resolution INTEGER)` | `ARRAY(BIGINT)` | Get all child cells |
| `h3_cell_to_center_child(cell BIGINT, resolution INTEGER)` | `BIGINT` | Get center child cell |
| `h3_cell_to_children_size(cell BIGINT, childRes INTEGER)` | `BIGINT` | Get number of children |
| `h3_cell_to_child_pos(child BIGINT, parentRes INTEGER)` | `BIGINT` | Get child position index |
| `h3_child_pos_to_cell(pos BIGINT, parent BIGINT, childRes INTEGER)` | `BIGINT` | Get child at position |
| `h3_compact_cells(cells ARRAY(BIGINT))` | `ARRAY(BIGINT)` | Compact cell array |
| `h3_uncompact_cells(cells ARRAY(BIGINT), resolution INTEGER)` | `ARRAY(BIGINT)` | Uncompact cell array |

### Traversal
| Function | Return Type | Description |
|----------|-------------|-------------|
| `h3_grid_disk(origin BIGINT, k INTEGER)` | `ARRAY(BIGINT)` | Get cells within distance k |
| `h3_grid_disk_distances(origin BIGINT, k INTEGER)` | `ARRAY(ARRAY(BIGINT))` | Get cells grouped by distance |
| `h3_grid_disk_unsafe(origin BIGINT, k INTEGER)` | `ARRAY(BIGINT)` | Fast grid disk (returns null on pentagons) |
| `h3_grid_ring(origin BIGINT, k INTEGER)` | `ARRAY(BIGINT)` | Get cells at exactly distance k |
| `h3_grid_ring_unsafe(origin BIGINT, k INTEGER)` | `ARRAY(BIGINT)` | Fast grid ring (returns null on pentagons) |
| `h3_grid_path_cells(origin BIGINT, destination BIGINT)` | `ARRAY(BIGINT)` | Get path between cells |
| `h3_grid_distance(origin BIGINT, destination BIGINT)` | `BIGINT` | Get grid distance |
| `h3_cell_to_local_ij(origin BIGINT, cell BIGINT)` | `ROW(i INTEGER, j INTEGER)` | Convert to local IJ coordinates |
| `h3_local_ij_to_cell(origin BIGINT, ij ROW(i INTEGER, j INTEGER))` | `BIGINT` | Convert from local IJ coordinates |

### Directed Edges
| Function | Return Type | Description |
|----------|-------------|-------------|
| `h3_are_neighbor_cells(cell1 BIGINT, cell2 BIGINT)` | `BOOLEAN` | Check if cells are neighbors |
| `h3_cells_to_directed_edge(origin BIGINT, destination BIGINT)` | `BIGINT` | Create directed edge |
| `h3_is_valid_directed_edge(edge BIGINT)` | `BOOLEAN` | Check if edge is valid |
| `h3_get_directed_edge_origin(edge BIGINT)` | `BIGINT` | Get edge origin cell |
| `h3_get_directed_edge_destination(edge BIGINT)` | `BIGINT` | Get edge destination cell |
| `h3_directed_edge_to_cells(edge BIGINT)` | `ARRAY(BIGINT)` | Get both cells of edge |
| `h3_origin_to_directed_edges(cell BIGINT)` | `ARRAY(BIGINT)` | Get all edges from cell |
| `h3_directed_edge_to_boundary(edge BIGINT)` | `GEOMETRY` | Get edge as linestring |

### Vertex
| Function | Return Type | Description |
|----------|-------------|-------------|
| `h3_cell_to_vertex(cell BIGINT, vertexNum INTEGER)` | `BIGINT` | Get vertex at index |
| `h3_cell_to_vertexes(cell BIGINT)` | `ARRAY(BIGINT)` | Get all vertices |
| `h3_vertex_to_latlng(vertex BIGINT)` | `GEOMETRY` | Get vertex as point |
| `h3_is_valid_vertex(vertex BIGINT)` | `BOOLEAN` | Check if vertex is valid |

### Region
| Function | Return Type | Description |
|----------|-------------|-------------|
| `h3_polygon_to_cells(polygon GEOMETRY, resolution INTEGER)` | `ARRAY(BIGINT)` | Fill polygon with cells |
| `h3_cells_to_multi_polygon(cells ARRAY(BIGINT))` | `GEOMETRY` | Convert cells to multipolygon |

### Miscellaneous
| Function | Return Type | Description |
|----------|-------------|-------------|
| `h3_get_hexagon_area_avg(resolution INTEGER, unit VARCHAR)` | `DOUBLE` | Average hexagon area (unit: km2, m2) |
| `h3_cell_area(cell BIGINT, unit VARCHAR)` | `DOUBLE` | Cell area (unit: rads2, km2, m2) |
| `h3_get_hexagon_edge_length_avg(resolution INTEGER, unit VARCHAR)` | `DOUBLE` | Average edge length (unit: rads, km, m) |
| `h3_edge_length(edge BIGINT, unit VARCHAR)` | `DOUBLE` | Edge length (unit: rads, km, m) |
| `h3_great_circle_distance(lat1 DOUBLE, lng1 DOUBLE, lat2 DOUBLE, lng2 DOUBLE, unit VARCHAR)` | `DOUBLE` | Great circle distance (unit: rads, km, m) |
| `h3_get_num_cells(resolution INTEGER)` | `BIGINT` | Total cells at resolution |
| `h3_get_res0_cells()` | `ARRAY(BIGINT)` | Get all resolution 0 cells (122 cells) |
| `h3_get_pentagons(resolution INTEGER)` | `ARRAY(BIGINT)` | Get pentagon cells (12 per resolution) |

## Development

Building the library requires JDK 21 and Gradle.

### Build

```sh
./gradlew build
```

### Build shadow JAR (for deployment)

```sh
./gradlew shadowJar
```

### Run tests

```sh
./gradlew test
```

### Format source code

```sh
./gradlew spotlessApply
```

### Check code formatting

```sh
./gradlew spotlessCheck
```

## Requirements

- Java 21+
- Trino 436+
- H3 4.3.2

## Legal and Licensing

This project is licensed under the [Apache 2.0 License](./LICENSE).

Based on [foursquare/h3-presto](https://github.com/foursquare/h3-presto) - Copyright 2022 Foursquare Labs, Inc.

H3 Copyright 2016 Uber Technologies, Inc.

DGGRID Copyright (c) 2015 Southern Oregon University
