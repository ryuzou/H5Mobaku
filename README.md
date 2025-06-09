# H5Mobaku (H5MobakuReader)

A high-performance C library for efficiently reading large-scale HDF5 files containing population data with geographic mesh ID lookups.

## Overview

H5Mobaku is designed to handle massive datasets (74,160 × 1,553,332 int32 matrix) representing population counts across different geographic locations (mesh IDs) over time. The library uses minimal perfect hash functions for O(1) mesh ID lookups and supports both time-based and datetime-based queries.

## Features

- **High Performance I/O**: Leverages io_uring for asynchronous I/O operations
- **O(1) Mesh ID Lookups**: Uses CMPH (C Minimal Perfect Hashing) library
- **Flexible Query API**: Support for single point, multi-point, and time series queries
- **Datetime Support**: Query data using human-readable datetime strings
- **CLI Tool**: Command-line interface for quick data retrieval
- **Environment Configuration**: Support for .env files to configure database paths

## Dependencies

- HDF5 C library
- liburing (for io_uring support)
- CMPH library (for minimal perfect hashing)
- CMake 3.26+
- C23 standard compiler
- C++17 (for tests)

## Installation

### Prerequisites

Install required dependencies:

```bash
# Rocky Linux / RHEL / CentOS
sudo dnf install epel-release
sudo dnf install hdf5-devel liburing-devel cmake gcc gcc-c++ make

# Install CMPH (needs to be built from source)
# Note: The original CMPH uses autotools. If not available, try:
# Alternative 1: Install from SourceForge
wget https://sourceforge.net/projects/cmph/files/v2.0.2/cmph-2.0.2.tar.gz
tar -xzf cmph-2.0.2.tar.gz
cd cmph-2.0.2
./configure && make && sudo make install

# Alternative 2: Some distributions may have it in their repos
# sudo dnf install cmph-devel
```

### Building

```bash
mkdir build && cd build
cmake ..
make
sudo make install
```

### Build Options

- `H5MR_BUILD_SHARED`: Build shared library (default: ON)
- `H5MR_BUILD_STATIC`: Build static library (default: ON)
- `H5MR_BUILD_TESTS`: Build test suite (default: ON)
- `H5MR_BUILD_CLI`: Build CLI tools (default: ON)
- `H5MR_ENABLE_PYBIND`: Build Python bindings (default: OFF)

Example with options:
```bash
cmake -DH5MR_BUILD_TESTS=ON -DH5MR_BUILD_CLI=ON ..
```

## Configuration

Create a `.env` file in your working directory to specify the HDF5 file path:

```bash
HDF5_FILE_PATH=/path/to/your/mobaku_base.h5
```

## Usage

### C API

```c
#include <H5MR/h5mr.h>
#include "h5mobaku_ops.h"
#include "meshid_ops.h"

// Initialize
struct h5mobaku *ctx;
h5mobaku_open("path/to/data.h5", &ctx);
cmph_t *hash = meshid_prepare_search();

// Query single population at specific datetime
int32_t population = h5mobaku_read_population_single_at_time(
    ctx, hash, 533946395, "2016-01-01 12:00:00"
);

// Query time series
int32_t *series = h5mobaku_read_population_time_series_between(
    ctx, hash, 533946395, 
    "2016-01-01 00:00:00", 
    "2016-01-01 23:00:00"
);

// Cleanup
h5mobaku_free_data(series);
cmph_destroy(hash);
h5mobaku_close(ctx);
```

### CLI Tools

#### h5m-reader - Data Query Tool

The `h5m-reader` command-line tool provides easy access to the data:

```bash
# Single time query
h5m-reader -f data.h5 -m 533946395 -t "2016-01-01 12:00:00"

# Time range query
h5m-reader -f data.h5 -m 533946395 -s "2016-01-01 00:00:00" -e "2016-01-01 23:00:00"

# Raw output for piping to other tools
h5m-reader -f data.h5 -m 533946395 -s "2016-01-01 00:00:00" -e "2016-01-01 01:00:00" -r | analysis_tool
```

Options:
- `-f, --file`: HDF5 file path (required)
- `-m, --mesh`: Mesh ID (required)
- `-t, --time`: Single datetime (YYYY-MM-DD HH:MM:SS)
- `-s, --start`: Start datetime for range query
- `-e, --end`: End datetime for range query
- `-r, --raw`: Output raw uint32 byte stream
- `-h, --help`: Show help message

#### csv-to-h5 - CSV to HDF5 Converter

The `csv-to-h5` tool converts CSV population data files to HDF5 format:

```bash
# Convert single CSV file
./csv-to-h5 -v -o output.h5 data.csv

# Convert multiple CSV files
./csv-to-h5 -v -o output.h5 file1.csv file2.csv file3.csv

# Process all CSV files in a directory
./csv-to-h5 -d ./data -p "*_mesh_pop_*.csv" -o processed.h5

# Append to existing HDF5 file
./csv-to-h5 -a -o existing.h5 new_data.csv
```

CSV Format (expected columns):
```csv
date,time,area,residence,age,gender,population
20240101,0100,362257264,-1,-1,-1,19
20240101,0100,362257272,-1,-1,-1,16
20240101,0200,362257264,-1,-1,-1,21
```

Options:
- `-o, --output <file>`: Output HDF5 file (default: cmake-build-debug/population_debug.h5)
- `-b, --batch-size <size>`: Batch size for processing (default: 10000)
- `-d, --directory <dir>`: Process all CSV files in directory
- `-p, --pattern <pattern>`: File pattern to match (default: *.csv)
- `-a, --append`: Append to existing HDF5 file
- `-v, --verbose`: Enable verbose output
- `-h, --help`: Show help message

## API Reference

### Core Functions

#### File Operations
- `h5mobaku_open(const char *filepath, struct h5mobaku **ctx)`: Open HDF5 file
- `h5mobaku_close(struct h5mobaku *ctx)`: Close HDF5 file

#### Single Point Queries
- `h5mobaku_read_population_single(ctx, hash, mesh_id, time_index)`: Read single value by indices
- `h5mobaku_read_population_single_at_time(ctx, hash, mesh_id, datetime)`: Read single value by datetime

#### Multi-Point Queries
- `h5mobaku_read_population_multi(ctx, hash, mesh_ids, num_meshes, time_index)`: Read multiple meshes at one time
- `h5mobaku_read_population_multi_at_time(ctx, hash, mesh_ids, num_meshes, datetime)`: Read multiple meshes by datetime

#### Time Series Queries
- `h5mobaku_read_population_time_series(ctx, hash, mesh_id, start_time, end_time)`: Read time series by indices
- `h5mobaku_read_population_time_series_between(ctx, hash, mesh_id, start_dt, end_dt)`: Read time series by datetime range
- `h5mobaku_read_multi_mesh_time_series(ctx, hash, mesh_ids, num_meshes, start_time, end_time)`: Optimized multi-mesh time series

#### Memory Management
- `h5mobaku_free_data(int32_t *data)`: Free allocated memory

### Mesh ID Operations
- `meshid_prepare_search()`: Initialize CMPH hash table
- `meshid_search_id(hash, mesh_id)`: Get index for mesh ID
- `meshid_get_time_index_from_datetime(datetime)`: Convert datetime to time index
- `meshid_get_datetime_from_time_index(time_index)`: Convert time index to datetime

## Data Format

The HDF5 file contains:
- Dataset: "population_data"
- Dimensions: 74,160 (time) × 1,553,332 (mesh IDs)
- Data type: int32
- Time base: 2016-01-01 00:00:00 (hourly intervals)
- Mesh IDs: Japanese geographic identifiers

## Testing

Run the test suite:

```bash
cd build
ctest

# Or run individual tests
./test_meshid_ops
./test_h5mobaku_ops
./verify_layout
```

## Performance

The library is optimized for:
- Large sequential reads using io_uring
- O(1) mesh ID to index conversion
- Efficient batch operations
- Memory-mapped hash tables

Typical performance:
- Single point query: < 1ms
- 10,000 hour time series: < 100ms
- Multi-mesh batch operations: 5-10x faster than individual queries

## License

MIT License

## Authors

- Ryuzo Takahashi

## Acknowledgments

- Uses CMPH library for minimal perfect hashing
- Built on HDF5 for efficient data storage
- io_uring for high-performance I/O