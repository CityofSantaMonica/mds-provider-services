# fake

Generate fake MDS `provider` data for testing and development.

## Running

Run the container to generate randomized data. The data is persisted in a `data/` subdirectory (even after the container is torn down), via a Docker volume.

This command first ensures the image is up to date locally, then runs the data generator. The container is torn down after the run completes.

Run this command from the parent directory, where the `docker-compose.yml` file lives:

```console
fake/bin/run.sh [OPTIONS]
```

## `[OPTIONS]`

Customize data generation by appending any combination of the following to the above command:

### `--boundary BOUNDARY`

Path (within the container) or URL to a `.geojson` file with geographic bounds for the generated data.

This parameter must be provided if no environment variable has been configured:

```console
MDS_BOUNDARY=https://opendata.arcgis.com/datasets/bcc6c6245c5f46b68e043f6179bab153_3.geojson
```

In either case (parameter or environment variable), it should be the path or URL to a `.geojson` file in [4326][4326],
containing a FeatureCollection of (potentially overlapping) Polygons. See the file at the above URL for an example.

The generation process will use the unioned area of these Polygons as a reference.

### `--close CLOSE`

The hour of the day (24-hr format) that provider stops operations.

Overrides `--start` and `--end`.

### `--date_format FORMAT`

Format for datetime input (to this CLI) and output (files and stdout).

Options:

* `unix` for Unix timestamps (default)
* `iso8601` for ISO 8601 format
* `<python format string>` for custom formats, see [`strftime()` and `strptime()` Behavior](https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior)

### `--devices DEVICES`

The number of devices to model in the generated data.

### `--end END`

The latest event in the generated data, in `--date_format` format.

### `--inactivity INACTIVITY`

Describes the portion of the fleet that remains inactive.

For example:

```console
--inactivity 0.05
```

Means approximately 5 percent of the fleet remains inactive.

### `--open OPEN`

The hour of the day (24-hr format) that provider begins operations.

Overrides `--start` and `--end`.

### `--output OUTPUT`

Path to a directory (*in the container*) to write the resulting data file(s).

### `--propulsion_types PROPULSION_TYPE [PROPULSION_TYPE ...]`

One or more `propulsion_type` to use for the generated data

For example:

```console
--propulsion_types human electric
```

### `--provider PROVIDER`

The name of the fake mobility as a service provider.

### `--speed_mph SPEED`

The average speed of devices in miles per hour.

Overridden by `--speed_ms`.

### `--speed_ms SPEED`

The average speed of devices in meters per second.

Overrides `--speed_mph`.

### `--start START`

The earliest event in the generated data, in `--date_format` format.

### `--vehicle_types VEHICLE_TYPE [VEHICLE_TYPE ...]`

One or more `vehicle_type` to use for the generated data.

For example:

```console
--vehicle_types scooter bike
```

[4326]: http://epsg.io/4326