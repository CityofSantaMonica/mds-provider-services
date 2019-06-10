# fake

Generate fake MDS `provider` data for testing and development.

## Running

Run the container to generate randomized data. The data is saved in a mounted subdirectory.

Run this command from the parent directory, where the `docker-compose.yml` file lives:

```console
docker-compose run [--rm] fake [OPTIONS]
```

## [OPTIONS]

### `--boundary BOUNDARY`

Path (within the container) or URL to a `.geojson` file with geographic bounds for the generated data.

This parameter must be provided if no environment variable has been configured:

```console
MDS_BOUNDARY=https://opendata.arcgis.com/datasets/bcc6c6245c5f46b68e043f6179bab153_3.geojson
```

In either case (parameter or environment variable), it should be the path or URL to a `.geojson` file in [4326][4326],
containing a FeatureCollection of (potentially overlapping) Polygons. See the file at the above URL for an example.

The generation process will use the unioned area of these Polygons as a reference.

For a complete list of options, see the help/usage output:

```bash
$ docker-compose run fake --help

usage: main.py [-h] [--boundary BOUNDARY] [--close CLOSE]
               [--date_format DATE_FORMAT] [--devices DEVICES] [--end END]
               [--inactivity INACTIVITY] [--open OPEN] [--output OUTPUT]
               [--propulsion_types PROPULSION_TYPE [PROPULSION_TYPE ...]]
               [--provider_name PROVIDER_NAME] [--provider_id PROVIDER_ID]
               [--start START] [--speed_mph SPEED_MPH] [--speed_ms SPEED_MS]
               [--vehicle_types VEHICLE_TYPE [VEHICLE_TYPE ...]]
               [--version VERSION]

optional arguments:
  -h, --help            show this help message and exit
  --boundary BOUNDARY   Path to a data file with geographic bounds for the
                        generated data. Overrides the MDS_BOUNDARY environment
                        variable.
  --close CLOSE         The hour of the day (24-hr format) that provider stops
                        operations. Overrides --start and --end.
  --date_format DATE_FORMAT
                        Format for datetime input (to this CLI) and output (to
                        stdout and files). Options: - 'unix' for Unix
                        timestamps (default) - 'iso8601' for ISO 8601 format -
                        '<python format string>' for custom formats, see https
                        ://docs.python.org/3/library/datetime.html#strftime-
                        strptime-behavior
  --devices DEVICES     The number of devices to model in the generated data
  --end END             The latest event in the generated data, in
                        --date_format format
  --inactivity INACTIVITY
                        Describes the portion of the fleet that remains
                        inactive.
  --open OPEN           The hour of the day (24-hr format) that provider
                        begins operations. Overrides --start and --end.
  --output OUTPUT       Path to a directory to write the resulting data
                        file(s)
  --propulsion_types PROPULSION_TYPE [PROPULSION_TYPE ...]
                        A list of propulsion_types to use for the generated
                        data, e.g. 'combustion electric electric_assist human'
  --provider_name PROVIDER_NAME
                        The name of the fake mobility as a service provider
  --provider_id PROVIDER_ID
                        The ID of the fake mobility as a service provider
  --start START         The earliest event in the generated data, in
                        --date_format format
  --speed_mph SPEED_MPH
                        The average speed of devices in miles per hour. Cannot
                        be used with --speed_ms
  --speed_ms SPEED_MS   The average speed of devices in meters per second.
                        Always takes precedence
  --vehicle_types VEHICLE_TYPE [VEHICLE_TYPE ...]
                        A list of vehicle_types to use for the generated data,
                        e.g. 'bicycle scooter'
  --version VERSION     The release version at which to reference MDS, e.g.
                        0.3.1
```

[4326]: http://epsg.io/4326