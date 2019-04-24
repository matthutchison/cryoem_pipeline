This folder should be used for administrator-provided .json configuration files that will automatically be imported on launching the pipeline and apply default values for all projects.  Since configuration values are imported in order, any user-selected values can override the values provided by files in this directory, just as these values will override the values from config/default/base_system_config.json

If you wish to have files that are user-selectable at runtime, place them in config/user/ instead.

A common example of content for a system file could be something like the following, which would set defaults for the microscope acceleration voltage and globus transfer parameters:
```JSON
{
    "voltage": 300,
    "globus_source_endpoint_id": "06b2ccd1-3bbe-45de-a9ac-cddd7be5b4ed",
    "globus_source_endpoint_path": "/path/to/endpoint/directory/",
    "globus_destination_endpoint_id": "06b2ccd1-3bbe-45de-a9ac-cddd7be5b4ee",
    "globus_destination_endpoint_path": "/path/to/endpoint/directory/",
}
```