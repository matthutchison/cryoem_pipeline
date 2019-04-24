This folder should be used for administrator-provided .json configuration files that will be available for users to select at runtime and apply specific values for a given project.  Since configuration values are imported in order, any user-selected values will override the values provided in config/system/ and config/default/base_system_config.json.

If you wish to have files that are defaults for all projects, place them in config/system instead.

One way to use these user files would be to put individual camera parameters into separate files that could be selected at runtime. This system is flexible, so it can extend to all of the parameters used in the pipeline (see config/default/base_system_config.json and example_project_config.json for the complete set)

in K3.json
```JSON
{
    "physical_pixel_size": 5,
    "super_resolution": null
}
```

in Falcon3.json
```JSON
{
    "physical_pixel_size": 14,
    "super_resolution": false
}

Which at runtime would prompt the user to select K3.json and/or Falcon3.json to load into the run configuration.