import gsw


def generate_standardized_variables(ds):
    # Generate depth if not available but pressure is
    if "PRES_01" in ds and "latitude" in ds:
        ds["depth"] = -1 * gsw.z_from_p(ds["PRES_01"], ds["latitude"])
        ds["depth"].attrs(
            {
                "long_name": "Depth (spatial coordinate) relative to water surface in the water body",
                "units": "meters",
                "standard_name": "depth",
                "sdn_parameter_urn": "SDN:P01::ADEPZZ01",
                "sdn_parameter_name": "Depth (spatial coordinate) relative to water surface in the water body",
                "sdn_uom_urn": "SDN:P06::ULAA",
                "sdn_uom_name": "Metres",
                "comments": "The distance of a sensor or sampling point below the sea surface",
                "legacy_gf3_code": "DEPH",
                "alternative_name": "DepBelowSurf",
                "coverage_content_type": "coordinate",
                "ioos_category": "Location",
            }
        )
    # Standardize temperature IPTS-68 to ITS-90
    if "TEMPS601" in ds and "TEMPS901" not in ds:
        ds["TEMPS901"] = gsw.t90_from_t68(ds["TEMPS601"])
        ds["TEMPS901"].attrs.update(
            {
                "scale": "ITS-90",
                "sdn_parameter_urn": "SDN:P01::TEMPS901",
                "sdn_parameter_name": "Temperature (ITS-90) of the water body by CTD or STD",
            }
        )

    if "TEMPP681" in ds and "TEMPP901" not in ds:
        ds["TEMPP901"] = gsw.t90_from_t68(ds["TEMPP681"])
        ds["TEMPP901"].attrs.update(
            {
                "scale": "ITS-90",
                "sdn_parameter_urn": "SDN:P01::TEMPP901",
                "sdn_parameter_name": "Temperature (ITS-90) of the water body",
            }
        )

    if "TEMPR601" in ds and "TEMPR901" not in ds:
        ds["TEMPR901"] = gsw.t90_from_t68(ds["TEMPR601"])
        ds["TEMPR901"].attrs.update(
            {
                "scale": "ITS-90",
                "sdn_parameter_urn": "SDN:P01::TEMPR901",
                "sdn_parameter_name": "Temperature (IPTS-68) of the water body by reversing thermometer",
            }
        )

    if "TEMPS602" in ds and "TEMPS902" not in ds:
        ds["TEMPS902"] = gsw.t90_from_t68(ds["TEMPS602"])
        ds["TEMPS902"].attrs.update(
            {
                "scale": "ITS-90",
                "sdn_parameter_urn": "SDN:P01::TEMPS901",
                "sdn_parameter_name": "Temperature (ITS-90) of the water body by CTD or STD",
            }
        )

    if "TEMPP682" in ds and "TEMPP902" not in ds:
        ds["TEMPP902"] = gsw.t90_from_t68(ds["TEMPP682"])
        ds["TEMPP902"].attrs.update(
            {
                "scale": "ITS-90",
                "sdn_parameter_urn": "SDN:P01::TEMPP902",
                "sdn_parameter_name": "Temperature (ITS-90) of the water body",
            }
        )

    if "TEMPR602" in ds and "TEMPR902" not in ds:
        ds["TEMPR902"] = gsw.t90_from_t68(ds["TEMPR602"])
        ds["TEMPR902"].attrs.update(
            {
                "scale": "ITS-90",
                "sdn_parameter_urn": "SDN:P01::TEMPR902",
                "sdn_parameter_name": "Temperature (IPTS-68) of the water body by reversing thermometer",
            }
        )

    # Retrieve conductivity from CRAT
    if "CRAT_01" in ds and "CNDCST01" not in ds:
        ds["CNDCST01"] = 42.814 * ds["CRAT_01"] / 10
        ds["CNDCST01"].attrs.update(
            {
                "long_name": "Electrical Conductivity",
                "units": "S/m",
                "standard_name": "sea_water_electrical_conductivity",
                "sdn_parameter_urn": "SDN:P01::CNDCST01",
                "sdn_parameter_name": "Electrical conductivity of the water body by CTD",
                "sdn_uom_urn": "SDN:P06::UECA",
                "sdn_uom_name": "Siemens per metre",
                "legacy_gf3_code": "CNDC",
                "alternative_name": "CTDCond",
                "coverage_content_type": "physicalMeasurement",
                "ioos_category": "Other",
            }
        )
    if "CRAT_02" in ds and "CNDCST02" not in ds:
        ds["CNDCST02"] = 42.814 * ds["CRAT_02"] / 10
        ds["CNDCST02"].attrs.update(
            {
                "long_name": "Second Electrical Conductivity",
                "units": "S/m",
                "standard_name": "sea_water_electrical_conductivity",
                "sdn_parameter_urn": "SDN:P01::CNDCST02",
                "sdn_parameter_name": "Electrical conductivity of the water body by CTD",
                "sdn_uom_urn": "SDN:P06::UECA",
                "sdn_uom_name": "Siemens per metre",
                "legacy_gf3_code": "CNDC",
                "alternative_name": "CTDCond",
                "coverage_content_type": "physicalMeasurement",
                "ioos_category": "Other",
            }
        )
