"""Module that regroups tools used to integrate data from seabird instruments."""
import difflib
import json
import logging
import re
from xml.parsers.expat import ExpatError

import pandas as pd
import xmltodict

no_file_logger = logging.getLogger(__name__)
logger = logging.LoggerAdapter(no_file_logger, extra={"file": None})

seabird_to_bodc = {
    "Temperature": ["TEMPP681", "TEMPP901", "TEMPS601", "TEMPS901", "TEMPPR01"],
    "Temperature, 2": ["TEMPP682", "TEMPP902", "TEMPS602", "TEMPS902", "TEMPPR02"],
    "Pressure, Digiquartz with TC": ["PRESPR01"],
    "Pressure, Strain Gauge": ["PRESPR01"],
    "Conductivity": ["CNDCST01"],
    "Conductivity, 2": ["CNDCST02"],
    "Altimeter": ["AHSFZZ01"],
    "PAR/Logarithmic, Satlantic": ["IRRDUV01"],
    "PAR/Irradiance, Biospherical/Licor": ["IRRDUV01"],
    "Oxygen, SBE 43": ["DOXYZZ01", "OXYOCPVL01"],
    "Oxygen, SBE 43, 2": ["DOXYZZ02", "OXYOCPVL02"],
    "Oxygen Current, Beckman/YSI": ["DOXYZZ01", "OXYOCPVL01"],
    "Oxygen Temperature, Beckman/YSI": ["DOXYZZ01", "OXYOCPVL01"],
    "Optode 4330F - O2 Temp": ["DOXYZZ01", "OXYTPR01"],
    "Optode 4330F - O2 Temperature": ["DOXYZZ01", "OXYTPR01"],
    "Optode 4330F - O2 D-Phase": ["DOXYZZ01", "OXYOCPFR"],
    "Optode 4330F - D Phase": ["DOXYZZ01", "OXYOCPFR"],
    "Optode 4330F - O2 Concentration": ["DOXYZZ01", "OXYOCPFR"],
    "Fluorometer, Seapoint Ultraviolet": ["CDOMZZ01", "CDOMZZ02"],
    "Fluorometer, WET Labs ECO CDOM": ["CDOMZZ01", "CDOMZZ02"],
    "Fluorometer, Chelsea UV Aquatracka": ["CDOMZZ01", "CDOMZZ02"],
    "Fluorometer, Seapoint": ["CPHLPR01", "CPHLPR02"],
    "Fluorometer, WET Labs WETstar": ["CPHLPR01", "CPHLPR02"],
    "Fluorometer, Wetlabs Wetstar": ["CPHLPR01", "CPHLPR02"],
    "Fluorometer, Wetlab Wetstar": ["CPHLPR01", "CPHLPR02"],
    "Fluorometer, WET Labs ECO-AFL/FL": ["CPHLPR01", "CPHLPR02"],
    "Fluorometer, Chelsea Aqua": ["CPHLPR01", "CPHLPR02"],
    "Fluorometer, Chelsea Aqua 3": ["CPHLPR01", "CPHLPR02"],
    "Fluorometer, Chelsea Minitracka": ["CPHLPR01", "CPHLPR02"],
    "Fluorometer, Seatech/WET Labs FLF": ["CPHLPR01", "CPHLPR02"],
    "Transmissometer, WET Labs C-Star": ["ATTNZS01", "ATTNZR01", "ATTNXXZZ"],
    "Transmissometer, Chelsea/Seatech": ["ATTNZR01", "ATTNXXZZ"],
    "Turbidity Meter, WET Labs, ECO-NTU": ["TURBXX01", "VSCTXX01"],
    "Turbidity Meter, Seapoint": ["TURBXX01", "VSCTXX01"],
    "OBS, Backscatterance (D & A)": ["TURBXX01", "VSCTXX01"],
    "pH": ["PHMASS01", "PHXXZZ01"],
    "OBS, WET Labs, ECO-BB": ["TURBXX01", "VSCTXX01"],
    "OBS, Seapoint Turbidity": ["VSCTXX01", "TURBXX01"],
    "SPAR/Surface Irradiance": ["IRRDSV01"],
    "SPAR, Biospherical/Licor": ["IRRDSV01"],
    "SUNA": [],
    "Dr. Haardt BackScatter Fluorometer": [],
    "User Polynomial": [],
    "User Polynomial, 2": [],
    "User Polynomial, 3": [],
}

sbe_data_processing_modules = [
    "datcnv",
    "filter",
    "align",
    "celltm",
    "loopedit",
    "derive",
    "Derive",
    "binavg",
    "split",
    "strip",
    "section",
    "wild",
    "window",
]


def get_seabird_instrument_from_header(seabird_header):
    """Retrieve main instrument model from Sea-Bird CNV header"""
    instrument = re.findall(
        r"\* (?:Sea\-Bird ){0,1}SBE\s*(?P<sensor>\d+[^\s]*)(?P<extra>.*)",
        seabird_header,
    )
    instrument = [inst for inst, extra in instrument if " = " not in extra]
    if instrument:
        return f"Sea-Bird SBE {''.join(instrument)}"


def get_sbe_instrument_type(instrument):
    """Map SBE instrument number a type of instrument"""
    if re.match(r"SBE\s*(9|16|19|25|37)", instrument):
        return "CTD"
    logger.warning("Unknown instrument type for %s", instrument)


def get_seabird_processing_history(seabird_header):
    """
    Retrieve the different rows within a Seabird header associated
    with the sbe data processing tool
    """
    if "# datcnv" in seabird_header:
        sbe_hist = r"\# (" + "|".join(sbe_data_processing_modules) + r").*"
        return "\n".join(
            [line for line in seabird_header.split("\n") if re.match(sbe_hist, line)]
        )
    logger.warning("Failed to retrieve Seabird Processing Modules history")


def generate_binned_attributes(dataset, seabird_header):
    """Retrieve from the Seabird header binned information and
    apply it to the different related attributes and variable attributes."""

    binavg = re.search(
        r"\# binavg_bintype \= (?P<bintype>.*)\n\# binavg_binsize \= (?P<binsize>\d+)\n",
        seabird_header,
    )
    if binavg:
        bintype, binsize = binavg.groups()
    else:
        return dataset

    bin_str = f"{binsize} {bintype}"
    dataset.attrs["geospatial_vertical_resolution"] = bin_str
    if "decibar" in bintype:
        binvar = "prdM"
    elif "second" in bin_str or "hour" in bin_str:
        binvar = "time"
    elif "meter" in bin_str:
        binvar = "depth"
    elif "scan" in bin_str:
        binvar = "scan"
    else:
        logger.error("Unknown binavg method: %s", bin_str)

    # Add cell method attribute and geospatial_vertical_resolution global attribute
    if "decibar" in bin_str or "meter" in bin_str:
        dataset.attrs["geospatial_vertical_resolution"] = bin_str
    elif "second" in bin_str or "hour" in bin_str:
        dataset.attrs["time_coverage_resolution"] = pd.Timedelta(bin_str).isoformat()
    for var in dataset:
        if (
            len(dataset.dims) == 1 and len(dataset[var].dims) == 1
        ) or binvar in dataset[var].dims:

            dataset[var].attrs["cell_method"] = f"{binvar}: mean (interval: {bin_str})"
    return dataset


def update_attributes_from_seabird_header(
    dataset, seabird_header, parse_manual_inputs=False
):
    """Add Seabird specific attributes parsed from Seabird header into a xarray dataset"""
    # sourcery skip: identity-comprehension, remove-redundant-if
    # Instrument
    dataset.attrs["instrument"] = get_seabird_instrument_from_header(seabird_header)

    # Bin Averaged
    dataset = generate_binned_attributes(dataset, seabird_header)

    # Manual inputs
    manual_inputs = re.findall(r"\*\* (?P<key>.*): (?P<value>.*)\n", seabird_header)
    if parse_manual_inputs:
        for key, value in manual_inputs:
            dataset.attrs[key.replace(r" ", r"_").lower()] = value

    return dataset


def generate_instruments_variables_from_xml(dataset, seabird_header):
    """Generate IOOS 1.2 standard instrument variables and associated variables
    instrument attribute based on Seabird XML header."""
    # Retrieve Sensors xml section within seabird header
    calibration_xml = re.sub(
        r"\n\#\s",
        r"\n",
        re.search(r"\<Sensors .+\<\/Sensors\>", seabird_header, re.DOTALL)[0],
    )

    # Read XML and commented lines, drop encoding line
    try:
        sensors = xmltodict.parse(calibration_xml)["Sensors"]["sensor"]
    except ExpatError:
        logger.error("Failed to parsed Sea-Bird Instrument Calibration XML")
        return dataset, {}

    sensors_comments = re.findall(
        r"\s*\<!--\s*(Frequency \d+|A/D voltage \d+|.* voltage|Count){1}, (.*)-->\n",
        calibration_xml,
    )
    # Consider only channels with sensor mounted
    sensors = [sensor for sensor in sensors if len(sensor) > 1]
    sensors_comments = [
        (con, name)
        for con, name in sensors_comments
        if not name.startswith(("Free", "Unavailable"))
    ]

    # Make sure that the sensor count match the sensor_comments count
    if len(sensors_comments) != len(sensors):
        logger.error("Failed to detect same count of sensors and sensors_comments")
        return dataset, {}

    # Split each sensor calibrations to a dictionary
    sensors_map = {}
    for sensor, sensor_comment in zip(sensors, sensors_comments):
        sensor_key = list(sensor.keys())[1].strip()
        attrs = sensor[sensor_key]
        channel, description = sensor_comment

        # Define senor variable name
        if "UserPolynomial" in sensor_key and attrs.get("SensorName"):
            sensor_name = attrs.pop("SensorName").strip()
            sensor_var_name = re.sub(r"[^\d\w]+", "_", sensor_name)
        else:
            sensor_var_name = sensor_key
            sensor_name = description.strip()

        if "Oxygen" in sensor_name:
            subsensors = re.search(r"Current|Temp|Phase|Concentration", description)
            if subsensors:
                sensor_var_name += "_" + subsensors[0]

        # Add trailing number if present
        if re.search(r", \d+", sensor_name):
            sensor_number = int(re.search(r", (\d+)", sensor_name)[1])
            sensor_var_name += f"_{sensor_number}"
        else:
            sensor_number = 1

        if sensor_var_name in dataset:
            logger.warning("Duplicated instrument variable %s", sensor_var_name)

        # Try fit IOOS 1.2 which request to add a instrument variable for each
        # instruments and link this variable to data variable by using the instrument attribute
        # https://ioos.github.io/ioos-metadata/ioos-metadata-profile-v1-2.html#instrument
        dataset[sensor_var_name] = json.dumps(attrs)
        dataset[sensor_var_name].attrs = {
            "calibration_date": pd.to_datetime(
                attrs.pop("CalibrationDate"),
                errors="ignore",
                infer_datetime_format=True,
            ),  # IOOS 1.2, NCEI 2.0
            "component": f"{sensor_var_name}_sn{attrs['SerialNumber']}",  # IOOS 1.2
            "discriminant": str(sensor_number),  # IOOS 1.2
            "make_model": sensor_name,  # IOOS 1.2, NCEI 2.0
            "channel": channel,
            "sbe_sensor_id": int(attrs.pop("@SensorID")),
            "serial_number": attrs.pop("SerialNumber"),  # NCEI 2.0
            "calibration": json.dumps(attrs),
        }
        sensors_map[sensor_name] = sensor_name

    return dataset, sensors_map


def generate_instruments_variables_from_sensor(dataset, seabird_header):
    """Parse older Seabird Header sensor information and generate instrument variables"""
    sensors = re.findall(r"\# sensor (?P<id>\d+) = (?P<text>.*)\n", seabird_header)
    for index, sensor in sensors:
        if "Voltage" in sensor:
            sensor_items = sensor.split(",", 1)
            attrs = {
                "channel": sensor_items[0],
                "sensor_description": sensor_items[0].replace("Voltage", "").strip()
                + sensor_items[1],
            }
        else:
            attrs_dict = re.search(
                r"(?P<channel>Frequency\s+\d+|Stored Volt\s+\d+|Extrnl Volt  \d+|Pressure Number\,)\s+"
                + r"(?P<sensor_description>.*)",
                sensor,
            )
            if attrs_dict is None:
                logger.error("Failed to read sensor item: %s", sensor)
                continue
            attrs = attrs_dict.groupdict()
        sensor_code = f"sensor_{index}"
        dataset[sensor_code] = sensor
        dataset[sensor_code].attrs = attrs
    return dataset


def add_seabird_instruments(dataset, seabird_header, match_by="long_name"):
    """
    Extract seabird sensor information and generate instrument variables which
    follow the IOOS 1.2 convention
    """
    # Retrieve sensors information
    if "# <Sensors count" in seabird_header:
        dataset, sensors_map = generate_instruments_variables_from_xml(
            dataset, seabird_header
        )
    elif "# sensor" in seabird_header:
        dataset = generate_instruments_variables_from_sensor(dataset, seabird_header)
        logger.info("Unable to map old seabird sensor header to appropriate variables")
        return dataset
    else:
        # If no calibration detected give a warning and return dataset
        logger.info("No Seabird sensors information was detected")
        return dataset

    # Match instrument variables to their associated variables
    for name, sensor_variable in sensors_map.items():
        if match_by == "sdn_parameter_urn":
            if name not in seabird_to_bodc:
                logger.warning("Missing Seabird to BODC mapping of: %s", name)
                continue
            values = [f"SDN:P01::{item}" for item in seabird_to_bodc[name]]
        else:
            values = [name]

        has_matched = False
        for value in values:
            matched_variables = dataset.filter_by_attrs(**{match_by: value})

            # Some variables are not necessearily BODC specifc
            # we'll try to match them based on the long_name
            if (
                len(matched_variables) > 1
                and match_by == "sdn_parameter_urn"
                and ("Fluorometer" in name or "Turbidity" in name)
            ):
                # Find the closest match based on the file name
                var_longname = difflib.get_close_matches(
                    name,
                    [
                        matched_variables[var].attrs["long_name"]
                        for var in matched_variables
                    ],
                )
                matched_variables = matched_variables[
                    [
                        var
                        for var in matched_variables
                        if dataset[var].attrs["long_name"] in var_longname
                    ]
                ]

                # If there's still multiple matches give a warning
                if len(matched_variables) > 1:
                    logger.warning(
                        "Unable to link multiple %s instruments via sdn_parameter_urn attribute.",
                        name,
                    )

            for var in matched_variables:
                if "instrument" in dataset[var].attrs:
                    dataset[var].attrs["instrument"] += "," + sensor_variable
                else:
                    dataset[var].attrs["instrument"] = sensor_variable
                has_matched = True
        if not has_matched:
            logger.info("Failed to match instrument %s to any variables.", name)

    return dataset
