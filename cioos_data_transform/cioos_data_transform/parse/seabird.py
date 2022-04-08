"""Module that regroups tools used to integrate data from seabird instruments."""
import re
import xmltodict
import json

import pandas as pd
import difflib
from xml.parsers.expat import ExpatError
import logging

logger = logging.getLogger(__name__)

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
    "Transmissometer, WET Labs C-Star": ["ATTNZS01"],
    "Transmissometer, Chelsea/Seatech": ["ATTNZS01"],
    "Turbidity Meter, WET Labs, ECO-NTU": ["TURBXX01", "VSCTXX01"],
    "Turbidity Meter, Seapoint": ["TURBXX01", "VSCTXX01"],
    "OBS, Backscatterance (D & A)": ["TURBXX01", "VSCTXX01"],
    "pH": ["PHMASS01", "PHXXZZ01"],
    "OBS, WET Labs, ECO-BB": ["VSCTXX01"],
    "OBS, Seapoint Turbidity": ["VSCTXX01", "TURBXX01", "VSCTXX01"],
    "SPAR/Surface Irradiance": ["IRRDSV01"],
    "SPAR, Biospherical/Licor": ["IRRDSV01"],
    "Dr. Haardt BackScatter Fluorometer": [],
    "User Polynomial": [],
    "User Polynomial, 2": [],
    "User Polynomial, 3": [],
}


def get_seabird_instrument_from_header(seabird_header):
    """ Retrieve main instrument model from Sea-Bird CNV header"""
    instrument = re.findall(
        "\* (?:Sea\-Bird ){0,1}SBE (?P<sampler>\d[^\s]*)", seabird_header
    )
    if instrument:
        return f"Sea-Bird SBE {''.join(instrument)}"
    else:
        None


def get_sbe_instrument_type(instrument):
    if re.match("SBE\s*(9|16|19|25|37)"):
        return "CTD"
    else:
        logger.warning(f"Unknown instrument typt for {instrument}")
        None


def get_seabird_processing_history(seabird_header):
    sbe_hist = "\# (datcnv|filter|align|celltm|loopedit|derive|Derive|binavg|split|strip|section|wild|window).*"
    if "# datcnv" in seabird_header:
        return "\n".join(
            [line for line in seabird_header.split("\n") if re.match(sbe_hist, line)]
        )
    else:
        logger.warning("Failed to retrieve Seabird Processing Modules history")
        return None


def update_attributes_from_seabird_header(
    ds, seabird_header, parse_manual_inputs=False
):
    # Instrument
    ds.attrs["instrument"] = get_seabird_instrument_from_header(seabird_header)

    # Bin Averaged
    binavg = re.search(
        "\# binavg_bintype \= (?P<bintype>.*)\n\# binavg_binsize \= (?P<binsize>\d+)\n",
        seabird_header,
    )
    if binavg:
        bin_str = f"{binavg['binsize']} {binavg['bintype']}"
        ds.attrs["geospatial_vertical_resolution"] = bin_str
        if "decibar" in binavg["bintype"]:
            vars = [
                var for var in ds.filter_by_attrs(standard_name="sea_water_pressure")
            ]
            binvar = vars[0] if vars else "sea_water_pressure"
        elif "second" in bin_str or "hour" in bin_str:
            vars = [var for var in ds.filter_by_attrs(standard_name="time")]
            binvar = vars[0] if vars else "time"
        elif "meter" in bin_str or "hour" in bin_str:
            vars = [var for var in ds.filter_by_attrs(standard_name="depth")]
            binvar = vars[0] if vars else "depth"
        elif "scan" in bin_str:
            binvar = "Scan Count"
        else:
            logger.error(f"Unknown binavg method: {bin_str}")

        # Add cell method attribute and geospatial_vertical_resolution global attribute
        if "decibar" in bin_str or "meter" in bin_str:
            ds.attrs["geospatial_vertical_resolution"] = bin_str
        elif "second" in bin_str or "hour" in bin_str:
            ds.attrs["time_coverage_resolution"] = pd.Timedelta(bin_str).isoformat()
        for var in ds:
            if (len(ds.dims) == 1 and len(ds[var].dims) == 1) or binvar in ds[var].dims:
                ds[var].attrs["cell_method"] = f"{binvar}: mean (interval: {bin_str})"

    # Manual inputs
    manual_inputs = re.findall("\*\* (?P<key>.*): (?P<value>.*)\n", seabird_header)
    if parse_manual_inputs:
        for key, value in manual_inputs:
            ds.attrs[key.replace(" ", "_").lower()] = value

    return ds


def generate_instruments_variables_from_xml(ds, seabird_header):

    # Retrieve Sensors xml section within seabird header
    calibration_xml = re.sub(
        "\n\#\s",
        "\n",
        re.search("\<Sensors .+\<\/Sensors\>", seabird_header, re.DOTALL)[0],
    )

    # Read XML and commented lines, drop encoding line
    try:
        sensors = xmltodict.parse(calibration_xml)["Sensors"]["sensor"]
    except ExpatError:
        logger.error("Failed to parsed Sea-Bird Instrument Calibration XML")
        return ds, {}

    sensors_comments = re.findall(
        "\s*\<!--\s*(Frequency \d+|A/D voltage \d+|.* voltage|Count){1}, (.*)-->\n",
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
        return ds, {}

    # Split each sensor calibrations to a dictionary
    sensors_map = {}
    for sensor, sensor_comment in zip(sensors, sensors_comments):
        sensor_key = list(sensor.keys())[1].strip()
        attrs = sensor[sensor_key]
        channel, description = sensor_comment

        # Define senor variable name
        if "UserPolynomial" in sensor_key and attrs.get("SensorName"):
            sensor_name = attrs.pop("SensorName").strip()
            sensor_var_name = re.sub("[^\d\w]+", "_", sensor_name)
        else:
            sensor_var_name = sensor_key
            sensor_name = description.strip()

        if "Oxygen" in sensor_name:
            subsensors = re.search("Current|Temp|Phase|Concentration", description)
            if subsensors:
                sensor_var_name += "_" + subsensors[0]

        # Add trailing number if present
        if re.search(", \d+", sensor_name):
            sensor_number = int(re.search(", (\d+)", sensor_name)[1])
            sensor_var_name += f"_{sensor_number}"
        else:
            sensor_number = 1

        if sensor_var_name in ds:
            logger.warning(f"Duplicated instrument variable {sensor_var_name}")

        # Try fit IOOS 1.2 which request to add a instrument variable for each
        # instruments and link this variable to data variable by using the instrument attribute
        # https://ioos.github.io/ioos-metadata/ioos-metadata-profile-v1-2.html#instrument
        ds[sensor_var_name] = json.dumps(attrs)
        ds[sensor_var_name].attrs = {
            "calibration_date": pd.to_datetime(
                attrs.pop("CalibrationDate"), errors="ignore"
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

    return ds, sensors_map


def generate_instruments_variables_from_sensor(ds, seabird_header):
    """Parse older Seabird Header sensor information and generate instrument variables"""
    sensors = re.findall("\# sensor (?P<id>\d+) = (?P<text>.*)\n", seabird_header)
    for id, sensor in sensors:
        if "Voltage" in sensor:
            sensor_items = sensor.split(",", 1)
            attrs = {
                "channel": sensor_items[0],
                "sensor_description": sensor_items[0].replace("Voltage", "").strip()
                + sensor_items[1],
            }
        else:
            attrs_dict = re.search(
                "(?P<channel>Frequency\s+\d+|Stored Volt\s+\d+|Extrnl Volt  \d+)\s+(?P<sensor_description>.*)",
                sensor,
            )
            if attrs_dict == None:
                logger.error(f"Failed to read sensor item: {sensor}")
                continue
            attrs = attrs_dict.groupdict()
        sensor_code = f"sensor_{id}"
        ds[sensor_code] = sensor
        ds[sensor_code].attrs = attrs
    return ds


def add_seabird_instruments(ds, seabird_header, match_by="long_name"):
    """
    Extract seabird sensor information and generate instrument variables which follow the IOOS 1.2 convention
    """
    # Retrieve sensors information
    if "# <Sensors count" in seabird_header:
        ds, sensors_map = generate_instruments_variables_from_xml(ds, seabird_header)
    elif "# sensor" in seabird_header:
        ds = generate_instruments_variables_from_sensor(ds, seabird_header)
        logger.info("Unable to map old seabird sensor header to appropriate variables")
        return ds
    else:
        # If no calibration detected give a warning and return dataset
        logger.info("No Seabird sensors information was detected")
        return ds

    # Match instrument variables to their associated variables
    for name, sensor_variable in sensors_map.items():
        if match_by == "sdn_parameter_urn":
            if name not in seabird_to_bodc:
                logger.warning(f"Missing Seabird to BODC mapping of: {name}")
                continue
            values = [f"SDN:P01::{item}" for item in seabird_to_bodc[name]]
        else:
            values = [name]

        has_matched = False
        for value in values:
            vars = ds.filter_by_attrs(**{match_by: value})

            # Some variables are not necessearily BODC specifc, we'll try to match them based on the long_name
            if (
                len(vars) > 1
                and match_by == "sdn_parameter_urn"
                and ("Fluorometer" in name or "Turbidity" in name)
            ):
                # Find the closest match based on the file name
                var_longname = difflib.get_close_matches(
                    name, [vars[var].attrs["long_name"] for var in vars]
                )
                vars = vars[
                    [var for var in vars if ds[var].attrs["long_name"] in var_longname]
                ]

                # If there's still multiple matches give a warning
                if len(vars) > 1:
                    logger.warning(
                        f"We can't link easily multiple {name} instruments via sdn_parameter_urn attribute. Any related data will be link to both instuments."
                    )

            for var in vars:
                if "instrument" in ds[var].attrs:
                    ds[var].attrs["instrument"] += "," + sensor_variable
                else:
                    ds[var].attrs["instrument"] = sensor_variable
                has_matched = True
        if has_matched == False:
            logger.info(f"Failed to match instrument {name} to any variables.")

    return ds
