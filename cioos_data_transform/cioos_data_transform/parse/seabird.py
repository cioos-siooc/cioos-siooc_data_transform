"""Module that regroups tools used to integrate data from seabird instruments."""
import re
import xmltodict
import logging
import json

import pandas as pd

logger = logging.getLogger(__name__)


def update_attributes_from_seabird_header(ds, seabird_header):
    # Instrument
    instrument = re.search("\* Sea\-Bird (.*) Data File:\n", seabird_header)
    sampler = re.search("SBE (?P<sampler>11plus).*\n", seabird_header)
    if instrument:
        ds.attrs[
            "instrument"
        ] = f"Sea-Bird {instrument[1]}{sampler[1] if sampler else ''}"

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
            binvar = vars[0] if vars else "Sea Pressure"
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
        if "decibar" in bin_str or  "meter" in bin_str:
            ds.attrs["geospatial_vertical_resolution"] = bin_str
        elif "second" in bin_str or "hour" in bin_str:
            ds.attrs["time_coverage_resolution"] = pd.Timedelta(bin_str).isoformat()
        for var in ds:
            if (len(ds.dims) == 1 and len(ds[var].dims) == 1) or binvar in ds[var].dims:
                ds[var].attrs["cell_method"] = f"{binvar}: mean (interval: {bin_str})"

    # Manual inputs
    station = re.search("\*\* Station_Name: (.*)\n", seabird_header)
    if station:
        ds.attrs["station"] = station[1]
        ds['station'] = station[1]
    return ds


def add_seabird_xmlcon_calibration_as_attributes(ds, seabird_header):
    """
    Extract seabird xml calibration and give back to each respective variables based on 
    sdn_parameter_urn attribute as an attribute.
    """

    seabird_to_bodc = {
        "Temperature": ["TEMPP681", "TEMPP901", "TEMPS601", "TEMPS901", "TEMPPR01"],
        "Temperature, 2": ["TEMPP682", "TEMPP902", "TEMPS602", "TEMPS902", "TEMPPR02"],
        "Pressure, Digiquartz with TC": ["PRESPR01"],
        "Conductivity": ["CNDCST01"],
        "Conductivity, 2": ["CNDCST02"],
        "Altimeter": ["AHSFZZ01"],
        "PAR/Logarithmic, Satlantic": ["IRRDUV01"],
        "Oxygen, SBE 43": ["DOXYZZ01", "OXYOCPVL01"],
        "Oxygen, SBE 43, 2": ["DOXYZZ02", "OXYOCPVL02"],
        "Fluorometer, Seapoint Ultraviolet": ["CDOMZZ01"],
        "Fluorometer, Seapoint": ["CPHLPR01"],
        "pH": ["PHMASS01", "PHXXZZ01"],
        "OBS, WET Labs, ECO-BB": ["VSCTXX01"],
        "SPAR/Surface Irradiance": ["IRRDSV01"],
    }
    # Retrieve instrument calibration xml
    calibration_xml = re.findall("\#(\s*\<.*)\n", seabird_header)

    # If no calibration detected give a warning and return dataset
    if calibration_xml == None:
        logger.warn("No Seabird XML Calibration was detected")
        return ds

    # Read XML and commented lines
    calibration_xml = "\n".join(calibration_xml)
    sensors = xmltodict.parse(calibration_xml)["Sensors"]["sensor"]
    sensors_comments = re.findall(
        "\s*\<!--\s*(Frequency \d+|A/D voltage \d+|SPAR voltage), (.*)-->\n",
        calibration_xml,
    )
    # Split each sensor calibrations to a dictionary
    sensors_attrs = {}
    for sensor in sensors:
        if len(sensor.keys()) < 2:
            continue
        sensor_key = list(sensor.keys())[1].strip()
        attrs = sensor[sensor_key]
        id = int(sensor["@Channel"])
        sensor_name = sensors_comments[id - 1][1].strip()
        sensors_attrs[sensor_name] = {
            "instrument_id": id,
            "instrument_name": sensor_name,
            "instrument_channel": sensors_comments[id - 1][0],
            "instrument_sbe_sensor_id": int(attrs.pop("@SensorID")),
            "instrument_serial_number": attrs.pop("SerialNumber"),
            "instrument_calibration_date": attrs.pop("CalibrationDate"),
            "instrument_calibration_coefficients": json.dumps(attrs),
        }
    # TODO it would be good to map Seabird SensorIDs to an L22 instrument term.

    # Add calibrations to each corresponding variable based on the gf3 code
    for name, sensor_cal in sensors_attrs.items():
        if name not in seabird_to_bodc:
            logger.warning(f"Missing Seabird to BODC mapping of: {name}")
            continue
        for bodc in seabird_to_bodc[name]:
            vars = ds.filter_by_attrs(sdn_parameter_urn=f"SDN:P01::{bodc}")
            for var in vars:
                ds[var].attrs.update(sensor_cal)

    return ds
