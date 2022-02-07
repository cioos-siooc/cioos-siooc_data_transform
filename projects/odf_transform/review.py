"""
Set of tools used to test ODF data and metadata. This is mostly used to return
back to data provider potential issues encountered while converting ODF data.
"""
def run_bio_tests(ds):


def bio_flag_convention(quality_header):
    """Test QUALITY_HEADER to make sure the convention within the ODF is consistent."""
    bio_convention = [
        "QUALITY CODES",
        "0: Value has not been quality controlled",
        "1: Value seems to be correct",
        "2: Value appears inconsistent with other values",
        "3: Value seems doubtful",
        "4: Value seems erroneous",
        "5: Value was modified",
        "9: Value is missing",
    ]


def location_test(lat, long):
    """Review if the latitude and longitude values are good."""

