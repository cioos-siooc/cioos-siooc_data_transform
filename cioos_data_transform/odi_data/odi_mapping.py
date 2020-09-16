
import uuid
from netCDF4 import Dataset

class OdiCtdMapping(object):

    def __init__(self, ncfile):
        self.nc_obj = Dataset(ncfile, 'r', format='NETCDF4')
        self.nc = self.nc_obj.__dict__
        self.standard_names = []
        self.odi_dict = {}

    def create_dict(self):
        self.collect_standard_names()
        self.create_odi_dict()

    def collect_standard_names(self):
        """ Get all the standard names for the data variables into a list. """
        for nc_var in self.nc_obj.variables.values():
            if hasattr(nc_var, 'standard_name'):
                self.standard_names.append(nc_var.standard_name)

    def create_odi_dict(self):
        self.odi_dict = {
            'metadata': self.create_metadata(),
            'spatial': self.create_spatial(),
            'identification': self.create_identification(),
            'contact': self.create_contact(),
            'distribution': self.create_distribution(),
            'platform': self.create_platform(),
        }

    def write_yaml(self):
        pass

    def create_metadata(self):
        metadata = {
            'naming_authority': {
                'en': 'ca-gc-dfo-mpo',
                'fr': 'ca-gc-dfo-mpo',
            },
            'identifier': uuid.uuid4(),
            'language': 'en',
            'use_constraints': {
                'limitations': 'licence',
                'licence': {
                    'title': 'Open Government Licence - Canada',
                    'code': '',
                    'url': 'https://open.canada.ca/en/open-government-licence-canada',
                }
            },
            'comment': '',
            'history': {
                'en': self.nc['history'],
            }
        }
        return metadata

    def create_spatial(self):
        vertical_crs = ''
        if self.nc['geospatial_vertical_positive'] == 'down':
            # Positive depth from surface: https://epsg.io/5831
            vertical_crs = 'EPSG:5831'
        elif self.nc['geospatial_vertical_positive'] == 'up':
            # Positive height from sea floor https://epsg.io/5829
            vertical_crs = 'EPSG:5829'
        spatial = {
            'bbox': [
                self.nc['geospatial_lat_min'],
                self.nc['geospatial_lon_min'],
                self.nc['geospatial_lat_max'],
                self.nc['geospatial_lon_max']
            ],
            'vertical': [
                self.nc['geospatial_vertical_min'],
                self.nc['geospatial_vertical_max']
            ],
            'vertical_crs': vertical_crs
        }
        return spatial

    def create_identification(self):
        identification = {
            'title': {
                'en': f"{self.nc['platform']} - {self.nc['data_type']}",
                'fr': f"{self.nc['platform']} - {self.nc['data_type']}"
            },
            'abstract': {
                'en': 'Abstract',
                'fr': 'Abstrait'
            },
            'dates': {
                'created': self.nc['NCO']
            },
            'keywords': {
                'default': {
                    'en': self.standard_names,
                    'fr': self.standard_names
                },
                # TODO: create mapping between DFO standard names and CIOOS EOVs
                'eov': ['seaWaterTemperature', 'salinity', 'oxygen']
            },
            'temporal_begin': self.nc['time_coverage_start'],
            'temporal_end': self.nc['time_coverage_end'],
            'temporal_duration': self.nc['time_coverage_duration'],
            # TODO: Does nc['sampling_interval'] go here or in instrument?
            'status': 'completed',
        }
        return identification

    def create_contact(self):
        contact = [
            {
                'roles': ['owner'],
                'organization': {
                    'name': self.nc['institution'],
                    'url': 'https://www.bio.gc.ca/index-en.php',
                    'address': (
                        'Bedford Institute of Oceanography'
                        'P.O. Box: 1006'
                        'B2Y 4A2'
                    ),
                    'city': 'Dartmouth, NS',
                    'country': 'Canada',
                    'email': 'WebmasterBIO-IOB@dfo-mpo.gc.ca',
                },
                'individual': {
                    'name': 'BIO Webmaster',
                    'position': 'Webmaster',
                    'email': 'WebmasterBIO-IOB@dfo-mpo.gc.ca',
                }
            },
            {
                'roles': ['principalInvestigator'],
                'individual': {
                    'name': self.nc['chief_scientist'],
                    # TODO: fix following hardcoded values
                    'position': 'Research Scientist',
                    'email': 'david.hebert@dfo-mpo.gc.ca'
                }
            },
            {
                'roles': ['distributor'],
                'organization': {
                    'name': 'DFO Data Shop',
                    'url': 'https://inter-j01.dfo-mpo.gc.ca/odiqry/index-e.html',
                    'email': 'datashop@mar.dfo-mpo.gc.ca',
                }
            },
            {
                'roles': ['custodian'],
                'individual': {
                    'name': 'Diana Cardoso',
                    'email': ''
                },
            },
        ]
        return contact

    def create_distribution(self):
        distribution = [
            {
                # TODO: put ERDDAP URL for dataset here
                'url': ''
                # TODO: 'name' and 'description' elements also mandatory here...
            }
        ]
        return distribution

    def create_platform(self):
        platform_name = (
            f"{self.nc['platform']} ({self.nc['cruise_number']}) "
            f"mooring {self.nc['mooring_number']})"
        )
        platform = {
            # Note: platform->name maps to CI_Citation title
            'name': platform_name,
            # platform->id maps to identifier->code (NERC C17 recommended)
            # 18HU = NERC C17 ID for DFO CCGS Hudson vessel
            # http://vocab.nerc.ac.uk/collection/C17/current/18HU/
            'id': '18HU',
            'authority': 'International Council for the Exploration of the Sea (ICES)',
            # TODO: role should be owner or resourceProvider or publisher?
            'role': 'owner',
            # TODO: add URL http://vocab.nerc.ac.uk/collection/C17/current/18HU/ to identifier reference
            # TODO: add email: info@ices.dk

            # TODO: 'description' not yet populated in metadata-xml (present in profile).
            # TODO: 'CCGS Hudson' info could be parsed from element 'ODF_HISTORY_9' from nc file.
            'description': (
                f"{platform_name}, NERC L06 keywords: (research vessel, mooring),"
                f"CCGS Hudson"
            )
        }
        return platform








