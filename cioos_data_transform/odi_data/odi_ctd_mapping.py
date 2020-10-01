
import uuid
import yaml
from netCDF4 import Dataset


class OdiCtdMapping(object):

    def __init__(self, ncfile):
        self.nc_obj = Dataset(ncfile, 'r', format='NETCDF4')
        self.nc = self.nc_obj.__dict__
        self.standard_names = []
        self.odi_dict = {}

    def create_dict(self):
        self.collect_standard_names()
        self.add_metadata(),
        self.add_spatial(),
        self.add_identification(),
        self.add_contact(),
        self.add_distribution(),
        self.add_platform(),

    def collect_standard_names(self):
        """ Get all the standard names for the data variables into a list. """
        for nc_var in self.nc_obj.variables.values():
            if hasattr(nc_var, 'standard_name'):
                self.standard_names.append(nc_var.standard_name)

    def write_yaml(self, yaml_filename):
        with open(yaml_filename, 'w') as yaml_file:
            yaml.dump(self.odi_dict, yaml_file, default_flow_style=False, sort_keys=False)

    def add_metadata(self):
        self.odi_dict['metadata'] = {
            'naming_authority': {
                'en': 'ca.gc.dfo-mpo',
                'fr': 'ca.gc.dfo-mpo',
            },
            'identifier': str(uuid.uuid1()),
            'language': 'en',
            'use_constraints': {
                'limitations': 'limitations',
                'licence': {
                    'title': 'Open Government Licence - Canada',
                    'code': 'open-government-licence-canada',
                    'url': 'https://open.canada.ca/en/open-government-licence-canada',
                }
            },
            'history': {
                'en': self.nc['history'],
            }
        }

    def add_spatial(self):
        self.odi_dict['spatial'] = {
            'bbox': [
                str(self.nc['geospatial_lat_min']),
                str(self.nc['geospatial_lon_min']),
                str(self.nc['geospatial_lat_max']),
                str(self.nc['geospatial_lon_max'])
            ],
            'vertical': [
                str(self.nc['geospatial_vertical_min']),
                str(self.nc['geospatial_vertical_max'])
            ],
            'vertical_positive': self.nc['geospatial_vertical_positive']
        }

    def add_identification(self):
        title = (
            f"{self.nc['data_type']}_{self.nc['cruise_number']}_"
            f"{self.nc['mooring_number']}_{self.nc['serial_number']}_"
            f"{round(float(self.nc['sampling_interval']))}"
        )
        self.odi_dict['identification'] = {
            'title': {
                'en': title,
                'fr': title,
            },
            'abstract': {
                'en': 'Abstract',
                'fr': 'Abstrait'
            },
            'dates': {
                'creation': self.nc['NCO']
            },
            'keywords': {
                'default': {
                    'en': self.standard_names,
                    'fr': self.standard_names
                },
                # TODO: create mapping between DFO standard names and CIOOS EOVs
                'eov': ['subSurfaceTemperature', 'subSurfaceSalinity', 'oxygen']
            },
            'temporal_begin': self.nc['time_coverage_start'],
            'temporal_end': self.nc['time_coverage_end'],
            'temporal_duration': self.nc['time_coverage_duration'],
            # TODO: Does nc['sampling_interval'] go here or in instrument?
            'status': 'completed',
        }

    def add_contact(self):
        self.odi_dict['contact'] = [
            {
                'roles': ['owner'],
                'organization': {
                    'name': self.nc['institution'],
                    'url': 'https://www.bio.gc.ca/index-en.php',
                    'address': (
                        'Bedford Institute of Oceanography, '
                        'P.O. Box: 1006, '
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
                    'email': 'Diana.Cardoso@dfo-mpo.gc.ca'
                },
            },
        ]

    def add_distribution(self):
        self.odi_dict['distribution'] = [
            {
                # TODO: put ERDDAP URL for dataset here
                'url': 'https://inter-j01.dfo-mpo.gc.ca/odiqry/index-e.html'
                # TODO: 'name' and 'description' elements also mandatory here...
            }
        ]

    def add_platform(self):
        platform_name = (
            f"{self.nc['platform']} ({self.nc['cruise_number']}) "
            f"mooring {self.nc['mooring_number']}"
        )
        (inst_model, inst_version) = self.nc['model'].split(' ')
        self.odi_dict['platform'] = {
            # Note: platform->name maps to CI_Citation title
            'name': platform_name,
            # platform->id maps to identifier->code (NERC C17 recommended)
            # 18HU = NERC C17 ID for DFO CCGS Hudson vessel
            # http://vocab.nerc.ac.uk/collection/C17/current/18HU/
            'id': 'http://vocab.nerc.ac.uk/collection/C17/current/18HU/',
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
            ),
            'instruments': [
                {
                    # TODO: parse and lookup NERC L22 ID from inst_model
                    'id': self.nc['serial_number'],
                    'type': { 'en': 'http://vocab.nerc.ac.uk/collection/L22/current/TOOL1393/', },
                    'description': {
                        'en': ('A series of high accuracy conductivity and temperature recorders '
                            'with integrated pressure sensors designed for deployment on moorings.')
                    },
                    'manufacturer': 'Sea-Bird',
                    'version': inst_version,
                }
            ]
        }








