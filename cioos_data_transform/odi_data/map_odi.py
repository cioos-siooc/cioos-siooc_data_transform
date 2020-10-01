from cioos_data_transform.odi_data.odi_ctd_mapping import OdiCtdMapping

def main():
    ctd_file = 'C:/Users/jf482672/projects/data/bio-oesd-odi/MCTD_HUD2013004_1845_10515_1800.nc'
    yaml_file = 'C:/Users/jf482672/projects/data/bio-oesd-odi/MCTD_HUD2013004_1845_10515_1800.yaml'
    odi_ctd = OdiCtdMapping(ctd_file)
    odi_ctd.create_dict()
    odi_ctd.write_yaml(yaml_file)


if __name__ == '__main__':
    main()
