from tools import merge
import geopandas
import pandas as pd
import tqdm
from pmatcher import PrecinctMatcher
import json
import os


if __name__ == "__main__":
    with open("fips.json") as f:
        fips = json.load(f)

    for fips_code, each_state in tqdm.tqdm(fips.items()):
        if "state" not in each_state:
            continue
        postal_code = each_state["abbreviation"]
        name = each_state["name"]

        census_vtd_filename = f"/media/max/cabinet/census/tl_2020_{fips_code}_vtd20.shp"
        mcdonald_shapefile_filename = f"VEST/{postal_code.lower()}_2018.shp"
        if os.path.isfile(f"VEST/{postal_code.lower()}_2018_cleaned.shp"): # prefer cleaned shapefile over raw
            mcdonald_shapefile_filename = f"VEST/{postal_code.lower()}_2018_cleaned.shp"

        if os.path.isfile(census_vtd_filename) and os.path.isfile(mcdonald_shapefile_filename):
            # Setup
            mcdonald_shapefile = geopandas.read_file(mcdonald_shapefile_filename)
            primary_map = {"NAME": "NAMELSAD20", "COUNTYFP": "COUNTYFP20", "VTDST": "VTDST20"}
            secondary_map = {"NAME": "NAME", "COUNTYFP": "COUNTYFP", "VTDST": "VTDST"}
            if "COUNTYFP" in mcdonald_shapefile:
                if "VTDST" in mcdonald_shapefile:
                    pass
                elif "PCTNUM" in mcdonald_shapefile:
                    secondary_map["VTDST"] = "PCTNUM"
                else:
                    continue

                if "NAME" in mcdonald_shapefile:
                    pass
                elif "PRECINCTNA" in mcdonald_shapefile:
                    secondary_map["NAME"] = "PRECINCTNA"
                else:
                    continue

                print(f"Running on {mcdonald_shapefile_filename}")
            else:
                continue
            census_vtd = geopandas.read_file(census_vtd_filename)
            print(f"Census len: {len(census_vtd)}, VEST len: {len(mcdonald_shapefile)}")

            # 2018, VEST
            augmented_shapefile = merge.augment_shapefile(census_vtd, mcdonald_shapefile, primary_map=primary_map, secondary_map=secondary_map, threshold=-1)
            augmented_shapefile.to_file(f"drafts/{postal_code}_with_2018.shp") # checkpoint

        else:
            print(census_vtd_filename, mcdonald_shapefile_filename)
            with open("census-dl.txt", "a+") as f:
                fips_str = str(int(fips_code)).zfill(2)
                f.write(f"https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/{fips_str}_{name.upper().replace(' ', '_')}/ALL_ZIPPED/tl_2020_{fips_str}_all.zip\n")
                continue


        # 2016 MEDSL
        2016_medsl = pd.read_csv("medsl/")
