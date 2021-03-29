from tools import merge
from openelections import intify
import geopandas
from pmatcher import PrecinctMatcher
import pandas as pd
import tqdm
from pmatcher import PrecinctMatcher
import json
import os
import openelections

def merge_census_with_vest(census_vtd, mcdonald_shapefile_filename):
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
            return census_vtd, False

        if "NAME" in mcdonald_shapefile:
            pass
        elif "PRECINCTNA" in mcdonald_shapefile:
            secondary_map["NAME"] = "PRECINCTNA"
        else:
            return census_vtd, False

        print(f"Running on {mcdonald_shapefile_filename}")
    else:
        return census_vtd, False
    print(f"Census len: {len(census_vtd)}, VEST len: {len(mcdonald_shapefile)}")

    # 2018, VEST
    try:
        augmented_shapefile = merge.augment_shapefile(census_vtd, mcdonald_shapefile, primary_map=primary_map, secondary_map=secondary_map, threshold=-1)
    except IndexError:
        print("Skipping . . .")
        return census_vtd, False

    augmented_shapefile.to_file(f"drafts/{postal_code}_with_2018.shp") # checkpoint
    return augmented_shapefile, True

    # print(postal_code)
    # if postal_code.upper() == "PA":
    #     reader = openelections.OpenElectionsReader()
    #     year_data = reader.fetch_election_csv(handler=openelections.pa)
    #     print(year_data.head())
    #     finished_shapefile = merge.augment_shapefile(augmented_shapefile, year_data, primary_map=primary_map, threshold=2)
    #     finished_shapefile.to_file(f"drafts/{postal_code}_with_2014_2018.shp")


if __name__ == "__main__":
    with open("fips.json") as f:
        fips = json.load(f)

    for fips_code, each_state in tqdm.tqdm(fips.items()):
        if "state" not in each_state:
            continue

        postal_code = each_state["abbreviation"]
        name = each_state["name"]

        census_vtd_filename = f"../census/tl_2020_{str(int(fips_code)).zfill(2)}_vtd20.shp"

        if os.path.isfile(census_vtd_filename):
            census_vtd = geopandas.read_file(census_vtd_filename)
            suffix = "with"

            # VEST
            years_success = []
            """ for speed reasons
            for year in [2016, 2018, 2020]:
                mcdonald_shapefile_filename = f"VEST/{postal_code.lower()}_{year}.shp"
                if os.path.isfile(f"VEST/{postal_code.lower()}_{year}_cleaned.shp"): # prefer cleaned shapefile over raw
                    mcdonald_shapefile_filename = f"VEST/{postal_code.lower()}_{year}_cleaned.shp"

                if os.path.isfile(mcdonald_shapefile_filename):
                    census_vtd, success = merge_census_with_vest(census_vtd, mcdonald_shapefile_filename)
                    if success:
                        years_success.append(year)
                        print(f"Added year {year} to {postal_code}!")
                        suffix += "_" + str(year)
            """

            if suffix != "with":
                census_vtd.to_file(f"drafts/{postal_code}_{suffix}_VEST")
            else:
                census_vtd.to_file(f"drafts/{postal_code}")

            if postal_code.lower() == "pa":
                county_normalized_mappings = {x.lower().replace("county", "").rstrip():x for x in set(census_vtd["COUNTY_NAM"]) if x}

                # 2014 merge
                # if 2014 not in years_success:

                # 2020 merge
                if 2020 not in years_success:
                    pa_2020_data = pd.read_csv("openelections/openelections-data-pa/2020/20201103__pa__general__precinct.csv")
                    pa_2020_county_normalized_mappings = {x.lower().replace("county", "").rstrip():x for x in set(pa_2020_data["county"]) if x}
                    pa_2020_data["precinct"] = ["".join([y for y in x.split() if y.lower() not in ["mail", "prov"]]) for x in pa_2020_data["precinct"]]
                    pa_combined_data = pa_2020_data.groupby(["precinct", "county", "party", "candidate"]).sum(numeric_only=True).reset_index()

                    for normal_county_name, county_name in county_normalized_mappings.items():
                        assert normal_county_name in county_normalized_mappings
                        # matching counties!
                        primary_names = census_vtd[census_vtd["COUNTY_NAM"] == county_name]["NAME"]
                        secondary_names = pa_combined_data[pa_combined_data["county"] == pa_2020_county_normalized_mappings[normal_county_name]]["precinct"]

                        matcher = PrecinctMatcher([x for x in primary_names if x], [x for x in secondary_names if x])
                        progress_filename = f"drafts/{postal_code}-{normal_county_name}-progress.json"
                        if os.path.isfile(progress_filename):
                            matcher.load_progress(progress_filename)
                        try:
                            matches = matcher.default()
                        except (ValueError, KeyboardInterrupt) as e:
                            print(e)
                        matcher.save_progress(progress_filename)



        else:
            print(census_vtd_filename)
            with open("census-dl.txt", "a+") as f:
                fips_str = str(int(fips_code)).zfill(2)
                f.write(f"https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/{fips_str}_{name.upper().replace(' ', '_')}/ALL_ZIPPED/tl_2020_{fips_str}_all.zip\n")




        # 2016 MEDSL
