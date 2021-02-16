from tools import merge
import geopandas
import pandas as pd
from datamerger import MergerDF
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

        census_vtd_filename = f"census/tl_2020_{fips_code}_vtd20.shp"
        mcdonald_shapefile_filename = f"VEST/{postal_code.lower()}_2018.shp"
        if os.path.isfile(census_vtd_filename) and os.path.isfile(mcdonald_shapefile_filename):
            # Setup
            census_vtd = geopandas.read_file(census_vtd_filename)
            mcdonald_shapefile = geopandas.read_file(mcdonald_shapefile_filename)

            # 2018, VEST
            augmented_shapefile = merge.augment_shapefile(census_vtd, mcdonald_shapefile, threshold=-1)
            augmented_shapefile.to_file(f"drafts/{postal_code}_with_2018.shp") # checkpoint

            # Open elections
            medsl_returns = pd.read_csv("medsl/2016-precinct-state.csv", encoding = "ISO-8859-1")
            columns = map(lambda x: x.upper(), medsl_returns.columns)
            mapping = {**dict(zip(medsl_returns.columns, columns)), "county_fips": "COUNTYFP20", "state_fips": "STATEFP20", "precinct": "VTDST20", "state": "STATE_NAME"}

            # TODO: Flatten by county
            medsl_returns_pa = medsl_returns[medsl_returns["state_postal"] == "PA"].rename(columns=mapping)
            medsl_returns_pa["COUNTYFP20"] = medsl_returns_pa["COUNTYFP20"].apply(lambda x: str(int(x)-42000).zfill(3))
            medsl_returns_pa["VTDST20"] = medsl_returns_pa["VTDST20"].apply(lambda x: str(int(x)).zfill(6))
            del medsl_returns_pa["YEAR"]
            del medsl_returns_pa["SPECIAL"]
            medsl_returns_pa = medsl_returns_pa.pivot(columns="OFFICE")
            print(medsl_returns_pa.head(), medsl_returns_pa.columns)

            PA_2016_shapefile = augmented_shapefile.merge(medsl_returns_pa, how="left", on=["COUNTYFP20", "VTDST20"])
            PA_2016_shapefile.to_file("drafts/PA_with_2016.shp") # checkpoint
        else:
            print(census_vtd_filename, mcdonald_shapefile_filename)

    # for count, attributes in tqdm.tqdm(augmented_shapefile.iterrows()):
    #     if not attributes.get("COUNTYFP"):
    #         print(attributes)

    # shapefile_list = MergerDF(augmented_shapefile,
    #                           medsl_returns_pa,
    #                           sorter=lambda x: str(int(x["COUNTYFP"])) + str(x["VTDST"]).lstrip("0"),
    #                           normalize=lambda x: mapping.get(x)).merge(
    #                               lambda x, y: x["COUNTYFP"] == y["COUNTYFP"] and str(x["VTDST"]).lstrip("0") == str(y["VTDST"]).lstrip("0"),
    #                               MergerDF.append)
    # shapefile_additional = geopandas.GeoDataFrame(shapefile_list)

    # shapefile_additional.to_file("final/PA_all.shp")
