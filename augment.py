import geopandas
import pandas as pd
from datamerger import MergerDF
import tqdm


if __name__ == "__main__":
    shapefile = geopandas.read_file("draft_final/PA_final.shp")

    mapping = {"county_fips": "COUNTYFP", "state_fips": "STATEFP", "precinct": "VTDST", "state": "STATE_NAME"}

    medsl_returns = pd.read_csv("medsl/2016-precinct-state.csv", encoding = "ISO-8859-1")
    medsl_returns_pa = medsl_returns[medsl_returns["state_postal"] == "PA"]

    for count, attributes in tqdm.tqdm(shapefile.iterrows()):
        if not attributes.get("COUNTYFP"):
            print(attributes)

    shapefile_list = MergerDF(shapefile,
                              medsl_returns_pa,
                              sorter=lambda x: str(int(x["COUNTYFP"])) + str(x["VTDST"]).lstrip("0"),
                              normalize=lambda x: mapping.get(x)).merge(
                                  lambda x, y: x["COUNTYFP"] == y["COUNTYFP"] and str(x["VTDST"]).lstrip("0") == str(y["VTDST"]).lstrip("0"),
                                  MergerDF.append)
    shapefile_additional = geopandas.GeoDataFrame(shapefile_list)

    shapefile_additional.to_file("final/PA_all.shp")
