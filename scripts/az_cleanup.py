import geopandas
import string

CDE_MAP = {
    "GN": "011",
    "PM": "019",
    "MC": "013",
    "YA": "025",
    "YU": "027",
    "CH": "003",
    "GI": "007",
    "MO": "015",
    "SC": "023",
    "CN": "005",
    "PN": "021",
    "LP": "012",
    "AP": "001",
    "NA": "017",
    "GM": "009"
}

def standardize_precinct_name(name: str) -> str:
    return str(int("".join([x for x in name if x.isnumeric()]))).zfill(6)

orig_shapefile = geopandas.read_file("VEST/az_2018.shp")
precinct_list = []
for count, attributes in orig_shapefile.iterrows():
    attributes["VTDST"] = standardize_precinct_name(attributes["PCTNUM"])
    try:
        attributes["COUNTYFP"] = CDE_MAP[attributes["CDE_COUNTY"]]
    except KeyError:
        print(attributes["CDE_COUNTY"])

    precinct_list.append(attributes)

cleaned_shapefile = geopandas.GeoDataFrame(precinct_list)
cleaned_shapefile.to_file("VEST/az_2018_cleaned.shp")
