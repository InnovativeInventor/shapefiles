import geopandas
import json

shapefile = geopandas.read_file("drafts/PA_with_2018.shp")

mapping_1 = {}
mapping_2 = {}

for count, attributes in shapefile.iterrows():
    mapping_1[attributes["NAME"]] = attributes["NAME20"]
    mapping_2[attributes["NAME20"]] = attributes["NAMELSAD20"]

with open("drafts/mapping_PA_2018_1.json", "w") as f:
    json.dump(mapping_1, f)

with open("drafts/mapping_PA_2018_2.json", "w") as f:
    json.dump(mapping_2, f)
