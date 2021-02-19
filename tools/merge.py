import geopandas
from pmatcher import PrecinctMatcher
from shapely.strtree import STRtree
# from shapely.validation import make_valid
# import maup
import glob
import tqdm
import math

def l(string: str, n=6) -> str:
    """
    Left justify a VTDST with zeros
    """
    try:
        stripped_string = str(int(string)).rstrip()
    except ValueError:
        stripped_string = str(string).rstrip()

    return stripped_string.zfill(6-len(string))

def binary_merge_shapefiles(vtd_shapefiles):
    """
    Append/merge operations are expensive in geopandas.
    This implements a binary tree fold to efficiently merge large numbers of shapefiles.
    """
    aggregate_shapefile = geopandas.read_file(vtd_shapefiles.pop())
    split = round(len(vtd_shapefiles)**0.5)
    rounds = math.ceil(len(vtd_shapefiles)/split)

    for i in tqdm.tqdm(range(rounds)):
        if len(vtd_shapefiles) >= split:
            intermediate_vtd_shapefiles = vtd_shapefiles[:split]
            vtd_shapefiles = vtd_shapefiles[split:]
        else:
            intermediate_vtd_shapefiles = vtd_shapefiles
            vtd_shapefiles = []

        intermediate_shapefile = geopandas.read_file(intermediate_vtd_shapefiles.pop())
        for each_shapefile in tqdm.tqdm(intermediate_vtd_shapefiles):
            shp = geopandas.read_file(each_shapefile)
            intermediate_shapefile = intermediate_shapefile.append(shp)

        aggregate_shapefile = aggregate_shapefile.append(intermediate_shapefile)

    return aggregate_shapefile


def merge_shapefiles(vtd_shapefiles):
    """
    Naive fold for merging
    """
    aggregate_shapefile = geopandas.read_file(vtd_shapefiles.pop())
    for each_shapefile in tqdm.tqdm(vtd_shapefiles):
        shp = geopandas.read_file(each_shapefile)
        aggregate_shapefile = aggregate_shapefile.append(shp)

    return aggregate_shapefile

def make_valid(geo):
    """
    Drop in before shapely releases make_valid on stable
    """
    return geo.buffer(0)

def commonality(geo_1, geo_2):
    """
    Calculates commonality (as defined by intersection/union). Returns a float between 0 and 1 (inclusive).
    """
    try:
        return geo_1["geometry"].intersection(geo_2["geometry"]).area/geo_1["geometry"].union(geo_2["geometry"]).area
    except:
        geo_1_valid = make_valid(geo_1["geometry"])
        geo_2_valid = make_valid(geo_2["geometry"])
        # geo_1_valid = make_valid(geo_1["geometry"]).buffer(0)
        # geo_2_valid = make_valid(geo_2["geometry"]).buffer(0)
        try:
            return geo_1_valid.intersection(geo_2_valid).area/geo_1_valid.union(geo_2_valid).area
        except: # fail safe or deadly?
            return 1

def augment_shapefile(primary, secondary, identifying_cols=["COUNTYFP", "VTDST"],
                      primary_map = {"NAME": "NAMELSAD20", "COUNTYFP": "COUNTYFP20", "VTDST": "VTDST20"},
                      secondary_map = {"NAME": "NAME", "COUNTYFP": "COUNTYFP", "VTDST": "VTDST"}, threshold = 0.9):
    """
    primary is being augmented by shapefile 2 and shapefile 2's geometry holds precedence if threshold is between 0 and 1.
    primary's geometry holds extreme precedence if threshold is greater than 1
    secondary's geometry holds extreme precedence if threshold is less than 0
    """
    count = 0 # for debugging

    vtd_map = {}
    vtd_name_map = {}
    already_added_vtds = set()

    for location, attributes in tqdm.tqdm(secondary.iterrows()): # generate hash map
        unique_attributes = []
        for col in identifying_cols:
            unique_attributes.append(attributes[secondary_map[col]].rstrip())

        vtd_hash = hash(tuple(unique_attributes))
        vtd_map[vtd_hash] = location
        name = normalize(attributes[secondary_map["NAME"]])
        if not name in vtd_name_map: # in case of name collision
            vtd_name_map[name] = [location]
        else:
            vtd_name_map[name].append(location)

    geoseries_list = []
    for location, attributes in tqdm.tqdm(primary.iterrows()):
        attributes["from_census"] = False
        unique_attributes = []
        for col in identifying_cols:
            try:
                unique_attributes.append(attributes[primary_map[col]].rstrip())
            except TypeError as e:
                breakpoint()

        vtd_hash = hash(tuple(unique_attributes))
        name = normalize(attributes[primary_map["NAME"]])

        unique_attributes_dict = {}
        for col in identifying_cols:
            unique_attributes_dict[col] = (attributes[primary_map[col]])


        # criteria for updating, defaults to not updating
        if vtd_hash in vtd_map:
            corresponding_loc = vtd_map[vtd_hash]
            new_geometry = secondary.iloc[corresponding_loc]["geometry"]
            calculated_commonality = commonality(attributes, secondary.iloc[corresponding_loc])

            if normalize(secondary.iloc[corresponding_loc][secondary_map["NAME"]]) == normalize(name) or calculated_commonality > threshold:
                identifying_tuple = ((secondary.iloc[corresponding_loc][secondary_map["COUNTYFP"]], l(secondary.iloc[corresponding_loc][secondary_map["VTDST"]])))
                already_added_vtds.add(identifying_tuple)
                # attributes["geometry"] = new_geometry # update geometry
                attributes = {**secondary.iloc[corresponding_loc], **attributes}
                attributes["from_census"] = True

        elif name in vtd_name_map:
            commonality_list = []
            for corresponding_loc in vtd_name_map[name]:
                calculated_commonality = commonality(attributes, secondary.iloc[corresponding_loc])
                commonality_list.append(calculated_commonality)
                if secondary.iloc[corresponding_loc][secondary_map["COUNTYFP"]] == attributes[primary_map["COUNTYFP"]]: # shortcut if county names match
                    identifying_tuple = ((secondary.iloc[corresponding_loc][secondary_map["COUNTYFP"]], l(secondary.iloc[corresponding_loc][secondary_map["VTDST"]])))
                    already_added_vtds.add(identifying_tuple)
                    new_geometry = secondary.iloc[corresponding_loc]["geometry"]
                    # attributes["geometry"] = new_geometry
                    attributes = {**secondary.iloc[corresponding_loc], **attributes}
                    attributes["from_census"] = True
                    break
            else:
                if max(commonality_list) > threshold:
                    identifying_tuple = ((secondary.iloc[corresponding_loc][secondary_map["COUNTYFP"]], l(secondary.iloc[corresponding_loc][secondary_map["VTDST"]])))
                    already_added_vtds.add(identifying_tuple)
                    index = commonality_list.index(max(commonality_list))
                    corresponding_loc = vtd_name_map[name][index]
                    new_geometry = secondary.iloc[corresponding_loc]["geometry"]
                    attributes = {**secondary.iloc[corresponding_loc], **attributes}
                    # attributes["geometry"] = new_geometry
                    attributes["from_census"] = True

        geoseries_list.append(attributes)

    try:
        return geopandas.GeoDataFrame(geoseries_list)
    except IndexError as e:
        print(e)
        print(f"Precinct count: {len(geoseries_list)}")
        print(geoseries_list[0])
        return geopandas.GeoDataFrame([geoseries_list for x in geoseries_list if x.all()])

def normalize(string: str) -> str:
    p = PrecinctMatcher([],[])
    return p._normalize(string)

# vtd_shapefiles = glob.glob("census/*_vtd20.shp")
# print(vtd_shapefiles)
# merged_shapefile = merge_shapefiles(vtd_shapefiles)
# merged_shapefile.to_file("PA_vtd.shp")

# augmented_shapefile["geometry"] = maup.resolve_overlaps(augmented_shapefile["geometry"])

### working code:
# census_vtd = geopandas.read_file("census/tl_2020_42_vtd20.shp")
# mcdonald_shapefile = geopandas.read_file("data/pa_2018.shp")
# augmented_shapefile = augment_shapefile(census_vtd, mcdonald_shapefile, threshold=-1)

# augmented_shapefile.to_file("drafts/PA_with_2018.shp")
