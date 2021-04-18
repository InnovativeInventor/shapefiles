import geopandas
import pmatcher
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)
import pandas as pd
import openelections

def normalize_vtdst(vtdst: str) -> str:
    if isinstance(vtdst, int):
        return str(vtdst)
    else:
        try:
            return str(int(vtdst.rstrip()))
        except:
            if vtdst.rstrip().startswith("0"):
                return normalize_vtdst(vtdst.rstrip()[1:])
            else:
                return vtdst.rstrip()

def intify(df: pd.DataFrame, attr:str) -> pd.DataFrame:
    df[attr] = list(map(normalize_vtdst, list(df[attr])))
    return year_data


def make_net(attributes: dict, fallback: bool = True) -> dict:
    """
    For debugging/making chloropleth graphs in QGIS
    """
    if fallback == True:
        if "GOV14REP" not in attributes or attributes["GOV14REP"] == 0:
            attributes["GOV14REP"] = attributes["GOV14R"]

        if "GOV14DEM" not in attributes or attributes["GOV14DEM"] == 0:
            attributes["GOV14DEM"] = attributes["GOV14D"]

    try:
        try:
            attributes["NETGOVNORM"] = (attributes["GOV14REP"] - attributes["GOV14DEM"])/(attributes["GOV14REP"] + attributes["GOV14DEM"])
        except ZeroDivisionError:
            print("ZeroDivisonError")
    except KeyError:
        pass

    return attributes

if __name__ ==  "__main__":
    fetch = False

    if fetch:
        reader = openelections.OpenElectionsReader()
        year_data = reader.fetch_election_csv(handler=openelections.pa)
        year_data_1 = intify(year_data, "VTDST")
        year_data_final = intify(year_data, "COUNTYFP")

        year_data_final.to_csv("drafts/pa-aggregate.csv")

    year_data_final = pd.read_csv("drafts/pa-aggregate.csv")

    pa_old = geopandas.read_file("scratch/PA.shp")

    # Assert unique names
    matches = {}
    geoseries_list = []

    for loc, attributes in pa_old.iterrows():
        VTDST = attributes["VTDST10"]
        COUNTYFP = attributes["COUNTYFP10"]
        county_data = year_data_final[year_data_final["COUNTYFP"] == int(COUNTYFP)]
        precincts_match = county_data[year_data_final["VTDST"] == normalize_vtdst(VTDST)]
        if len(precincts_match) == 1:
            precinct = precincts_match.iloc[0]

            matches[precinct["NAME"]] = attributes["NAME10"]

    primary = set(pa_old["NAME10"]) - set(matches.keys())
    secondary = set(year_data_final["NAME"]) - set(matches.values())

    matcher = pmatcher.PrecinctMatcher(primary, secondary)
    print(len(matches))
    matcher.results = matches
    matcher.exact()
    matcher.insensitive()
    matcher.insensitive_normalized()
    final_matches = matcher.insensitive_normalized(aggressive=True)
    print(len(final_matches))
    matcher.save_progress("drafts/pa-progress.json")

    count = 0
    # Run through again because names are not guaranteed to be unique, tf
    for loc, attributes in pa_old.iterrows():
        VTDST = attributes["VTDST10"]
        COUNTYFP = attributes["COUNTYFP10"]
        county_data = year_data_final[year_data_final["COUNTYFP"] == int(COUNTYFP)]
        precincts_match = county_data[year_data_final["VTDST"] == normalize_vtdst(VTDST)]
        if len(precincts_match) == 1:
            precinct = precincts_match.iloc[0]

            del precinct["NAME"]
            attributes_final = {**attributes, **precinct}
            geoseries_list.append(attributes_final)
        elif len(precincts_match[precincts_match["PRECINCT"] == normalize_vtdst(VTDST)]) == 1:
            print("PRECINCT match")
            precinct = precincts_match[precincts_match["PRECINCT"] == VTDST].iloc[0]

            del precinct["NAME"]
            attributes_final = {**attributes, **precinct}
            geoseries_list.append(attributes_final)
        else:
            if attributes["NAME10"] in final_matches:
                other_name = final_matches[attributes["NAME10"]]
                del final_matches[attributes["NAME10"]] # no double-assignments please!

                other_precincts = year_data_final[year_data_final["NAME"] == other_name]
                if len(other_precincts) == 1:
                    other_precinct = dict(other_precincts.iloc[0])
                    print("Matched using pmatcher", attributes["NAME10"], other_precinct["NAME"])
                    del other_precinct["NAME"]
                    attributes_final = {**attributes, **other_precinct}
                    geoseries_list.append(attributes_final)
                else:
                    count += 1
                    attributes_final = attributes
                    geoseries_list.append(attributes_final)
                    # print(attributes, other_precincts)
            else:
                count += 1
                attributes_final = attributes
                geoseries_list.append(attributes_final)

    print(count)

    print(count, len(geoseries_list), len(pa_old))
    draft_PA = geopandas.GeoDataFrame(list(map(lambda x: make_net(x, fallback=False), geoseries_list)))
    draft_PA.to_file(f"drafts/PA_old_with_2014.shp")
    draft_fallback_PA = geopandas.GeoDataFrame(list(map(lambda x: make_net(x, fallback=True), geoseries_list)))
    draft_fallback_PA.to_file(f"drafts/PA_old_with_2014_fallback.shp")
