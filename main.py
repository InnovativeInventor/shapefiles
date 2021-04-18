from tools import merge
import IPython
from openelections import intify
import geopandas
from pmatcher import PrecinctMatcher
import pandas as pd
import tqdm
from pmatcher import PrecinctMatcher
import math
import json
import os
import openelections
import maup


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
        augmented_shapefile = merge.augment_shapefile(
            census_vtd,
            mcdonald_shapefile,
            primary_map=primary_map,
            secondary_map=secondary_map,
            threshold=-1,
        )
    except IndexError:
        print("Skipping . . .")
        return census_vtd, False

    augmented_shapefile.to_file(f"drafts/{postal_code}_with_2018.shp")  # checkpoint
    return augmented_shapefile, True

    # print(postal_code)
    # if postal_code.upper() == "PA":
    #     reader = openelections.OpenElectionsReader()
    #     year_data = reader.fetch_election_csv(handler=openelections.pa)
    #     print(year_data.head())
    #     finished_shapefile = merge.augment_shapefile(augmented_shapefile, year_data, primary_map=primary_map, threshold=2)
    #     finished_shapefile.to_file(f"drafts/{postal_code}_with_2014_2018.shp")


def load_state_cvap_shapes(postal_code="PA"):
    """
    From JN
    """
    state_cvap_bgs = pd.read_csv(f"../PA/{postal_code}_cvap_2015_2019.csv")
    race_names = {
        "Total": "TOT",
        "Not Hispanic or Latino": "NH",
        "American Indian or Alaska Native Alone": "NH_AMIN",
        "Asian Alone": "NH_ASIAN",
        "Black or African American Alone": "NH_BLACK",
        "Native Hawaiian or Other Pacific Islander Alone": "NH_NHPI",
        "White Alone": "NH_WHITE",
        "American Indian or Alaska Native and White": "NH_2MORE",
        "Asian and White": "NH_2MORE",
        "Black or African American and White": "NH_2MORE",
        "American Indian or Alaska Native and Black or African American": "NH_2MORE",
        "Remainder of Two or More Race Responses": "NH_2MORE",
        "Hispanic or Latino": "HISP",
    }

    state_cvap_bgs.replace(to_replace=race_names, inplace=True)

    state_cvap_bgs = (
        state_cvap_bgs.groupby(["geoname", "lntitle", "geoid"])
        .agg(
            {
                "cit_est": "sum",
                "cvap_est": "sum",
                # "tot_est": "sum",
            }
        )
        .reset_index()
    )
    state_cvap_bgs = state_cvap_bgs.pivot(
        # index="geoid", columns="lntitle", values=["cvap_est", "cit_est", "tot_est"]
        index="geoid", columns="lntitle", values=["cvap_est", "cit_est"]
    )
    # state_cvap_bgs.rename(columns={"cvap_est": "CVAP", "cit_est": "CPOP", "tot_est": "POP"}, inplace=True)
    state_cvap_bgs.rename(columns={"cvap_est": "CVAP", "cit_est": "CPOP"}, inplace=True)

    state_cvap_bgs.columns = [
        "_".join(col).strip() for col in state_cvap_bgs.columns.values
    ]
    state_cvap_bgs = state_cvap_bgs.rename(columns={"GEOID_": "GEOID"})

    to_rename = {
        "CVAP_HISP": "HCVAP",
        "CPOP_HISP": "HCPOP",
        "POP_HISP": "HPOP",
        "CVAP_NH": "NHCVAP",
        "CPOP_NH": "NHCPOP",
        "POP_NH": "NHPOP",
        "CVAP_NH_2MORE": "2MORECVAP",
        "CPOP_NH_2MORE": "2MORECPOP",
        "POP_NH_2MORE": "2MOREPOP",
        "CVAP_NH_AMIN": "AMINCVAP",
        "CPOP_NH_AMIN": "AMINCPOP",
        "POP_NH_AMIN": "AMINPOP",
        "CVAP_NH_ASIAN": "ASIANCVAP",
        "CPOP_NH_ASIAN": "ASIANCPOP",
        "POP_NH_ASIAN": "ASIANPOP",
        "CVAP_NH_BLACK": "BCVAP",
        "CPOP_NH_BLACK": "BCPOP",
        "POP_NH_BLACK": "BPOP",
        "CVAP_NH_NHPI": "NHPICVAP",
        "CPOP_NH_NHPI": "NHPICPOP",
        "POP_NH_NHPI": "NHPIPOP",
        "CVAP_NH_WHITE": "WCVAP",
        "CPOP_NH_WHITE": "WCPOP",
        "POP_NH_WHITE": "WPOP",
        "CVAP_TOT": "CVAP",
        "CPOP_TOT": "CPOP",
        "POP_TOT": "TOTPOP",
    }

    state_cvap_bgs = state_cvap_bgs.rename(columns=to_rename)
    state_cvap_bgs.reset_index(inplace=True)
    state_cvap_bgs["GEOID"] = state_cvap_bgs["geoid"].apply(lambda x: x[7:])

    return state_cvap_bgs


if __name__ == "__main__":
    with open("fips.json") as f:
        fips = json.load(f)

    county_fips_mapping = pd.read_csv("fips.csv")

    with open("office_abbreviations.json") as f:
        office_abbreviations = json.load(f)

    for fips_code, each_state in tqdm.tqdm(fips.items()):
        if "state" not in each_state:
            continue

        postal_code = each_state["abbreviation"]
        name = each_state["name"]

        census_vtd_filename = (
            f"../census/tl_2020_{str(int(fips_code)).zfill(2)}_vtd20.shp"
        )

        if os.path.isfile(census_vtd_filename):
            census_vtd = geopandas.read_file(census_vtd_filename)
            suffix = "with"

            # VEST
            years_success = []
            # """ for speed reasons
            for year in [2016, 2018, 2020]:
                mcdonald_shapefile_filename = f"VEST/{postal_code.lower()}_{year}.shp"
                if os.path.isfile(
                    f"VEST/{postal_code.lower()}_{year}_cleaned.shp"
                ):  # prefer cleaned shapefile over raw
                    mcdonald_shapefile_filename = (
                        f"VEST/{postal_code.lower()}_{year}_cleaned.shp"
                    )

                if os.path.isfile(mcdonald_shapefile_filename):
                    census_vtd, success = merge_census_with_vest(
                        census_vtd, mcdonald_shapefile_filename
                    )
                    if success:
                        years_success.append(year)
                        print(f"Added year {year} to {postal_code}!")
                        suffix += "_" + str(year)
            # """

            census_vtd = census_vtd[~census_vtd.GEOID20.duplicated()]
            # IPython.embed()
            if suffix != "with":
                census_vtd.to_file(f"drafts/{postal_code}_{suffix}_VEST")
            else:
                census_vtd.to_file(f"drafts/{postal_code}")

            if postal_code.lower() == "pa":
                county_normalized_mappings = {
                    x.lower().replace("county", "").rstrip(): x
                    for x in set(census_vtd["COUNTY_NAM"])
                    if x
                }

                # 2014 merge
                # if 2014 not in years_success:

                # 2020 merge
                if 2020 not in years_success:
                    pa_2020_data = pd.read_csv(
                        "openelections/openelections-data-pa/2020/20201103__pa__general__precinct.csv"
                    )
                    pa_2020_county_normalized_mappings = {
                        x.lower().replace("county", "").rstrip(): x
                        for x in set(pa_2020_data["county"])
                        if x
                    }
                    pa_2020_data["votes"] = [
                        int(str(x).replace(",", ""))
                        if not (isinstance(x, float) and math.isnan(x))
                        else 0
                        for x in pa_2020_data["votes"]
                    ]
                    pa_2020_data["precinct"] = [
                        "".join(
                            [y for y in x.split() if y.lower() not in ["mail", "prov"]]
                        )
                        for x in pa_2020_data["precinct"]
                    ]
                    pa_combined_data = (
                        pa_2020_data.groupby(
                            ["precinct", "county", "party", "candidate", "office"]
                        )
                        .sum(numeric_only=True)
                        .reset_index()
                    )

                    county_mappings = {}
                    for (
                        normal_county_name,
                        county_name,
                    ) in county_normalized_mappings.items():
                        assert normal_county_name in county_normalized_mappings
                        # matching counties!
                        primary_names = census_vtd[
                            census_vtd["COUNTY_NAM"] == county_name
                        ]["NAME"]
                        secondary_names = pa_combined_data[
                            pa_combined_data["county"]
                            == pa_2020_county_normalized_mappings[normal_county_name]
                        ]["precinct"]

                        matcher = PrecinctMatcher(
                            [x for x in primary_names if x],
                            [x for x in secondary_names if x],
                        )
                        progress_filename = (
                            f"drafts/{postal_code}-{normal_county_name}-progress.json"
                        )
                        if os.path.isfile(progress_filename):
                            matcher.load_progress(progress_filename)
                        matches = matcher.insensitive_normalized()

                        # try:
                        #     matches = matcher.default(n=16)
                        # except (ValueError, KeyboardInterrupt) as e:
                        #     print(e)
                        # matcher.save_progress(progress_filename)
                        # debug:
                        county_mappings[normal_county_name] = matches

                    pa_new_data = []
                    empty = 0
                    for count, row in census_vtd.iterrows():
                        # csv parsed from: https://www.nrcs.usda.gov/wps/portal/nrcs/detail/national/home/?cid=nrcs143_013697
                        if not row["COUNTY_NAM"]:
                            fips_mappings = county_fips_mapping[
                                county_fips_mapping["STATE"] == postal_code.upper()
                            ]
                            try:
                                alt_name = list(
                                    set(
                                        fips_mappings[
                                            fips_mappings["FIPS"]
                                            == str(int(row["COUNTYFP"])).zfill(3)
                                        ]["COUNTY"]
                                    )
                                )[0]
                            except TypeError:
                                alt_name = list(
                                    set(
                                        fips_mappings[
                                            fips_mappings["FIPS"]
                                            == str(int(row["COUNTYFP20"])).zfill(3)
                                        ]["COUNTY"]
                                    )
                                )[0]

                            print(row["COUNTYFP"], alt_name, row.keys())
                            row["COUNTY_NAM"] = alt_name
                            # pa_new_data.append(row)
                            # continue # skip, this is strange and needs further investigation

                        normal_county = (
                            row["COUNTY_NAM"].lower().replace("county", "").rstrip()
                        )
                        matches = county_mappings[normal_county]
                        other_row_county = pa_combined_data[
                            pa_combined_data["county"]
                            == pa_2020_county_normalized_mappings[normal_county]
                        ]

                        try:
                            other_row_precincts = other_row_county[
                                other_row_county["precinct"] == matches[row["NAME"]]
                            ]
                        except KeyError:
                            empty += 1

                        for _, other_row in other_row_precincts.iterrows():
                            try:
                                office = (
                                    "G20" + office_abbreviations[other_row["office"]]
                                )  # TODO: test
                                party = other_row["party"]
                                if len(party) == 3:
                                    party_abbrev = party.upper()
                                else:
                                    party_abbrev = {
                                        "democratic": "DEM",
                                        "republican": "REP",
                                        "libertarian": "LIB",
                                        "green": "GRN",
                                    }[other_row["party"]]

                                row[office + party_abbrev] = other_row["votes"]

                            except KeyError:
                                print("error", other_row)

                        pa_new_data.append(row)

                    print("unlabeled", empty, "total", count)

                    census_vtd = geopandas.GeoDataFrame(pa_new_data)
                    suffix += "_with_2020"
                    census_vtd.to_file(f"drafts/{postal_code}_{suffix}_VEST")

                # cleanup
                for key in [
                    "WRITEIN",
                    "PARTY",
                    "MODE",
                    "VOTES",
                    "CANDIDAT_1",
                    "CANDIDAT_2",
                    "STATEFP2_1",
                    "CANDIDATE",
                    "CANDIDATE_",
                    "CANDIDAT_3",
                    "CANDIDAT_4",
                    "CANDIDAT_5",
                    "CANDIDAT_6",
                    "CANDIDAT_7",
                    "CANDIDAT_8",
                    "CANDIDAT_9",
                    "CANDIDAT10",
                    "CANDIDAT11",
                    "CANDIDAT12",
                    "CANDIDAT13",
                    "CANDIDAT14",
                    "CANDIDAT15",
                    "from_censu",
                ]:
                    del census_vtd[key]
                state_cvap_shapes = load_state_cvap_shapes()
                block_group_shapes = geopandas.read_file(
                    "../census/tl_2010_42_bg10.shp"
                )
                block_group_with_acs = pd.merge(
                    left=block_group_shapes,
                    right=state_cvap_shapes,
                    left_on="GEOID10",
                    right_on="GEOID",
                )
                census_vtd.crs = "epsg:4269"  # figure out why this works
                census_vtd.to_crs(block_group_with_acs.crs)
                print("Converted CRS")
                census_vtd.to_file(f"drafts/{postal_code}_partial.shp")
                IPython.embed()
                # census_vtd = census_vtd[~census_vtd.GEOID20.duplicated() and ~census_vtd.VTDST.duplicated()]
                # block_group_with_acs = block_group_with_acs[~block_group_with_acs.index.duplicated()].drop_duplicates()
                census_vtd.to_file(f"drafts/{postal_code}_temp")
                bgs_to_blocks_cols = ['HCVAP', 'HCPOP', 'NHCVAP', 'NHCPOP', '2MORECVAP', '2MORECPOP', 'AMINCVAP', 'AMINCPOP', 'ASIANCVAP', 'ASIANCPOP', 'BCVAP', 'BCPOP', 'NHPICVAP', 'NHPICPOP', 'WCVAP', 'WCPOP', 'CVAP', 'CPOP']
                with maup.progress():
                    pieces = maup.intersections(
                        block_group_with_acs, census_vtd, area_cutoff=0
                    )
                    weights = (
                        block_group_with_acs["CVAP"]
                        .groupby(maup.assign(block_group_with_acs, pieces))
                        .sum()
                    )
                    weights = maup.normalize(weights, level=0)
                    census_vtd[bgs_to_blocks_cols] = maup.prorate(
                        pieces,
                        block_group_with_acs[bgs_to_blocks_cols],
                        weights=weights,
                    )
                    # precinct_block_group_assign = maup.assign(block_group_with_acs, pieces)
                # census_vtd[demo_cols] = block_group_with_acs[demo_cols].groupby(precinct_block_group_assign).sum()

                census_vtd.to_file(f"drafts/{postal_code}_{suffix}_with_ACS")

                # check suffix stuff before generalizing
                """
                shared_2010_cols = "ATG12D ATG12R GOV10D GOV10R PRES12D PRES12O PRES12R SEN10D SEN10R T16ATGD T16ATGR T16PRESD T16PRESOTH T16PRESR T16SEND T16SENR USS12D USS12R REMEDIAL GOV TS CD_2011 SEND HDIST 538DEM 538GOP 538CMPCT GOV14D GOV14R STH14DEM STH14REP USC14DEM USC14REP GOV14DEM GOV14REP STS14DEM STS14REP STH14F4B STH14R/D STH14IND USC14IND STH14LIB STH14GRN STH14NOA STH14MFH STS14R/D STS14VFI".split()

                pa_2010_shapes = geopandas.read_file("drafts/PA_old_with_2014_fallback.shp")
                pa_2010_shapes.crs = "epsg:4269"

                with maup.progress():
                    pieces = maup.intersections(pa_2010_shapes, census_vtd, area_cutoff=0)
                    weights = census_vtd["CVAP"].groupby(maup.assign(census_vtd, pieces)).sum()
                    weights = maup.normalize(weights, level=0)
                    census_vtd[shared_2010_cols] = maup.prorate(
                            pieces,
                            pa_2010_shapes[shared_2010_cols],
                            weights=weights
                    )
                """

                census_vtd.to_file(f"drafts/{postal_code}_final.shp")

        else:
            print(census_vtd_filename)
            with open("census-dl.txt", "a+") as f:
                fips_str = str(int(fips_code)).zfill(2)
                f.write(
                    f"https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/{fips_str}_{name.upper().replace(' ', '_')}/ALL_ZIPPED/tl_2020_{fips_str}_all.zip\n"
                )
