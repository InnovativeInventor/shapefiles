import gdutils
import pandas as pd
import gdutils.extract
import glob
import gdutils.datamine as dm
import gdutils.dataqa as dq
import tempfile
from typing import List, Dict
from pydantic import BaseModel
import tqdm
import re

class StateRepo(BaseModel):
    state: str
    repo_name: str
    repo_account: str
    repo_url: str

class OpenElectionsReader():
    """
    Reference: https://github.com/InnovativeInventor/automated-mggg-qa/blob/main/audit.py
    """
    def __init__(self, state: str = "PA"):
        self.state = state
        self.openelections_dir: str = tempfile.TemporaryDirectory(
            suffix="openelections"
        ).name

        # state_expr = re.compile(r"^openelections-data-\S\S$")
        state_expr = re.compile(f"^openelections-data-{self.state.lower()}$")
        self.openelections_repos: Dict[str, StateRepo] = {
            repo_name: StateRepo(
                state=repo_name.split("-")[-1],
                repo_name=repo_name,
                repo_account="openelections",
                repo_url=repo_url,
            )
            for repo_name, repo_url in dm.list_gh_repos(
                account="openelections", account_type="orgs"
            )
            if state_expr.match(repo_name)
        }

        dm.clone_gh_repos(account="openelections", account_type="orgs", repos=self.openelections_repos, outpath=self.openelections_dir)

    def fetch_election_csv(self, year=2014, kind="general", handler = lambda x: x) -> pd.DataFrame:
        loc = f"{self.openelections_dir}/openelections-data-{self.state.lower()}/{year}/*{self.state.lower()}*{kind}*precinct.csv"

        filename = glob.glob(loc)[0]

        self.election_types, dataframe = handler(pd.read_csv(filename), year=year)
        return dataframe

def pa(dataframe: pd.DataFrame, year = 2014) -> pd.DataFrame:
    if year <= 2014:
        dataframe.columns = ["year", "type", "county", "PRECINCT", "office_rank", "district", "party_rank", "ballot_position", "office", "party", "FECID", "last", "first", "middle", "suffix", "votes", "CD", "state senate district", "state house district", "municipality_type", "NAME", "breakdown_code_1", "breakdown_name_1", "breakdown_code_2", "breakdown_name_2", "bi-county-code", "MCD", "COUNTYFP", "VTDST", "PREVTDST", "PREVCD", "PREVSENDIST", "PREVHOUSEDIST"]


    election_types = []
    aggregated_data = {}
    for count, attributes in tqdm.tqdm(dataframe.iterrows()):
        uuid = hash((attributes["VTDST"],attributes["COUNTYFP"],attributes["year"],attributes["PRECINCT"]))

        office =  attributes["office"].rstrip()
        year = str(int(attributes["year"]))

        column_name = attributes["office"].upper()+str(attributes["year"])[2:]+attributes["party"]
        election_types.append(column_name)

        attributes[column_name] = int(attributes["votes"])

        del attributes["votes"]
        del attributes["FECID"]
        del attributes["party_rank"]
        del attributes["office"]
        del attributes["office_rank"]
        del attributes["party"]
        del attributes["first"]
        del attributes["last"]
        del attributes["middle"]
        del attributes["suffix"]

        if uuid in aggregated_data:
            if column_name in aggregated_data[uuid]: # combine
                attributes[column_name] += aggregated_data[uuid][column_name]
            aggregated_data[uuid] = dict({**dict(attributes), **dict(aggregated_data[uuid])})
        else:
            aggregated_data[uuid] = dict(attributes)

    dataframe =  pd.DataFrame(aggregated_data.values())

    return election_types, dataframe

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
    return df
