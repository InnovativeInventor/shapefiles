import openelections

def test_fetch():
    reader = openelections.OpenElectionsReader()
    assert reader
    assert reader.fetch_election_csv()
