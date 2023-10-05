import os

from dagster import build_op_context

from dagster_utils.lib.pubchem import *

STUBS_DIR = os.path.join(os.path.dirname(__file__), "_stub", "pubchem")

# Dagster tests
def test_pubchem_resource_init():
    resource = UtilsPubChemClient()

    assert type(resource) is UtilsPubChemClient


def test_fetch_pubchem_compound_by_name_op():
    with build_op_context(
        config={
            "compounds": ["glucose"],
            "return_parameters": [
                {"label": "Molecular Weight", "alias": "molecular_weight"},
                {"label": "Molecular Formula", "alias": "molecular_formula"},
                {"label": "InChI", "alias": "inchi"},
            ],
        },
        resources={"pubchem": StubUtilsPubChemClient(stubs_dir=STUBS_DIR)},
    ) as context:
        res = UtilsWebAPIOutputType(
            data=[
                {
                    "compound_name": "glucose",
                    "molecular_weight": "180.16",
                    "molecular_formula": "C6H12O6",
                    "inchi": "InChI=1S/C6H12O6/c7-1-2-3(8)4(9)5(10)6(11)12-2/h2-11H,1H2/t2-,3-,4+,5-,6?/m1/s1",
                },
            ],
        )

        assert fetch_pubchem_compound_by_name(context) == res


def test_pubchem_fetch_compounds_by_name():
    result = StubUtilsPubChemClient(stubs_dir=STUBS_DIR).fetch_compounds_by_name(
        compounds=["glucose"],
        return_parameters=[
            PubChemReturnParameters(label="Molecular Weight", alias="molecular_weight"),
            PubChemReturnParameters(
                label="Molecular Formula", alias="molecular_formula"
            ),
            PubChemReturnParameters(label="InChI", alias="inchi"),
        ],
    )
    expected_result = UtilsWebAPIOutputType(
        data=[
            {
                "compound_name": "glucose",
                "molecular_weight": "180.16",
                "molecular_formula": "C6H12O6",
                "inchi": "InChI=1S/C6H12O6/c7-1-2-3(8)4(9)5(10)6(11)12-2/h2-11H,1H2/t2-,3-,4+,5-,6?/m1/s1",
            },
        ],
    )

    assert result == expected_result
