import json
import os

import dagster._check as check
import requests
from dagster import Config, ConfigurableResource, get_dagster_logger, op
from pydantic import Field

from dagster_utils.utils.dicts import safeget

from ._types import UtilsWebAPIOutputType

logger = get_dagster_logger()

# ###############################
# DAGSTER SPECIFIC
# ###############################


class PubChemReturnParameters(Config):
    label: str = Field(..., description="PubChem property label")
    name: str = Field(None, description="PubChem property name, if available")
    alias: str = Field(..., description="Property identifier in the returned data")


class FetchPubChemCompoundByOutputName(Config):
    compounds: list[str]
    return_parameters: list[PubChemReturnParameters]


@op(
    description=(
        "Fetches all PubChem compounds passed under the config property `compounds`. "
        "Relevant properties from the PubChem response can be specified under the `return_parameters` property"
    ),
    required_resource_keys={"pubchem"},
)
def fetch_pubchem_compound_by_name(
    context,
    config: FetchPubChemCompoundByOutputName,
) -> UtilsWebAPIOutputType:
    pubchem_resource = context.resources.pubchem
    obj = pubchem_resource.fetch_compounds_by_name(
        compounds=config.compounds,
        return_parameters=config.return_parameters,
    )
    return obj


# ###############################
# API LIB
# ###############################


class UtilsPubChemClient(ConfigurableResource):
    uri: str = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
    return_type: str = "JSON"

    def fetch_compounds_by_name(
        self,
        compounds: list[str],
        return_parameters: PubChemReturnParameters,
    ):
        compounds = check.list_param(compounds, "compounds")
        return_parameters = check.list_param(return_parameters, "return_parameters")

        data = []
        not_found_compounds = []
        for compound_name in compounds:
            logger.info(f"Fetching compound with name {compound_name}")
            compound_uri = self._build_pubchem_uri("name", [compound_name])
            compound_contents = self._call_pubchem_api(compound_uri)
            props = safeget(compound_contents, "PC_Compounds", 0, "props")
            if props is None:
                not_found_compounds.append(compound_name)
            else:
                filtered_compound_contents = (
                    self._extract_values_from_compound_contents(
                        props, return_parameters
                    )
                )
                data.append(
                    {"compound_name": compound_name} | filtered_compound_contents
                )

        if len(not_found_compounds) > 0:
            logger.warning(f"Did not find compounds: {', '.join(not_found_compounds)}")

        return UtilsWebAPIOutputType(data=data)

    def _build_pubchem_uri(self, method: str = "name", *args) -> str:
        return f"{self.uri}/compound/{method}/{'/'.join(*args)}/{self.return_type}"

    def _call_pubchem_api(self, uri: str):
        res = requests.get(uri).json()
        return res

    def _extract_values_from_compound_contents(
        self,
        properties: list[dict],
        return_params: PubChemReturnParameters,
    ):
        res = {}
        for return_param in return_params:
            filtered = [
                prop
                for prop in properties
                if return_param.label == prop["urn"]["label"]
                and return_param.name in [safeget(prop, "urn", "name"), None]
            ][0]

            value_key = list(filtered["value"].keys())[0]

            res[return_param.alias] = filtered["value"][value_key]

        return res


# ###############################
# STUB
# ###############################


class StubUtilsPubChemClient(UtilsPubChemClient):
    stubs_dir: str
    uri: str = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
    return_type: str = "JSON"

    def _build_pubchem_uri(self, method: str = "name", *args) -> str:
        return [method] + list(*args)

    def _call_pubchem_api(self, uri):
        with open(os.path.join(self.stubs_dir, f"{uri[-1]}.json")) as f:
            return json.load(f)
