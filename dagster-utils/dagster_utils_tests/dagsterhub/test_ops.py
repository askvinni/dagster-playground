import pandas as pd
from dagster import build_op_context

from dagster_utils.dagsterhub.ops import webapioutput_to_sinkinput
from dagster_utils.lib import UtilsWebAPIOutputType


def test_webapi_to_sinkinput():
    with build_op_context(config={"dest_asset": "my_asset"}) as context:
        res = webapioutput_to_sinkinput(
            context,
            obj=UtilsWebAPIOutputType(data=[{"foo": "bar"}, {"foo": "baz"}]),
        )

        assert res.load_to_snow
        assert res.dest_asset == "my_asset"
        assert all(pd.DataFrame([{"foo": "bar"}, {"foo": "baz"}]) == res.data)
