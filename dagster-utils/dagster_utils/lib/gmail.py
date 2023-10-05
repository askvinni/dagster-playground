import base64
import json
import os
from typing import Optional

import requests
from dagster import Field, List, Out, get_dagster_logger, op

from dagster_utils.utils import check, safeget

from ._base_middleware import BaseGoogleAPI
from ._types import UtilsFileSystemOutputType

logger = get_dagster_logger()


# ###############################
# DAGSTER SPECIFIC
# ###############################


@op(
    description=str(
        "Fetches data from a GMail source. Source middlewares do only minimal transformation "
        "returns a list of dictionaries with the properties `filename`, representing the attachment name "
        "and `data`, with the base64 decoded attachment data."
        "The further processing should take care of decoding the data further, as this "
        "will make no attempt to guess the file encoding"
    ),
    required_resource_keys={"gmail"},
    config_schema={"function": {"args": Field(dict, is_required=False), "name": str}},
    out=Out(List[UtilsFileSystemOutputType]),
)
def fetch_from_gmail(context) -> List[UtilsFileSystemOutputType]:
    return context.resources.gmail.fetch(context.op_config)


# ###############################
# API LIB
# ###############################


class UtilsGMailClient(BaseGoogleAPI):
    uri: str = "https://gmail.googleapis.com/gmail/v1"
    auth_config: Optional[dict[str, str]] = {
        "name": "/access/gmail/dataextract",
        "type": "parameterStore",
    }

    def fetch(self, options) -> list[dict]:
        """Runs a different function depending on the function parameter called, provides a common starting point. Returns list of arrays."""

        funcs = {
            "extract_attachments": self._extract_attachments,
        }
        func_name = safeget(options, "function", "name")
        func_args = safeget(options, "function", "args")

        logger.info(f"Executing function {func_name} with args {json.dumps(func_args)}")

        return funcs[func_name](**func_args)

    def _extract_attachments(self, queryparams: Optional[dict] = None) -> list[dict]:
        queryparams = check.opt_dict_param(queryparams, "queryparams")

        queryparams["q"] = f"{queryparams.get('q', '')} has:attachment"
        messages = self._messages_get(queryparams=queryparams).get("messages")
        res = []

        for message in messages:
            message_id = message.get("id")

            message_attachments = self._get_attachments_from_message(message_id)
            res += message_attachments
            # Marks message as read by removing UNREAD label id.
            self._messages_modify(message_id, {"removeLabelIds": ["UNREAD"]})

        return res

    def _get_attachments_from_message(self, message_id: str) -> list:
        message_id = check.str_param(message_id, "message_id")
        message_contents = self._messages_get(message_id=message_id)
        attachments_res = []
        for part in safeget(message_contents, "payload", "parts"):
            if self._is_attachment_part(part):
                attachment_id = safeget(part, "body", "attachmentId")
                attachment_filename = safeget(part, "filename")
                attachment_contents = self._attachments_get(message_id, attachment_id)
                attachments_res.append(
                    UtilsFileSystemOutputType(
                        filename=attachment_filename,
                        content=base64.urlsafe_b64decode(attachment_contents["data"]),
                    )
                )

        return attachments_res

    def _messages_get(
        self,
        queryparams: Optional[dict] = None,
        message_id: Optional[str] = None,
    ) -> dict:
        queryparams = check.opt_dict_param(queryparams, "queryparams")
        message_id = check.opt_str_param(message_id, "message_id")
        logger.info(
            f"Querying Google get messages API for message id {message_id} with parameters {json.dumps(queryparams)}"
        )

        r = requests.get(
            f"{self.uri}/users/me/messages/{message_id}",
            params=queryparams,
            headers=self._headers,
        )

        return r.json()

    def _attachments_get(self, message_id: str, attachment_id: str) -> dict:
        message_id = check.str_param(message_id, "message_id")
        attachment_id = check.str_param(attachment_id, "attachment_id")

        logger.info(
            f"Querying Google get attachments API for message id {message_id} and attachment id {attachment_id}"
        )

        r = requests.get(
            f"{self.uri}/users/me/messages/{message_id}/attachments/{attachment_id}",
            headers=self._headers,
        )

        return r.json()

    def _messages_modify(self, message_id: str, json_body: dict) -> dict:
        message_id = check.str_param(message_id, "message_id")
        json_body = check.dict_param(json_body, "json_body")
        logger.info(
            f"Querying Google modify messages API for message id {message_id} with params {json.dumps(json_body)}"
        )

        r = requests.post(
            f"{self.uri}/users/me/messages/{message_id}/modify",
            json=json_body,
            headers=self._headers,
        )

        return r.json()

    def _is_attachment_part(self, part: dict) -> bool:
        part = check.dict_param(part, "part")

        if (
            part.get("filename", "") != ""
            and safeget(part, "body", "attachmentId") != None
        ):
            return True
        else:
            return False


# ###############################
# STUB
# ###############################


class StubUtilsGMailClient(UtilsGMailClient):
    stubs_dir: str
    message_id: str = None
    uri: str = "https://gmail.googleapis.com/gmail/v1"
    auth_config: Optional[dict[str, str]] = None

    def get_headers(self) -> str:
        return {"Authorization": f"Bearer"}

    def _messages_get(self, queryparams=None, message_id=None):
        message_id = message_id if message_id is not None else self.message_id
        if queryparams is not None:
            if message_id is not None:
                messages = [{"id": message_id, "threadId": "baz"}]
            else:
                messages = [
                    {"id": "foo", "threadId": "baz"},
                    {"id": "bar", "threadId": "qux"},
                ]
            return {
                "messages": messages,
                "resultSizeEstimate": 2,
            }
        elif message_id is not None:
            with open(os.path.join(self.stubs_dir, f"{message_id}.json")) as f:
                return json.load(f)
        else:
            return {
                "messages": [
                    {"id": "foo", "threadId": "baz"},
                    {"id": "bar", "threadId": "qux"},
                ],
                "resultSizeEstimate": 2,
            }

    def _attachments_get(self, message_id, attachment_id):
        try:
            with open(os.path.join(self.stubs_dir, f"{attachment_id}.json")) as f:
                return json.load(f)
        except:
            return {"size": 3, "data": "__5JACcAbQAgAGEAIABmAHUAbgAgAGcAdQB5AA=="}

    def _messages_modify(self, *args, **kwargs):
        return {
            "id": "foo",
            "threadId": "bar",
            "labelIds": ["INBOX"],
            "snippet": "",
            "sizeEstimate": 1,
            "historyId": "2",
            "internalDate": "3",
        }
