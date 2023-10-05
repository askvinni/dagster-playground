import json


class MockRequestResponse:
    def __init__(self, content):
        self.content = content

    def json(self):
        return json.loads(self.content)
