"""
Implementation for how documents are represented in pybool_ir.
"""

import json
from datetime import datetime

import lucene
import parsedatetime as pdt

assert lucene.getVMEnv() or lucene.initVM()

_cal = pdt.Calendar()


class Document(object):
    """
    This class is the representation for documents.
    Data to be indexed in pybool_ir should be wrapped in this class.
    Documents retrieved from the index will also be wrapped in this class.
    In py_bool_ir, documents must have an id and date field, and this class will ensure that both of these fields are present.
    """

    def __init__(self, **kwargs):
        super(Document, self).__setattr__("fields", {})
        for field_name, field_value in kwargs.items():
            self.set(field_name, field_value)

    @staticmethod
    def from_dict(data: dict):
        if "date" in data:  # Kind of a hack.
            if isinstance(data["date"], str):
                data["date"] = _cal.parseDT(data["date"])[0]
            else:
                data["date"] = datetime.utcfromtimestamp(data["date"])
        return Document(**data)

    @staticmethod
    def from_json(data: str):
        return Document.from_dict(json.loads(data))

    def to_dict(self):
        out = object.__getattribute__(self, "fields")
        if "date" in self.keys():
            out["date"] = out["date"].timestamp()
        return out

    def to_json(self):
        return json.dumps(self.to_dict())

    def set(self, key, value):
        self.__setattr__(key, value)

    def __repr__(self):
        return self.to_json()

    def keys(self):
        return object.__getattribute__(self, "fields").keys()

    def has_key(self, key: str):
        return key in self.keys()

    def remove(self, key: str):
        del self.fields[key]

    def __getitem__(self, item):
        return self.__getattr__(item)

    def __getattr__(self, item):
        return self.fields[item]

    def __setattr__(self, key, value):
        self.fields[key] = value
