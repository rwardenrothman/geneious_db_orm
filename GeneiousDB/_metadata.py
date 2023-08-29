import datetime
from abc import ABC
from typing import List, TYPE_CHECKING, Union, Type, Dict
from xmltodict import parse, unparse
JSON = Union[Dict[str, 'JSON'], List['JSON'], int, str, float, bool, Type[None]]

if TYPE_CHECKING:
    from ._database_obj import GeneiousDatabase
    from ._orm import SpecialElement


class NoteField:
    name: str
    java_type: str
    editable: bool = True
    user_modifiable: bool = True
    constraints: str = None

    # @classmethod
    # def generate_definition_dict(cls) -> JSON:


class NoteType(ABC):
    name: str
    author: str
    creation_date: datetime.datetime = None
    description: str = None
    visible: bool = True
    default_visible: bool = True
    modified_date: datetime.datetime = None

    @classmethod
    def generate_xml_definition(cls) -> str:
        cls.creation_date = cls.creation_date or datetime.datetime.now()
        cls.modified_date = cls.modified_date or datetime.datetime.now()

        note_code = f"{cls.author}-{cls.name}-{cls.creation_date.timestamp()*1000:0.0f}".replace(' ', '')
        mod_timestamp = f"{cls.modified_date.timestamp()*1000:0.0f}"

        xml_dict: JSON = {'NoteType': {
            'name': cls.name,
            'code': note_code,
            'description': cls.description,
            'isVisible': cls.visible,
            'isDefaultVisibleInTable': cls.default_visible,
            'modifiedDate': mod_timestamp
        }}
        return unparse(xml_dict, full_document=False, pretty=True, short_empty_elements=True)


if __name__ == '__main__':
    class TstNote(NoteType):
        name = 'Test Note'
        author = 'Rob Warden-Rothman'

    tn = TstNote()
    print(tn.generate_xml_definition())