from typing import Callable, Optional

from sqlalchemy import Text

from xmltodict import parse, unparse


class XML(Text):

    def bind_processor(self, dialect):
        text_processor: Optional[Callable] = super().bind_processor(dialect)

        def xml_process(value):
            if isinstance(value, dict):
                value = unparse(value)

            if text_processor is not None:
                return text_processor(value)
            else:
                return value

        return xml_process

    def result_processor(self, dialect, coltype):
        text_processor: Optional[Callable] = super().result_processor(dialect, coltype)

        def xml_process(value):
            if text_processor is not None:
                value = text_processor(value)

            if isinstance(value, str):
                return parse(value)
            else:
                return value

        return xml_process

    @property
    def python_type(self):
        return dict

