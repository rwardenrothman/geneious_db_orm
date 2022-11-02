from typing import Callable, Optional

from sqlalchemy import Text, TypeDecorator
from sqlalchemy.ext.mutable import MutableDict

from xmltodict import parse, unparse


class _XML(TypeDecorator):
    impl = Text
    cache_ok = True

    def process_bind_param(self, value, dialect) -> None:
        return unparse(value)

    def process_result_value(self, value, dialect) -> None:
        return parse(value)

    # def bind_processor(self, dialect):
    #     text_processor: Optional[Callable] = super().bind_processor(dialect)
    #
    #     def xml_process(value):
    #         if isinstance(value, dict):
    #             value = unparse(value)
    #
    #         if text_processor is not None:
    #             return text_processor(value)
    #         else:
    #             return value
    #
    #     return xml_process
    #
    # def result_processor(self, dialect, coltype):
    #     text_processor: Optional[Callable] = super().result_processor(dialect, coltype)
    #
    #     def xml_process(value):
    #         if text_processor is not None:
    #             value = text_processor(value)
    #
    #         if isinstance(value, str):
    #             return parse(value)
    #         else:
    #             return value
    #
    #     return xml_process

    # @property
    # def python_type(self):
    #     return dict

XML = MutableDict.as_mutable(_XML)
