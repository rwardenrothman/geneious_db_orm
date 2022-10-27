from contextlib import AbstractContextManager
from tempfile import TemporaryDirectory
from types import TracebackType
from typing import Type, Optional, List
import operator as op
from subprocess import run

from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
import boto3
import json

from GeneiousDB._orm import SEARCH_FIELD_BY_TYPE, SFV, AnnotatedDocument, IntegerSearchFieldValue, LongSearchFieldValue, \
    FloatSearchFieldValue, DoubleSearchFieldValue, StringSearchFieldValue
from GeneiousDB._parsing import parse_annotation


class GeneiousDatabase(AbstractContextManager):

    def __init__(self, secret_name: str) -> None:
        self.session: Optional[Session] = None
        self.secret_name = secret_name
        self.search_field = ''
        self.db_name = None

    def __enter__(self) -> "GeneiousDatabase":
        return self.open()

    def __exit__(self, __exc_type: Optional[Type[BaseException]], __exc_value: Optional[BaseException],
                 __traceback: Optional[TracebackType]) -> Optional[bool]:
        self.close()
        return True

    def open(self) -> "GeneiousDatabase":
        if isinstance(self.session, Session):
            return self
        sm_client = boto3.client('secretsmanager')
        db_config = json.loads(sm_client.get_secret_value(SecretId=self.secret_name)['SecretString'])
        db_config['engine'] = 'postgresql' if db_config['engine'] == 'postgres' else db_config['engine']
        db_uri = f"{db_config['engine']}://{db_config['username']}:{db_config['password']}" \
                 f"@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        engine = create_engine(db_uri)
        self.session = Session(engine)
        self.db_name = db_config['dbname']
        return self

    def close(self):
        if isinstance(self.session, Session):
            self.session.close()

    def search(self, field_name: str, value, search_class: Type[SFV] = None, operator=op.eq) -> List[AnnotatedDocument]:
        search_class: Type[SFV] = search_class if search_class else SEARCH_FIELD_BY_TYPE[type(value)]
        stmt = select(search_class)\
            .where(search_class.search_field_code == field_name)\
            .where(operator(search_class.value, value))

        vals = self.session.scalars(stmt).all()

        if vals:
            return [v.annotated_document for v in vals]
        elif search_class == IntegerSearchFieldValue:
            return self.search(field_name, value, LongSearchFieldValue, operator=operator)
        elif search_class == FloatSearchFieldValue:
            return self.search(field_name, value, DoubleSearchFieldValue, operator=operator)
        else:
            return []

    def search_equal_to(self, field_name: str, value, search_class: Type[SFV] = None):
        return self.search(field_name, value, search_class, op.eq)

    def search_greater_than(self, field_name: str, value, search_class: Type[SFV] = None):
        return self.search(field_name, value, search_class, op.gt)

    def search_greater_than_equal_to(self, field_name: str, value, search_class: Type[SFV] = None):
        return self.search(field_name, value, search_class, op.ge)

    def search_less_than(self, field_name: str, value, search_class: Type[SFV] = None):
        return self.search(field_name, value, search_class, op.lt)

    def search_less_than_equal_to(self, field_name: str, value, search_class: Type[SFV] = None):
        return self.search(field_name, value, search_class, op.le)

    def search_contains(self, field_name: str, value: str):
        if field_name == 'name':
            field_name = 'cache_name'
        if field_name.lower() == 'urn':
            stmt = select(AnnotatedDocument).where(AnnotatedDocument.urn == value)
            return self.session.scalars(stmt).all()

        stmt = select(StringSearchFieldValue)\
            .where(StringSearchFieldValue.search_field_code == field_name)\
            .where(StringSearchFieldValue.value.contains(value.upper()))
        vals = self.session.scalars(stmt).all()

        if vals:
            return [v.annotated_document for v in vals]
        else:
            return []

    def __getitem__(self, item: str) -> "GeneiousDatabase":
        self.search_field = item
        return self

    @property
    def doc_name(self) -> "GeneiousDatabase":
        return self['cache_name']

    @property
    def mod_date(self) -> "GeneiousDatabase":
        return self['modified_date']

    def __eq__(self, other) -> List[AnnotatedDocument]:
        return self.search_equal_to(self.search_field, other)

    def __gt__(self, other) -> List[AnnotatedDocument]:
        return self.search_greater_than(self.search_field, other)

    def __ge__(self, other) -> List[AnnotatedDocument]:
        return self.search_greater_than_equal_to(self.search_field, other)

    def __lt__(self, other) -> List[AnnotatedDocument]:
        return self.search_less_than(self.search_field, other)

    def __le__(self, other) -> List[AnnotatedDocument]:
        return self.search_less_than_equal_to(self.search_field, other)

    def __contains__(self, other) -> List[AnnotatedDocument]:
        return self.search_contains(self.search_field, other)

    def get_doc_path(self, doc: AnnotatedDocument) -> str:
        return f'{self.db_name}:{doc.folder.full_path.replace("Server Documents/", "")}/{doc.doc_name}'

    def get_SeqRecord(self, doc: AnnotatedDocument) -> SeqRecord:
        seq: str = doc.plugin_document_xml['XMLSerialisableRootElement']['charSequence']
        features = [parse_annotation(a) for a in
                    doc.plugin_document_xml['XMLSerialisableRootElement']['sequenceAnnotations']['annotation']]
        desc = doc.plugin_document_xml['XMLSerialisableRootElement'].get('description', '')
        annotations = {
            'molecule_type': doc.mol_type,
            'topology': 'circular' if doc.circular else 'linear',
            'accession': doc.doc_urn,
            'date': doc.modified
        }
        rec = SeqRecord(Seq(seq), name=doc.doc_name, description=desc, features=features, annotations=annotations)
        return rec


if __name__ == '__main__':
    from Bio import SeqIO
    with GeneiousDatabase('Geneious_Foundry_Backend') as gdb:
        p = gdb.search_contains('name', 'pGRO-A0136')[0]
        r = gdb.get_SeqRecord(p)
        print(p)

    SeqIO.write([r], 'out.gb', 'gb')
