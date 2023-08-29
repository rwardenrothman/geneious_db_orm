import asyncio
import time
from asyncio.proactor_events import _ProactorBasePipeTransport
from collections import Counter, defaultdict
from contextlib import AbstractContextManager, AbstractAsyncContextManager, asynccontextmanager
from datetime import datetime
from tempfile import TemporaryDirectory
from types import TracebackType
from typing import Type, Optional, List, Dict, Set, AsyncGenerator
import operator as op
from subprocess import run
from uuid import uuid4
import inspect

from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord
from sqlalchemy import create_engine, select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import Session
import boto3
import json
from tqdm import tqdm

from GeneiousDB._orm import SEARCH_FIELD_BY_TYPE, SFV, AnnotatedDocument, IntegerSearchFieldValue, \
    LongSearchFieldValue, FloatSearchFieldValue, DoubleSearchFieldValue, StringSearchFieldValue, Folder, NextTableId, \
    Base, IndexingQueue
from GeneiousDB._parsing import parse_annotation, unparse_annotations


def silence_event_loop_closed(func):
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except RuntimeError as e:
            if str(e) != 'Event loop is closed':
                raise

    return wrapper


_ProactorBasePipeTransport.__del__ = silence_event_loop_closed(_ProactorBasePipeTransport.__del__)


class GeneiousDatabase(AbstractContextManager, AbstractAsyncContextManager):
    """
    This class manages all the interactions between Python and Geneious.

    GeneiousDatabase can be used as a standalone object or as a context manager. When used as a context manager, it will
    handle opening and closing the database connection on its own. This is the recommended usage so that the connection
    gets closed correctly.

    Args:
        secret_name (str): The name of the AWS secret that contains the database configuration

    Attributes:
        session (Session): The SQLAlchemy Session object. This should only be interacted with if this object cannot
            perform the needed functionality
        secret_name (str): The name of the AWS secret that contains the database configuration

    Examples:
        Retrieve all sequences from a folder::

            records = []

            with GeneiousDatabase(secret_name) as gdb:
                folder = gdb.get_folder_by_name('LG Uploads')
                for cur_doc in folder.iter_docs('DNA'):
                    records.append(gdb.get_SeqRecord(cur_doc))

        Add a genbank plasmid document to the database::

            from Bio import SeqIO

            new_record = SeqIO.read("path/to/document")
            with GeneiousDatabase(secret_name) as gdb:
                folder = gdb.get_folder_by_name('Folder Name')

                new_doc = gdb.plasmid_from_seqrecord(new_record)
                new_doc.folder = folder

                gdb.add(new_doc)
                gdb.commit()

        Find all documents whose name contains "Uox"::

            with GeneiousDatabase(secret_name) as gdb:
                uox_docs = gdb.search_contains('name', 'Uox')

        Add LabGuru information to a document::

            from LabGuruAPI import Plasmid

            lg_plasmid = Plasmid.from_name('pGRO-C1227')

            with GeneiousDatabase(secret_name) as gdb:
                plas_record = gdb.search_equal_to('name', lg_plasmid.name)[0]
                plas_record.set_lg_info(
                    url = lg_plasmid.url,
                    collection = 'Plasmids',
                    lg_id = lg_plasmid.id
                )
                plas_record.force_xml_updates()

                gdb.commit()


    """

    def __init__(self, secret_name: str) -> None:
        self.session: Optional[Session] = None
        self.secret_name = secret_name
        self._search_field = ''
        self._db_name = None
        self._new_objects = defaultdict(list)

    def __enter__(self) -> "GeneiousDatabase":
        return self.open()

    def __exit__(self, __exc_type: Optional[Type[BaseException]], __exc_value: Optional[BaseException],
                 __traceback: Optional[TracebackType]) -> Optional[bool]:
        self.close()
        return __exc_type is None

    async def __aenter__(self) -> "AsyncGeneiousDatabase":
        self._async_manager = await AsyncGeneiousDatabase(self.secret_name).open()
        return self._async_manager

    async def __aexit__(self, __exc_type: Optional[Type[BaseException]], __exc_value: Optional[BaseException],
                        __traceback: Optional[TracebackType]) -> Optional[bool]:
        await self._async_manager.close()
        return __exc_type is None

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
        self._db_name = db_config['dbname']
        return self

    def add(self, obj: Base):
        self._new_objects[obj.__tablename__].append(obj)

    def delete(self, obj: Base):
        self.session.delete(obj)

    def commit(self):
        # Reserve IDs
        objs_to_add: List[Base] = []
        reservations: Dict[int, IndexingQueue] = {}
        for cur_table, new_objs in self._new_objects.items():
            next_id_row = self.session.get(NextTableId, cur_table)
            for cur_obj in new_objs:
                cur_obj.id = int(next_id_row.next_id)
                objs_to_add.append(cur_obj)
                next_id_row.next_id += 1

                if cur_table == 'annotated_document':
                    reservations[cur_obj.id] = IndexingQueue(document_id=cur_obj.id, g_user_id=1)

        # Commit reservation
        self.session.commit()

        # Add objects individually
        for cur_obj in objs_to_add:
            self.session.add(cur_obj)
            self.session.add(reservations[cur_obj.id])
            self.session.commit()
            reservations[cur_obj.id].reserved = datetime.now()
            self.session.commit()

        # Remove reservations individually
        for cur_res in reservations.values():
            self.session.delete(cur_res)
            self.session.commit()

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
        return self.search(field_name, value.upper(), search_class, op.eq)

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

    def string_search_name_from_id(self, id: int):
        stmt = select(StringSearchFieldValue).where(StringSearchFieldValue.annotated_document_id == id).where(StringSearchFieldValue.search_field_code == 'cache_name')
        return self.session.scalars(stmt).all()

    def string_search_content_from_id(self, id: int):
        stmt = select(StringSearchFieldValue).where(StringSearchFieldValue.annotated_document_id == id).where(StringSearchFieldValue.search_field_code == 'content')
        return self.session.scalars(stmt).all()

    def __getitem__(self, item: str) -> "GeneiousDatabase":
        self._search_field = item
        return self

    @property
    def doc_name(self) -> "GeneiousDatabase":
        return self['cache_name']

    @property
    def mod_date(self) -> "GeneiousDatabase":
        return self['modified_date']

    def __eq__(self, other) -> List[AnnotatedDocument]:
        return self.search_equal_to(self._search_field, other)

    def __gt__(self, other) -> List[AnnotatedDocument]:
        return self.search_greater_than(self._search_field, other)

    def __ge__(self, other) -> List[AnnotatedDocument]:
        return self.search_greater_than_equal_to(self._search_field, other)

    def __lt__(self, other) -> List[AnnotatedDocument]:
        return self.search_less_than(self._search_field, other)

    def __le__(self, other) -> List[AnnotatedDocument]:
        return self.search_less_than_equal_to(self._search_field, other)

    def __contains__(self, other) -> List[AnnotatedDocument]:
        return self.search_contains(self._search_field, other)

    def get_doc_path(self, doc: AnnotatedDocument) -> str:
        return f'{self._db_name}:{doc.folder.full_path.replace("Server Documents/", "")}/{doc.doc_name}'

    def get_SeqRecord(self, doc: AnnotatedDocument) -> SeqRecord:
        seq: str = doc.plugin_document_xml['XMLSerialisableRootElement']['charSequence']
        try:
            features = [parse_annotation(a) for a in
                        doc.plugin_document_xml['XMLSerialisableRootElement']['sequenceAnnotations']['annotation']
                        if a.get('intervals') is not None and a.get('qualifiers') is not None]
        except KeyError:
            features = []
        desc = doc.plugin_document_xml['XMLSerialisableRootElement'].get('description', doc.doc_name)
        annotations = {
            'molecule_type': doc.mol_type,
            'topology': 'circular' if doc.circular else 'linear',
            'accession': doc.doc_urn,
            'date': doc.modified
        }
        rec = SeqRecord(Seq(seq), name=doc.doc_name, description=desc, features=features, annotations=annotations)
        return rec

    def from_SeqRecord(self, record: SeqRecord, base_doc: AnnotatedDocument = None, template_id: int = 24994
                       ) -> AnnotatedDocument:
        xml_template: Optional[AnnotatedDocument] = self.session.get(AnnotatedDocument, template_id)

        base_doc = self._process_seqrecord(base_doc, record, xml_template)
        # base_doc.id = AnnotatedDocument.get_next_id(self.session)
        return base_doc

    def _process_seqrecord(self, base_doc, record, xml_template):
        if base_doc is None:
            # base_doc = AnnotatedDocument()
            base_doc = AnnotatedDocument()

            k: str
            for k in xml_template.xml.keys():
                if k.startswith('@'):
                    base_doc.xml[k] = xml_template.xml[k]
            # for k in xml_template.plugin_xml.keys():
            #     if k.startswith('@'):
            #         base_doc.plugin_xml[k] = xml_template.plugin_xml[k]
        base_doc.description = record.description
        base_doc.sequence_str = str(record.seq)
        base_doc.mol_type = record.annotations.get('molecule_type', '')
        base_doc.circular = record.annotations.get('topology', 'linear') == 'circular'
        base_doc.modified = record.annotations.get('date', None) or datetime.now()
        base_doc.doc_name = record.name
        if len(record.features) > 0:
            base_doc.plugin_xml['sequenceAnnotations'] = {'annotation': [unparse_annotations(f) for f in record.features
                                                                         if f.location is not None]}
        if not base_doc.urn:
            uuid_str = str(uuid4()).replace('-', '')
            new_urn = f'urn:local:biofoundry:{uuid_str[:3]}-{uuid_str[-7:]}'
            base_doc.urn = new_urn
            base_doc.doc_urn = new_urn
        base_doc.force_xml_updates()
        base_doc.modified = datetime.now()
        return base_doc

    def plasmid_from_seqrecord(self, record: SeqRecord, base_doc: AnnotatedDocument = None) -> AnnotatedDocument:
        return self.from_SeqRecord(record, base_doc, 24994)

    def oligo_from_seqrecord(self, record: SeqRecord, base_doc: AnnotatedDocument = None) -> AnnotatedDocument:
        out_doc = self.from_SeqRecord(record, base_doc, 3898)
        out_doc.xml['fields']['oligoType'] = 'Primer'
        out_doc.force_xml_updates()
        return out_doc

    def get_folder_by_name(self, folder_name: str) -> Folder:
        return self.session.scalar(select(Folder).where(Folder.name == folder_name))

    def get_folders_by_name(self, folder_name: str) -> List[Folder]:
        return self.session.scalars(select(Folder).where(Folder.name == folder_name)).all()

    def update_search_fields(self, ad: AnnotatedDocument):
        populated_fields = ad.get_search_values()
        populated_codes = {v[0] for v in populated_fields}
        types_by_code = {v[0]: v[1] for v in populated_fields}
        values_by_code = {v[0]: v[2].upper() if isinstance(v[2], str) else v[2] for v in populated_fields}

        # Update existing rows
        updated_codes = set()
        for cur_table in StringSearchFieldValue, IntegerSearchFieldValue:
            result = self.session.query(cur_table).where(cur_table.search_field_code.in_(populated_codes)).all()
            for cur_row in result:
                cur_row.value = values_by_code[cur_row.search_field_code]
                updated_codes.add(cur_row.search_field_code)

        # Add new rows
        new_codes = populated_codes - updated_codes
        for cur_code in new_codes:
            new_item_type: Type[SFV] = types_by_code[cur_code]
            new_item = new_item_type(anotated_document=self, search_field_code=cur_code)
            new_item.value = values_by_code[cur_code]
            self.add(new_item)


class AsyncGeneiousDatabase(GeneiousDatabase):

    def __init__(self, secret_name: str, pool_size=5, max_overflow=10) -> None:
        super().__init__(secret_name)
        self._lock = None
        self._limit: Optional[asyncio.Semaphore] = None
        self.engine = None
        self.session_maker = None
        self.open_sessions: List[AsyncSession] = []
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.total_limit = pool_size + max_overflow
        self.session_cycle = None

    async def open(self) -> "AsyncGeneiousDatabase":
        self._lock = asyncio.Lock()
        self._limit = asyncio.Semaphore(self.total_limit)
        if isinstance(self.session, Session):
            return self
        sm_client = boto3.client('secretsmanager')
        db_config = json.loads(sm_client.get_secret_value(SecretId=self.secret_name)['SecretString'])
        db_config['engine'] = 'postgresql+asyncpg' if db_config['engine'] == 'postgres' else db_config['engine']
        db_uri = f"{db_config['engine']}://{db_config['username']}:{db_config['password']}" \
                 f"@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        self.engine = create_async_engine(db_uri, pool_size=self.pool_size, max_overflow=self.max_overflow)
        self.session_maker = async_sessionmaker(self.engine, expire_on_commit=False)
        self._db_name = db_config['dbname']
        return self

    @property
    def session(self) -> Optional[AsyncSession]:
        if self.session_maker is None:
            return None
        if len(self.open_sessions) < self.total_limit:
            async_session = self.session_maker()
            self.open_sessions.append(async_session)
        elif self.session_cycle is None:
            self.session_cycle = cycle(self.open_sessions)
            async_session = next(self.session_cycle)
        else:
            async_session = next(self.session_cycle)
        return async_session

    @session.setter
    def session(self, value):
        pass

    @asynccontextmanager
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        await self._limit.acquire()
        try:
            yield self.session
        finally:
            self._limit.release()


    async def search(self, field_name: str, value, search_class: Type[SFV] = None,
                     operator=op.eq) -> List[AnnotatedDocument]:
        async with self.get_session() as cur_session:
            search_class: Type[SFV] = search_class if search_class else SEARCH_FIELD_BY_TYPE[type(value)]
            stmt = select(search_class) \
                .where(search_class.search_field_code == field_name) \
                .where(operator(search_class.value, value))

            # cur_session = self.session
            vals = (await cur_session.scalars(stmt)).fetchall()

            if vals:
                result = [await v.awaitable_attrs.annotated_document for v in vals]
            elif search_class == IntegerSearchFieldValue:
                result = await self.search(field_name, value, LongSearchFieldValue, operator=operator)
            elif search_class == FloatSearchFieldValue:
                result = await self.search(field_name, value, DoubleSearchFieldValue, operator=operator)
            else:
                result = []

            # await cur_session.close()
            return result

    async def delete(self, obj: Base):
        async with self._limit:
            super().delete(obj)

    async def commit(self):
        async with self._limit:
            # Commit dirty items
            for c_sess in self.open_sessions:
                await c_sess.commit()

        async with self.get_session() as cur_session:
            # Reserve IDs
            objs_to_add: List[Base] = []
            reservations: Dict[int, IndexingQueue] = {}
            for cur_table, new_objs in self._new_objects.items():
                next_id_row = await cur_session.get(NextTableId, cur_table)
                for cur_obj in new_objs:
                    cur_obj.id = int(next_id_row.next_id)
                    objs_to_add.append(cur_obj)
                    next_id_row.next_id += 1

                    if cur_table == 'annotated_document':
                        reservations[cur_obj.id] = IndexingQueue(document_id=cur_obj.id, g_user_id=1)

            # Commit reservation
            await cur_session.commit()

            # Add objects individually
            for cur_obj in objs_to_add:
                cur_session.add(cur_obj)
                if cur_obj.id in reservations:
                    cur_session.add(reservations[cur_obj.id])
                await cur_session.commit()
                if cur_obj.id in reservations:
                    reservations[cur_obj.id].reserved = datetime.now()
                    await cur_session.commit()

            # Remove reservations individually
            for cur_res in reservations.values():
                await cur_session.delete(cur_res)
                await cur_session.commit()

    async def close(self):
        for c_sess in self.open_sessions:
            await c_sess.close()
        await self.engine.dispose()

    async def from_SeqRecord(self, record: SeqRecord, base_doc: AnnotatedDocument = None,
                             template_id: int = 24994) -> AnnotatedDocument:
        async with self.get_session() as cur_session:
            xml_template: Optional[AnnotatedDocument] = await cur_session.get(AnnotatedDocument, template_id)

        base_doc = await self._process_seqrecord(base_doc, record, xml_template)
        # base_doc.id = await AnnotatedDocument.async_get_next_id(cur_session)
        return base_doc

    async def oligo_from_seqrecord(self, record: SeqRecord, base_doc: AnnotatedDocument = None) -> AnnotatedDocument:
        out_doc = await self.from_SeqRecord(record, base_doc, 3898)
        out_doc.xml['fields']['oligoType'] = 'Primer'
        out_doc.force_xml_updates()
        return out_doc

    async def update_search_fields(self, ad: AnnotatedDocument):
        populated_fields = ad.get_search_values()
        populated_codes = {v[0] for v in populated_fields}
        types_by_code = {v[0]: v[1] for v in populated_fields}
        values_by_code = {v[0]: v[2].upper() if isinstance(v[2], str) else v[2] for v in populated_fields}

        # Update existing rows
        async def _update_existing(cur_table: Type[SFV]) -> Set[str]:
            async with self.get_session() as cur_session:
                table_codes = set()
                query = select(cur_table).where(cur_table.search_field_code.in_(populated_codes),
                                                cur_table.annotated_document == ad)
                result = (await cur_session.scalars(query)).all()
                for cur_row in result:
                    cur_row.value = values_by_code[cur_row.search_field_code]
                    table_codes.add(cur_row.search_field_code)
                return table_codes

        updated_codes = set()
        if ad.id:
            potential_tables = set(types_by_code.values())
            for cur_code_set in await asyncio.gather(*[_update_existing(t) for t in potential_tables]):
                updated_codes |= cur_code_set

        # Add new rows
        new_codes = populated_codes - updated_codes
        for cur_code in new_codes:
            new_item_type: Type[SFV] = types_by_code[cur_code]
            new_item = new_item_type(search_field_code=cur_code)
            new_item.annotated_document = ad
            new_item.value = values_by_code[cur_code]
            await self.add(new_item)

    def __getattribute__(self, item):
        cur_attr = object.__getattribute__(self, item)
        if item in ['get_doc_by_id', 'get_session']:
            return cur_attr
        if inspect.ismethod(cur_attr) and not inspect.iscoroutinefunction(cur_attr):
            async def _wrapper(*args, **kwargs):
                r_val = cur_attr(*args, **kwargs)
                return (await r_val) if inspect.isawaitable(r_val) else r_val
            return _wrapper
        return cur_attr


if __name__ == '__main__':
    from tqdm.asyncio import tqdm_asyncio
    from itertools import chain, cycle
    import pydna.all as pyd


    async def find_doc(adb: AsyncGeneiousDatabase, name: str):
        docs: List[AnnotatedDocument] = await adb.search_equal_to('cache_name', name.upper())
        for d in docs:
            d.folder_id = 8577
        return docs

    async def _main(*names: str):
        async with GeneiousDatabase('GeneiousDB') as gdb:
            results = await tqdm_asyncio.gather(*[find_doc(gdb, n) for n in names])

            await gdb.commit()

        return results

    docs = asyncio.run(_main(*[f"CXr-a-{i+1:02d}" for i in range(20)]))
    for d in chain(*docs):
        if d:
            print(d.doc_name)
