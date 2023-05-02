# coding: utf-8
from datetime import datetime
from functools import lru_cache
from typing import Dict, Type, TypeVar, Optional, Any, Union, List, Iterable

from sqlalchemy import BigInteger, Boolean, CheckConstraint, Column, Date, DateTime, Float, ForeignKey, Integer, \
    String, Table, Text, text, func
from sqlalchemy.dialects.postgresql import OID
from sqlalchemy.orm import relationship, Session, backref
from sqlalchemy.ext.declarative import declarative_base, AbstractConcreteBase
from xmltodict import parse, unparse

Base = declarative_base()
metadata = Base.metadata


class DocumentFileDatum(Base):
    __tablename__ = 'document_file_data'

    id = Column(Integer, primary_key=True)
    data = Column(OID)
    local_file_path = Column(Text)
    local_file_size = Column(BigInteger)
    last_needed = Column(DateTime)


class GGroup(Base):
    __tablename__ = 'g_group'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)


class GRole(Base):
    __tablename__ = 'g_role'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)


class Metadatum(Base):
    __tablename__ = 'metadata'

    identifier = Column(String(80), primary_key=True)
    value = Column(String(255))


class NextTableId(Base):
    __tablename__ = 'next_table_id'

    table_name = Column(String(50), primary_key=True)
    next_id = Column(Integer)


class SearchField(Base):
    __tablename__ = '_search_field'

    code = Column(String(255), primary_key=True)
    field_xml = Column(Text, nullable=False)


class Folder(Base):
    __tablename__ = 'folder'
    __table_args__ = (
        CheckConstraint('id <> parent_folder_id'),
    )

    id = Column(Integer, primary_key=True)
    g_group_id = Column(ForeignKey('g_group.id'), nullable=False)
    parent_folder_id = Column(ForeignKey('folder.id'), index=True)
    visible = Column(Boolean, nullable=False)
    modified = Column(DateTime, nullable=False)
    name = Column(String(255), index=True)

    g_group = relationship('GGroup')
    subfolders: List["Folder"] = relationship('Folder', backref=backref('parent_folder', remote_side=[id]))
    users = relationship('GUser', secondary='hidden_folder_to_user')
    documents: List["AnnotatedDocument"] = relationship('AnnotatedDocument', back_populates='folder')

    def __str__(self):
        return f"<Folder: {self.full_path}>"

    @property
    def full_path(self):
        if self.parent_folder:
            return f'{self.parent_folder.full_path}/{self.name}'
        else:
            return self.name

    def iter_docs(self, mol_type: str = None, recursive=False) -> Iterable["AnnotatedDocument"]:
        for d in self.documents:
            if mol_type is None or d.mol_type == mol_type:
                yield d

        if recursive:
            for sub_dir in self.subfolders:
                for d in sub_dir.iter_docs(mol_type, recursive):
                    yield d


class GUser(Base):
    __tablename__ = 'g_user'

    id = Column(Integer, primary_key=True)
    primary_group_id = Column(ForeignKey('g_group.id'), nullable=False, index=True)
    username = Column(String(255), nullable=False)

    primary_group = relationship('GGroup')


class AnnotatedDocument(Base):
    """
    This class represents all document types in the Geneious database.

    Attributes:
        id (int): the database id of the document. Do not change this.
        folder (Folder): the Folder that contains the document
        modified (datetime): the time that the document was last modified.
        urn (str): the URN of the Geneious object

    """
    __tablename__ = 'annotated_document'

    id = Column(Integer, primary_key=True, nullable=True)
    folder_id = Column(ForeignKey('folder.id', ondelete='CASCADE'), nullable=False, index=True)
    modified = Column(DateTime, nullable=False)
    urn = Column(String(255), nullable=False, unique=True)
    _document_xml = Column(Text, nullable=False, name='document_xml')
    _plugin_document_xml = Column(Text, nullable=False, name='plugin_document_xml')
    reference_count = Column(Integer, nullable=False, default=0)

    folder = relationship('Folder', back_populates='documents')
    g_users = relationship('GUser', secondary='document_read')
    file_datas = relationship('DocumentFileDatum', secondary='document_to_file_data')

    _doc_xml_dict = {}
    _plugin_xml_dict = {}

    @staticmethod
    def get_next_id(s: Session) -> int:
        """
        Returns the next available document id.

        Args:
            s (Session): a SQLAlchemy session

        Returns:
            int: the next id
        """
        max_id = s.scalar(func.max(AnnotatedDocument.id))
        return max_id + 1

    @property
    def document_xml(self):
        if not self._doc_xml_dict:
            if not self._document_xml:
                self._doc_xml_dict = {'document': {}}
                self._document_xml = unparse(self._doc_xml_dict, full_document=False, pretty=True)
                return self.document_xml

            self._doc_xml_dict = parse(self._document_xml)
            return self._doc_xml_dict

        try:
            return self._doc_xml_dict
        finally:
            new_xml = unparse(self._doc_xml_dict, full_document=False, pretty=True)
            if new_xml != self._document_xml:
                self._document_xml = new_xml

    @property
    def plugin_document_xml(self):
        if not self._plugin_xml_dict:
            if not self._plugin_document_xml:
                self._plugin_xml_dict = {'XMLSerialisableRootElement': {}}
                self._plugin_document_xml = unparse(self._plugin_xml_dict, full_document=False, pretty=True)
                return self.plugin_document_xml

            self._plugin_xml_dict = parse(self._plugin_document_xml)
            return self._plugin_xml_dict

        try:
            return self._plugin_xml_dict
        finally:
            new_xml = unparse(self._plugin_xml_dict, full_document=False, pretty=True)
            if new_xml != self._plugin_document_xml:
                self._plugin_document_xml = new_xml

    def force_xml_updates(self):
        """
        Forces the xml fields to update. It's a good idea to run this method before a commit
        """
        new_xml = unparse(self._plugin_xml_dict, full_document=False, pretty=True)
        self._plugin_document_xml = new_xml

        new_xml = unparse(self._doc_xml_dict, full_document=False, pretty=True)
        self._document_xml = new_xml

    @property
    def xml(self) -> dict:
        return self.document_xml['document']

    @property
    def plugin_xml(self) -> dict:
        return self.plugin_document_xml['XMLSerialisableRootElement']

    @property
    def doc_name(self) -> str:
        """
        The name of the document
        """
        return self.xml['hiddenFields'].get('override_cache_name',
                                            self.xml['hiddenFields'].get('cahce_name',
                                                                         self.plugin_xml.get('name', '')))

    @doc_name.setter
    def doc_name(self, value: str):
        if 'hiddenFields' not in self.xml:
            self.xml['hiddenFields'] = {}
        self.xml['hiddenFields']['cache_name'] = value
        self.plugin_xml['name'] = value

    @property
    def doc_urn(self) -> str:
        """The document URN. If you need to change it. also set it here."""
        return self.xml['hiddenFields']['cache_urn']['#text']

    @doc_urn.setter
    def doc_urn(self, value: str):
        if 'hiddenFields' not in self.xml:
            self.xml['hiddenFields'] = {}
        self.xml['hiddenFields']['cache_urn'] = {'@type': 'urn', '#text': value}

    @property
    def accession(self) -> str:
        """The accession number of the DNA object"""
        hf = self.xml['hiddenFields']
        if 'override_accession' in hf:
            return hf['override_accession']
        if 'cache_urn' in hf:
            return hf['cache_urn']['#text']
        if 'referenced_documents' in hf:
            return hf['referenced_documents']['referenced_documents'].split('@')[0]
        return ''

    @accession.setter
    def accession(self, value: str):
        if 'hiddenFields' not in self.xml:
            self.xml['hiddenFields'] = {}
        self.xml['hiddenFields']['override_accession'] = value

    @property
    def mol_type(self) -> str:
        """
        The geneious type of the document. This must be one of the following values:

        - DNA
        - RNA
        - Primer
        - Protein
        """
        return self.xml['fields'].get('molType', self.xml['fields'].get('oligoType', self.xml['@class'].split('.')[-1]))

    @mol_type.setter
    def mol_type(self, value: str):
        if 'fields' not in self.xml:
            self.xml['fields'] = {}
        self.xml['fields']['molType'] = value

    @property
    def linear(self) -> bool:
        return self.xml['fields']['topology'] == 'linear'

    @linear.setter
    def linear(self, value: bool):
        if 'fields' not in self.xml:
            self.xml['fields'] = {}
        self.xml['fields']['topology'] = 'linear' if value else 'circular'

        if value:
            try:
                del self.plugin_xml['fields']['isCircular']
            except KeyError:
                pass
        else:
            if 'fields' not in self.plugin_xml:
                self.plugin_xml['fields'] = {}
            self.plugin_xml['fields']['isCircular'] = not value

    @property
    def circular(self) -> bool:
        return not self.linear

    @circular.setter
    def circular(self, value: bool):
        self.linear = not value

    @property
    def sequence_str(self) -> str:
        return self.plugin_xml['charSequence']

    @sequence_str.setter
    def sequence_str(self, value: str):
        if 'fields' not in self.xml:
            self.xml['fields'] = {}
        self.xml['fields']['sequence_residues'] = value
        self.plugin_xml['charSequence'] = value

    @property
    def description(self) -> str:
        return self.xml['hiddenFields']['description']

    @description.setter
    def description(self, value: str):
        if 'hiddenFields' not in self.xml:
            self.xml['hiddenFields'] = {}
        self.xml['hiddenFields']['description'] = value
        self.plugin_xml['description'] = value

    @property
    def lg_info(self) -> Optional[Dict[str, Any]]:
        try:
            for i in range(27):
                cur_node = self.xml['notes']['note'][i]
                if 'LGLink' in cur_node:
                    return {
                        'url': cur_node['LGLink'],
                        'collection': cur_node['Collection'],
                        'id': cur_node['ID']['#text']
                    }
        except (KeyError, IndexError):
            return None

    def set_lg_info(self, url: str, collection: str, lg_id: Union[int, str]):
        lg_xml = f'<note code="RobWarden-Rothman-LabGuruInfo-1659621929188" type="note">' \
                 f'<LGLink>{url}</LGLink>' \
                 f'<Collection>{collection}</Collection>' \
                 f'<ID type="int">{str(lg_id)}</ID>' \
                 f'</note>'

        if 'notes' in self.xml:
            if isinstance(self.document_xml['document']['notes']['note'], list):
                for n in self.document_xml['document']['notes']['note']:
                    if n['@code'] == 'RobWarden-Rothman-LabGuruInfo-1659621929188':
                        cur_note = n
                        break
                else:
                    cur_note = {'@code': 'RobWarden-Rothman-LabGuruInfo-1659621929188', '@type': 'note'}
                    self.document_xml['document']['notes']['note'] += [cur_note]

                cur_note.update(dict(LGLink=url, Collection=collection, ID={'@type': 'int', '#text': str(lg_id)}))
                self.document_xml['document']['notes']['note'] += []
            elif isinstance(self.document_xml['document']['notes']['note'], dict):
                old_note = self.document_xml['document']['notes']['note']
                self.document_xml['document']['notes']['note'] = [old_note]
                self.set_lg_info(url, collection, lg_id)
            else:
                raise ValueError(f"Cannot handle notes like {repr(self.document_xml['document']['notes']['note'])}")
        else:
            cur_note = {'@code': 'RobWarden-Rothman-LabGuruInfo-1659621929188', '@type': 'note'}
            cur_note.update(dict(LGLink=url, Collection=collection, ID={'@type': 'int', '#text': str(lg_id)}))
            self.document_xml['document']['notes'] = {'note': [cur_note]}

        # self.document_xml = self.document_xml.copy()

    def __str__(self):
        return f'<Annotated {self.mol_type} Document: {self.doc_name} ({self.id:d})>'


class IndexingQueue(Base):
    __tablename__ = 'indexing_queue'

    document_id = Column(Integer, primary_key=True)
    g_user_id = Column(ForeignKey('g_user.id', ondelete='SET NULL'), index=True)
    reserved = Column(DateTime)

    g_user = relationship('GUser')


class FolderView(Base):
    __tablename__ = 'folder_view'

    folder_id = Column(ForeignKey('folder.id', ondelete='CASCADE'), primary_key=True, nullable=False, index=True)
    document_urn = Column(String(255), primary_key=True, nullable=False)
    modified = Column(DateTime, nullable=False)

    folder = relationship('Folder')


class GUserGroupRole(Base):
    __tablename__ = 'g_user_group_role'

    g_user_id = Column(ForeignKey('g_user.id', ondelete='CASCADE'), primary_key=True, nullable=False, index=True)
    g_group_id = Column(ForeignKey('g_group.id', ondelete='CASCADE'), primary_key=True, nullable=False, index=True)
    g_role_id = Column(ForeignKey('g_role.id', ondelete='CASCADE'), primary_key=True, nullable=False, index=True)

    g_group = relationship('GGroup')
    g_role = relationship('GRole')
    g_user = relationship('GUser')


t_hidden_folder_to_user = Table(
    'hidden_folder_to_user', metadata,
    Column('hidden_folder_id', ForeignKey('folder.id', ondelete='CASCADE'), primary_key=True),
    Column('user_id', ForeignKey('g_user.id', ondelete='CASCADE'), index=True)
)


class SpecialElement(Base):
    __tablename__ = 'special_element'

    folder_id = Column(ForeignKey('folder.id', ondelete='CASCADE'), primary_key=True, nullable=False, index=True)
    modified = Column(DateTime, nullable=False)
    xml = Column(Text, nullable=False)
    name = Column(String(255), primary_key=True, nullable=False)

    folder = relationship('Folder')


class AdditionalDocumentXml(Base):
    __tablename__ = 'additional_document_xml'

    document_urn = Column(ForeignKey('annotated_document.urn', ondelete='CASCADE'), primary_key=True, nullable=False, index=True)
    element_key = Column(String(255), primary_key=True, nullable=False)
    g_user_id = Column(ForeignKey('g_user.id', ondelete='CASCADE'), primary_key=True, nullable=False, index=True)
    xml_element = Column(Text, nullable=False)
    geneious_major_version_1 = Column(Integer, primary_key=True, nullable=False, server_default=text("0"))
    geneious_major_version_2 = Column(Integer, primary_key=True, nullable=False, server_default=text("0"))

    annotated_document = relationship('AnnotatedDocument')
    g_user = relationship('GUser')


class AdditionalXmlTimestamp(Base):
    __tablename__ = 'additional_xml_timestamp'

    document_urn = Column(ForeignKey('annotated_document.urn', ondelete='CASCADE'), primary_key=True, nullable=False, index=True)
    g_user_id = Column(ForeignKey('g_user.id', ondelete='CASCADE'), primary_key=True, nullable=False, index=True)
    modified = Column(DateTime)

    annotated_document = relationship('AnnotatedDocument')
    g_user = relationship('GUser')


class BooleanSearchFieldValue(Base):
    __tablename__ = 'boolean_search_field_value'

    id = Column(Integer, primary_key=True)
    annotated_document_id = Column(ForeignKey('annotated_document.id', ondelete='CASCADE'), nullable=False, index=True)
    search_field_code = Column(ForeignKey('_search_field.code', ondelete='CASCADE'), nullable=False, index=True)
    value = Column(Boolean, nullable=False)

    annotated_document = relationship('AnnotatedDocument')
    search_field = relationship('SearchField')


class DateSearchFieldValue(Base):
    __tablename__ = 'date_search_field_value'

    id = Column(Integer, primary_key=True)
    annotated_document_id = Column(ForeignKey('annotated_document.id', ondelete='CASCADE'), nullable=False, index=True)
    search_field_code = Column(ForeignKey('_search_field.code', ondelete='CASCADE'), nullable=False, index=True)
    value = Column(Date, nullable=False)

    annotated_document = relationship('AnnotatedDocument')
    search_field = relationship('SearchField')


t_document_read = Table(
    'document_read', metadata,
    Column('g_user_id', ForeignKey('g_user.id', ondelete='CASCADE'), primary_key=True, nullable=False, index=True),
    Column('annotated_document_id', ForeignKey('annotated_document.id', ondelete='CASCADE'), primary_key=True, nullable=False, index=True)
)


t_document_to_file_data = Table(
    'document_to_file_data', metadata,
    Column('document_urn', ForeignKey('annotated_document.urn', ondelete='CASCADE'), primary_key=True, nullable=False, index=True),
    Column('file_data_id', ForeignKey('document_file_data.id', ondelete='CASCADE'), primary_key=True, nullable=False, index=True)
)


class DoubleSearchFieldValue(Base):
    __tablename__ = 'double_search_field_value'

    id = Column(Integer, primary_key=True)
    annotated_document_id = Column(ForeignKey('annotated_document.id', ondelete='CASCADE'), nullable=False, index=True)
    search_field_code = Column(ForeignKey('_search_field.code', ondelete='CASCADE'), nullable=False, index=True)
    value = Column(Float(53), nullable=False)

    annotated_document = relationship('AnnotatedDocument')
    search_field = relationship('SearchField')


class FloatSearchFieldValue(Base):
    __tablename__ = 'float_search_field_value'

    id = Column(Integer, primary_key=True)
    annotated_document_id = Column(ForeignKey('annotated_document.id', ondelete='CASCADE'), nullable=False, index=True)
    search_field_code = Column(ForeignKey('_search_field.code', ondelete='CASCADE'), nullable=False, index=True)
    value = Column(Float, nullable=False)

    annotated_document = relationship('AnnotatedDocument')
    search_field = relationship('SearchField')


class IntSearchFieldValue(AbstractConcreteBase, Base):
    pass

class IntegerSearchFieldValue(IntSearchFieldValue):
    __tablename__ = 'integer_search_field_value'

    id = Column(Integer, primary_key=True)
    annotated_document_id = Column(ForeignKey('annotated_document.id', ondelete='CASCADE'), nullable=False, index=True)
    search_field_code = Column(ForeignKey('_search_field.code', ondelete='CASCADE'), nullable=False, index=True)
    value = Column(Integer, nullable=False)

    annotated_document = relationship('AnnotatedDocument')
    search_field = relationship('SearchField')


class LongSearchFieldValue(IntSearchFieldValue):
    __tablename__ = 'long_search_field_value'

    id = Column(Integer, primary_key=True)
    annotated_document_id = Column(ForeignKey('annotated_document.id', ondelete='CASCADE'), nullable=False, index=True)
    search_field_code = Column(ForeignKey('_search_field.code', ondelete='CASCADE'), nullable=False, index=True)
    value = Column(BigInteger, nullable=False)

    annotated_document = relationship('AnnotatedDocument')
    search_field = relationship('SearchField')


class StringSearchFieldValue(Base):
    __tablename__ = 'string_search_field_value'

    id = Column(Integer, primary_key=True)
    annotated_document_id = Column(ForeignKey('annotated_document.id', ondelete='CASCADE'), nullable=False, index=True)
    search_field_code = Column(ForeignKey('_search_field.code', ondelete='CASCADE'), nullable=False, index=True)
    value = Column(Text, nullable=False)

    annotated_document = relationship('AnnotatedDocument')
    search_field = relationship('SearchField')


SFV = TypeVar('SFV', bound=Base)

SEARCH_FIELD_BY_TYPE: Dict[type, Type[SFV]] = {
    bool: BooleanSearchFieldValue,
    datetime: DateSearchFieldValue,
    float: DoubleSearchFieldValue,
    int: IntegerSearchFieldValue,
    str: StringSearchFieldValue
}
