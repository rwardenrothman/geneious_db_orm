from collections import defaultdict
from typing import Dict, List, DefaultDict, Union

from Bio.SeqFeature import FeatureLocation, BeforePosition, AfterPosition, ExactPosition, SeqFeature, CompoundLocation


def parse_interval_dict(i_data: Dict[str, str]) -> FeatureLocation:
    if isinstance(i_data, list):
        i = 0
        for cur_i in i_data:
            i += parse_interval_dict(cur_i)
        return i

    # Generate min location
    min_index = int(i_data['minimumIndex']) - 1
    if '@beginsBeforeMinimumIndex' in i_data:
        min_pos = BeforePosition(min_index)
    else:
        min_pos = ExactPosition(min_index)

    # Generate max location
    max_index = str(i_data['maximumIndex'])
    if '@endsAfterMaximumIndex' in i_data:
        max_pos = AfterPosition(max_index)
    else:
        max_pos = ExactPosition(max_index)

    if i_data['direction'] == 'leftToRight':
        strand = 1
    elif i_data['direction'] == 'rightToLeft':
        strand = -1
    else:
        strand = 0

    return FeatureLocation(min_pos, max_pos, strand)


def unparse_interval_dict(fl: FeatureLocation) -> Union[list, Dict[str, str]]:
    if isinstance(fl, CompoundLocation):
        return [unparse_interval_dict(loc) for loc in fl.parts]

    out_dict = {
        'minimumIndex': fl.start + 1,
        'maximumIndex': fl.end + 0
    }

    if isinstance(fl.start, BeforePosition):
        out_dict['@beginsBeforeMinimumIndex'] = 'true'
    if isinstance(fl.end, BeforePosition):
        out_dict['@endsAfterMaximumIndex'] = 'true'

    if fl.strand == 1:
        out_dict['direction'] = 'leftToRight'
    elif fl.strand == 0:
        out_dict['direction'] = 'rightToLeft'

    return out_dict


def parse_qualifiers(q_data: Dict[str, List[Dict[str, str]]]) -> DefaultDict[str, List[str]]:
    out_dict = defaultdict(list)
    if q_data is None or q_data.get('qualifier', None) is None:
        return out_dict
    if isinstance(q_data['qualifier'], list):
        for q in q_data['qualifier']:
            try:
                out_dict[q['name']].append(q['value'])
            except KeyError:
                q_name = q.pop('name')
                for k, v in q.items():
                    if isinstance(v, dict):
                        out_dict[q_name].append(v.get('#text', str(v)))
                    else:
                        out_dict[q_name].append(str(v))
    elif isinstance(q_data['qualifier'], dict):
        q = q_data['qualifier']
        out_dict[q['name']].append(q['value'])
    return out_dict


def unparse_qualifiers(q_data: Dict[str, List[str]]):
    out_list = []
    for key, value_list in q_data.items():
        out_list.append({'name': key, 'value': value_list[0]})
    return {'qualifier': out_list}


def parse_annotation(a_data: dict, enforce_len: bool = False) -> SeqFeature:
    loc = parse_interval_dict(a_data['intervals']['interval'])
    quals = parse_qualifiers(a_data['qualifiers'])
    quals['label'] = [a_data['description']]

    f_type: str = a_data['type']
    if enforce_len and len(f_type) > 16 and f_type.endswith(')'):
        f_type = f_type.split('(')[-1]
        f_type = f_type.strip(')')
        f_type = f_type[:16]

    return SeqFeature(loc, f_type, id=a_data['description'], qualifiers=quals)


def unparse_annotations(feature: SeqFeature) -> dict:
    for name_qualifier in ['label', 'name', 'Name', 'id']:
        if name_qualifier in feature.qualifiers:
            description = feature.qualifiers[name_qualifier][0]
            break
    else:
        description = feature.id
    out_dict = {
        'intervals': {'interval': unparse_interval_dict(feature.location)},
        'qualifiers': unparse_qualifiers(feature.qualifiers),
        'description': description,
        'type': feature.type
    }
    return out_dict
