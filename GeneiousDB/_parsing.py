from collections import defaultdict
from typing import Dict, List, DefaultDict

from Bio.SeqFeature import FeatureLocation, BeforePosition, AfterPosition, ExactPosition, SeqFeature


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


def parse_qualifiers(q_data: Dict[str, List[Dict[str, str]]]) -> DefaultDict[str, List[str]]:
    out_dict = defaultdict(list)
    for q in q_data['qualifier']:
        out_dict[q['name']].append(q['value'])
    return out_dict


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

