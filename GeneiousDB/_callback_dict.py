from collections.abc import MutableMapping, MutableSequence, Collection
from copy import copy
from typing import Iterator, TypeVar, Callable

VT = TypeVar('VT')


def _empty_callback(key: str, old_value: VT, new_value: VT, fxn_called: str):
    pass


class CallbackDict(MutableMapping):

    def __init__(self, callback: Callable[[str, VT, VT, str], None],
                 initial_dict: Collection) -> None:
        self._internal_dict = dict()
        self._get_cache = {}
        self._callback = _empty_callback
        if isinstance(initial_dict, dict):
            for k, v in initial_dict.items():
                self[k] = self._add_callback(v, callback)
        self._callback = callback

    def _add_callback(self, value, callback=None) -> VT:
        if isinstance(value, dict):
            return CallbackDict(callback or self._callback, value)
        return value

    def __setitem__(self, key: str, value: VT) -> None:
        if key in self._get_cache:
            old_value = self._get_cache[key]
            del self._get_cache[key]
        else:
            old_value = copy(self._internal_dict.get(key, None))
        self._internal_dict[key] = self._add_callback(value)
        self._callback(key, old_value, value, 'set')

    def __delitem__(self, key: str) -> None:
        value = self._internal_dict.pop(key)
        self._callback(key, value, None, 'del')

    def __getitem__(self, key: str) -> VT:
        old_value = copy(self._internal_dict[key])
        self._get_cache[key] = old_value
        try:
            return self._internal_dict[key]
        finally:
            self._callback(key, old_value, self._internal_dict[key], 'get')

    def __len__(self) -> int:
        return len(self._internal_dict)

    def __iter__(self) -> Iterator[MutableMapping]:
        return iter(self._internal_dict)

    def __repr__(self):
        return 'CD' + repr(self._internal_dict)

    def __str__(self):
        return str(self._internal_dict)


if __name__ == '__main__':
    def print_callback(key: str, old_value: VT, value: VT, fxn_called: str):
        if fxn_called == 'set':
            if old_value == value or old_value is None:
                print(f"Used {fxn_called} function: key={key}, value={str(value)}")
            else:
                print(f"Used {fxn_called} function: key={key}, value={str(old_value)} -> {str(value)}")

    idict = {'hello': 'how', 'are': ['you'], 'doing': {'today': '?'}, 'Fine': {'thanks'}}
    cd = CallbackDict(print_callback, idict)
    print()
    cd['hello'] += 'a'
    print()
    cd['are'].append('xyz')
    print()
    cd['are'] += ['abc']
    print()
    cd['doing']['a'] = 'b'
    print()
    cd['doing']['today'] += '?'
    print()
    cd['Fine'] |= {'!'}
    print()
    print(repr(cd))
