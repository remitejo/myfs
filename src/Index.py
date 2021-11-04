import json
import os
from typing import List, Tuple, Union


class Index:
    index_filename = "index.json"
    index_lock_filename = f"{index_filename}.lock"
    index = {}

    def __init__(self, files_path):
        self.files_path = files_path
        self.index_path = "/".join([self.files_path, self.index_filename])
        self.index_lock_path = "/".join([self.files_path, self.index_lock_filename])

    def load_existing_index(self) -> None:
        self.index = self._read_index()

    def _read_index(self) -> dict:
        if not os.path.exists(self.files_path):
            return {}
        return json.load(open(self.index_path, "r")) if self.index_filename in os.listdir(self.files_path) else None

    def build_index(self, new_files: List[Tuple[dict, str]], inPlace: bool = True) -> Union[dict, None]:
        """
        :param new_files: list of (partitioning values, filename)
        :param inPlace: should overwrite current index (True) or return potential new one (False)
        :return:
        """
        if inPlace == False: return self._merge_index(self.index if self.index else {}, new_files)
        self.index = self._merge_index(self.index if self.index else {}, new_files)

    def _merge_index(self, curr_index: dict, new_files: List[Tuple[dict, str]]) -> dict:
        """
        :param new_files:
        :return:
        """
        for d, filename in new_files:
            sub_dict = curr_index
            key_values = list(d.items())
            for depth in range(len(key_values)):
                k, v = key_values[depth]
                sub_dict[f"{k}={v}"] = sub_dict.get(f"{k}={v}", {} if depth < len(key_values) - 1 else [])
                sub_dict = sub_dict[f"{k}={v}"]
            sub_dict.append(filename)
        return curr_index

    def write_index(self) -> None:
        json.dump(self.index, open(self.index_path, "w"))


if __name__ == '__main__':
    package_path = "/".join(os.getcwd().replace("\\", "/").split("/")[:-1])
    index = Index(os.path.join(package_path, 'tests/models'))
    index.load_existing_index()
    print(index.index)

