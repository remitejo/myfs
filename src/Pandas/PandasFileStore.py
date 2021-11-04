import pandas as pd
from src.Index import Index
from Pandas.PandasPartitioner import PandasPartitioner
from typing import List
import uuid
import os


class PandasFileStore:
    @staticmethod
    def _find_index(path) -> str:
        if path == '': raise Exception(f"This path doesn't have index")
        if Index.index_filename in os.listdir(path): return path
        return PandasFileStore._find_index("/".join(path.split("/")[:-1]))

    @staticmethod
    def _get_files(index: dict) -> List[str]:
        queue = [('', index)]
        files = []
        while queue:
            sub_path, e = queue.pop(0)
            if type(e) == dict:
                queue += [(f"{sub_path}/{key}", sub_dict) for key, sub_dict in e.items()]
            else:
                files += [f"{sub_path}/{filename}" for filename in e]
        return files

    @staticmethod
    def pandas_read(path) -> pd.DataFrame:
        path = path.replace("\\", "/")
        assert(os.path.exists(path))
        base_path = PandasFileStore._find_index(path)
        index = Index(files_path=base_path)
        index.load_existing_index()

        # Retrieve files related to given path from index
        partition_filter = path[len(base_path)+1:]
        if len(partition_filter):
            partition_filter = partition_filter.split("/")
            sub_index = index.index
            if len(partition_filter):
                for p in partition_filter:
                    sub_index = sub_index[p]
            files = PandasFileStore._get_files(sub_index)
        else:
            files = PandasFileStore._get_files(index.index)
        return pd.concat([pd.read_csv(f"{path}/{filename}") for filename in files])

    @staticmethod
    def _map_partition_filepath(path: str, filename: str,
                                partition_filter: dict) -> str:
        return "/".join([path.replace("\\", "/"),
                         filename,
                        *[f"{k}={v}" for k,v in partition_filter.items()]])

    @staticmethod
    def pandas_write(df: pd.DataFrame, path: str, filename: str, partitionBy: list,
                     file_type: str = "csv", mode: str = "append", failsOnLock: bool = False) -> None:
        # Need to make it work as a transaction to ensure index and data are either both written or none is
        # using temp dirs (?)
        if f".{file_type}" not in filename: filename = f"{filename}.{file_type}"
        path = path.replace("\\", "/")
        p = PandasPartitioner(partitionBy=partitionBy)
        partitioned_df = p.get_partitions(df)
        partitions_name = [f"part_{uuid.uuid4().hex}.{file_type}" for i in range(len(partitioned_df))]

        index = Index(files_path="/".join([path, filename]))
        index.load_existing_index()
        index.build_index(new_files=[(partitioned_df[i][0], partitions_name[i])
                                     for i in range(len(partitions_name))])

        for p_df_tuple, part_name in zip(partitioned_df, partitions_name):
            curr_partition_path = PandasFileStore._map_partition_filepath(path, filename, p_df_tuple[0])
            if not os.path.exists(curr_partition_path): os.makedirs(curr_partition_path)
            p_df_tuple[1].to_csv(f"{curr_partition_path}/{part_name}", index=False)

        index.write_index()
        return


if __name__ == '__main__':
    package_path = "/".join(os.getcwd().replace("\\", "/").split("/")[:-2])

    test_data = pd.DataFrame({"A": [1, 2, 2], "B": [3, 2, 2], "C": [1, 1, 3]})
    test_w_data_path = os.path.join(package_path, 'tests')
    PandasFileStore.pandas_write(test_data, test_w_data_path, 'test_file', ["A", "B", "C"])

    test_r_data_path = os.path.join(package_path, 'tests/test_file.csv/A=2/B=2')
    print(PandasFileStore.pandas_read(test_r_data_path))
