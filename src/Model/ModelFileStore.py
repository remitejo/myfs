import pandas as pd
import datetime
from src.Index import Index
from Model.ModelCaseClass import Model
from typing import List, Union
import os


class ModelFileStore:
    partitioning_model_name = "model_name"

    @staticmethod
    def _serialize_date(d: Union[datetime.datetime, datetime.date]) -> str:
        return d.strftime('%Y%m%d_%H%M%S') if type(d) == datetime.datetime else d.strftime('%Y%m%d')

    @staticmethod
    def _find_index(path) -> str:
        if path == '': raise Exception(f"This path doesn't have index")
        if Index.index_filename in os.listdir(path): return path
        return ModelFileStore._find_index("/".join(path.split("/")[:-1]))

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
    def model_read(path: str) -> pd.DataFrame:
        path = path.replace("\\", "/")
        assert(os.path.exists(path))
        base_path = ModelFileStore._find_index(path)
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
            files = ModelFileStore._get_files(sub_index)
        else:
            files = ModelFileStore._get_files(index.index)
        return [Model.load(f"{path}/{filename}") for filename in files]

    @staticmethod
    def get_models(path: str, model_name: str,
                   last: bool = True) -> List[Model]:
        models = ModelFileStore.model_read(os.path.join(path, f"{ModelFileStore.partitioning_model_name}={model_name}"))
        if last:
            return max(models, key=lambda x: x.date)
        return models

    @staticmethod
    def _map_partition_filepath(path: str, partition_filter: dict) -> str:
        return os.path.join(path.replace("\\", "/"),
                            *[f"{k}={v}" for k, v in partition_filter.items()])

    @staticmethod
    def model_write(model: Model, path: str,
                    mode: str = "append", failsOnLock: bool = False) -> None:
        # Need to make it work as a transaction to ensure index and data are either both written or none is
        # using temp dirs (?)
        path = path.replace("\\", "/")
        serialized_date = ModelFileStore._serialize_date(model.date)
        partition_model_name = f"{model.name.replace('.pickle', '')}_{serialized_date}.pickle"

        index = Index(files_path=path)
        index.load_existing_index()
        partitionBy = {ModelFileStore.partitioning_model_name: model.name}
        index.build_index(new_files=[(partitionBy, partition_model_name)])
        partition_model_path = ModelFileStore._map_partition_filepath(path, partitionBy)
        if not os.path.exists(partition_model_path): os.makedirs(partition_model_path)
        model.save(f"{partition_model_path}/{partition_model_name}")
        index.write_index()
        return


if __name__ == '__main__':
    package_path = "/".join(os.getcwd().replace("\\", "/").split("/")[:-2])
    test_model = Model('model', 'mymodel.pickle', ['A', 'B'])
    test_w_model_path = os.path.join(package_path, 'tests/models')
    ModelFileStore.model_write(test_model, test_w_model_path)

    # Test reading of a last saved model under mymodel.pickle
    test_r_model_path = os.path.join(package_path, 'tests/models/model_name=mymodel.pickle')
    print(ModelFileStore.model_read(test_r_model_path))
    test_r2_model_path = os.path.join(package_path, 'tests/models/')
    print(ModelFileStore.get_models(test_r2_model_path, "mymodel.pickle", last=True))
