import pandas as pd
from typing import List, Tuple, Union


class PartitioningSerializer:
    @staticmethod
    def serialize(values: dict) -> str:
        return "/".join([f"{k}={v}" for k, v in values.items()])

    @staticmethod
    def deserialize(partitioning: str) -> dict:
        kv_pairs = [kv.split("=") for kv in partitioning.replace("\\", "/").split("/") if "=" in kv]
        return {k: v for k, v in kv_pairs}


class PandasPartitioner:
    def __init__(self, partitionBy: list = []):
        self.partitionBy = partitionBy
        self.serializer = PartitioningSerializer()

    def _map_partitions(self, df: pd.DataFrame, partitions_values: list) -> pd.DataFrame:
        """
        Add to the pandas.DataFrame as many columns as we have different combination of values
        in the self.partitionBy columns
        :param df:
        :param partitions_values:
        :return:
        """
        col_number = len(self.partitionBy)
        for partition_number in range(len(partitions_values)):
            df[f"_part_{partition_number}"] = True
            for col_index in range(col_number):
                df[f"_part_{partition_number}"] = df[f"_part_{partition_number}"] & \
                                                  (df[self.partitionBy[col_index]] ==
                                                   partitions_values[partition_number][col_index])
        return df

    def get_partitions(self, data: Union[dict, pd.DataFrame]) -> List[Tuple[dict, Union[dict, pd.DataFrame]]]:
        if type(data) == pd.DataFrame: return self._get_pandas_dataframe_partitions(data)
        return [({}, {})]

    def _get_pandas_dataframe_partitions(self, df: pd.DataFrame) -> List[Tuple[dict, pd.DataFrame]]:
        """
        Transform one pandas.DataFrame into a list of pandas.DataFrame depending on values in partitionBy columns
        :param df: pandas.DataFrame
        :return: list of tuple of {partition values} and pandas.DataFrame's subset
        """
        partitions_filter = df[self.partitionBy].drop_duplicates().values
        partitions_name = [f"_part_{p_number}" for p_number in range(len(partitions_filter))]
        partitioned_df = self._map_partitions(df, partitions_filter)
        return [({self.partitionBy[i]:partitions_filter[p_number][i] for i in range(len(self.partitionBy))},
                 partitioned_df.loc[partitioned_df[partitions_name[p_number]] == True]
                               .drop(columns=partitions_name))
                for p_number in range(len(partitions_filter))]


if __name__ == '__main__':
    print(PartitioningSerializer.serialize({"year": '123', "month": 1234}))
    print(PartitioningSerializer.deserialize("year=123/month=1234"))

