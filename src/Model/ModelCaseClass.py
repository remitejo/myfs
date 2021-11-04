from typing import List, Union
from Model.ModelMetrics import ClassificationMetrics, RegressionMetrics
import datetime
import pickle


class ModelType:
    classification: str = "Classification"
    regression: str = "Regression"


class Model:
    name: str = "MyModel"
    type: str = ModelType.classification
    features: List[str] = []
    metric: Union[ClassificationMetrics, RegressionMetrics] = ClassificationMetrics()

    def __init__(self, model, name: str, features: List[str]):
        self.model = model
        self.name: str = name
        self.features: List[str] = features
        self.date: datetime.datetime = datetime.datetime.now()

    def generate_metrics(self, X: List[list], y: list) -> None:
        metrics = ClassificationMetrics() if self.type == ModelType.classification else RegressionMetrics()
        metrics.build_metrics(y, self.predict(X))

    def predict(self, X: List[list]) -> list:
        return self.model.predict(X)

    def save(self, full_path: str) -> None:
        pickle.dump(self, open(f"{full_path}", 'wb'))

    @staticmethod
    def load(path):
        return pickle.load(open(f"{path}", "rb"))
