from sklearn.metrics import precision_score, recall_score, fbeta_score, roc_curve, auc, confusion_matrix
from typing import List

import plotly.express as px


class ClassificationMetrics:
    precision: float = 0.0
    recall: float = 0.0
    f_score: float = 0.0
    f_beta: int = 1
    tp, fp, fn, tn = 0, 0, 0, 0
    roc_curve: List[float] = []
    fpr, tpr, thresholds = [], [], []
    threshold: float = .5

    def build_metrics(self, y_true: list, y_predicted: list, threshold: float = .5) -> None:
        self.threshold = threshold
        binary_y_predicted = [e > self.threshold for e in y_predicted]
        self.precision = precision_score(y_true, binary_y_predicted)
        self.recall = recall_score(y_true, binary_y_predicted)
        self.f_score = fbeta_score(y_true, binary_y_predicted, beta=self.f_beta)
        self.tp, self.fp, self.fn, self.tn = confusion_matrix(y_true, y_predicted)
        self.roc_curve = roc_curve(y_true, y_predicted)
        self.fpr, self.tpr, self.thresholds = roc_curve(y_true, y_predicted)

    def plot_roc_curve(self):
        fig = px.area(x=self.fpr, y=self.tpr,
                      title=f'ROC Curve (AUC={auc(self.fpr, self.tpr):.4f})',
                      labels=dict(x='False Positive Rate', y='True Positive Rate'))
        fig.show()

    def to_json(self):
        return {"threshold": self.threshold,
                "precision": self.precision, "recall": self.recall,
                "f_score": self.f_beta,
                "tp": self.tp, "fp": self.fp, "fn": self.fn, "tn": self.tn}


class RegressionMetrics:
    MSE = 0
    MAE = 0
    MAPE = 0

    def build_metrics(self, y_true: list, y_predicted: list) -> None:
        return


if __name__ == '__main__':
    m = ClassificationMetrics()
    m.build_metrics([1,1,1,0,0,1], [.8,.7,.5,.2,.8,.1])
    m.plot_roc_curve()