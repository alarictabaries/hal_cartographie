import os
import eland as ed
from sklearn import datasets
from sklearn.tree import DecisionTreeClassifier
from eland.ml import MLModel


df = ed.DataFrame(
   es_client="http://elastic:" + os.environ.get('ES_PASSWORD') + "@localhost:9200/",
   es_index_pattern="hal2",
)

training_data = datasets.make_classification(n_features=5, random_state=0)
test_data = [[-50.1, 0.2, 0.3, -0.5, 1.0], [1.6, 2.1, -10, 50, -1.0]]
classifier = DecisionTreeClassifier()
classifier = classifier.fit(training_data[0], training_data[1])

print(classifier.predict(test_data))