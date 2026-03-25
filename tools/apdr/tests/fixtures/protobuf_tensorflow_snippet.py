import tensorflow as tf
import numpy as np
from google.protobuf import descriptor

model = tf.keras.Sequential([
    tf.keras.layers.Dense(128, activation='relu'),
    tf.keras.layers.Dense(10, activation='softmax'),
])
data = np.random.randn(32, 784)
print(model(data).shape)
