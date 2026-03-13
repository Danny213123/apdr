import gym
from keras.layers import Dense
from keras.models import Sequential
import numpy as np
import tensorflow as tf


def build():
    model = Sequential()
    model.add(Dense(8, input_shape=(4,), activation="relu"))
    return gym, Dense, Sequential, np, tf, model
