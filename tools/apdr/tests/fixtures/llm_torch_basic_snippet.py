"""Basic PyTorch neural network — torch is a large package that needs
pre-built wheels. Maps to 'torch' on PyPI."""
import torch
from torch import nn
from torch.autograd import Variable

class SimpleNet(nn.Module):
    def __init__(self, input_size, hidden_size, output_size):
        super(SimpleNet, self).__init__()
        self.fc1 = nn.Linear(input_size, hidden_size)
        self.relu = nn.ReLU()
        self.fc2 = nn.Linear(hidden_size, output_size)
