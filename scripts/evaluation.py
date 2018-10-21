# -*- coding: utf-8 -*-
"""
Evaluation file
Author: HJ
"""

import pandas as pd, numpy as np, matplotlib.pyplot as plt
import scipy.sparse as sparse
from scipy.sparse import coo_matrix

cn10 = pd.read_csv("../result/first-CommonNeighbours(10).csv", header=None)
actual = pd.read_csv("../data/second_nodate.csv", header=None)

full = pd.read_csv("../data/twitter.csv")

num_vertices = max(full['Source'].unique())

# Preparing the matrix
source_vertices = full['Source']
source_vertices.columns = ['vertex']
target_vertices = full['Target']
target_vertices.columns = ['vertex']

all_vertices = pd.concat([source_vertices, target_vertices], join='outer')
unique_vertices = all_vertices.unique()

# Calculating true positive
# True positive: in cn10 and result

