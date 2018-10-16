# -*- coding: utf-8 -*-
"""
This is a script to create a small dataset (small.csv) and convert data into csv format with datetime

"""

import pandas as pd, numpy as np

print("Reading data")
data = pd.read_csv("../data/flickr-growth-sorted.txt", delim_whitespace = True)

print("Renaming columns")
data.columns = ['From', 'To', 'Date']
print("Converting date to datetime")
data['Date'] = pd.to_datetime(data['Date'])

# Creating small dataset for testing
print("creating smaller dataset")
small = data.head(2000)

# Saving csv files
print('Saving small dataset')
small.to_csv("../data/small.csv")
print('Saving full dataset')
data.to_csv("../data/flickr-full.csv")
