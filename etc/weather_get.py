#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct  2 15:09:20 2025

@author: marika
"""
# Import Meteostat library and dependencies
from datetime import datetime
import matplotlib.pyplot as plt
from meteostat import Point, Daily

# Set time period
start = datetime(2025, 9, 30)
end = datetime(2025, 10, 1)

# Create Point for Tallinn
tallinn = Point(59.4370, 24.7536)

# Get daily data for 2018
data = Daily(tallinn, start, end)
data = data.fetch()

# Plot line chart including average, minimum and maximum temperature
data.plot(y=['tavg', 'tmin', 'tmax'])
plt.show()