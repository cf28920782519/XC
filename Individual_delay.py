# 个体延误评估
from jdbc.Connect import get_connection
import datetime
import pandas as pd
import numpy as np

# 个体旅行时间延误情况,road_length单位：米；free_flow_speed 单位：km/h
def Ideal_road_travel_time(road_length, free_flow_speed):
