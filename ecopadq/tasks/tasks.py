# This code is modified by original tasks.py script, which is used to connect the website APIs.
#    1. test mudules: 
#           test_run_simulation;  test_run_pull_date; test_run_data_assimilation; 
#           test_run_forecasting; test_run_spinup.
#    2. run_auto_forecast
# ------------------------------------------------------------------------------------------------


from celery import Celery
import celeryconfig

# from os import getenv
import os
import yaml
#from subprocess import call,STDOUT
from jinja2 import Template
from shutil import copyfile, move
import pandas as pd
import numpy as np
from .ecopadLibs import ecopadObj

basedir="/data/ecopad_test"                 # Default base directory

app = Celery()
app.config_from_object(celeryconfig)

@app.task(bind=True)
def run_auto_forecast(self, modname, sitname):
    ''' 
    '''
    print("This is auto_forecasting ...")
    task_id = str(self.request.id)          # Get the task id from portal 
    taskObj = ecopadObj("local_fortran_example", task_id, modname, sitname)
    results = taskObj.auto_forecasting()

# --------------------------------------------------------------------
# test modules ...
@app.task(bind=True)
def test_run_simulation(self, modname, sitname):
    print("This is the simulaiton ...")
    task_id = str(self.request.id)          # Get the task id from portal
    taskObj = ecopadObj("local_fortran_example", task_id, modname, sitname)
    # results = taskObj.run_simulation()
    results = taskObj.auto_forecasting()

@app.task(bind=True)
def test_run_data_assimilation(self, modname, sitname):
    print("This is the test_run_data_assimilation ...")
    task_id = str(self.request.id)          # Get the task id from portal

@app.task(bind=True)
def test_run_forecasting(self, modname, sitname):
    print("This is the test_run_forecasting ...")
    task_id = str(self.request.id)          # Get the task id from portal

@app.task(bind=True)
def test_run_spinup(self, modname, sitname):
    print("This is the test_run_spinup ...")
    task_id = str(self.request.id)          # Get the task id from portal

@app.task(bind=True)
def test_run_pull_data(self, modname, sitname):
    print("This is the test_run_pull_data ...")
    task_id = str(self.request.id)          # Get the task id from portal

@app.task(bind=True)
def test_run_matrix_models(self, modname, sitname):
    print("This is the test_run_matrix_models ...")
    task_id = str(self.request.id)          # Get the task id from portal