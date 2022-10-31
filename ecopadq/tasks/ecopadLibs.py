## this code is used to provide the common APIs for EcoPad.
##    including: 
##         run_simulation         ---> call model code (hourly) 
##         run_spinup             ---> structure of spinup and call model code 
##         run_data_assimilation  ---> structure of data assimilation and call model code
##         run_forecast           ---> structure of forecast and call model code
##  ---------------------------------------------------------------------------------
##   each model module must be a docker, and can connect by SSH
##  ---------------------------------------------------------------------------------
##   Edit: Jian Zhou
##   Date: 10/07/2022
## ========================================================================================

from paramiko import SSHClient, AutoAddPolicy
import os

client=SSHClient()
client.set_missing_host_key_policy(AutoAddPolicy())
client.load_system_host_keys()

basedir="/data/ecopad_test"

class ecopadObj:
    def __init__(self, dockerName, task_id, modname, sitname):
        # use the "local_fortran_example", which will be wroten a Docker named as model_name
        client.connect(dockerName,username=os.getenv('CELERY_SSH_USER'),password=os.getenv('CELERY_SSH_PASSWORD')) 
        self.task_id = task_id
        self.modname = modname
        self.sitname = sitname 
        # self.setup_result_directory()

    def run_simulation(self):
        # call for the run.py in each model docker. 
        # ssh_cmd = "python run.py"
        # print(ssh_cmd)
        # stdin, stdout, stderr = client.exec_command(ssh_cmd)
        # result = str(stdout.read())
        # print(result)
        return "jzhou"

    def run_spinup(self):
        print("spinup ...")

    def run_data_assimilation(self):
        print("data assimilation ...")

    def run_forecast(self):
        print("forecast ...")

    def setup_result_directory(self):
        resultDir = os.path.join(basedir, 'ecopad_tasks/', self.task_id)
        os.makedirs(resultDir)
        os.makedirs("{0}/input".format(resultDir))
        os.makedirs("{0}/output".format(resultDir))
        os.makedirs("{0}/plot".format(resultDir))
        return resultDir 