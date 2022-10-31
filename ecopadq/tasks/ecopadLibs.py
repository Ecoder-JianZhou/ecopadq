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
client=SSHClient()
client.set_missing_host_key_policy(AutoAddPolicy())
client.load_system_host_keys()

class ecopadObj:
    def __init__(self, dockerName, modelName, sitname):
        # use the "local_fortran_example", which will be wroten a Docker named as model_name
        client.connect(dockerName,username=os.getenv('CELERY_SSH_USER'),password=os.getenv('CELERY_SSH_PASSWORD')) 

    def run_simulation(self):
        print("simulation ...")
        ssh_cmd = "./test {0} {1} {2} {3} {4} {5}".format(param_filename, input_files["forcing"], 'input/SPRUCE_obs.txt', resultDir+'/output/', '0', 'input/SPRUCE_da_pars.txt')
        print(ssh_cmd)
        stdin, stdout, stderr = client.exec_command(ssh_cmd)
        result = str(stdout.read())

    def run_spinup(self):
        print("spinup ...")

    def run_data_assimilation(self):
        print("data assimilation ...")

    def run_forecast(self):
        print("forecast ...")