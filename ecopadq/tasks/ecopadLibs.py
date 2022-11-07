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
import os, yaml
from .spruce_tasks import pull_data, merge_data
import shutil, random
import pandas as pd

client=SSHClient()
client.set_missing_host_key_policy(AutoAddPolicy())
client.load_system_host_keys()

basedir="/data/ecopad_test"
# startYr = 2015

class ecopadObj:
    def __init__(self, dockerName, task_id, modname, sitname):
        # use the "local_fortran_example", which will be wroten a Docker named as model_name
        client.connect(dockerName,username=os.getenv('CELERY_SSH_USER'),password=os.getenv('CELERY_SSH_PASSWORD')) 
        self.task_id = task_id
        self.modname = modname
        self.sitname = sitname 
        self.setup_result_directory()
        self.experiment = "lastest_forecast_results_380ppm_0degree"

    def auto_forecasting(self):
        # 1. pull data
        # 2. read future forcing data
        # 3. run simulation
        # ----------------------------------
        # pull data
        pull_data((os.path.join(basedir,'sites_data', self.sitname, 'forcing_data' ,'pull_forcing_data')))
        pulledFile = shutil.copyfile(os.path.join(basedir,'sites_data', self.sitname, 'forcing_data' ,'pull_forcing_data',"SPRUCE_forcing.txt"), os.path.join(basedir,'sites_data', self.sitname, 'forcing_data' ,"SPRUCE_forcing.txt"))
        # read future forcing data: read preset(2011-2024) data
        os.makedirs(self.resultDir+"/input/forcing", exist_ok = True) # create a fold to put the new forcing data
        preWeatherFile = os.path.join(basedir,'sites_data', self.sitname, 'forcing_data', 'weather_generate','preset_2011-2024') # 300 files
        temp_rand      = random.sample(range(1,301), 10) # 100 random from [1,300]
        ls_new_forcing     = []
        for idx, iRand in enumerate(temp_rand):
            str_iRand   = str(iRand).zfill(3)
            weatherPath = preWeatherFile+"/EMforcing"+str_iRand+".csv"
            newFile     = self.resultDir+"/input/forcing"+"/SPRUCE_forcing"+str_iRand+".txt"
            merge_data(pulledFile, weatherPath, newFile)
            ls_new_forcing.append(newFile)
        # read different parameters: parameters/data_assimilation
        df_params_org = pd.read_csv(os.path.join(basedir,'sites_data', self.sitname, "parameters","data_assimilation","Paraest.txt"), header=None)
        ls_params = ["id","SLA",  "GLmax",    "GRmax",       "Gsmax", "Vcmax0", "Tau_Leaf", "Tau_Wood", "Tau_Root","Tau_F",
                     "Tau_C","Tau_Micro","Tau_SlowSOM", "Tau_Passive", "gddonset", "Q10", "Rl0","Rs0","Rr0","nan"]
        df_params_org.columns = ls_params
        df_params = df_params_org.loc[:,ls_params[1:-1]]
        # input: paramters
        task_inParams = os.path.join(self.resultDir,"input","parameters")
        os.makedirs(task_inParams, exist_ok=True)
        yml_setting = os.path.join(basedir, "sites_data", self.sitname,"setting.yml")
        yml_pars    = os.path.join(basedir, "sites_data", self.sitname,"parameters",self.sitname+"_pars.yml")
        rand_par    = random.sample(range(1,len(df_params)+1), 100)
        # We just test the 100 Parameters sets.
        for iSimu, iRand in enumerate(rand_par):  # setting.yml; SPRUCE_pars.yml
            # Jian: This part must be modified becuase it is confused.
            iParamFile = os.path.join(task_inParams,"parameter_"+str(iSimu)+".yml")
            shutil.copyfile(yml_pars, iParamFile)
            with open(iParamFile) as f:
                doc = yaml.safe_load(f)
            params_new = df_params.iloc[iRand].to_dict()
            for key, value in params_new.items():
                doc["params"][key]=value
            with open(iParamFile, 'w') as f:
                yaml.safe_dump(doc, f, default_flow_style=False)    
            # ---------------------------------------------------------------
            iSetting = os.path.join(task_inParams,"setting_"+str(iSimu)+".yml")  
            shutil.copyfile(yml_setting, iSetting)
            with open(iSetting) as f:
                doc = yaml.safe_load(f)
            doc["TECO_SPRUCE"]["paraFile"]=iParamFile
            doc["TECO_SPRUCE"]["forcingFile"]=ls_new_forcing[0]
            with open(iSetting, 'w') as f:
                yaml.safe_dump(doc, f, default_flow_style=False)
            # ----------------------------------------------------------------------------------------------
            i_output=self.resultDir+"/output/output_"+str(iSimu)
            os.makedirs(i_output, exist_ok=True)
            ssh_cmd = "python3 run.py {0} {1} {2}".format(iSetting, self.modname, i_output)
            print(ssh_cmd)
            stdin, stdout, stderr = client.exec_command(ssh_cmd)
            result = str(stdout.read())
            # if self.modname == "all":  
            #     self.transfer2WebShow() # test forecasting
        # delete the input forcing data
        # for iFile in lsWeatherPath
        return result


    def run_simulation(self):
        # call for the run.py in each model docker. 
        ssh_cmd = "python3 run.py {0} {1} {2}".format(os.path.join(basedir, "sites_data", self.sitname,"setting.yml"), self.modname, self.resultDir)
        print(ssh_cmd)
        stdin, stdout, stderr = client.exec_command(ssh_cmd)
        result = str(stdout.read())
        if self.modname == "all":  
            self.transfer2WebShow() # test forecasting
        return result

    def run_spinup(self):
        print("spinup ...")
        # Just for TECO_SPRUCE model because the matrix models have the process of spinup
        

    def run_data_assimilation(self):
        print("data assimilation ...")
        # MIDA or the data assimilation process in TECO_SPRUCE? 


    def run_forecast(self):
        print("forecast ...")

    def setup_result_directory(self):
        resultDir = os.path.join(basedir, 'ecopad_tasks/', self.task_id)
        os.makedirs(resultDir)
        os.makedirs("{0}/input".format(resultDir))
        os.makedirs("{0}/output".format(resultDir))
        os.makedirs("{0}/plot".format(resultDir))
        self.resultDir = resultDir
        return resultDir 
    
    def transfer2WebShow(self):
        # id/output/gpp.csv|npp.csv|nee.csv|er.csv|ra.csv|rh.csv|cStorage.csv
        ls2show = ["gpp.csv","npp.csv","nee.csv","er.csv","ra.csv","rh.csv","cStorage.csv"]
        for ifile in ls2show:
            sourceFile      = self.resultDir+"/output/"+ifile
            destinationFile = "/webData/show_forecast_results/"+self.experiment+"/"+ifile
            if os.path.exists(destinationFile): os.remove(destinationFile) 
            temp = shutil.copyfile(sourceFile, destinationFile)
        
    def set_state(self, file_name, dictState):
        with open(file_name) as f:
            doc = yaml.safe_load(f)
        for key, value in dictState.items():
            doc[key] = value
        with open(file_name, 'w') as f:
            yaml.safe_dump(doc, f, default_flow_style=False)