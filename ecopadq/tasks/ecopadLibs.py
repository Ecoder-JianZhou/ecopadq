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
# from .spruce_tasks import pull_data, merge_data
import shutil, random
import pandas as pd
import numpy as np
import importlib

client=SSHClient()
client.set_missing_host_key_policy(AutoAddPolicy())
client.load_system_host_keys()

basedir="/data/ecopad_test"
# startYr = 2015
ls_spec_sites  = ["SPRUCE"]
ls_spec_models = ["TECO_SPRUCE","matrix_models","all"]  # Jian: "all" is used to test the TECO_SPRUCE AND matrix_models at SPRUCE site
# dict_sites     = {}
# for iSite in ls_spec_sites:
#     script_site = iSite + "_tasks"  
#     dict_sites[iSite] = ecec("import "+ script_site) # importlib.import_module(script_site)
# ------------------------------------------------------------------

class ecopadObj:
    def __init__(self, dockerName, task_id, modname, sitname):
        client.connect(dockerName,username=os.getenv('CELERY_SSH_USER'),password=os.getenv('CELERY_SSH_PASSWORD')) 
        self.task_id = task_id
        self.modname = modname
        self.sitname = sitname
        # ---------------------
        self.check_model_site()
        self.setup_result_directory()
        self.experiment = "lastest_forecast_results_380ppm_0degree"

    def check_model_site(self):
        if not self.modname in ls_spec_models:
            print("Your select model name ("+self.modname+ ") is not in the system. Please check it.")
            print("Valid models: ", ls_spec_models)
            exit(0)
        if not self.sitname in ls_spec_sites:
            print("Your select site name ("+self.modname+ ") is not in the system. Please check it.")
            print("Valid sites: ", ls_spec_sites)
            exit(0)
    
    def run_auto_forecast(self):
        ''' To automatically forecast, we must finish three steps:
                1. pull the data from specific site server.
                2. read future forcing data and parameter set
                3. run simulation
        '''
        try:
            script_site   = self.sitname+"_tasks"
            mod_site      =  importlib.import_module("ecopadq.tasks."+script_site.lower())   # import specific site task module
            # exec("import "+self.sitname+" as mod_site")
            # 1. pull data must have specific script to finish it. uniform script name format: "{sitname}_tasks". pull the data and update the forcing data.
            # ----- set the pull data paths
            path_pullData   = os.path.join(basedir,'sites_data', self.sitname, 'forcing_data' ,'pull_forcing_data') 
            path_newForcing = os.path.join(path_pullData, self.sitname + "_forcing.txt")
            path_desForcing = os.path.join(basedir,'sites_data', self.sitname, 'forcing_data' ,self.sitname + "_forcing.txt") 
            # ----- run pull data
            mod_site.pull_data(path_pullData)  # each site must have this path
            # ----- update the default forcing of the specific site. 
            temp = shutil.copyfile(path_newForcing, path_desForcing)
            # ----- create the forcing fold in the task output fold. also copy the updated forcing data to task dir.
            taskDir_forcing = os.path.join(self.taskDir_in, "forcing")
            os.makedirs(taskDir_forcing, exist_ok=True) 
            temp = shutil.copyfile(path_newForcing, os.path.join(taskDir_forcing,self.sitname + "_forcing.txt"))
            # ------------------------------------------------------------------------------------------------------------------
            # 2. get the future forcing data and parameter set.
            lsPath_newForcing = mod_site.weather_generate(1, temp , taskDir_forcing)  # Jian: return list, here just use 1 (len) for test, temp=pulled data; 
            taskDir_params    = os.path.join(self.taskDir_in, "parameters")
            os.makedirs(taskDir_params, exist_ok=True)
            temp = shutil.copyfile(os.path.join(basedir, 'sites_data', self.sitname, 'parameters' ,self.sitname + "_pars.yml"), os.path.join(taskDir_params,self.sitname + "_pars.yml"))
            lsPath_newParams  = mod_site.get_params_set(10, self.modname, taskDir_params)            # Jian: return list, here just use 10 parameter sets for testing
            # ------------------------------------------------------------------------------------------------------------------
            # 3. run simulation
            path_temp_simu = os.path.join(self.taskDir, "simulation_temp")
            os.makedirs(path_temp_simu, exist_ok=True)
            if (len(lsPath_newForcing)>0) and (len(lsPath_newForcing)>0):
                # for iFile_forcing in lsPath_newForcing:
                iFile_forcing = lsPath_newForcing[0]    # Jian: Just use 1 forcing for testing
                for iSimu, iFile_params in enumerate(lsPath_newParams):
                    iPath_simu = os.path.join(path_temp_simu, "simulation_"+str(iSimu)) 
                    os.makedirs(iPath_simu, exist_ok=True)
                    ssh_cmd = "python3 run.py {0} {1} {2} {3} {4} {5}".format(self.modname, iFile_params, iFile_forcing, self.taskDir, iPath_simu)
                    print(ssh_cmd)
                    stdin, stdout, stderr = client.exec_command(ssh_cmd)
                    result = str(stdout.read())
        except Exception as e:
            print(e)
                

        

    # def auto_forecasting_old(self):
    #     # 1. pull data
    #     # 2. read future forcing data
    #     # 3. run simulation
    #     # ----------------------------------
    #     # pull data
    #     pull_data((os.path.join(basedir,'sites_data', self.sitname, 'forcing_data' ,'pull_forcing_data')))
    #     pulledFile = shutil.copyfile(os.path.join(basedir,'sites_data', self.sitname, 'forcing_data' ,'pull_forcing_data',"SPRUCE_forcing.txt"), os.path.join(basedir,'sites_data', self.sitname, 'forcing_data' ,"SPRUCE_forcing.txt"))
    #     # read future forcing data: read preset(2011-2024) data
    #     os.makedirs(self.resultDir+"/input/forcing", exist_ok = True) # create a fold to put the new forcing data
    #     preWeatherFile = os.path.join(basedir,'sites_data', self.sitname, 'forcing_data', 'weather_generate','preset_2011-2024') # 300 files
    #     temp_rand      = random.sample(range(1,301), 10) # 100 random from [1,300]
    #     ls_new_forcing     = []
    #     for idx, iRand in enumerate(temp_rand):
    #         str_iRand   = str(iRand).zfill(3)
    #         weatherPath = preWeatherFile+"/EMforcing"+str_iRand+".csv"
    #         newFile     = self.resultDir+"/input/forcing"+"/SPRUCE_forcing"+str_iRand+".txt"
    #         merge_data(pulledFile, weatherPath, newFile)
    #         ls_new_forcing.append(newFile)
    #     # read different parameters: parameters/data_assimilation
    #     df_params_org = pd.read_csv(os.path.join(basedir,'sites_data', self.sitname, "parameters","data_assimilation","Paraest.txt"), header=None)
    #     ls_params = ["id","SLA",  "GLmax",    "GRmax",       "Gsmax", "Vcmax0", "Tau_Leaf", "Tau_Wood", "Tau_Root","Tau_F",
    #                  "Tau_C","Tau_Micro","Tau_SlowSOM", "Tau_Passive", "gddonset", "Q10", "Rl0","Rs0","Rr0","nan"]
    #     df_params_org.columns = ls_params
    #     df_params = df_params_org.loc[:,ls_params[1:-1]]
    #     # input: paramters
    #     task_inParams = os.path.join(self.resultDir,"input","parameters")
    #     os.makedirs(task_inParams, exist_ok=True)
    #     yml_setting = os.path.join(basedir, "sites_data", self.sitname,"setting.yml")
    #     yml_pars    = os.path.join(basedir, "sites_data", self.sitname,"parameters",self.sitname+"_pars.yml")
    #     rand_par    = random.sample(range(1,len(df_params)+1), 10)
    #     # We just test the 100 Parameters sets.
    #     ls_outs = []
    #     for iSimu, iRand in enumerate(rand_par):  # setting.yml; SPRUCE_pars.yml
    #         # Jian: This part must be modified becuase it is confused.
    #         iParamFile = os.path.join(task_inParams,"parameter_"+str(iSimu)+".yml")
    #         shutil.copyfile(yml_pars, iParamFile)
    #         with open(iParamFile) as f:
    #             doc = yaml.safe_load(f)
    #         params_new = df_params.iloc[iRand].to_dict()
    #         for key, value in params_new.items():
    #             doc["params"][key]=value
    #         with open(iParamFile, 'w') as f:
    #             yaml.safe_dump(doc, f, default_flow_style=False)    
    #         # ---------------------------------------------------------------
    #         iSetting = os.path.join(task_inParams,"setting_"+str(iSimu)+".yml")  
    #         shutil.copyfile(yml_setting, iSetting)
    #         with open(iSetting) as f:
    #             doc = yaml.safe_load(f)
    #         doc["TECO_SPRUCE"]["paraFile"]=iParamFile
    #         doc["TECO_SPRUCE"]["forcingFile"]=ls_new_forcing[0]
    #         with open(iSetting, 'w') as f:
    #             yaml.safe_dump(doc, f, default_flow_style=False)
    #         # ----------------------------------------------------------------------------------------------
    #         i_output=self.resultDir+"/output/output_"+str(iSimu)
    #         os.makedirs(i_output, exist_ok=True)
    #         ssh_cmd = "python3 run.py {0} {1} {2}".format(iSetting, self.modname, i_output)
    #         print(ssh_cmd)
    #         stdin, stdout, stderr = client.exec_command(ssh_cmd)
    #         result = str(stdout.read())
    #         ls_outs.append(i_output)
    #         # if self.modname == "all":  
    #         #     self.transfer2WebShow() # test forecasting
    #     # delete the input forcing data
    #     # for iFile in lsWeatherPath
    #     # --------------------------------------------------
    #     ls2show = ["gpp.csv","npp.csv","nee.csv","er.csv","ra.csv","rh.csv","cStorage.csv"]
    #     ls_models = ["TECO_SPRUCE","TEM","DALEC","TECO","FBDC","CASA","CENTURY","CLM","ORCHIDEE"]
    #     arr_gpp  = pd.read_csv(ls_outs[0]+"/gpp.csv").loc[:,"TECO_SPRUCE"].to_numpy()
    #     arr_npp  = pd.read_csv(ls_outs[0]+"/npp.csv").loc[:,ls_models].to_numpy()
    #     arr_nee  = pd.read_csv(ls_outs[0]+"/nee.csv").loc[:,ls_models].to_numpy()
    #     arr_er   = pd.read_csv(ls_outs[0]+"/er.csv").loc[:,ls_models].to_numpy()
    #     arr_ra   = pd.read_csv(ls_outs[0]+"/ra.csv").loc[:,ls_models].to_numpy()
    #     arr_rh   = pd.read_csv(ls_outs[0]+"/rh.csv").loc[:,ls_models].to_numpy()
    #     arr_csto = pd.read_csv(ls_outs[0]+"/cStorage.csv").loc[:,ls_models].to_numpy()
    #     all_gpp  = np.full((len(ls_outs), len(arr_gpp)), np.nan)
    #     all_npp  = np.full((len(ls_outs), arr_npp.shape[0], arr_npp.shape[1]), np.nan)
    #     all_nee  = np.full((len(ls_outs), all_nee.shape[0], all_nee.shape[1]), np.nan)
    #     all_er  = np.full((len(ls_outs), all_er.shape[0], all_er.shape[1]), np.nan)
    #     all_ra  = np.full((len(ls_outs), all_ra.shape[0], all_ra.shape[1]), np.nan)
    #     all_rh  = np.full((len(ls_outs), all_rh.shape[0], all_rh.shape[1]), np.nan)
    #     all_csto  = np.full((len(ls_outs), all_csto.shape[0], all_csto.shape[1]), np.nan)
    #     for idx, iout in enumerate(ls_outs):
    #         all_gpp[idx,:]  = pd.read_csv(iout+"/gpp.csv").loc[:,"TECO_SPRUCE"].to_numpy()
    #         all_npp[idx,:]  = pd.read_csv(iout+"/npp.csv").loc[:,ls_models].to_numpy()
    #         all_nee[idx,:]  = pd.read_csv(iout+"/nee.csv").loc[:,ls_models].to_numpy()
    #         all_er[idx,:]   = pd.read_csv(iout+"/er.csv").loc[:,ls_models].to_numpy()
    #         all_ra[idx,:]   = pd.read_csv(iout+"/ra.csv").loc[:,ls_models].to_numpy()
    #         all_rh[idx,:]   = pd.read_csv(iout+"/rh.csv").loc[:,ls_models].to_numpy()
    #         all_csto[idx,:] = pd.read_csv(iout+"/cStorage.csv").loc[:,ls_models].to_numpy()
    #     df_time  = pd.read_csv(ls_outs[0]+"/gpp.csv").loc[:,["id","year","doy"]]

    #     df_gpp   = df_time.copy(); df_gpp["TECO_SPRUCE"] = np.nanmean(all_gpp, axis=0)
    #     df_npp   = df_time.copy(); df_npp[ls_models]     = np.nanmean(all_npp, axis=0)
    #     df_nee   = df_time.copy(); df_nee[ls_models]     = np.nanmean(all_nee, axis=0)
    #     df_er    = df_time.copy(); df_er[ls_models]      = np.nanmean(all_er, axis=0)
    #     df_ra    = df_time.copy(); df_ra[ls_models]      = np.nanmean(all_ra, axis=0)
    #     df_rh    = df_time.copy(); df_rh[ls_models]      = np.nanmean(all_rh, axis=0)
    #     df_csto  = df_time.copy(); df_csto[ls_models]    = np.nanmean(all_csto, axis=0)
        
    #     return result


    def run_simulation(self):
        # call for the run.py in each model docker. 
        print("simulation ....")
        # ssh_cmd = "python3 run.py {0} {1} {2}".format(os.path.join(basedir, "sites_data", self.sitname,"setting.yml"), self.modname, self.resultDir)
        # print(ssh_cmd)
        # stdin, stdout, stderr = client.exec_command(ssh_cmd)
        # result = str(stdout.read())
        # if self.modname == "all":  
        #     self.transfer2WebShow() # test forecasting
        # return result

    def run_spinup(self):
        print("spinup ...")
        # Just for TECO_SPRUCE model because the matrix models have the process of spinup
        

    def run_data_assimilation(self):
        print("data assimilation ...")
        # MIDA or the data assimilation process in TECO_SPRUCE? 


    def run_forecast(self):
        print("forecast ...")

    def setup_result_directory(self):
        taskDir     = os.path.join(basedir, 'ecopad_tasks/', self.task_id)
        taskDir_in  = "{0}/input".format(taskDir)
        taskDir_out = "{0}/output".format(taskDir)
        os.makedirs(taskDir,     exist_ok=True)
        os.makedirs(taskDir_in,  exist_ok=True)
        os.makedirs(taskDir_out, exist_ok=True)
        self.taskDir     = taskDir
        self.taskDir_in  = taskDir_in
        self.taskDir_out = taskDir_out
    
    def transfer2WebShow(self):
        # id/output/gpp.csv|npp.csv|nee.csv|er.csv|ra.csv|rh.csv|cStorage.csv
        ls2show = ["gpp.csv","npp.csv","nee.csv","er.csv","ra.csv","rh.csv","cStorage.csv"]
        for ifile in ls2show:
            sourceFile      = self.resultDir+"/output/"+ifile
            destinationFile = "/webData/show_forecast_results/"+self.experiment+"/"+ifile
            if os.path.exists(destinationFile): os.remove(destinationFile) 
            temp = shutil.copyfile(sourceFile, destinationFile)
    
    # def transferMutiRes2WebShow(self):

        
    # def set_state(self, file_name, dictState):
    #     with open(file_name) as f:
    #         doc = yaml.safe_load(f)
    #     for key, value in dictState.items():
    #         doc[key] = value
    #     with open(file_name, 'w') as f:
    #         yaml.safe_dump(doc, f, default_flow_style=False)