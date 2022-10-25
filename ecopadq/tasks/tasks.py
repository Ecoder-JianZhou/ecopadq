from celery import Celery
import celeryconfig
#from dockertask import docker_task
from paramiko import SSHClient, AutoAddPolicy
# from os import getenv
import os
import yaml
#from subprocess import call,STDOUT
from jinja2 import Template
from shutil import copyfile, move
import pandas as pd
import numpy as np
#from glob import glob
# import requests,os  # Jian: no module named 'requests'
#from pymongo import MongoClient
#from datetime import datetime
##Default base directory 
basedir="/data/ecopad_test"
#spruce_data_folder="/data/local/spruce_data"
#host= 'ecolab.cybercommons.org'
# host_data_dir = os.environ["host_data_dir"] 
## "/home/ecopad/ecopad/data/static"
# add by Jian:
from .datatask import teco_spruce_pulldata

client=SSHClient()
client.set_missing_host_key_policy(AutoAddPolicy())
client.load_system_host_keys()

app = Celery()
app.config_from_object(celeryconfig)
##New Example task
# @app.task()
# def add(a, b):
#     """ Example task that add two numbers or strings
#         args: x and y
#         return substraction of strings
#     """
#     result1 = a + b
#     return result1


@app.task(bind=True)
def run_simulation(self, model_name, site_name):
    ''' setup task convert model name and input path (forcing data and parameters) from html portal to files
        store the files in the input file of EcoPad.
        SSH call model to run the simulation.
    '''
    task_id = str(self.request.id) # Get the task id from portal
    # check the files in input_path: forcing_data.txt; paramater_data.txt;
    input_files = check_files(model_name, site_name) # dict: def_ls_pars; def_ls_da_pars; tmpl_da_pars; tmpl_pars; forcing; da_pars; pars.
    resultDir   = setup_result_directory(task_id)
    # create param file 
    params         = readYml2Dict(input_files["pars"])
    print(params)
    param_filename = create_template(input_files['tmpl_pars'], 'pars',params,resultDir+"/input",check_params, input_files['def_ls_pars'])
    # Run Model code 
    client.connect('local_fortran_example',username=os.getenv('CELERY_SSH_USER'),password=os.getenv('CELERY_SSH_PASSWORD')) # Jian: 20220930 - use the "local_fortran_example", which will be wroten a Docker named as model_name
    # Jian: change the argments in TECO to parameters, forcing data, resultDir, mode, da_pars, obs_file, 
    ssh_cmd = "./test {0} {1} {2} {3} {4} {5}".format(param_filename, input_files["forcing"], 'input/SPRUCE_obs.txt', resultDir+'/output/', '0', 'input/SPRUCE_da_pars.txt')
    print(ssh_cmd)
    stdin, stdout, stderr = client.exec_command(ssh_cmd)
    #     stdin, stdout, stderr = client.exec_command(ssh_cmd)
    result = str(stdout.read())
    dates = pd.read_csv(resultDir+'/output/Simu_dailyflux14001.txt')
    data4w = dates.iloc[:,:2]
    data4w.columns = ["ts","xs"]
    result_file_path='/webData/output/'+"output_test_{0}.txt".format(task_id) # Jian: the path in TECO docker that links to "/web/data"
    data4w.to_csv(result_file_path, index=None) 
    return '/data/output/'+"output_test_{0}.txt".format(task_id) # Jian: the path of /web/data in website

@app.task(bind=True)
def run_forecast(self, model_name, site_name): #def teco_spruce_forecast(pars,forecast_year,forecast_day,temperature_treatment=0.0,co2_treatment=380.0,da_task_id=None,public=None):
    """
        --Jian Zhou. Oct. 05 2022
        Forecasting: initial version to 
        args: model_name, site_name. 
    """
    task_id     = str(self.request.id) # Get the task id from portal
    resultDir   = setup_result_directory(task_id)
    # forcing data: data\ecopad_test\sites_data\SPRUCE\forcing_data\weather_generate\preset_2011-2024
    # parameter sets: data\ecopad_test\sites_data\SPRUCE\parameters\data_assimilation
    input_files = check_files(model_name, site_name) # dict: def_ls_pars; def_ls_da_pars; tmpl_da_pars; tmpl_pars; forcing; da_pars; pars.
    # 1. parameters
    params        = readYml2Dict(input_files["pars"])
    paraset       = pd.read_csv(os.path.join(basedir,"sites_data/SPRUCE/parameters/data_assimilation","Paraest.txt")).sample(n=100, replace=False, random_state=None)
    paraset_array = paraset.to_numpy()
    param_mean    = np.nanmean(paraset_array, axis=0) # id, param-1 ...
    for i in range(10):
        params["SLA"] = paraset_array[i,1]
        params["GLmax"] = paraset_array[i,2]
        params["GRmax"] = paraset_array[i,3]
        params["Gsmax"] = paraset_array[i,4]
        params["Vcmax0"] = paraset_array[i,5]
        params["Tau_Leaf"] = paraset_array[i,6]
        params["Tau_Wood"] = paraset_array[i,7]
        params["Tau_Root"] = paraset_array[i,8]
        params["Tau_F"] = paraset_array[i,9]
        params["Tau_C"] = paraset_array[i,10]
        params["Tau_Micro"] = paraset_array[i,11]
        params["Tau_slowSOM"] = paraset_array[i,12]
        params["Tau_Passive"] = paraset_array[i,13]
        params["gddonset"] = paraset_array[i,14]
        params["Q10"] = paraset_array[i,15]
        params["RL0"] = paraset_array[i,16]
        params["Rs0"] = paraset_array[i,17]
        params["Rr0"] = paraset_array[i,18] 
        # create param file 
        os.makedirs(resultDir+"/input/input_para_"+str(i))
        os.makedirs(resultDir+"/output/output_para_"+str(i))
        param_filename = create_template(input_files['tmpl_pars'], 'pars',params,resultDir+"/input/input_para_"+str(i),check_params, input_files['def_ls_pars'])
        client.connect('local_fortran_example',username=os.getenv('CELERY_SSH_USER'),password=os.getenv('CELERY_SSH_PASSWORD')) # Jian: 20220930 - use the "local_fortran_example", which will be wroten a Docker named as model_name
        # Jian: change the argments in TECO to parameters, forcing data, resultDir, mode, da_pars, obs_file, 
        ssh_cmd = "./test {0} {1} {2} {3} {4} {5}".format(param_filename, input_files["forcing"], 'input/SPRUCE_obs.txt', resultDir+'/output/output_para_'+str(i), '0', 'input/SPRUCE_da_pars.txt')
        print(ssh_cmd)
        stdin, stdout, stderr = client.exec_command(ssh_cmd)
        #     stdin, stdout, stderr = client.exec_command(ssh_cmd)
        result = str(stdout.read())
    # 2. forcing
    params["SLA"] = param_mean[1]
    params["GLmax"] = param_mean[2]
    params["GRmax"] = param_mean[3]
    params["Gsmax"] = param_mean[4]
    params["Vcmax0"] = param_mean[5]
    params["Tau_Leaf"] = param_mean[6]
    params["Tau_Wood"] = param_mean[7]
    params["Tau_Root"] = param_mean[8]
    params["Tau_F"] = param_mean[9]
    params["Tau_C"] = param_mean[10]
    params["Tau_Micro"] = param_mean[11]
    params["Tau_slowSOM"] = param_mean[12]
    params["Tau_Passive"] = param_mean[13]
    params["gddonset"] = param_mean[14]
    params["Q10"] = param_mean[15]
    params["RL0"] = param_mean[16]
    params["Rs0"] = param_mean[17]
    params["Rr0"] = param_mean[18] 
    for i in range(10):
        # params = param_mean
        if i<9:
            input_files["forcing"] = "/data/ecopad_test/sites_data/SPRUCE/forcing_data/weather_generate/preset_2011-2024/EMforcing00"+str(i+1)+".csv"
        elif i<99:
            input_files["forcing"] = "/data/ecopad_test/sites_data/SPRUCE/forcing_data/weather_generate/preset_2011-2024/EMforcing0"+str(i+1)+".csv"
        else:
            input_files["forcing"] = "/data/ecopad_test/sites_data/SPRUCE/forcing_data/weather_generate/preset_2011-2024/EMforcing"+str(i+1)+".csv"
        # create param file 
        os.makedirs(resultDir+"/input/input_forcing_"+str(i))
        os.makedirs(resultDir+"/output/output_forcing_"+str(i))
        param_filename = create_template(input_files['tmpl_pars'], 'pars',params,resultDir+"/input/input_forcing_"+str(i),check_params, input_files['def_ls_pars'])
        client.connect('local_fortran_example',username=os.getenv('CELERY_SSH_USER'),password=os.getenv('CELERY_SSH_PASSWORD')) # Jian: 20220930 - use the "local_fortran_example", which will be wroten a Docker named as model_name
        # Jian: change the argments in TECO to parameters, forcing data, resultDir, mode, da_pars, obs_file, 
        ssh_cmd = "./test {0} {1} {2} {3} {4} {5}".format(param_filename, input_files["forcing"], 'input/SPRUCE_obs.txt', resultDir+'/output/output_forcing_'+str(i), '0', 'input/SPRUCE_da_pars.txt')
        print(ssh_cmd)
        stdin, stdout, stderr = client.exec_command(ssh_cmd)
        #     stdin, stdout, stderr = client.exec_command(ssh_cmd)
        result = str(stdout.read())

    # import pandas as pd
    dates = pd.read_csv(resultDir+'/output/output_1/Simu_dailyflux14001.txt')
    dates.columns = ["sdoy", "GPP", "NEE", "ER", "NPP", "Ra", "QC1", "QC2", "QC3", "QC4", "QC5", "QC6", "QC7", "QC8", "Rh"]
    result_file_path='/webData/show_forecast_results/'+"lastest_forecast_results_380ppm_0degree.txt" # Jian: the path in TECO docker that links to "/web/data"
    dates.to_csv(result_file_path, index=None) 
        

@app.task(bind=True)
def run_pull_data(self, model_name, site_name):
    '''
        pull forcing data from website, for example, SPRUCE
    '''
    task_id     = str(self.request.id) # Get the task id from portal
    resultDir   = setup_result_directory(task_id)
    teco_spruce_pulldata(os.path.join(basedir,'sites_data', site_name, 'forcing_data' ,'pull_forcing_data'))
    copyfile(os.path.join(basedir,'sites_data', site_name, 'forcing_data' ,'pull_forcing_data',"SPRUCE_forcing.txt"), os.path.join(basedir,'sites_data', site_name, 'forcing_data' ,"SPRUCE_forcing.txt"))
    return "sucessfull!"
    



# @app.task(bind=True)
# def run_data_assimilation(self, model_name, site_name): # def teco_spruce_data_assimilation(pars):
#     """
#         DA TECO Spruce
#         args: pars - Initial parameters for TECO SPRUCE
#         kwargs: da_params - Which DA variable and min and max range for 18 variables
#     """
#     task_id = str(self.request.id)
#     # check the files in input_path: forcing_data.txt; paramater_data.txt;
#     input_files = check_files(model_name, site_name)
#     if input_files['da_pars'] is None:
#         print("The data assimulation parameters of {0} model to run {1} site is invalid. Please check it in folder of {3}".format( model_name, site_name, 
#            os.path.join(basedir, "model_infos/", model, site, site+"_da_pars.txt")))
#         exit(1)
#     resultDir = setup_result_directory(task_id)
#     #parm template file
#     params = readYml2Dict(input_files["pars"])
#     # param_filename = create_template('SPRUCE_pars',pars,resultDir,check_params)
#     param_filename = create_template(model_name,site_name, 'pars',params,resultDir,check_params, input_files['pars_list'])
#     params = readYml2Dict(input_files["da_pars"])
#     da_param_filename = create_template(model_name,site_name,'da_pars',params,resultDir,check_params, os.path.join(basedir, "model_infos/", model_name, "default_da_parameters_list.txt"))
#     #Run Model code 
#     client.connect('local_fortran_example',username=os.getenv('CELERY_SSH_USER'),password=os.getenv('CELERY_SSH_PASSWORD')) # Jian: 20220930 - use the "local_fortran_example", which will be wroten a Docker named as model_name
#     ssh_cmd = "./test {0} {1} {2} {3} {4} {5}".format(param_filename, input_files["forcing"], 'input/SPRUCE_obs.txt', resultDir+'/output/', '1', 'input/SPRUCE_da_pars.txt')
#     print(ssh_cmd)
#     stdin, stdout, stderr = client.exec_command(ssh_cmd)
#     #     stdin, stdout, stderr = client.exec_command(ssh_cmd)
#     result = str(stdout.read())
#     import pandas as pd
#     dates = pd.read_csv(resultDir+'/output/Simu_dailyflux001.txt')
#     data4w = dates.iloc[:,:2]
#     data4w.columns = ["ts","xs"]
#     result_file_path='/webData/output/'+"output_jian_{0}.txt".format(task_id)
#     data4w.to_csv(result_file_path, index=None)
#     return '/data/output/'+"output_jian_{0}.txt".format(task_id)

# @app.task(bind=True)
# def run_forecast(self, model_name, site_name): #def teco_spruce_forecast(pars,forecast_year,forecast_day,temperature_treatment=0.0,co2_treatment=380.0,da_task_id=None,public=None):
#     """
#         --Jian Zhou. Oct. 05 2022
#         Forecasting: initial version to 
#         args: model_name, site_name. 
#     """
#     task_id = str(teco_spruce_forecast.request.id)
#     resultDir = setup_result_directory(task_id)
#     param_filename = create_template('SPRUCE_pars',pars,resultDir,check_params)  # Jian: get parameter to run simulation
#     #da_param_filename = create_template('SPRUCE_da_pars',pars,resultDir,check_params)
#     da_param_filename ="SPRUCE_da_pars.txt"                                      # Jian: get range of parameters to run data assimilation
#     host_data_dir_spruce_data="{0}/local/spruce_data".format(host_data_dir)
#     #Set Param estimation file from DA 
#     if not da_task_id:
#         try:
#             copyfile("{0}/Paraest.txt".format(spruce_data_folder),"{0}/Paraest.txt".format(resultDir))
#             copyfile("{0}/SPRUCE_da_pars.txt".format(spruce_data_folder),"{0}/SPRUCE_da_pars.txt".format(resultDir))
#         except:
#             error_file = "{0}/Paraest.txt or SPRUCE_da_pars.txt".format(spruce_data_folder)
#             raise Exception("Parameter Estimation file location problem. {0} file not found.".format(error_file))
#     else:
#         try:
#             copyfile("{0}/ecopad_tasks/{1}/input/Paraest.txt".format(basedir,da_task_id),"{0}/Paraest.txt".format(resultDir))
#             copyfile("{0}/ecopad_tasks/{1}/input/SPRUCE_da_pars.txt".format(basedir,da_task_id),"{0}/SPRUCE_da_pars.txt".format(resultDir))
#         except:
#             error_file = "{0}/ecopad_tasks/{1}/input/Paraest.txt or SPRUCE_da_pars.txt".format(basedir,da_task_id)
#             raise Exception("Parameter Estimation file location problem. {0} file not found.".format(error_file))
#     #Run Spruce TECO code
#     host_data_resultDir = "{0}/static/ecopad_tasks/{1}".format(host_data_dir,task_id)
#     host_data_dir_spruce_data="{0}/local/spruce_data".format(host_data_dir)
#     docker_opts = "-v {0}:/data:z -v {1}:/spruce_data".format(host_data_resultDir,host_data_dir_spruce_data)
#     docker_cmd = "{0} {1} {2} {3} {4} {5} {6} {7} {8} {9} {10}".format("/data/{0}".format(param_filename),
#                                     "/spruce_data/SPRUCE_forcing.txt", "/spruce_data/SPRUCE_obs.txt",
#                                     "/data",2, "/data/{0}".format(da_param_filename),
#                                     "/spruce_data/Weathergenerate",forecast_year, forecast_day,
#                                     temperature_treatment,co2_treatment)
#     result = docker_task(docker_name="teco_spruce",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
#     #Run R Plots
#     docker_opts = "-v {0}:/data:z ".format(host_data_resultDir)
#     docker_cmd ="Rscript ECOPAD_forecast_viz.R {0} {1} {2} {3}".format("obs_file/SPRUCE_obs.txt","/data","/data",100)
#     result = docker_task(docker_name="ecopad_r",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)

#     # Yuanyuan add to reformat output data
#     docker_opts = "-v {0}:/data:z ".format(host_data_resultDir)
#     docker_cmd = "Rscript reformat_to_csv.R {0} {1} {2} {3} {4}".format("/data","/data",100,temperature_treatment,co2_treatment)
#     #docker_opts = "-v {0}:/data:z ".format(host_data_resultDir)
#     #docker_cmd = "Rscript reformat_to_csv_backup.R {0} {1} {2}".format("/data","/data",100)
#     # docker_opts = None
#     # docker_cmd = None
#     result = docker_task(docker_name="ecopad_r",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)

#     #Clean up result Directory
#     clean_up(resultDir)
#     #Create Report
#     report_data ={'zero_label':'GPP Forecast','zero_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'gpp_forecast.png'),
#                 'one_label':'ER Forecast','one_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'er_forecast.png'),
#                 'two_label':'Foliage Forecast','two_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'foliage_forecast.png'),
#                 'three_label':'Wood Forecast','three_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'wood_forecast.png'),
#                 'four_label':'Root Forecast','four_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'root_forecast.png'),
#                 'five_label':'Soil Forecast','five_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'soil_forecast.png')}
#     report_data['title']="SPRUCE Ecological Forecast Task Report"
#     desc = "Use constrained parameters from Data Assimilation to predict carbon fluxes and pool sizes. "
#     desc = desc + "Forcing inputs are genereated by auto-regression model using historical climate data of the SPRUCE site. "
#     desc = desc + "Allow users to choose which year and day to make predictations of ecosystem in response to treatment effects."
#     report_data['description']=desc
#     report_name = create_report('report',report_data,resultDir)
#     #return {"data":"http://{0}/ecopad_tasks/{1}".format(result['host'],result['task_id']),
#     #        "report": "http://{0}/ecopad_tasks/{1}/{2}".format(result['host'],result['task_id'],report_name)}
#     result_url = "http://{0}/ecopad_tasks/{1}".format(result['host'],result['task_id'])
#     if public:
#         data={'tag':public,'result_url':result_url,'task_id':task_id,'timestamp':datetime.now()}
#         db=MongoClient('ecopad_mongo',27017)
#         db.forecast.public.save(data)

#     return result_url

# @app.task(bind=True)
# def run_pull_data(self, a, b):
#     task_id = str(self.request.id) # Get the task id from portal
#     resultDir   = setup_result_directory(task_id)
#     teco_spruce_pulldata(basedir+'/model_infos/teco_spruce_v2/SPRUCE/pull_forcing_data_SPRUCE')
#     return "sucessfull!"


# Jian: to check whether the input data is existing.
def check_files(model, site):
    """ rule: the basedir of ecopad includes the folds of "models_configs" and "sites_data".
            check: 1. models_configs/model; 2. sites_data/site; 
                   3. models_configs/model/default_da_parameters_list.txt;      4. models_configs/model/default_parameters_list.txt;
                   5. models_configs/model/templates/tmpl_da_pars.tmpl          6. models_configs/model/templates/tmpl_pars.tmpl
                   7. sites_data/siteName/forcing_data/siteName_forcing.txt 
                   8. sites_data/siteName/parameters/siteName_pars.yml
                   9. sites_data/siteName/parameters/siteName_da_pars.yml
            site_name folder has the forcing data and parameters data, named as "siteName_forcing.txt" and "siteName_pars.txt" (and "siteName_da_pars.txt")
        return 
    """
    for ipath in ["models_configs/"+model, "sites_data/"+site]:
        check_path = os.path.join(basedir,ipath)
        if not os.path.exists(check_path): 
            print("Error: the file of ", check_path, "is not existing. The ecopad stoped, and check it. (exit 1)") # Jian: maybe put it into log file?
            exit(1)
    # read the pre-set input information:
    input_files = {}
    input_files["def_ls_pars"]    = os.path.join(basedir, "models_configs", model, "default_parameters_list.txt")
    input_files["def_ls_da_pars"] = os.path.join(basedir, "models_configs", model, "default_da_parameters_list.txt")
    input_files["tmpl_da_pars"]   = os.path.join(basedir, "models_configs", model, "templates", "tmpl_da_pars.tmpl")
    input_files["tmpl_pars"]      = os.path.join(basedir, "models_configs", model, "templates", "tmpl_pars.tmpl")

    input_files["forcing"]        = os.path.join(basedir, "sites_data", site, "forcing_data", site+"_forcing.txt")
    input_files["da_pars"]        = os.path.join(basedir, "sites_data", site, "parameters",   site+"_da_pars.yml")
    input_files["pars"]           = os.path.join(basedir, "sites_data", site, "parameters",   site+"_pars.yml")

    for key, f_path in input_files.items():
        if key == "da_pars" or key == "def_ls_da_pars" or key == "tmpl_da_pars":
            if not os.path.exists(f_path): input_files[key] == None
        else:
            if not os.path.exists(f_path):
                print("Error: the file of ", key, "is not existing. The ecopad stoped, and check the file of ", f_path, ". (exit 1)")
                exit(1)
    return input_files
# Jian: end of check_files

def setup_result_directory(task_id):
    resultDir = os.path.join(basedir, 'ecopad_tasks/', task_id)
    os.makedirs(resultDir)
    os.makedirs("{0}/input".format(resultDir))
    os.makedirs("{0}/output".format(resultDir))
    os.makedirs("{0}/plot".format(resultDir))
    return resultDir 

def create_template(tmpl, tmpl_name,params,resultDir,check_function, fp_pars_ls): # Jian: put the template to model_name/site_name/templates/tmpl_xxx.tmpl
    # tmpl = os.path.join(basedir, "model_infos", model, 'templates/tmpl_{0}.tmpl'.format(tmpl_name)) # Jian: not os.path.dirname(__file__)
    print("test_tmpl:", tmpl)
    with open(tmpl,'r') as f:
        template=Template(f.read())
    params_file = os.path.join(resultDir,'{0}.txt'.format(tmpl_name))
    with open(params_file,'w') as f2:
        obj = check_function(fp_pars_ls, params)
        print(obj)
        f2.write(template.render(check_function(fp_pars_ls, params)))
    return params_file#'{0}.txt'.format(tmpl_name)

def check_params(filePath_pars_ls, pars):
    """ Check params and make floats."""
    dat_ls_pars = open(filePath_pars_ls, "r").read()   # Jian: get the list of pars from model folder
    ls_pars     = dat_ls_pars.replace("\n",'').replace("\"",'').split(",") # Jian: parser the list of pars
    # for param in ["latitude","longitude","wsmax","wsmin","LAIMAX","LAIMIN","SapS","SLA","GLmax","GRmax","Gsmax",
    #                 "extkU","alpha","Tau_Leaf","Tau_Wood","Tau_Root","Tau_F","Tau_C","Tau_Micro","Tau_SlowSOM",
    #                 "gddonset","Rl0" ]:
    for param in ls_pars:
        try:
            inside_check(pars,param)
        except Exception as e:
            # print("ettttt:", e)
            pass
        try:
            inside_check(pars, "min_{0}".format(param))
        except:
            pass
        try:
            inside_check(pars, "max_{0}".format(param))
        except:
            pass
    return pars 

def inside_check(pars,param):
   if not "." in str(pars[param]):
       pars[param]="%s." % (str(pars[param]))
   else:
       pars[param]=str(pars[param])  
    
def readYml2Dict(filePath):
    # Open the file and load the file
    with open(filePath) as f:
        data = yaml.load(f, Loader=yaml.loader.SafeLoader)
    return data



# =================================================================================================================================

# @app.task(bind=True)
# def test(self, input_a, input_b):
#     # This is a prototype for the new interface
#     # The old implementation uses dockertask which is now deprecated
#     # The new implementation uses ssh (via the paramiko lib)
#     # This requires the sshd demom to be started on the container we are connecting
#     # The username and  passwords are shared via environ variables which are also used
#     # to initialize the containers
#     # Similar to the old implementation we share file paths instead of the actual data
#     # This is the reason why this function returns a result_file_path. 
#     # In this case this is the location of text file written by the fortran container.
#     # The javascript code of the frontend can see the return value of this function by querying the api.
#     # and so can find the loacation of the actual file. (In many of the old examples this is an image)
#     # The javascript code of the frontend (running in the browser of an ecopad user on a different machine) 
#     # then makes an request to the 
#     # webserver which can serve the file since the path points to a location under the 
#     # webroot directory.
#     task_id = str(self.request.id)
    
#     client.connect('local_fortran_example',username=os.getenv('CELERY_SSH_USER'),password=os.getenv('CELERY_SSH_PASSWORD'))
#     result_file_path="/data/output_{0}.txt".format(task_id)
#     # ssh_cmd = "./test {0} {1} {2}".format(input_a, input_b, result_file_path)
#     ssh_cmd = "cp {0} {1}".format("/data/test.txt", result_file_path)
#     stdin, stdout, stderr = client.exec_command(ssh_cmd)
#     result = str(stdout.read())
#     return result_file_path

# @app.task(bind=True)
# def jian(self, input_a, input_b):
#     task_id = str(self.request.id)
#     client.connect('local_fortran_example',username=os.getenv('CELERY_SSH_USER'),password=os.getenv('CELERY_SSH_PASSWORD'))
#     result_file_path="/data/output_jian_{0}.txt".format(task_id)
#     ssh_cmd = "./test {0} {1} {2} {3} {4} {5}".format('input/SPRUCE_pars.txt', 'input/SPRUCE_forcing.txt', 'input/SPRUCE_obs.txt', '/data/output/', '0', 'input/SPRUCE_da_pars.txt')
#     stdin, stdout, stderr = client.exec_command(ssh_cmd)
#     result = str(stdout.read())
#     import pandas as pd
#     dates = pd.read_csv('/data/output/Simu_soiltemp.txt')
#     data4w = dates.iloc[:,:2]
#     data4w.columns = ["ts","xs"]
#     data4w.to_csv(result_file_path, index=None)
#     # result_file_path=""
#     return result_file_path

# changed by Jian: 
#   Ecopad includes the functions:
#       1. choose model, forcing data, parameter files.
#           input values: "model_name" represents the model name and related files (list of params)
#           site value: paramters file, forcing data file.
#       2. functions:
#           run_simulation;
#           run_data_assimilation;
#           run_forecast;





# =====================================================================================================================
#
# @app.task(bind=True)
# def teco_spruce_simulation(self, pars): # ,model_type="0", da_params=None): # Jian: add self to get the task ID
#     """ Setup task convert parameters from html portal
#     to file, and store the file in input folder.
#     call teco_spruce_model.
#     """
#     # task_id = str(teco_spruce_simulation.request.id)
#     task_id = str(self.request.id)
#     resultDir = setup_result_directory(task_id)
#     #create param file 
#     # param_filename = create_template('SPRUCE_pars',pars,resultDir,check_params)
#     #Run Spruce TECO code 
#     host_data_resultDir = "{0}/static/ecopad_tasks/{1}".format(host_data_dir,task_id)
#     host_data_dir_spruce_data="{0}/local/spruce_data".format(host_data_dir)	

#     # Jian: no docker for teco, now using the SSH to local fortran
#     #    docker_opts = "-v {0}:/data:z -v {1}:/spruce_data:z".format(host_data_resultDir,host_data_dir_spruce_data)
#     #    docker_cmd = "{0} {1} {2} {3} {4} {5}".format("/data/{0}".format(param_filename),"/spruce_data/SPRUCE_forcing.txt",
#     #                                    "/spruce_data/SPRUCE_obs.txt",
#     #                                    "/data", 0 , "/spruce_data/SPRUCE_da_pars.txt")
#     #    result = docker_task(docker_name="teco_spruce",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
#     #    #Run R Plots
#     #    #os.makedirs("{0}/graphoutput".format(host_data_resultDir)) #make plot directory
#     #    docker_opts = "-v {0}:/usr/local/src/myscripts/graphoutput:z ".format(host_data_resultDir)
#     #    docker_cmd = None
#     #    result = docker_task(docker_name="ecopad_r",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
#     # end Jian

#     # test Jian
#     client.connect('local_fortran_example',username=os.getenv('CELERY_SSH_USER'),password=os.getenv('CELERY_SSH_PASSWORD'))
#     result_file_path="/data/output_jian_{0}.txt".format(task_id)
#     ssh_cmd = "./test {0} {1} {2} {3} {4} {5}".format('input/SPRUCE_pars.txt', 'input/SPRUCE_forcing.txt', 'input/SPRUCE_obs.txt', '/data/output/', '0', 'input/SPRUCE_da_pars.txt')
#     stdin, stdout, stderr = client.exec_command(ssh_cmd)
#     result = str(stdout.read())
#     import pandas as pd
#     dates = pd.read_csv('/data/output/Simu_soiltemp.txt')
#     data4w = dates.iloc[:,:2]
#     data4w.columns = ["ts","xs"]
#     data4w.to_csv(result_file_path, index=None)


#     #Clean up result Directory
#     # clean_up(resultDir)
#     #Create Report
#     # report_data ={'zero_label':'GPP','zero_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'gpp.png'),
#     #             'one_label':'ER','one_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'er.png'),
#     #             'two_label':'Foliage','two_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'foliage.png'),
#     #             'three_label':'Wood','three_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'wood.png'),
#     #             'four_label':'Root','four_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'root.png'),
#     #             'five_label':'Soil','five_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'soil.png')}
#     report_data['title']="SPRUCE Ecological Simulation Task Report"
#     report_data['description']="Simulations of carbon fluxes and pool sizes for SPRUCE experiment based on user defined initial parameters."

#     report = create_report('report',report_data,resultDir)
#     result_url ="http://{0}/ecopad_tasks/{1}".format(result['host'],result['task_id'])
#     #report_url = "http://{0}/ecopad_tasks/{1}/{2}".format(result['host'],result['task_id'],"report.htm")
#     #{"report":report_url,"data":result_url}
#     return result_file_path
#  
#@task()
#def teco_spruce_data_assimilation(pars):
#    """
#        DA TECO Spruce
#        args: pars - Initial parameters for TECO SPRUCE
#        kwargs: da_params - Which DA variable and min and max range for 18 variables
#
#    """
#    task_id = str(teco_spruce_data_assimilation.request.id)
#    resultDir = setup_result_directory(task_id)
#    #parm template file
#    param_filename = create_template('SPRUCE_pars',pars,resultDir,check_params)
#    da_param_filename = create_template('SPRUCE_da_pars',pars,resultDir,check_params)
#    #if da_params:
#    #    da_param_filename = create_template('spruce_da_pars',da_params,resultDir,check_params)
#    #else:
#    #    copyfile("{0}/ecopad_tasks/default/SPRUCE_da_pars.txt".format(basedir),"{0}/SPRUCE_da_pars.txt".format(resultDir))
#    #    da_param_filename ="SPRUCE_da_pars.txt"
#    #Run Spruce TECO code
#    host_data_resultDir = "{0}/static/ecopad_tasks/{1}".format(host_data_dir,task_id)
#    host_data_dir_spruce_data="{0}/local/spruce_data".format(host_data_dir)
#    docker_opts = "-v {0}:/data:z -v {1}:/spruce_data".format(host_data_resultDir,host_data_dir_spruce_data)
#    docker_cmd = "{0} {1} {2} {3} {4} {5}".format("/data/{0}".format(param_filename),"/spruce_data/SPRUCE_forcing.txt",
#                                    "/spruce_data/SPRUCE_obs.txt",
#                                    "/data",1, "/data/{0}".format(da_param_filename))
#    result = docker_task(docker_name="teco_spruce",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
#    #Run R Plots
#    docker_opts = "-v {0}:/data:z ".format(host_data_resultDir)
#    docker_cmd ="Rscript ECOPAD_da_viz.R {0} {1}".format("/data/Paraest.txt","/data")
#    result = docker_task(docker_name="ecopad_r",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
#    #Clean up result Directory
#    clean_up(resultDir)
#    #Create Report
#    report_data ={'zero_label':'Results','zero_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'histogram.png')}
#    report_data['title']="SPRUCE Ecological Data Assimilation Task Report"
#    desc= "Multiple data streams from SPRUCE are assimilated to TECO model using MCMC algorithm. "\
#            "The current dataset are mainly from pre-treatment measurement from 2011 to 2014. "\
#            "This will be updated regularly when new data stream is available. 5 out of 18 parameters are constrained from pre-treatment data. "\
#            "The 18 parameters are (1) specific leaf area, (2) maximum leaf growth rate, (3) maximum root growth rate, "\
#            "(4) maximum stem growth rate, (5) maximum rate of carboxylation, (6) turnover rate of foliage pool, "\
#            "(7) turnover rate of woody pool, (8) turnover rate of root pool, (9) turnover rate of fine litter pool, "\
#            "(10) turnover rate of coarse litter pool, (11) turnover rate of fast soil pool, (12) turnover rate of slow soil pool, "\
#            "(13) turnover rate of passive soil pool, (14) onset of growing degree days, (15) temperature sensitivity Q10, "\
#            "(16) baseline leaf respiration, (17) baseline stem respiration, (18) baseline root respiration"
#        
#    report_data['description']=desc
#    report_name = create_report('report_da',report_data,resultDir)
#    return "http://{0}/ecopad_tasks/{1}".format(result['host'],result['task_id'])
#
#@task()
#def teco_spruce_forecast(pars,forecast_year,forecast_day,temperature_treatment=0.0,co2_treatment=380.0,da_task_id=None,public=None):
#    """
#        Forecasting 
#        args: pars - Initial parameters for TECO SPRUCE
#              forecast_year,forecast_day
#    """
#    task_id = str(teco_spruce_forecast.request.id)
#    resultDir = setup_result_directory(task_id)
#    param_filename = create_template('SPRUCE_pars',pars,resultDir,check_params)
#    #da_param_filename = create_template('SPRUCE_da_pars',pars,resultDir,check_params)
#    da_param_filename ="SPRUCE_da_pars.txt"
#    host_data_dir_spruce_data="{0}/local/spruce_data".format(host_data_dir)
#    #Set Param estimation file from DA 
#    if not da_task_id:
#        try:
#            copyfile("{0}/Paraest.txt".format(spruce_data_folder),"{0}/Paraest.txt".format(resultDir))
#            copyfile("{0}/SPRUCE_da_pars.txt".format(spruce_data_folder),"{0}/SPRUCE_da_pars.txt".format(resultDir))
#        except:
#            error_file = "{0}/Paraest.txt or SPRUCE_da_pars.txt".format(spruce_data_folder)
#            raise Exception("Parameter Estimation file location problem. {0} file not found.".format(error_file))
#    else:
#        try:
#            copyfile("{0}/ecopad_tasks/{1}/input/Paraest.txt".format(basedir,da_task_id),"{0}/Paraest.txt".format(resultDir))
#            copyfile("{0}/ecopad_tasks/{1}/input/SPRUCE_da_pars.txt".format(basedir,da_task_id),"{0}/SPRUCE_da_pars.txt".format(resultDir))
#        except:
#            error_file = "{0}/ecopad_tasks/{1}/input/Paraest.txt or SPRUCE_da_pars.txt".format(basedir,da_task_id)
#            raise Exception("Parameter Estimation file location problem. {0} file not found.".format(error_file))
#    #Run Spruce TECO code
#    host_data_resultDir = "{0}/static/ecopad_tasks/{1}".format(host_data_dir,task_id)
#    host_data_dir_spruce_data="{0}/local/spruce_data".format(host_data_dir)
#    docker_opts = "-v {0}:/data:z -v {1}:/spruce_data".format(host_data_resultDir,host_data_dir_spruce_data)
#    docker_cmd = "{0} {1} {2} {3} {4} {5} {6} {7} {8} {9} {10}".format("/data/{0}".format(param_filename),
#                                    "/spruce_data/SPRUCE_forcing.txt", "/spruce_data/SPRUCE_obs.txt",
#                                    "/data",2, "/data/{0}".format(da_param_filename),
#                                    "/spruce_data/Weathergenerate",forecast_year, forecast_day,
#                                    temperature_treatment,co2_treatment)
#    result = docker_task(docker_name="teco_spruce",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
#    #Run R Plots
#    docker_opts = "-v {0}:/data:z ".format(host_data_resultDir)
#    docker_cmd ="Rscript ECOPAD_forecast_viz.R {0} {1} {2} {3}".format("obs_file/SPRUCE_obs.txt","/data","/data",100)
#    result = docker_task(docker_name="ecopad_r",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
#    
#    # Yuanyuan add to reformat output data
#    docker_opts = "-v {0}:/data:z ".format(host_data_resultDir)
#    docker_cmd = "Rscript reformat_to_csv.R {0} {1} {2} {3} {4}".format("/data","/data",100,temperature_treatment,co2_treatment)
#    #docker_opts = "-v {0}:/data:z ".format(host_data_resultDir)
#    #docker_cmd = "Rscript reformat_to_csv_backup.R {0} {1} {2}".format("/data","/data",100)
#    # docker_opts = None
#    # docker_cmd = None
#    result = docker_task(docker_name="ecopad_r",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
#    
#    #Clean up result Directory
#    clean_up(resultDir)
#    #Create Report
#    report_data ={'zero_label':'GPP Forecast','zero_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'gpp_forecast.png'),
#                'one_label':'ER Forecast','one_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'er_forecast.png'),
#                'two_label':'Foliage Forecast','two_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'foliage_forecast.png'),
#                'three_label':'Wood Forecast','three_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'wood_forecast.png'),
#                'four_label':'Root Forecast','four_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'root_forecast.png'),
#                'five_label':'Soil Forecast','five_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'soil_forecast.png')}
#    report_data['title']="SPRUCE Ecological Forecast Task Report"
#    desc = "Use constrained parameters from Data Assimilation to predict carbon fluxes and pool sizes. "
#    desc = desc + "Forcing inputs are genereated by auto-regression model using historical climate data of the SPRUCE site. "
#    desc = desc + "Allow users to choose which year and day to make predictations of ecosystem in response to treatment effects."
#    report_data['description']=desc
#    report_name = create_report('report',report_data,resultDir)
#    #return {"data":"http://{0}/ecopad_tasks/{1}".format(result['host'],result['task_id']),
#    #        "report": "http://{0}/ecopad_tasks/{1}/{2}".format(result['host'],result['task_id'],report_name)}
#    result_url = "http://{0}/ecopad_tasks/{1}".format(result['host'],result['task_id'])
#    if public:
#        data={'tag':public,'result_url':result_url,'task_id':task_id,'timestamp':datetime.now()}
#        db=MongoClient('ecopad_mongo',27017)
#        db.forecast.public.save(data)
#
#    return result_url
#
#
#def clean_up(resultDir):
#
#    move("{0}/SPRUCE_pars.txt".format(resultDir),"{0}/input/SPRUCE_pars.txt".format(resultDir))
#    move("{0}/SPRUCE_yearly.txt".format(resultDir),"{0}/output/SPRUCE_yearly.txt".format(resultDir))
#    for mvfile in glob("{0}/Simu_dailyflux*.txt".format(resultDir)):
#        move(mvfile, "{0}/output".format(resultDir))
#    for mvfile in glob("{0}/*.png".format(resultDir)):
#        move(mvfile, "{0}/plot".format(resultDir))
#
#    # Yuanyuan add to clear up forecast_csv
#    #current_date=datetime.now().strftime("%Y-%m-%d")
#    current_date=datetime.now().strftime("%Y")
#    #if not os.path.exists("{0}/forecast_csv/{1}".format(basedir,current_date)):
#    #    os.makedirs("{0}/forecast_csv/{1}".format(basedir,current_date))
#    
#    # make one folder for all the time, changed 01_04_2017
#    if not os.path.exists("{0}/forecast_csv/ecopad_vdv".format(basedir)):
#        os.makedirs("{0}/forecast_csv/ecopad_vdv".format(basedir))
#
#    #for afile in glob.iglob("{0}/forecast_csv/{1}*".format(basedir,current_date)):
#    #	print afile
#    # 	os.remove(afile)
#   
#    try: 
#        for mvfile in glob("{0}/*.csv".format(resultDir)):
#            head,tail=os.path.split(mvfile)
#            #dst_file=os.path.join("{0}/forecast_csv/{1}/{2}".format(basedir,current_date,tail))
#            # modified 01_04_2017
#            dst_file=os.path.join("{0}/forecast_csv/ecopad_vdv/{1}".format(basedir,tail))
#            i=1 
#            if os.path.exists(dst_file):
#                with open(dst_file, 'a') as singleFile:
#                    for line in open(mvfile, 'r'):
#                       if i > 1:
#                          singleFile.write(line)          
#                          #print i
#                       i=2
#                os.remove(mvfile)
#        else: 
#            #move(mvfile,"{0}/forecast_csv/{1}".format(basedir,current_date))
#            move(mvfile,"{0}/forecast_csv/ecopad_vdv".format(basedir)) 
#    except:
#        pass 
#
#    try:
#        move("{0}/SPRUCE_da_pars.txt".format(resultDir),"{0}/input/SPRUCE_da_pars.txt".format(resultDir))
#        move("{0}/Paraest.txt".format(resultDir),"{0}/input/Paraest.txt".format(resultDir))
#    except:
#        pass
#
# def create_template(tmpl_name,params,resultDir,check_function):
#    tmpl = os.path.join(os.path.dirname(__file__),'templates/{0}.tmpl'.format(tmpl_name))
#    with open(tmpl,'r') as f:
#        template=Template(f.read())
#    params_file = os.path.join(resultDir,'{0}.txt'.format(tmpl_name))
#    with open(params_file,'w') as f2:
#        f2.write(template.render(check_function(params)))
#    return '{0}.txt'.format(tmpl_name)
#
#def create_report(tmpl_name,data,resultDir):
#    tmpl = os.path.join(os.path.dirname(__file__),'templates/{0}.tmpl'.format(tmpl_name))
#    with open(tmpl,'r') as f:
#        template=Template(f.read())
#    report_file = os.path.join(resultDir,'{0}.htm'.format(tmpl_name))
#    with open(report_file,'w') as f2:
#        f2.write(template.render(data))
#    return '{0}.htm'.format(tmpl_name)
#
# def setup_result_directory(task_id):
#    resultDir = os.path.join(basedir, 'ecopad_tasks/', task_id)
#    os.makedirs(resultDir)
#    os.makedirs("{0}/input".format(resultDir))
#    os.makedirs("{0}/output".format(resultDir))
#    os.makedirs("{0}/plot".format(resultDir))
#    return resultDir 
#
# def check_params(pars):
#    """ Check params and make floats."""
#    for param in ["latitude","longitude","wsmax","wsmin","LAIMAX","LAIMIN","SapS","SLA","GLmax","GRmax","Gsmax",
#                    "extkU","alpha","Tau_Leaf","Tau_Wood","Tau_Root","Tau_F","Tau_C","Tau_Micro","Tau_SlowSOM",
#                    "gddonset","Rl0" ]:
#        try:
#            inside_check(pars,param)
#        except:
#            pass
#        try:
#            inside_check(pars, "min_{0}".format(param))
#        except:
#            pass
#        try:
#            inside_check(pars, "max_{0}".format(param))
#        except:
#            pass
#    return pars  
#
#def inside_check(pars,param):
#    if not "." in str(pars[param]):
#        pars[param]="%s." % (str(pars[param]))
#    else:
#        pars[param]=str(pars[param])  
