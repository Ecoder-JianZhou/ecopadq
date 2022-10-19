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


def run_simulation(model_name):
    ''' 
    '''
    