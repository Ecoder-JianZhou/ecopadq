# from celery.task import task
import pandas as pd
from datetime import datetime
from ftplib import FTP
import urllib, shutil
import sys
from itertools import groupby
from operator import itemgetter
# sys.path.append('/code/task_config')
from .config import ftp_username, ftp_password


#@task
def check_na_values(teco_spruce1):
    if(teco_spruce1.isnull().values.any()):
        #gives the rows which contain NaN values
        na_df=teco_spruce1[pd.isnull(teco_spruce1).any(axis=1)]
        #getting the index of the NaN rows
        data=na_df.index.values
        #data=[12,13,14,45,46,47,48,49,50]
        # for k, g in groupby(enumerate(data), lambda (i, x): i-x):
        #     print map(itemgetter(1),g) 
        temp=[]
        for k, g in groupby(enumerate(data), lambda i, x: i-x): # Jian: not know what it does, and this module is error...
            temp.append(map(itemgetter(1),g))
        len(temp)  
        #for consec in temp:
        #print(consec,len(consec))
        for consec in temp:
            if len(consec)>5:
                for a in consec:
                    teco_spruce1.loc[a,('Tair')]=teco_spruce1.iloc[a-24]['Tair']
                    #teco_spruce1.Tsoil.fillna(teco_spruce1.Tsoil.shift(24), inplace=True)
                    teco_spruce1.loc[a,('Tsoil')]=teco_spruce1.iloc[a-24]['Tsoil']
                    #teco_spruce1.RH.fillna(teco_spruce1.RH.shift(24), inplace=True)
                    teco_spruce1.loc[a,('RH')]=teco_spruce1.iloc[a-24]['RH']
                    #teco_spruce1.VPD.fillna(teco_spruce1.VPD.shift(24), inplace=True)
                    teco_spruce1.loc[a,('VPD')]=teco_spruce1.iloc[a-24]['VPD']
                    #teco_spruce1.Rain.fillna(teco_spruce1.Rain.shift(24), inplace=True)
                    teco_spruce1.loc[a,('Rain')]=teco_spruce1.iloc[a-24]['Rain']
                    #teco_spruce1.WS.fillna(teco_spruce1.WS.shift(24), inplace=True)
                    teco_spruce1.loc[a,('WS')]=teco_spruce1.iloc[a-24]['WS']
                    #teco_spruce1.PAR.fillna(teco_spruce1.PAR.shift(24), inplace=True)
                    teco_spruce1.loc[a,('PAR')]=teco_spruce1.iloc[a-24]['PAR']
                        
            else:
                for a in consec:
                    teco_spruce1.loc[a,('Tair')]=teco_spruce1.iloc[a-1]['Tair']
                    #teco_spruce1.Tsoil.fillna(teco_spruce1.Tsoil.shift(24), inplace=True)
                    teco_spruce1.loc[a,('Tsoil')]=teco_spruce1.iloc[a-1]['Tsoil']
                    #teco_spruce1.RH.fillna(teco_spruce1.RH.shift(24), inplace=True)
                    teco_spruce1.loc[a,('RH')]=teco_spruce1.iloc[a-1]['RH']
                    #teco_spruce1.VPD.fillna(teco_spruce1.VPD.shift(24), inplace=True)
                    teco_spruce1.loc[a,('VPD')]=teco_spruce1.iloc[a-1]['VPD']
                    #teco_spruce1.Rain.fillna(teco_spruce1.Rain.shift(24), inplace=True)
                    teco_spruce1.loc[a,('Rain')]=teco_spruce1.iloc[a-1]['Rain']
                    #teco_spruce1.WS.fillna(teco_spruce1.WS.shift(24), inplace=True)
                    teco_spruce1.loc[a,('WS')]=teco_spruce1.iloc[a-1]['WS']
                    #teco_spruce1.PAR.fillna(teco_spruce1.PAR.shift(24), inplace=True)
                    teco_spruce1.loc[a,('PAR')]=teco_spruce1.iloc[a-1]['PAR'] 
          
   
# @task()
def teco_spruce_pulldata(destination='./spruce_data'):
    initial_text=open("{0}/initial.txt".format(destination),"r")
    #pulling data from the url
    print('trying to pull datapppp')   
    url = 'ftp://{0}:{1}@sprucedata.ornl.gov/DataFiles/EM_Table1.dat'.format(ftp_username,ftp_password)
    # print(url)
    #    sp_data=pd.read_csv(url,skiprows=5)
    sp_data = pd.read_fwf(url)
    try:
        sp_data=pd.read_csv(url,skiprows=3)
        print ('I am in try loop')
        #      columnnames = ["TIMESTAMP","RECORD","Tair","RH","AirTCHumm_Avg","RH_Humm_Avg","VPD","Rain","WS","WindDir_D1_WVT","WindDir_SD1_WVT","WSDiag_Tot","SmplsF_Tot","Axis1Failed_Tot","Axis2Failed_Tot","BothAxisFailed_Tot","NVMerror_Tot","ROMerror_Tot","MaxGain_Tot","NNDF_Tot","HollowSurf_Avg","Hollow5cm_Avg","Tsoil","Hollow40cm_Avg","Hollow80cm_Avg","Hollow160cm_Avg","Hollow200cm_Avg","HummockSurf_Avg","Hummock5cm_Avg","Hummock20cm_Avg","Hummock40cm_Avg","Hummock80cm_Avg","Hummock160cm_Avg","Hummock200cm_Avg","PAR","PAR_NTree1_Avg","PAR_NTree2_Avg","PAR_SouthofHollow1_Avg","PAR_SouthofHollow2_Avg","PAR_NorthofHollow1_Avg","PAR_NorthofHollow2_Avg","PAR_Srub1_Avg","PAR_Srub2_Avg","PAR_Srub3_Avg","PAR_Srub4_Avg","TopofHummock_Avg","MidofHummock_Avg","Surface1_Avg","Surface2_Avg","D1-20cm_Avg","D2-20cm_Avg","TopH_Avg","MidH_Avg","S1_Avg","S2_Avg","Deep-20cm_Avg","short_up_Avg","short_dn_Avg","long_up_Avg","long_dn_Avg","CNR4_Temp_C_Avg","CNR4_Temp_K_Avg","long_up_corr_Avg","long_dn_corr_Avg","Rs_net_Avg","Rl_net_Avg","albedo_Avg","Rn_Avg","SPN1_Total_Avg","SPN1_Diffuse_Avg","Water_Height_Avg","Water_Temp_Avg","Watertable","Dewpoint","Dewpoint_Diff"]
        # 2017 columnnames = ["TIMESTAMP","RECORD","AirTC_2M_Avg","RH_2M_Avg","AirTCHumm_Avg","RH_Humm_Avg","BP_kPa_Avg","Rain_mm_Tot","WS_ms_S_WVT","WindDir_D1_WVT","WindDir_SD1_WVT","WSDiag_Tot","SmplsF_Tot","Axis1Failed_Tot","Axis2Failed_Tot","BothAxisFailed_Tot","NVMerror_Tot","ROMerror_Tot","MaxGain_Tot","NNDF_Tot","HollowSurf_Avg","Hollow5cm_Avg","Hollow20cm_Avg","Hollow40cm_Avg","Hollow80cm_Avg","Hollow160cm_Avg","Hollow200cm_Avg","HummockSurf_Avg","Hummock5cm_Avg","Hummock20cm_Avg","Hummock40cm_Avg","Hummock80cm_Avg","Hummock160cm_Avg","Hummock200cm_Avg","PAR_2_M_Avg","PAR_NTree1_Avg","PAR_NTree2_Avg","PAR_SouthofHollow1_Avg","PAR_SouthofHollow2_Avg","PAR_NorthofHollow1_Avg","PAR_NorthofHollow2_Avg","PAR_Srub1_Avg","PAR_Srub2_Avg","PAR_Srub3_Avg","PAR_Srub4_Avg","TopofHummock_Avg","MidofHummock_Avg","Surface1_Avg","Surface2_Avg","D1-20cm_Avg","D2-20cm_Avg","TopH_Avg","MidH_Avg","S1_Avg","S2_Avg","Deep-20cm_Avg","short_up_Avg","short_dn_Avg","long_up_Avg","long_dn_Avg","CNR4_Temp_C_Avg","CNR4_Temp_K_Avg","long_up_corr_Avg","long_dn_corr_Avg","Rs_net_Avg","Rl_net_Avg","albedo_Avg","Rn_Avg","SPN1_Total_Avg","SPN1_Diffuse_Avg","Water_Height_Avg","Water_Temp_Avg","Watertable","Dewpoint","Dewpoint_Diff"]
        # 2022 columnnames = ["TIMESTAMP","RECORD","Air_Temp_2M_A_Avg","RH_2M_A_Avg","Air_Temp_2M_B_Avg","RH_2M_B_Avg","BP_kPa_Avg","Rain_mm_Tot","Daily_Rain","Monthly_Rain","WS_ms_S_WVT","WindDir_D1_WVT","WindDir_SD1_WVT","WSDiag_Tot","SmplsF_Tot","Axis1Failed_Tot","Axis2Failed_Tot","BothAxisFailed_Tot","NVMerror_Tot","ROMerror_Tot","MaxGain_Tot","NNDF_Tot","A_Surface_Avg","A-5cm_Avg","A_10cm_Avg","A_20cm_Avg","A_30cm_Avg","A_40cm_Avg","A_50cm_Avg","A_100cm_Avg","A_200cm_Avg","B_Surface_Avg","B_5cm_Avg","B_10cm_Avg","B_20cm_Avg","B_30cm_Avg","B_40cm_Avg","B_50cm_Avg","B_100cm_Avg","B_200cm_Avg","EAST_2M_Avg","A_Shrub_Height_Avg","A_Shrub_Height_N_of_Trees_Avg","A_Hummock_Tops_Avg","A_Underwater_Hollow_Avg","WEST_2M_Avg","B_Shrub_Height_Avg","B_Shrub_Height_N_of_Trees_Avg","B_Hummock_Tops_Avg","B_Underwater_Hollow_Avg","Sensor_Avg(1)","Sensor_Avg(2)","Sensor_Avg(3)","Sensor_Avg(4)","Sensor_Avg(5)","Sensor_Avg(6)","Sensor_Avg(7)","Sensor_Avg(8)","Sensor_Avg(9)","Sensor_Avg(10)","cnr4_T_C_Avg","cnr4_T_K_Avg","SWTop_Avg","SWBottom_Avg","LWTopC_Avg","LWBottomC_Avg","Rs_net_Avg","Rl_net_Avg","albedo_Avg","Rn_Avg","SPN1_Total_Avg","SPN1_Diffuse_Avg","Water_Level_1_Avg","Water_Temp_1_Avg","Watertable_1","Water_Level_2_Avg","Water_Temp_2_Avg","Watertable_2","Q","DBTCDT_Avg","WT_Elevation_1_Avg"]
        # we need: 
        #   teco-spruce     2017 name           2022 name
        #   Date_Time  ---> TIMESTAMP           TIMESTAMP
        #   Tair       ---> AirTC_2M_Avg        Air_Temp_2M_A_Avg
        #   Tsoil      ---> Hollow20cm_Avg      A_20cm_Avg
        #   RH         ---> RH_2M_Avg           RH_2M_A_Avg
        #   VPD        ---> BP_kPa_Avg          BP_kPa_Avg
        #   Rain       ---> Rain_mm_Tot         Rain_mm_Tot
        #   WS         ---> WS_ms_S_WVT         WS_ms_S_WVT
        #   PAR        ---> PAR_2_M_Avg         EAST_2M_Avg
        # 2022
        columnnames = ["TIMESTAMP","RECORD","Tair","RH","Air_Temp_2M_B_Avg","RH_2M_B_Avg","VPD","Rain","Daily_Rain","Monthly_Rain","WS","WindDir_D1_WVT","WindDir_SD1_WVT","WSDiag_Tot","SmplsF_Tot","Axis1Failed_Tot","Axis2Failed_Tot","BothAxisFailed_Tot","NVMerror_Tot","ROMerror_Tot","MaxGain_Tot","NNDF_Tot","A_Surface_Avg","A-5cm_Avg","A_10cm_Avg","Tsoil","A_30cm_Avg","A_40cm_Avg","A_50cm_Avg","A_100cm_Avg","A_200cm_Avg","B_Surface_Avg","B_5cm_Avg","B_10cm_Avg","B_20cm_Avg","B_30cm_Avg","B_40cm_Avg","B_50cm_Avg","B_100cm_Avg","B_200cm_Avg","PAR","A_Shrub_Height_Avg","A_Shrub_Height_N_of_Trees_Avg","A_Hummock_Tops_Avg","A_Underwater_Hollow_Avg","WEST_2M_Avg","B_Shrub_Height_Avg","B_Shrub_Height_N_of_Trees_Avg","B_Hummock_Tops_Avg","B_Underwater_Hollow_Avg","Sensor_Avg(1)","Sensor_Avg(2)","Sensor_Avg(3)","Sensor_Avg(4)","Sensor_Avg(5)","Sensor_Avg(6)","Sensor_Avg(7)","Sensor_Avg(8)","Sensor_Avg(9)","Sensor_Avg(10)","cnr4_T_C_Avg","cnr4_T_K_Avg","SWTop_Avg","SWBottom_Avg","LWTopC_Avg","LWBottomC_Avg","Rs_net_Avg","Rl_net_Avg","albedo_Avg","Rn_Avg","SPN1_Total_Avg","SPN1_Diffuse_Avg","Water_Level_1_Avg","Water_Temp_1_Avg","Watertable_1","Water_Level_2_Avg","Water_Temp_2_Avg","Watertable_2","Q","DBTCDT_Avg","WT_Elevation_1_Avg"]
        # 2014
        # columnnames = ["TIMESTAMP","RECORD","Tair","RH","AirTCHumm_Avg","RH_Humm_Avg","VPD","Rain","WS","WindDir_D1_WVT","WindDir_SD1_WVT","WSDiag_Tot","SmplsF_Tot","Axis1Failed_Tot","Axis2Failed_Tot","BothAxisFailed_Tot","NVMerror_Tot","ROMerror_Tot","MaxGain_Tot","NNDF_Tot","HollowSurf_Avg","Hollow5cm_Avg","Tsoil","Hollow40cm_Avg","Hollow80cm_Avg","Hollow160cm_Avg","Hollow200cm_Avg","HummockSurf_Avg","Hummock5cm_Avg","Hummock20cm_Avg","Hummock40cm_Avg","Hummock80cm_Avg","Hummock160cm_Avg","Hummock200cm_Avg","PAR","PAR_NTree1_Avg","PAR_NTree2_Avg","PAR_SouthofHollow1_Avg","PAR_SouthofHollow2_Avg","PAR_NorthofHollow1_Avg","PAR_NorthofHollow2_Avg","PAR_Srub1_Avg","PAR_Srub2_Avg","PAR_Srub3_Avg","PAR_Srub4_Avg","TopofHummock_Avg","MidofHummock_Avg","Surface1_Avg","Surface2_Avg","D1-20cm_Avg","D2-20cm_Avg","TopH_Avg","MidH_Avg","S1_Avg","S2_Avg","Deep-20cm_Avg","short_up_Avg","short_dn_Avg","long_up_Avg","long_dn_Avg","CNR4_Temp_C_Avg","CNR4_Temp_K_Avg","long_up_corr_Avg","long_dn_corr_Avg","Rs_net_Avg","Rl_net_Avg","albedo_Avg","Rn_Avg","SPN1_Total_Avg","SPN1_Diffuse_Avg","Water_Height_Avg","Water_Temp_Avg","Watertable","Dewpoint","Dewpoint_Diff"]

        sp_data.columns = columnnames
        # trying to bring the timestamp into a format
        df  =sp_data
        data=df['TIMESTAMP']
        df['Date_Time']=pd.to_datetime(df['TIMESTAMP'])    
        #Trim columns
        teco_spruce =df[['Date_Time','Tair','Tsoil','RH','VPD','Rain','WS','PAR']]
        #adding it to the existing data frame
        df['year']=df['Date_Time'].dt.year
        df['doy'] =df['Date_Time'].dt.dayofyear
        df['hour']=df['Date_Time'].dt.hour

        teco_spruce=df[['year','doy','hour','Tair','Tsoil','RH','VPD','Rain','WS','PAR']]
        #getting the mean of 'Tair','Tsoil','RH','VPD','WS','PAR' n sum of ,'Rain' by combining half n full hour(e.i.12 & 12:30)
        group_treat = teco_spruce.groupby(['year','doy','hour'])
        tair  = group_treat['Tair'].mean()
        tsoil = group_treat['Tsoil'].mean()
        rh    = group_treat['RH'].mean()
        vpd   = group_treat['VPD'].mean()
        rain  = group_treat['Rain'].sum()
        ws    = group_treat['WS'].mean()
        par   = group_treat['PAR'].mean()
        #Taking only the even coulums(as half hourly details not required) i.e. 12:30 not required only 12 required 
        # teco_spruce1=teco_spruce.iloc[::2]
        teco_spruce1=teco_spruce.iloc[::2,:3].drop_duplicates().reset_index(drop=True)
        #need to reset the index number[from 0 2 4 8] [to 0 1 2 3]
        teco_spruce1=teco_spruce1.reset_index(drop=True)
        #setting the mean of 'Tair','Tsoil','RH','VPD','WS','PAR' n sum of ,'Rain' to this new dataframe teco_spruce1
        # teco_spruce1['Tair']=tair.reset_index(drop=True)	    
        # teco_spruce1['Tsoil']=tsoil.reset_index(drop=True)
        # teco_spruce1['RH']=rh.reset_index(drop=True)
        # teco_spruce1['VPD']=vpd.reset_index(drop=True)
        # teco_spruce1['Rain']=rain.reset_index(drop=True)
        # teco_spruce1['WS']=ws.reset_index(drop=True)
        # teco_spruce1['PAR']=par.reset_index(drop=True)
        # Jian: change  ...
        teco_spruce1=teco_spruce1.merge(tair,  on=["year","doy","hour"])
        teco_spruce1=teco_spruce1.merge(tsoil, on=["year","doy","hour"])
        teco_spruce1=teco_spruce1.merge(rh,    on=["year","doy","hour"])
        teco_spruce1=teco_spruce1.merge(vpd,   on=["year","doy","hour"])
        teco_spruce1=teco_spruce1.merge(rain,  on=["year","doy","hour"])
        teco_spruce1=teco_spruce1.merge(ws,    on=["year","doy","hour"])
        teco_spruce1=teco_spruce1.merge(par,   on=["year","doy","hour"])

        # Jian: save the pulled data 
        time_now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        teco_spruce1.to_csv('{0}/SPRUCE_forcing_{1}.txt'.format(destination, time_now),'\t',index=False)
        # Jian: drop the over range values
        teco_spruce1 = teco_spruce1.drop(teco_spruce1[(teco_spruce1.year<2011)].index)
        #file which contain earlier data(2011-2015)
        j1=pd.read_csv(initial_text,'\t')

        #file which contain the new data
        #j2=pd.read_csv('teco_spruce.txt','\t')
        #joining both the files together and removing the duplicate rows
        j3=pd.concat([j1,teco_spruce1]).drop_duplicates().reset_index(drop=True)
        #checking for na values
        print('now I will check na values')
        if(teco_spruce1.isnull().values.any()):
            teco_spruce1 = teco_spruce1.interpolate()
        print('I have finished checking the na values')
        time_now=datetime.now()
            # time_now =time_now.strftime("%Y_%m_%d_%H_%M_%S")
            #writing it to a file
        print('now I am writing to the file')
        j3.to_csv('{0}/SPRUCE_forcing.txt'.format(destination),'\t',index=False) 
            #teco_spruce1.to_csv('final.txt','\t',index=False)  
            # j3.to_csv('{0}/SPRUCE_forcing_{1}.txt'.format(destination,time_now),'\t',index=False) 
        #teco_spruce1.to_csv('final_{0}.txt'.format(time_now),'\t',index=False)
        print('finished writing to the file')
	
       
    except Exception as e:
        #raise Exception('the ftp site is down..Using the old sprucing file...')    
        print(e)
        print('the ftp site is down..Using the old sprucing file...')   
  
# teco_spruce_pulldata()