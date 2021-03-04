# mtr_oeds.vsensor
Python package for generating v-sensor track monitoring report, every Wednesdays

`./test/` contains some working examples. 
* `./distance_mapping_util.py` contains functions that convert v-sensor raw data to vibration with estimated location
* `./weeklyReport.py` contains functions that convert mutliple vibration result to a single file for tableau and email
* `./test/mainbatch.py` example usage `./distance_mapping_util.py`
* `./test/generateWeeklyReport.py` example usage `./weeklyReport.py`

## from v-sensor raw data to estimate inter-station vibration
1. download and prepare the v-sensor data, TSSW 
2. config the .cfg file (i.e. config_mainbatch.cfg)

  **EXAMPLE:**
  ```cfg
  [v44] # unique name of the batch 
  dummy=version44 # name
  path_Vsensor=C:/Users/oeds-rsmd/Documents/Vsensor_estimating_location/TEST_DATA/ISL_2020-10-01_2020-10-04/ # absolute path of the vsensor raw data
  linename=ISL # line name {ISL/KTL/TWL}
  car_number=102 # lead/trail cab of the target
  TSSW_folder=C:/Users/oeds-rsmd/Documents/Vsensor_estimating_location/TEST_TSSW/ # absolute path of the TSSW file
  TSSW_start=20200930 # first 8 character to compare => to include the TSSW
  TSSW_end=20201005 # first 8 character to compare <= to include the TSSW
  auto_time_shift = True # program to perform auto timeshift or not {True/False}
  time_shift_value = 0 # second to shift if auto_time_shift is False
  auto_z_offset = True # program to perform x,y,z offset 
  z_offset_value = 0 # mG for z axis to offset if auto_z_offset is False
  gen_detail=True # calculate down to inter-station distance or not {True/False}  
  save_checkpoint_bool=True # save the final working to parquet {True/False}, recommend to True
  savecheckpoint_foldername=C:/Users/oeds-rsmd/Documents/Vsensor_estimating_location/checkpoint/version44_ISL_Oct # absolute path for saving output
  forward_axis=x # axis in the raw file representing forward motion
  lateral_axis=y # axis in the raw file representing lateral motion
  vertical_axis=z # axis in the raw file representing vertical motion
  ```
  
3. run the python program

  ```bash
  python mainbatch.py -c ./config_mainbatch.cfg -v v42 v43 v44
  ```
  Above command will use `./config_mainbatch.cfg` as config file and run the v42, v43 and v44 

## from inter-station vibration to reporting v.s. historical report
1. after running the `./config_mainbatch.cfg`, some `\checkpoint\*\df_result.csv` will be generated
2. config the cfg file (i.e. config_summary.cfg)

  **EXAMPLE:**
  ```cfg
  [general]
  path=C:/Users/oeds-rsmd/Documents/Vsensor_estimating_location/checkpoint/ # absolute of the checkpoint folders
  output_location=./ # location for the output file
  output_filename=ISL_summary.csv # filename for the output csv (for tableau)
  gen_table=True # generate the excel table (5 summary tables) or not {True/False}

  [reference] # batch of data that use to calculate 'grey area', usually 2 week (i.e. 6 batches)
  v36:version36_ISL_Sep_rerun/df_result.csv
  v37:version37_ISL_Sep_rerun/df_result.csv
  v38:version38_ISL_Sep_rerun/df_result.csv
  v39:version39_ISL_Sep/df_result.csv
  v40:version40_ISL_Sep/df_result.csv
  v41:version41_ISL_Sep/df_result.csv
  [target] # batch of data that is current reporting week (dot in the scatter plots), usually 1 week (i.e. 3 batches)
  v42:version42_ISL_Sep/df_result.csv
  v43:version43_ISL_Sep/df_result.csv
  v44:version44_ISL_Oct/df_result.csv
  ```
3. run the python program

  ```bash
  python generateWeeklyReport.py -c ./config_sumamry.cfg 
  ```
  Above command will use `./config_sumamry.cfg` as config file and compare the [target] with [reference] and output csv and/or excel file for tableau and email
