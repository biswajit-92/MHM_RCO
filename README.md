# MHM_RCO
Feature test for MHM-Re/Config.
Author  : BISWAJIT MAHARANA(BM186030)
Created : 10/05/2017
Purpose:  This is the test frame work we are going to use as part of feature testing of MHM-Re/Config.
# Description   :  Below are allowed CONFIG changes(or Functions) in this script:
#                   1.  Add_AMPs_to_exist
#                   2.  Add_AMPs_to_New_Exist
#                   3.  Add_AMPs_to_new
#                   4.  Del_Mod_AMPs
#                   5.  Delete_End_AMPs
#                   6.  Delete_Begin_AMPs
#                   7.  Overlap_Map
#                   8. Random_MOD_AMPs
#                   9. MOD_AMPs
#                   10. Sys_Dict_MAP_change
#                   11. Add_Mod_AMPs
#                   12. Alter Table
#                   13. Drop Maps
#                   14. Create possible maps
#                   15. MakeAMPDown


# Files:
+++++++++
input.json: This contains the test cases which end user wants to run.
MHM_RCO.py: This contains the python source code to execute the given tests.
ParseInput.py: This contains the code which will parse the input.json file and pass commands to MHM_RCO file.

# How to Run   : python ParseInput.py Input.json

# Synopsis     
#        Methods(MHM_RCO.py):
#           config(class : CreateMAPInConfig):
               Config_Parameters(self)          : It contains all the required informations regarding status of AMPS.
               IsClusterAssignmentGood(self)    : It checks whether the system is having good cluster configuration or not.
               ChangeClusterWithFixedSize(self, dc_size, FixedMod_Log) : It creates map with cluster change with fixed cluster size.
               AddWithClusterChange(self, cluster_size, AddWithCC_Log) : It performs add amp along with cluster change.
               DelWithClusterChange(self, percentage, cluster_size, DelWithCC_Log) : It performs del amp along with clster change.
               CreateConfigAddAmpCommand(self, CreateAMPGroup, AvailableClusterList) : It creates a command which will be given to config utility to create a map for add amp operation.
               GetNoOfAmpsInCluster(self, ClusterNo) : It gives no of amps in the given cluster.
               AssignAmpToCluster(self, AmpGroup, ClusterList) : This method will be called when we have to decide which group of amps will go to which cluster. Only used when availablecluster*8 == NewReadyAmps
               AddAmpToExistingCluster(self, AddExist_Log) : It is used to add NewReady amps to the existing clusters.
               AddAmpToNewCluster(self, StartFromThisAmp, AddNew_Log) : It is used to to add NewReady amps to New Cluster.
               DelampMAP(self, PercentageMode, Type, Config_del_Log, IsPhysical = 'no') : It is used to del amps Physically or Logically
               MakeAMPDown(self, MapName, DownAMP_Log) : It is used to make an AMP Down in the given MAP.
               AlterTableMap(self, source_map, target_map) : It will alter all the tables from a source map to a target map.
               UserTblOnSourceMap(self) : It will find which maps are having user tables on it.
               CreateSpecificNoOfMaps(self, mapcount, CreateMap_Log) : It will create possible no of maps from the system.
               CreateCommandForSpecificMaps(self, AmpsInEachCluster, NoOfCluster) : It will be used to create all possible types of maps.
               ExecuteCommandForSpecificMaps(self, CreateMapCmd) : It will be used to pass the command created in CreateSpecificNoOfMaps method.
               DropMap(self, auto_select, map_name) : It will be used to drop a map.
               
#         Reconfig(class : Reconfig):
               ChangeGenralFlag(self, Flags, dbscntrl_Log) : It is used to change the value of a general flag.
               IsADDAmpReconfig(self, SPA_Change, NewMap_as_sys_default, sys_default_map, option, Add_R_Log) : It is used to run ADD amp reconfig.
               IsLogicalChngReconfig(self, dict_map_change, dict_map, sys_default_change, sys_default_map, OnlyLogicalRco_Log) : will be used to run reconfig to change the Dictionary map or system default map.
               IsCommonReconfig(self, sys_default_change, sys_default_map, CommonRco_Log) : It will be used to run Del/Del+Mod/Mod amp reconfigs.
               IsAddCCReconfig(self, SPA_Change, sys_default_change, sys_default_map, AddaCCR_Log) : It will be used to run Add+ClusterChange amp reconfig.
               
#         TdUtility(class : TDUtility):
               CheckDBSstate(self) : It will be used to check the state of DBS.
               IsSystemQuiescent(self) : To check whether the system is Quiescent or not.
               CheckDBSstateForSYSINIT(self) : To check the state of DBS for SYSINIT.
               restartDBS(self, reason) : To restart the database.
               RemoveGDO(self,reason="GDORemove") : To remove all the GDOs and recreate new ones.
               Sysinit(self,out_file,IsGDORemove="YES") : To perform sysinit, returns false if fail.
               DIP(self,out_file) : To perform DIP.
               CheckTable(self,table,out_file) : To run checktable utility.
               ScanDisk(self,out_file): : To run Scandisk utility.

# Example Input.json:

3 test cases with proper method names and with the required parameters.

{
    "System_Name": "porthos1.labs.teradata.com",
    "Test_Case": [
        [
            "DelampMAP",
            [
                40,
                1,
                "log_delHigh",
                "no"
            ],
            "IsLogicalChngReconfig",
            [
                "yes",
                "td_map2",
                "yes",
                "td_map2",
                "Onlylog_Rco"
            ],
            "CheckTable",
            [
                "all tables",
                "Check_TblLog"
            ]
        ],
        [
            "ChangeClusterWithFixedSize",
            [
                4,
                "Mod_AmpLog"
            ],
            "IsCommonReconfig",
            [
                "yes",
                "td_map3",
                "Mod_AmpRCO"
            ]
        ],
        [
            "DelWithClusterChange",
            [
                20,
                2,
                "Del_CCLog"
            ],
            "IsCommonReconfig",
            [
                "yes",
                "td_map4",
                "Mod_AmpRCO"
            ]
        ]
    ],
    "sysinit": [
        "sysinit_log",
        "no"
    ]
}


