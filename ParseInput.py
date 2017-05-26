#!/usr/bin/python

#from MHM_RCO import CreateMAPInConfig,Reconfig,TDUtility
import teradata
import MHM_RCO
import json
from itertools import islice
import sys
import errno
import os
import random
from Extd_IF_header import *
from datetime import datetime

#Flags
data = ''
__NoOfTest__ = ''
__SystemName__ = ''
__sysinit__ = '' 
__gdo__remove_ = ''
__use_random_mapname__ = False
__config__ = False
__reconfig__ = False
__checktable__ = False
outdir = ''
outdir = os.path.join(
            os.getcwd(),
            datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
try:
    os.makedirs(outdir)
except OSError as e:
    if e.errno != errno.EEXIST:
        raise

class __parseTest__(object):
    """
    This method will be called to parse the test file.
    """
    def __init__(self):

# Now parse through each test case to perform the job
        self.__count_co = 0
        self.__count_rco = 0
        self.__count_util = 0
        self.__count_misc = 0
        self.__test__opt_ = [] 
        self.__config_operation_ = ["_mod_amp",
                                    "_mod_amp_with_shuffle_cluster",
                                    "_mod_amp_with_random_cluster",
                                    "_add_and_cc",
                                    "_del_and_cc",
                                    "_aa_to_exist_cluster",
                                    "_aa_to_existandnew_cluster",
                                    "_aa_to_new_cluster",
                                    "_del_amp_map",
                                    "_make_amp_down",
                                    "_create_specific_maps"
            ]
        self.__reconfig_operation_ = ["_is_addamp_rco",
                                      "_is_logicalchng_rco",
                                      "_is_common_rco",
                                      "_is_add_cc_rco"
            ]
        self.__tdutility_operation_ = ["_sysinit",
                                       "_dip",
                                       "_checktable",
                                       "_scandisk"
            ]
        self.__misc_operation_ = ["_enable_logons",
                                  "_disable_logons",
                                  "_restart_dbs",
                                  "_change_dbscontrol_flag",
                                  "_drop_map",
                                  "_alter_table_map"
            ]
        
    def __getOperation__(self, Test_file):
        

        """
            "Test_Case" : 
                [
                    [ ###### x ########
                        "_del_amp_map" ,         ###### y #######
                        {
                            "PercentageMode" : 40,
                            "Type" : 1,
                            "Config_del_Log" : "log_delHigh",
                            "IsPhysical" : "no"
                        },
                        "_is_logicalchng_rco" , 
                        {
                            "dict_map_change" : "yes",
                            "dict_map" : "td_map2",
                            "sys_default_change" : "yes",
                            "sys_default_map" : "td_map2",
                            "OnlyLogicalRco_Log" : "Onlylog_Rco"
                        },
                        "CheckTable" , 
                        {
                            "table" : "all tables",
                            "out_file" : "Check_TblLog"
                        }
                    ],


                    [
                        "_mod_amp_with_dc_cmd" , 
                        {
                            "dc_size" : 4,
                            "FixedMod_Log" : "Mod_AmpLog"
                        },
                        "_is_common_rco" , 
                        {
                            "sys_default_change" : "yes",
                            "sys_default_map" : "td_map3",
                            "CommonRco_Log" : "Mod_AmpRCO"
                        }
                    ]
                ]
        """        
        
        global data
        with open(Test_file) as data_file:    
            data = json.load(data_file)        # This has all the contents of the test input file     
            
        global __SystemName__,__NoOfTest__,__use_random_mapname__
        if data["Use_random_map_in_reconfig"] == "yes":
            __use_random_mapname__ = True
        __SystemName__ = data["System_Name"]   # This holds the system on which the test has to run
        __NoOfTest__ = len(data["Test_Case"])  # This holds the total no of test case in the input file                     
        # Parse the complete test file once to know all the tests to perform
        for x in range(len(data["Test_Case"])):
            for y in range(len(data["Test_Case"][x])):
                self.__test__opt_.append(data["Test_Case"][x][y])

        self.__test__opt_ = self.__test__opt_[::2] #Remove the odd position from it so that it will contain only the operation names.
        # We should maintain a two dimensional list to map each test no to test operation
        __boundary__ = [] #This will keep the size of the test in each test case.
        for x in range(__NoOfTest__):
            __boundary__.append(len(data["Test_Case"][x]))  #Contains both operation and parameters

        __div__ = 2
        __boundary__ = map(lambda x: x/__div__, __boundary__) #Removed the parameters from the size of each test
 
        it = iter(self.__test__opt_)
        self.__test__opt_ = [list(islice(it,i)) for i in __boundary__]

    def __get_all_online_maps__(self):
        '''
        This method will be called to initialize/update self.__online_map_list__.
        '''
        self.__online_map_list__ = []
        ###add task to make the logons enable#####
        udaexec = teradata.UdaExec(appName='MHM',version=1)
        session = udaexec.connect(method='odbc',username='dbc',password='dbc',system=__SystemName__,dbType='Teradata Database ODBC Driver 16.00')
        for row in session.execute("SEL MapNameI FROM MAPS WHERE MapKind='C' AND MapSlot>1 ORDER BY MapNameI"):
            for i in row:
                self.__online_map_list__.append(i)
        self.__online_map_list__.sort()
            
    def __get_new_dictionary_mapname__(self):
        '''
        This method will be called to get a new dictionary map name from database.
        we have to select a map such that it shouldn't match the current dictionary map configuration.
        Will be used during dictionary/system-default map change reconfigs.
        '''
        #__amps_boundary_ will hold the ContiguousStartAmp and ContiguousEndAmp values of Dictionary map.
        __amps_boundary_ = []
        __new_dict_map_ = []
        udaexec = teradata.UdaExec(appName='MHM',version=1)
        session = udaexec.connect(method='odbc',username='dbc',password='dbc',system=__SystemName__,dbType='Teradata Database ODBC Driver 16.00')
        for row in session.execute("SEL CONTIGUOUSSTARTAMP,CONTIGUOUSENDAMP FROM MAPS WHERE MAPSLOT=1"):
            for amp_no in row:
                __amps_boundary_.append(str(amp_no))
        #Let's get all the maps whose configuration isn't same with the dictionary map.
        for row in session.execute("SEL MAPNAME FROM MAPS WHERE CONTIGUOUSSTARTAMP="+__amps_boundary_[0]+" AND CONTIGUOUSENDAMP<>"+__amps_boundary_[1]+" AND MAPSLOT NOT IN(0,1) AND MAPKIND='C'"):
            for map_name in row:
                __new_dict_map_.append(str(map_name))
                
        return (random.choice(__new_dict_map_))
                
    def __get_map_in_defined_state__(self):
        '''
        This method will be called to get the mapname in defined state.
        Will be used during add amp,mod amp reconfigs.
        '''      
        __all_maps_ = (os.popen( "/usr/bin/tdsh "+self.__host+ " /usr/tdbms/bin/dmpgdo mapinfo | grep 'TD_MAP' | awk -F' ' '{print $1}' ").read()).split(":\n")
        __all_maps_.pop(-1)
        __all_maps_.sort() #Now we have all the maps list present in mapinfo GDO
        #Now we have to search what is the map which is not there in the self.__online_map_list__ 
        return str(set(__all_maps_)-set(self.__online_map_list__))

    def __getcmdargs__(self, x , Opt):
        """
        This method will be used to retrieve the parameters of each methods.
        @param x: This is the test case no.
        @param Opt: Which method are we parsing.
        @return: List containing the actual arguments to pass with the method
        """  
        __command_args = []
        __method_index = data["Test_Case"][x].index(Opt) #The position of the test in json
        __command_args.extend(data["Test_Case"][x][__method_index+1]) #Contains the parameters of the method key:value   
     
        return __command_args
    
    def __execTest__(self):
        """
        This method will be used to execute the commands.
        """

        for x in range(__NoOfTest__):
            for y in range(len(self.__test__opt_[x])):
                # If the current operation is one of the below config task then call that method with the config object "__config_ob_" #
                if  self.__test__opt_[x][y] in self.__config_operation_:
                    if self.__count_co == 0:
                        __config_ob_ = MHM_RCO.CreateMAPInConfig(__SystemName__,"Config_Logs")

                    elif self.__test__opt_[x][y] == self.__config_operation_[0]:
                        """_mod_amp(self, amps_per_cluster, mod_amp_log)"""
                        __command_args = self.__getcmdargs__(x, self.__config_operation_[0])
                        __config_ob_._mod_amp(*__command_args)
                        
                    elif self.__test__opt_[x][y] == self.__config_operation_[1]:
                        """_mod_amp_with_shuffle_cluster(self,amps_per_cluster, mod_amp_shuffle_log)"""
                        __command_args = self.__getcmdargs__(x, self.__config_operation_[1])
                        __config_ob_._mod_amp_with_shuffle_cluster(*__command_args)                        
                                        
                    elif self.__test__opt_[x][y] == self.__config_operation_[2]:
                        """__mod_amp_with_random_cluster(self, mod_amp_random_log)"""
                        __command_args = self.__getcmdargs__(x, self.__config_operation_[2])
                        __config_ob_.__mod_amp_with_random_cluster(*__command_args)

                    elif self.__test__opt_[x][y] == self.__config_operation_[3]:
                        """_add_and_cc(self, cluster_size, add_cc_log)"""
                        __command_args = self.__getcmdargs__(x, self.__config_operation_[3])
                        __config_ob_._add_and_cc(*__command_args)

                    elif self.__test__opt_[x][y] == self.__config_operation_[4]:
                        """_del_and_cc(self, percentage, cluster_size, del_cc_log)"""
                        __command_args = self.__getcmdargs__(x, self.__config_operation_[4])
                        __config_ob_._del_and_cc(*__command_args)
                                                                       
                    elif self.__test__opt_[x][y] == self.__config_operation_[5]:
                        """_aa_to_exist_cluster(self, startf_this_amp, end_at_this_amp, add_exi_log)"""
                        __command_args = self.__getcmdargs__(x, self.__config_operation_[5])
                        __config_ob_._aa_to_exist_cluster(*__command_args)
                                                                       
                    elif self.__test__opt_[x][y] == self.__config_operation_[6]:
                        """_aa_to_existandnew_cluster(self, add_existandnew_log)"""
                        __command_args = self.__getcmdargs__(x, self.__config_operation_[6])
                        __config_ob_._aa_to_existandnew_cluster(*__command_args)
                                                                       
                    elif self.__test__opt_[x][y] == self.__config_operation_[7]:
                        """_aa_to_new_cluster(self, startf_this_amp, end_at_this_amp, add_new_log)"""
                        __command_args = self.__getcmdargs__(x, self.__config_operation_[7])
                        __config_ob_._aa_to_new_cluster(*__command_args)
                                                                       
                    elif self.__test__opt_[x][y] == self.__config_operation_[8]:
                        """_del_amp_map(self, percentage, op_type, del_amp_log, is_physical = 'no')"""
                        __command_args = self.__getcmdargs__(x, self.__config_operation_[8])
                        __config_ob_._del_amp_map(*__command_args)
                                                                       
                    elif self.__test__opt_[x][y] == self.__config_operation_[9]:
                        """_make_amp_down(self, map_name, down_amp_log)"""
                        __command_args = self.__getcmdargs__(x, self.__config_operation_[9])
                        __config_ob_._make_amp_down(*__command_args)
                                                                       
                    elif self.__test__opt_[x][y] == self.__config_operation_[10]:
                        """_create_specific_maps(self, map_count, create_specific_log)"""
                        __command_args = self.__getcmdargs__(x, self.__config_operation_[10])
                        __config_ob_._create_specific_maps(*__command_args)

                    self.__count_co +=1
            
                # if the current operation is one of the below reconfig task then call that method with the reconfig object "__rco_ob_" #
                elif self.__test__opt_[x][y] in self.__reconfig_operation_:
                    if self.__count_rco == 0:
                        __rco_ob_ = MHM_RCO.Reconfig(__SystemName__,"Reconfig") 
                    if self.__test__opt_[x][y] == self.__reconfig_operation_[0]:
                        """_is_addamp_rco(self, spa_change, newmap_as_sysdef, sys_default_map, option, is_addamp_log)"""
                        __command_args = self.__getcmdargs__(x, self.__reconfig_operation_[0])
                        if __command_args[1] == "no" and __use_random_mapname__ == True:
                            __command_args[2] = self.__get_map_in_defined_state__()
                        __rco_ob_._is_addamp_rco(*__command_args)
                
                    elif self.__test__opt_[x][y] == self.__reconfig_operation_[1]:
                        """_is_logicalchng_rco(self, dict_map_change, dict_map, sys_default_change, sys_default_map, _is_logicalchng_log)"""
                        __command_args = self.__getcmdargs__(x, self.__reconfig_operation_[1])
                        if __command_args[0] == "yes" and __use_random_mapname__ == True:
                            __command_args[1] = __command_args[3] = self.__get_new_dictionary_mapname__()
                        __rco_ob_._is_logicalchng_rco(*__command_args)
                
                    elif self.__test__opt_[x][y] == self.__reconfig_operation_[2]:
                        """_is_common_rco(self, sys_default_change, sys_default_map, is_common_rco_log)"""
                        __command_args = self.__getcmdargs__(x, self.__reconfig_operation_[2])
                        if __command_args[0] == "yes" and __use_random_mapname__ == True:
                            __command_args[1] = self.__get_map_in_defined_state__()
                        __rco_ob_._is_common_rco(*__command_args)
                
                    elif self.__test__opt_[x][y] == self.__reconfig_operation_[3]:
                        """_is_add_cc_rco(self, spa_change, sys_default_change, sys_default_map, is_add_cc_rco_log)"""
                        __command_args = self.__getcmdargs__(x, self.__reconfig_operation_[3])
                        if __command_args[1] == "yes" and __use_random_mapname__ == True:
                            __command_args[2] = self.__get_map_in_defined_state__()
                        __rco_ob_._is_add_cc_rco(*__command_args)
                
                    self.__count_rco +=1
                # if the current operation is one of the below utility task then call that method with the utility object "__util_ob_" #
                elif self.__test__opt_[x][y] in self.__tdutility_operation_:
                    if self.__count_util == 0:
                        __util_ob_ = MHM_RCO.TDUtility(__SystemName__,"Utility_Logs")
                         
                    if self.__test__opt_[x][y] == self.__tdutility_operation_[0]:
                        """_sysinit(self,out_file,IsGDORemove="YES")"""
                        __command_args = self.__getcmdargs__(x, self.__tdutility_operation_[0])
                        __util_ob_._sysinit(*__command_args)
                
                    elif self.__test__opt_[x][y] == self.__tdutility_operation_[1]:
                        """_dip(self,out_file)"""
                        __command_args = self.__getcmdargs__(x, self.__tdutility_operation_[1])
                        __util_ob_._dip(*__command_args)
                        
                    elif self.__test__opt_[x][y] == self.__tdutility_operation_[2]:
                        """_checktable(self,table,out_file)"""
                        __command_args = self.__getcmdargs__(x, self.__tdutility_operation_[2])
                        __util_ob_._checktable(*__command_args)
                        
                    elif self.__test__opt_[x][y] == self.__tdutility_operation_[3]:
                        """_scandisk(self,out_file)"""
                        __command_args = self.__getcmdargs__(x, self.__tdutility_operation_[3])
                        __util_ob_._scandisk(*__command_args)                                                
                
                    self.__count_util +=1

                # if the current operation is one of the below miscellaneous task then call that method with the utility object "__util_ob_" #
                elif self.__test__opt_[x][y] in self.__misc_operation_:
                    if self.__count_util == 0:
                        __util_ob_ = MHM_RCO.TDUtility(__SystemName__,"Utility_Logs")
                         
                    if self.__test__opt_[x][y] == self.__misc_operation_[0]:
                        """_sysinit(self,out_file,IsGDORemove="YES")"""
                        __command_args = self.__getcmdargs__(x, self.__misc_operation_[0])
                        __util_ob_._enable_logons(*__command_args)
                
                    elif self.__test__opt_[x][y] == self.__misc_operation_[1]:
                        """_dip(self,out_file)"""
                        __command_args = self.__getcmdargs__(x, self.__misc_operation_[1])
                        __util_ob_._disable_logons(*__command_args)
                        
                    elif self.__test__opt_[x][y] == self.__misc_operation_[2]:
                        """_checktable(self,table,out_file)"""
                        __command_args = self.__getcmdargs__(x, self.__misc_operation_[2])
                        __util_ob_._restart_dbs(*__command_args)
                        
                    elif self.__test__opt_[x][y] == self.__misc_operation_[3]:
                        """_scandisk(self,out_file)"""
                        __command_args = self.__getcmdargs__(x, self.__misc_operation_[3])
                        __util_ob_._change_dbscontrol_flag(*__command_args)                                                
                
                    elif self.__test__opt_[x][y] == self.__misc_operation_[4]:
                        """_scandisk(self,out_file)"""
                        __command_args = self.__getcmdargs__(x, self.__misc_operation_[4])
                        __util_ob_._drop_map(*__command_args)     
                        
                    elif self.__test__opt_[x][y] == self.__misc_operation_[5]:
                        """_scandisk(self,out_file)"""
                        __command_args = self.__getcmdargs__(x, self.__misc_operation_[5])
                        __util_ob_._alter_table_map(*__command_args)     
                                                                
                    self.__count_misc +=1
                    
if __name__ == '__main__':
    
    try:
        __testFile__ = sys.argv[1]
    except IndexError:
        print "Usage: ParseInput.py <Test.json>"
        sys.exit(1)
    
    Test = __parseTest__()
    Test.__getOperation__(__testFile__)
    Test.__get_all_online_maps__()
    Test.__execTest__()
