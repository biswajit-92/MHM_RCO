#!/usr/bin/python

#from MHM_RCO import CreateMAPInConfig,Reconfig,TDUtility
import MHM_RCO
import json
from itertools import islice
import sys
import errno
import os
from datetime import datetime

#Flags
data = ''
__NoOfTest__ = ''
__SystemName__ = ''
__sysinit__ = '' 
__gdo__remove_ = '' 
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
        self.__test__opt_ = []
        
    def __getOperation__(self, Test_file):
        
        global data
        with open(Test_file) as data_file:    
            data = json.load(data_file)        # This has all the contents of the test input file     
            
        global __SystemName__,__NoOfTest__
        __SystemName__ = data["System_Name"]   # This holds the system on which the test has to run
        __NoOfTest__ = len(data["Test_Case"])  # This holds the total no of test case in the input file                     
        # Parse the complete test file once to know all the tests to perform
        self.__test__opt_ = []   # store all the operations in this list 
        for x in range(len(data["Test_Case"])):
            for y in range(len(data["Test_Case"][x])):
                self.__test__opt_.append(data["Test_Case"][x][y])

        self.__test__opt_ = self.__test__opt_[::2] #Remove the odd position from it so that it will contain only the operation names.

        """
            "Test_Case" : 
                [
                    [ ###### x ########
                        "DelampMAP" ,         ###### y #######
                        {
                            "PercentageMode" : 40,
                            "Type" : 1,
                            "Config_del_Log" : "log_delHigh",
                            "IsPhysical" : "no"
                        },
                        "IsLogicalChngReconfig" , 
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
                        "ChangeClusterWithFixedSize" , 
                        {
                            "dc_size" : 4,
                            "FixedMod_Log" : "Mod_AmpLog"
                        },
                        "IsCommonReconfig" , 
                        {
                            "sys_default_change" : "yes",
                            "sys_default_map" : "td_map3",
                            "CommonRco_Log" : "Mod_AmpRCO"
                        }
                    ]
                ]
        """

        # We should maintain a two dimensional list to map each test no to test operation
        __boundary__ = [] #This will keep the size of the test in each test case.
        for x in range(__NoOfTest__):
            __boundary__.append(len(data["Test_Case"][x]))  #Contains both operation and parameters

        __div__ = 2
        __boundary__ = map(lambda x: x/__div__, __boundary__) #Removed the parameters from the size of each test
 
        it = iter(self.__test__opt_)
        self.__test__opt_ = [list(islice(it,i)) for i in __boundary__]


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
                if  self.__test__opt_[x][y] == "ChangeClusterWithFixedSize" or "AddWithClusterChange" or "DelWithClusterChange" or "AddAmpToExistingCluster" or "AddAmpToNewCluster" or "DelampMAP":
                    if self.__count_co == 0:
                        __config_ob_ = MHM_RCO.CreateMAPInConfig(__SystemName__,"Config_Logs")
                
                    if self.__test__opt_[x][y] == "ChangeClusterWithFixedSize":
                        """ChangeClusterWithFixedSize(self, dc_size, FixedMod_Log)"""
                        __command_args = self.__getcmdargs__(x, "ChangeClusterWithFixedSize")
                        __config_ob_.ChangeClusterWithFixedSize(*__command_args)
                
                    elif self.__test__opt_[x][y] == "AddWithClusterChange":
                        """AddWithClusterChange(self, cluster_size, AddWithCC_Log)"""
                        __command_args = self.__getcmdargs__(x, "AddWithClusterChange")
                        __config_ob_.AddWithClusterChange(*__command_args)
                
                    elif self.__test__opt_[x][y] == "DelWithClusterChange":
                        """DelWithClusterChange(self, percentage, cluster_size, DelWithCC_Log)"""
                        __command_args = self.__getcmdargs__(x, "DelWithClusterChange")                
                        __config_ob_.DelWithClusterChange(*__command_args)
                
                    elif self.__test__opt_[x][y] == "AddAmpToExistingCluster":
                        """AddAmpToExistingCluster(self, AddExist_Log)"""
                        __command_args = self.__getcmdargs__(x, "AddAmpToExistingCluster")
                        __config_ob_.AddAmpToExistingCluster(*__command_args)
                
                    elif self.__test__opt_[x][y] == "AddAmpToNewCluster":
                        """AddAmpToNewCluster(self, StartFromThisAmp, AddNew_Log)"""
                        __command_args = self.__getcmdargs__(x, "AddAmpToNewCluster")
                        __config_ob_.AddAmpToNewCluste(*__command_args)
                
                    elif self.__test__opt_[x][y] == "DelampMAP":
                        """DelampMAP(self, PercentageMode, Type, Config_del_Log, IsPhysical = 'no')"""
                        __command_args = self.__getcmdargs__(x, "DelampMAP")
                        __config_ob_.DelampMAP(*__command_args)
                
                    self.__count_co +=1
            
                # if the current operation is one of the below reconfig task then call that method with the reconfig object "__rco_ob_" #
                elif self.__test__opt_[x][y] == "IsADDAmpReconfig" or "IsLogicalChngReconfig" or "IsCommonReconfig" or "IsAddCCReconfig":
                    if self.__count_rco == 0:
                        __rco_ob_ = MHM_RCO.Reconfig(__SystemName__,"Reconfig") 
                    if self.__test__opt_[x][y] == "IsADDAmpReconfig":
                        """IsADDAmpReconfig(self, SPA_Change, NewMap_as_sys_default, sys_default_map, option, Add_R_Log)"""
                        __command_args = self.__getcmdargs__(x, "IsADDAmpReconfig")
                        __rco_ob_.IsADDAmpReconfig(*__command_args)
                
                    elif self.__test__opt_[x][y] == "IsLogicalChngReconfig":
                        """IsLogicalChngReconfig(self, dict_map_change, dict_map, sys_default_change, sys_default_map, OnlyLogicalRco_Log)"""
                        __command_args = self.__getcmdargs__(x, "IsLogicalChngReconfig")
                        __rco_ob_.IsLogicalChngReconfig(*__command_args)
                
                    elif self.__test__opt_[x][y] == "IsCommonReconfig":
                        """IsCommonReconfig(self, sys_default_change, sys_default_map, CommonRco_Log)"""
                        __command_args = self.__getcmdargs__(x, "IsCommonReconfig")
                        __rco_ob_.IsCommonReconfig(*__command_args)
                
                    elif self.__test__opt_[x][y] == "IsAddCCReconfig":
                        """IsAddCCReconfig(self, SPA_Change, sys_default_change, sys_default_map, AddaCCR_Log)"""
                        __command_args = self.__getcmdargs__(x, "IsAddCCReconfig")
                        __rco_ob_.IsAddCCReconfig(*__command_args)
                
                    self.__count_rco +=1
                # if the current operation is one of the below utility task then call that method with the utility object "__util_ob_" #
                elif self.__test__opt_[x][y] == "CheckTable" or "ScanDisk":
                    if self.__count_util == 0:
                        __util_ob_ = MHM_RCO.TDUtility(__SystemName__,"Utility_Logs") 
                    if self.__test__opt_[x][y] == "CheckTable":
                        """CheckTable(self,table,out_file)"""
                        __command_args = self.__getcmdargs__(x, "CheckTable")
                        __util_ob_.CheckTable(*__command_args)
                
                    elif self.__test__opt_[x][y] == "ScanDisk":
                        """ScanDisk(self,out_file)"""
                        __command_args = self.__getcmdargs__(x, "ScanDisk")
                        __util_ob_.ScanDisk(*__command_args)
                
                    self.__count_util +=1

if __name__ == '__main__':
    
    try:
        __testFile__ = sys.argv[1]
    except IndexError:
        print "Usage: ParseInput.py <Test.json>"
        sys.exit(1)
    
    Test = __parseTest__()
    Test.__getOperation__(__testFile__)
    Test.__execTest__()
