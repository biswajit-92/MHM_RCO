#!/usr/bin/python

from __future__ import division
import teradata
import os
from datetime import datetime
import re
from Extd_IF_header import *
from subprocess import Popen, PIPE
import shutil
import subprocess
import sys
import time
import math
import random
from ParseInput import *
from ParseInput import outdir

class TDUtility(object):
    '''
    Class to provide usability to defined utilities
    '''
    def __init__(self,system,path):
        '''
        Constructor
        parameters: 
        system : System to connect
        path   : The log Directory
        '''

        self.__host = system
        self.__LogDIR__ = path
        shutil.rmtree(os.path.join(os.getcwd(),self.__LogDIR__), ignore_errors=True)

        try:
            os.makedirs(self.__LogDIR__)
        except OSError:
            if not os.path.isdir(self.__LogDIR__):
                raise


    def CheckDBSstate(self):
        '''
        To check the state of DBS
        '''

        clock = 10
        while True:
            dbsstate = os.popen("/usr/bin/tdsh "+self.__host+" '/usr/pde/bin/pdestate -a'").read()
            if re.search(r"Logons are enabled - The system is quiescent",dbsstate):
                return 5
            elif re.search(r"Logons are disabled - The system is quiescent",dbsstate):
                return 3
            elif re.search(r"DBS is not running",dbsstate):
                return 0
            elif re.search(r"DOWN/HARDSTOP",dbsstate):
                return 1
            else:
                print "\nThe PDE State isn't in the expected states, Kindly check the state"
                return False
            
        clock.sleep(10)
        clock += 10
        if clock >= 150:
            print "DBS is not coming up, please check"
            return False
        
    def IsSystemQuiescent(self):
        '''
        To check whether the system is Quiescent or not.
        '''

        clock = 10
        while True:
            dbsstate = os.popen("/usr/bin/tdsh "+self.__host+" '/usr/pde/bin/pdestate -a'").read()
            if re.search(r"The system is quiescent",dbsstate):
                return True
        clock.sleep(10)
        clock += 10
        if clock >= 150:
            print "DBS is not coming up, please check"
            return False        

    def CheckDBSstateForSYSINIT(self):
        '''
        To check the state of DBS for SYSINIT
        '''

        clock = 10
        while True:
            dbsstate = os.popen("/usr/bin/tdsh "+self.__host+" '/usr/pde/bin/pdestate -a'").read()
            if re.search(r"DBS is not running",dbsstate):
                return True
        time.sleep(10)
        clock += 10
        if clock >= 150:
            print "DBS is not in proper state to perform SYSINIT, please check"
            return False
        
    def restartDBS(self, reason):
        '''
        To restart the database
        '''

        subprocess.call("/usr/bin/tdsh "+self.__host+" '/usr/pde/bin/tpareset -f -y"+" "+ reason +" ' ",shell=True)

    def RemoveGDO(self,reason="GDORemove"):
        '''
        To remove all the GDOs and recreate new ones.
        '''
        if self.CheckDBSstate() != 1:
            print "Bringing Database down For GDO Removal"
            subprocess.call("/usr/bin/tdsh "+self.__host+" '/usr/pde/bin/tpareset -x -y"+" "+ reason +" ' ",shell=True)
            if self.CheckDBSstate() == 1:
                print "\n**************DBS State************** \nPDE state: DOWN/HARDSTOP"
                time.sleep(5)
            
        print "\nRemoving system GDOs.."
        proc = Popen("/usr/bin/tdsh "+self.__host+" '/usr/pde/bin/pcl -s /bin/rm /etc/opt/teradata/tdconfig/*.gdo' ", stdout=PIPE, stderr=PIPE, shell=True)
        output, error = proc.communicate()
        exitcode = proc.returncode
        while True:
            if exitcode == 0:
                print "\nSuccessfully removed the GDOs"
                break
            else:
                print "\nError: Removing GDOs\nErrorCode: ",exitcode
                print error
                sys.exit(0)
        time.sleep(5)
        
        print "\nVCONFIg is running.."
        proc = Popen("/usr/bin/tdsh "+self.__host+" '/usr/pde/bin/pcl -s /usr/pde/bin/vconfig' ", stdout=PIPE, stderr=PIPE, shell=True)
        output, error = proc.communicate()
        exitcode = proc.returncode
        while True:
            if exitcode == 0:
                print "\nSuccessfully written VCONFIG"
                print output
                break
            else:
                print "\nError: VCONFIG write\nErrorCode: ",exitcode
                print error
                sys.exit(0)
        time.sleep(5)
        
        print "\nrun_tdgssconfig is running.."
        proc = Popen("/usr/bin/tdsh "+self.__host+" '/usr/pde/bin/pcl -s /opt/teradata/tdgss/bin/run_tdgssconfig' ", stdout=PIPE, stderr=PIPE, shell=True)
        output, error = proc.communicate()
        exitcode = proc.returncode
        while True:
            if exitcode == 0:
                print "\nSuccessfully ran run_tdgssconfig"
                print output
                break
            else:
                print "\nError: run_tdgssconfig\nErrorCode: ",exitcode
                print error
                sys.exit(0)
        time.sleep(5)
        
        print "\ntvsaInitAll is Running.."
        proc = Popen("/usr/bin/tdsh "+self.__host+" '/usr/pde/bin/pcl -s /usr/pde/bin/tvsaInitAll' ", stdout=PIPE, stderr=PIPE, shell=True)
        output, error = proc.communicate()
        exitcode = proc.returncode
        while True:
            if exitcode == 1:
                print "\nSuccessfully ran tvsaInitAll"
                print output
                break
            else:
                print"\nError: tvsaInitAll\nErrorCode: ",exitcode
                print error
                sys.exit(0)
        time.sleep(5)
        
        print "\ntvsaprofiler is Running.."
        proc = Popen("/usr/bin/tdsh "+self.__host+" '/usr/pde/bin/pcl -s /usr/pde/bin/tvsaprofiler -all' ", stdout=PIPE, stderr=PIPE, shell=True)
        output, error = proc.communicate()
        exitcode = proc.returncode
        while True:
            if exitcode == 0:
                print "\nSuccessfully ran tvsaprofiler"
                print output
                break
            else:
                print "\nError: tvsaprofiler -all\nErrorCode: ",exitcode
                print error
                sys.exit(0)
        time.sleep(5)
        
        print "\ntpa is going start.."
        subprocess.call("/usr/bin/tdsh "+self.__host+" '/usr/pde/bin/pcl -s  /etc/init.d/tpa start' ",shell=True)
        if self.CheckDBSstate() == 0:
            return
        else:
            print "\nThe system is not in a proper state to Perform Sysinit\nExiting"
            sys.exit(0)
            
        
    def Sysinit(self,out_file,IsGDORemove="YES"):
        '''
        To perform sysinit, returns false if fail.
        '''

        if IsGDORemove == "YES" :
            self.RemoveGDO()
        subprocess.call("/usr/bin/tdsh "+self.__host+" '/usr/pde/bin/ctl -first \"Start DBS=off; wr ;quit\"'",shell=True)
        self.restartDBS("sysinit")
        print "\n***********SYSINIT Started***********"       
        time.sleep(10) 
        sys_output = os.popen("/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility sysinit -output -force -commands \"{yes} {no} {yes} {1} {yes}\" -nostop'").read()
        sys_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,out_file),"w")
        sys_fd.write(sys_output)
        sys_fd.close()
        if re.search(r"SYSINIT complete.",sys_output):
            return True
        else:
            return False


    def DIP(self,out_file):
        '''
        To perform DIP.
        '''

        if self.CheckDBSstate() == 5:
            print "\n**************DIP Started************"
        dip_output = os.popen("/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility DIP -output -force -commands \"{dbc} {DIPALL} {y} {DIPACC} {n}\" -nostop'").read()
        dip_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,out_file),"w")
        dip_fd.write(dip_output)
        dip_fd.close()
        if re.search(r"DIPACC is complete",dip_output):
            return True
        else: 
            return False
        

    def CheckTable(self,table,out_file):
        '''
        To run checktable utility.
        '''
#        cmd = "/usr/bin/tdsh "+self.__host+'/usr/pde/bin/pdestate -a | grep " Logons are disabled - The system is quiescent"'
#        if subprocess.check_output(cmd, shell=True) == "" :
#            os.system("/usr/bin/tdsh "+self.__node+'echo "disable logons"| /usr/pde/bin/cnscons')
        dbsstate = os.popen("/usr/bin/tdsh "+self.__host+" '/usr/pde/bin/pdestate -a'").read()
        if re.search(r"Logons are enabled - The system is quiescent",dbsstate):
            os.system("/usr/bin/tdsh "+self.__node+'echo "disable logons"| /usr/pde/bin/cnscons')
        print "\n***********Checktable Started*************"
        chcktbl_out = os.popen("/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility checktable -output -force -commands \"{CHECK "+table+" AT LEVEL THREE IN PARALLEL PRIORITY=H ERROR ONLY;} {QUIT;}\" -nostop'").read()
        chcktbl_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,out_file),"w")
        chcktbl_fd.write(chcktbl_out)
        chcktbl_fd.close()
        if re.search(r"0 table\(s\) failed the check",chcktbl_out):
            return True
        else:
            return False
            
            
    def ScanDisk(self,out_file):
        '''
        To run Scandisk utility.
        '''
        print "\n**************Scandisk Started***********"
        scandisk_out = os.popen("/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility ferret -output -force -prompt \"Ferret  ==>.*\" -commands \"{scandisk/y} {quit}\" -nostop'").read()
        scandisk_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,out_file),"w")
        scandisk_fd.write(scandisk_out)
        scandisk_fd.close()
        if re.search(r"vprocs responded with no messages or errors",scandisk_out):
            return True
        else:
            return False

'''
Global Variables.
'''
SYSTEM_DEFAULT_MAP = 'TD_MAP1'


class CreateMAPInConfig(object):
    '''
    class to create all type of MAPs in config utility
    '''
    
    def __init__(self,system,path):
        '''
        Constructor
        @param :
        system    : Name of the system
        '''
        
        self.__host = system
        self.__LogDIR__ = path
        self.TotalAMPCount = ''
        self.MAPCOUNT = ''
        self.OnlineAMPCount = ''
        self.AMPCount = ''
        self.NoOfDownAMP = ''
        self.NewReadyAMPCount = ''
        self.UsedMapSLotCount = ''
        self.EndOnlineAMP = ''
        self.MaxClusterSize = ''
        self.HighestClusterNo = ''
        self.AllDistinctCluster = ''
        self.MinClusterSize = 2
         
        
        self.__LogDIR__ = os.path.join(outdir,self.__LogDIR__)
        try:
            os.makedirs(self.__LogDIR__)
        except OSError:
            if not os.path.isdir(self.__LogDIR__):
                raise

        
        self.Config_Parameters()

    def Config_Parameters(self):
        '''
        This will be used to update the parameters used for creation of maps.
        '''
        self.TotalAMPCount = os.popen("/usr/bin/tdsh "+self.__host+" '/usr/pde/bin/vconfig -x | egrep vprtype\.*1  | wc -l' ").read()
        self.MAPCOUNT = os.popen( "/usr/bin/tdsh " +self.__host+ " '/usr/tdbms/bin/dmpgdo mapinfo | grep TD_* | wc -l' " ).read()
#        self.MapInDefinedState = 
        self.OnlineAMPCount =  os.popen( "/usr/bin/tdsh " +self.__host+ " /usr/tdbms/bin/dmpgdo mapinfo | grep OnlineAmpCnt | head -n 1 | awk -F' ' '{print $3}'  " ).read()
        self.AMPCount = os.popen( "/usr/bin/tdsh " +self.__host+ " /usr/tdbms/bin/dmpgdo mapinfo | grep AmpCnt | head -n 1 | awk -F' ' '{print $3}'  " ).read()
        self.NoOfDownAMP = int(self.AMPCount) - int(self.OnlineAMPCount)
        self.NewReadyAMPCount = int(self.TotalAMPCount) - int(self.AMPCount)
        self.UsedMapSLotCount = int(self.MAPCOUNT)
        self.EndOnlineAMP = os.popen( "/usr/bin/tdsh " +self.__host+ " /usr/tdbms/bin/dmpgdo dbsconfig 0| grep AMP | grep Online | tail -n 1| awk -F' ' '{print $1}' ").read()
        self.ExistNoOfCluster = os.popen( "/usr/bin/tdsh " +self.__host+ " /usr/tdbms/bin/dmpgdo dbsconfig 0 | grep AMP | grep Online | awk -F' ' '{print $4}' | sort -nu | wc -l ").read()
        self.MaxClusterSize = os.popen( "/usr/bin/tdsh " +self.__host+ " /usr/tdbms/bin/dmpgdo mapinfo | grep MaxClustersize | sort -n | tail -n 1 | awk -F ' ' '{print $3}' ").read()
        self.HighestClusterNo = os.popen( "/usr/bin/tdsh " +self.__host+ " /usr/tdbms/bin/dmpgdo dbsconfig 0 | grep AMP | grep Online | awk -F' ' '{print $4}' | sort -nu | tail -n 1 ").read()
        self.AllDistinctCluster = os.popen( "/usr/bin/tdsh "+self.__host+ " /usr/tdbms/bin/dmpgdo dbsconfig 0 | grep AMP | grep Online | awk -F' ' '{print $4}' | sort -nu ").read()
        self.AllDistinctCluster = filter(None, list(map(str.strip,self.AllDistinctCluster)))
        #Debug prints
#        print "\nTotalAMPCount: "+self.TotalAMPCount
#        print "\nMAPCOUNT : "+self.MAPCOUNT
#        print "\nDownAMPCount: "+str(self.NoOfDownAMP)
#        print "\nOnlineAMPCount: "+self.OnlineAMPCount
#        print "\nNewReadyAMPCount: "+str(self.NewReadyAMPCount)        

    
    def IsClusterAssignmentGood(self):
        '''
        This function will be called to check whether the system is having good cluster arrangement or not
        @return: The function will return True if the clustering is good.
        '''
        CheckCluster = os.popen("/usr/bin/tdsh "+self.__host+" /usr/tdbms/bin/dmpgdo dbsconfig 0 | grep AMP | grep Online |awk -F' ' '{print $4}' ").read()
        cluster_temp = CheckCluster.split()
        print cluster_temp
        i = 0
        while i < len(cluster_temp)/2:
            if cluster_temp[i] < cluster_temp[i+1]:
                if (cluster_temp[i] == cluster_temp[i-1] and i!=0):
                    i+=1
                elif int(self.HighestClusterNo) ==  (int(self.OnlineAMPCount) / 2)-1:
                    return False
                    break
            else:
                i+=1

        return True
    
    
    def ChangeClusterWithFixedSize(self, dc_size, FixedMod_Log):
        '''
        This function will be called to make fixed cluster size arrangement in the configuration
        @param dc_size: The new cluster size
        '''
        DC_Command = " {bc} {dc %d} {ec} {s}" %(dc_size)
        print DC_Command
        
        Config_dc_output = os.popen("/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility config -output -force -commands \""+DC_Command+"\"  -nostop'").read()
        dc_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,FixedMod_Log),"w")
        dc_fd.write(Config_dc_output)
        dc_fd.close()
        
        if re.search(".*TD_Map(.*)saved.*" ,Config_dc_output):
            return True
        else:
            return False
                
        
    def AddWithClusterChange(self, cluster_size, AddWithCC_Log):
        '''
        This function will be called to perform add amp along with cluster change.
        @param cluster_size: The new cluster size. 
        '''
        Add_CC_Command = ''
        NewReadyAMPList = list(range(int(self.AMPCount),int(self.TotalAMPCount)))
        Add_CC_Command += "{aa %d-%d} " %(NewReadyAMPList[0], NewReadyAMPList[len(NewReadyAMPList)-1])
        if (cluster_size != int(self.MaxClusterSize)):
            Add_CC_Command += "{dc %d} " %(cluster_size)
        else:
            Add_CC_Command += "{dc %d} " %(cluster_size+1)
        Add_CC_Command = "{bc} "+Add_CC_Command+"{ec} {s}"
        Add_cc_output = os.popen("/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility config -output -force -commands \""+Add_CC_Command+"\"  -nostop'").read()
        aa_ex_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,AddWithCC_Log),"w")
        aa_ex_fd.write(Add_cc_output)
        aa_ex_fd.close()
        
        if re.search(".*TD_Map(.*)saved.*" ,Add_cc_output):
            return True
        else:
            return False    
        
    def DelWithClusterChange(self, percentage, cluster_size, DelWithCC_Log):
        '''
        This function will be called to perform del amp along with clster change.
        @param percentage: The percentage of amps the user wants to delete.
        @param cluster_size:  The new cluster size.
        '''
        Del_cc_command = ''
        del_amp_count = int(math.floor(percentage/100*int(self.OnlineAMPCount)))
        amp_from_end = int(self.EndOnlineAMP) - del_amp_count +1
        Del_cc_command += "{da %s-%s} " %(amp_from_end,self.EndOnlineAMP)
        if (cluster_size != int(self.MaxClusterSize)):
            Del_cc_command += "{dc %d} " %(cluster_size)
        else:
            Del_cc_command += "{dc %d} " %(cluster_size-1)
        Del_cc_command = "{bc} "+Del_cc_command+"{ec} {s}"
        Del_cc_output = os.popen("/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility config -output -force -commands \""+Del_cc_command+"\"  -nostop'").read()
        aa_ex_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,DelWithCC_Log),"w")
        aa_ex_fd.write(Del_cc_output)
        aa_ex_fd.close()
        
        if re.search(".*TD_Map(.*)saved.*" ,Del_cc_output):
            return True
        else:
            return False    
                
        
    def CreateConfigAddAmpCommand(self, CreateAMPGroup, AvailableClusterList):
        '''
        This method will be called to create a command which will be given to config utility to create a map for add amp operation.
        @param CreateAMPGroup: List having all the required amps.
        @param AvailableClusterList: List of all the required clusters.
        @return: The config command which involves the AmpGroup and AvailableClusterList.
        '''
        Command = ''
        for i in range(len(CreateAMPGroup)):
            if CreateAMPGroup[i][0] == CreateAMPGroup[i][len(CreateAMPGroup[i])-1] :
                Command += " {aa %d,cn=%d}" %(CreateAMPGroup[i][0], int(AvailableClusterList[i]))   #only a single amp is available to add in the command
            else:
                Command += " {aa %d-%d,cn=%d}" %(CreateAMPGroup[i][0], CreateAMPGroup[i][len(CreateAMPGroup[i])-1], int(AvailableClusterList[i])) #This creates contiguous amp command
                 
        return Command
    
    def GetNoOfAmpsInCluster(self, ClusterNo):
        '''
        This method will be used to get the no of online amps in the given cluster.
        @return: The no of amps in the given cluster.
        '''
        return (os.popen( "/usr/bin/tdsh " +self.__host+ " /usr/tdbms/bin/dmpgdo dbsconfig 0 | grep AMP | grep Online | awk -F' ' '{print $4}' | sort -n | grep -w '"+ClusterNo+"' | wc -l ").read()).strip()
        
    def AssignAmpToCluster(self, AmpGroup, ClusterList):
        '''
        This method will be called when we have to decide which group of amps will go to which cluster.
        We have all the amps which we have to add to the existing clusters, but it is very much necessary to know
        which cluster contains how many amps, so that we can add the groups of amps to correct cluster.
        Fucntion receives the actual copy of the AmpGroup,ClusterList
        
        @param AmpGroup:  This is the Group of NewReady amps to be allocated to a specific cluster.(existing/new)
        @param ClusterList: List contains valid cluster numbers for the amps.
        '''
        print AmpGroup,ClusterList
        TempClusterList = ClusterList[:]
        if len(AmpGroup) != len(ClusterList):
            return False
        
        '''
        if all the NewReady amps can be accomodated to NoOfAvailableCluster*8 then check the ampgroup is proper or not.
        Ex:
        if cluster 0 is having 3 amps, then the ampgroup for cluster 0 must have 5 NewReady amps,
        if not then below if condition will arrange the ampgroups and return it to the calling method.
        
        '''
        if (int(self.ExistNoOfCluster)*8-int(self.AMPCount)) == int(self.NewReadyAMPCount):
            for index,i in enumerate(ClusterList):
                if (8-int(self.GetNoOfAmpsInCluster(i))) != len(AmpGroup[index]):
                    k = int(self.GetNoOfAmpsInCluster(i))
                    if (8-k < len(AmpGroup[index])):
                        while(8-k < len(AmpGroup[index])):
                            AmpGroup[index+1].insert(0,AmpGroup[index][8-k])
                            del AmpGroup[index][8-k]
                    else:
                        while(k > len(AmpGroup[index])):
                            AmpGroup[index].append(AmpGroup[index+1][0])
                            del AmpGroup[index+1][0]
                else:
                    continue
            return AmpGroup
        
            '''
            if you have the required no of ampgroups correspondent to no of clusters but the ampgroups for the clusters aren't really
            correct.
            Ex:
            if cluster 0 is having 3 amps and the ampgroup correspondent to it is having 4 amps then, search the required ampgroup
            from the list and assign cluster 0 to it. make the arrangements accordingly and return the cluster list.
            ''' 
        else:
            for i in ClusterList:
                for index,j in enumerate(AmpGroup):
                    if int(self.GetNoOfAmpsInCluster(i)) == 8-len(j):
                        TempClusterList[index]=i
                        break
                    else:
                        continue
            return TempClusterList
                      
    def AddAmpToExistingCluster(self, AddExist_Log):
        '''
        This function will be called to add NewReady amps to the existing clusters
        '''
        AA_Command = ''
        NewReadyAMPList = list(range(int(self.AMPCount),int(self.TotalAMPCount)))
        AvailableClusterList = list(self.AllDistinctCluster)

        if int(self.ExistNoOfCluster) <= int(self.NewReadyAMPCount):

            if (int(self.ExistNoOfCluster)*8-int(self.AMPCount)) >= int(self.NewReadyAMPCount):
                '''
                The number of NewReady amps are less than the max number of amps that can accomodate the available cluster.
                '''
                boundary = int(math.ceil(float(len(NewReadyAMPList))/float(self.ExistNoOfCluster)))
                CreateAMPGroup = [NewReadyAMPList[x:x+boundary] for x in range(0,len(NewReadyAMPList),boundary)]
                if (int(self.ExistNoOfCluster)*8-int(self.AMPCount)) == int(self.NewReadyAMPCount):
                    ArrangedAmpGrp = self.AssignAmpToCluster(CreateAMPGroup,AvailableClusterList)   
                    AA_Command += self.CreateConfigAddAmpCommand(ArrangedAmpGrp, AvailableClusterList)
                else:
                    AA_Command += self.CreateConfigAddAmpCommand(CreateAMPGroup, AvailableClusterList)

            else:
                '''
                As the number of NewReady amps is much higher than the available cluster list and also every cluster
                can have max 8 amps,so we will add few of the NewReady amps to existing cluster and rest of them to new cluster.
                '''
                AddToExistingClAmp = list(range(int(self.EndOnlineAMP)+1,int(self.EndOnlineAMP)+1+int(self.ExistNoOfCluster)*8-int(self.AMPCount)))
                GetTheLastAmp = AddToExistingClAmp[-1]
                boundary = int(math.ceil(float(len(AddToExistingClAmp))/float(self.ExistNoOfCluster)))
                CreateAMPGroup = [AddToExistingClAmp[x:x+boundary] for x in range(0,len(AddToExistingClAmp),boundary)]
#                ArrangedCluster = self.MappingAmpToCluster(sorted(CreateAMPGroup, key=len),AvailableClusterList)
                ArrangedCluster = self.AssignAmpToCluster(CreateAMPGroup,AvailableClusterList)
                AA_Command += self.CreateConfigAddAmpCommand(CreateAMPGroup, ArrangedCluster) #which contains the command for adding NewReady amps to existing cluster.

        else:
            '''
            The number of clusters are more than the number of NewReady amps, so add all the amps to the exisiting cluster.
            '''
            boundary = int(math.ceil(float(self.NewReadyAMPCount)/float(self.MinClusterSize)))
            NewClusterList = list(range(0,boundary+1))
            NewClusterList.sort()
            CreateAMPGroup = [NewReadyAMPList[x:x+boundary] for x in range(0,len(NewReadyAMPList),boundary)]
            AA_Command += self.CreateConfigAddAmpCommand(CreateAMPGroup, NewClusterList)

        AA_Command = "{bc}"+AA_Command+" {ec} {s}"

        Config_aa_ex_output = os.popen("/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility config -output -force -commands \""+AA_Command+"\"  -nostop'").read()
        aa_ex_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,AddExist_Log),"w")
        aa_ex_fd.write(Config_aa_ex_output)
        aa_ex_fd.close()

        if re.search(".*TD_Map(.*)saved.*" ,Config_aa_ex_output):
            return True
            self.AddAmpToNewCluster(GetTheLastAmp, "Add_Amp_Log")
        else:
            return False

        
    def AddAmpToNewCluster(self, StartFromThisAmp, AddNew_Log):
        '''
        This function will be called to add NewReady amps to New Cluster
        @param StartFromThisAmp: If it's a valid amp no then start adding NewReady amps from this amp.
        '''
        AaToNewCommand = ''
        if (StartFromThisAmp != ''):
            NewReadyAMPList = list(range(StartFromThisAmp+1, int(self.TotalAMPCount)))
        else:
            NewReadyAMPList = list(range(int(self.AMPCount), int(self.TotalAMPCount)))

        if (int(self.NewReadyAMPCount)%self.MinClusterSize) != 0:
            boundary = int((self.NewReadyAMPCount-1)/((self.NewReadyAMPCount-1)/self.MinClusterSize))
            NewClusterList = list(range(int(self.HighestClusterNo)+1,int(self.HighestClusterNo)+1+int((self.NewReadyAMPCount-1)/self.MinClusterSize)))
            NewClusterList.sort()
            LastNewReadyAMP = os.popen( "/usr/bin/tdsh "+self.__host+ " /usr/tdbms/bin/dmpgdo dbsconfig 0 | grep AMP | grep NewReady | tail -n 1| awk -F' ' '{print $1}' ").read()
            LastAmpInCluster = random.randint(0,int(self.ExistNoOfCluster)-1)
            AaToNewCommand += " {aa %d,cn=%d}" %(int(LastNewReadyAMP), LastAmpInCluster)
            NewReadyAMPList.remove(int(LastNewReadyAMP))
            if len(NewReadyAMPList) == 2:
                AaToNewCommand += " {aa %d-%d,cn=%d}" %(NewReadyAMPList[0], NewReadyAMPList[1], NewClusterList[0])
            else:
                CreateAMPGroup = [NewReadyAMPList[x:x+boundary] for x in range(0,len(NewReadyAMPList),boundary)]
                for i in range(len(CreateAMPGroup)):
                    if CreateAMPGroup[i][0] == CreateAMPGroup[i][len(CreateAMPGroup[i])-1] :
                        AaToNewCommand += " {aa %d,cn=%d}" %(CreateAMPGroup[i][0], NewClusterList[i])
                    else:
                        AaToNewCommand += " {aa %d-%d,cn=%d}" %(CreateAMPGroup[i][0], CreateAMPGroup[i][len(CreateAMPGroup[i])-1], NewClusterList[i])

        else:
            NoOfNewCluster = int(int(self.NewReadyAMPCount)/self.MinClusterSize)
            boundary = int(int(self.NewReadyAMPCount)/NoOfNewCluster)
            NewClusterList = list(range(int(self.HighestClusterNo)+1,int(self.HighestClusterNo)+NoOfNewCluster+1))
            CreateAMPGroup = [NewReadyAMPList[x:x+boundary] for x in range(0,len(NewReadyAMPList),boundary)]
            for i in range(len(CreateAMPGroup)):
                if CreateAMPGroup[i][0] == CreateAMPGroup[i][len(CreateAMPGroup[i])-1] :
                    AaToNewCommand += " {aa %d,cn=%d}" %(CreateAMPGroup[i][0], NewClusterList[i])
                else:
                    AaToNewCommand += " {aa %d-%d,cn=%d}" %(CreateAMPGroup[i][0], CreateAMPGroup[i][len(CreateAMPGroup[i])-1], NewClusterList[i])

        AaToNewCommand = "{bc}"+AaToNewCommand+" {ec} {s}"
        Config_aa_new_output = os.popen("/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility config -output -force -commands \""+AaToNewCommand+"\"  -nostop'").read()
        aa_new_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,AddNew_Log),"w")
        aa_new_fd.write(Config_aa_new_output)
        aa_new_fd.close()

        if re.search(".*TD_Map(.*)saved.*" ,Config_aa_new_output):
            return True
        else:
            return False
            
        
    def DelampMAP(self, PercentageMode, Type, Config_del_Log, IsPhysical = 'no'):
        '''
        This will be used whenever we want to create a map with Delete amp operation
        
        @param Type: 
        1: Delete from the higher End(From highest AMP Vproc)
        2: Delete from the lower End(From AMP 0)
        3: Overlap
        
        @param PercentageMode:
        The no of AMP deletion depends on the percentage
        
        @param IsPhysical: 
        Whether you want to delete it physically or not
        '''
        DA_Command = ''
        del_amp_count = int(math.floor(PercentageMode/100*int(self.OnlineAMPCount)))
        amp_from_end = int(self.EndOnlineAMP) - del_amp_count +1
        amp_from_start = del_amp_count-1
        if (Type == 1 and  (IsPhysical.lower() == 'no' or IsPhysical.lower() == 'n' )):
            DA_Command = " {bc} {da %s-%s} {ec} {no} {s} " %(amp_from_end,self.EndOnlineAMP.strip())

        elif (Type == 1 and  (IsPhysical.lower() == 'yes' or IsPhysical.lower() == 'y' )):
            DA_Command = " {bc} {da %s-%s} {ec} {yes} {s}" %(amp_from_end,self.EndOnlineAMP.strip())

        elif Type == 2:
            DA_Command =  " {bc} {da 0-%s} {ec} {s}" %(amp_from_start)

        elif Type == 3:
            DA_Command = "{bc} {da 0-%s} {da %s-%s} {ec} {s}" %(amp_from_start,amp_from_end,self.EndOnlineAMP.strip())

        else:
            print "\nWrong config command.Please check the command"
#        print DA_Command

#        Config_del_output = os.popen("/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility config -output -force -commands \""+DA_Command+"\"  -nostop'").read()
        cmd = "/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility config -output -force -commands \""+DA_Command+"\"  -nostop'"

        Config_del_output = os.popen(cmd).read()
        del_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,Config_del_Log),"w")
        del_fd.write(Config_del_output)
        del_fd.close()

        self.Config_Parameters()

        if re.search(".*TD_Map(.*)saved.*" ,Config_del_output):
            return True
        else:
            return False

            
    def MakeAMPDown(self, MapName, DownAMP_Log):
        '''
        To make an AMP Down in the given MAP
        @param MapName: In this map the method will make one amp down.
        '''
        DownAMPCmd = ''
        print DownAMPCmd
        MapSlotOfMap = os.popen( "/usr/bin/tdsh "+self.__host+" /usr/tdbms/bin/dmpgdo mapinfo | grep -A 3 "+MapName+" | grep MapSlot | awk -F' ' '{print $3}' " ).read()
        MapSlotOfMap = MapSlotOfMap.strip()
        StartAMPInMap = os.popen( "/usr/bin/tdsh "+self.__host+" /usr/tdbms/bin/dmpgdo dbsconfig "+MapSlotOfMap+" | grep AMP | head -n 1 | awk -F' ' '{print $1}' ").read()
        StartAMPInMap = StartAMPInMap.strip()
        EndAMPInMap = os.popen( "/usr/bin/tdsh " +self.__host+ " /usr/tdbms/bin/dmpgdo dbsconfig "+MapSlotOfMap+" | grep AMP | tail -n 1 | awk -F' ' '{print $1}' ").read()
        EndAMPInMap = EndAMPInMap.strip()
        MakeThisDown = random.randint(int(StartAMPInMap),int(EndAMPInMap))
        DownAMPCmd += "{set %d offline}" %(MakeThisDown)
        DownAMPCmd += " {RESTART COLDWAIT DOWN} {YES}"

        DownAMP_Output = os.popen("/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility vprocmanager -output -force -commands \""+DownAMPCmd+"\"  -nostop'").read()
        downamp_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,DownAMP_Log),"w")
        downamp_fd.write(DownAMP_Output)
        downamp_fd.close()   
        
        """TO DO:    CHECK DBS STATE"""
        if MakeThisDown == int(os.popen( "/usr/bin/tdsh " +self.__host+ " /usr/tdbms/bin/dmpgdo dbsconfig "+MapSlotOfMap+" | grep AMP | grep Down | awk -F' ' '{print $1}' ").read()):
            return True
        else:
            return False
     
    def AlterTableMap(self, source_map, target_map):
        '''
        This method will alter all the tables from a source map to a target map
        @param source_map: List containing all the maps having user tables.
        @param target_map: The destination map to which all the tables will be moved.
        '''
        AlterTableCmd = []
        udaexec = teradata.UdaExec(appName='MHM',version=1,logConsole = True)
        session = udaexec.connect(method='odbc',username='dbc',password='dbc',system=self.__host,dbType='Teradata Database ODBC Driver 16.00')

        for map_v in source_map:
            for row in session.execute("SELECT A.DATABASENAMEI,B.TVMNAMEI FROM DBC.DBASE A INNER JOIN DBC.TVM B ON A.DATABASEID=B.DATABASEID WHERE MAPNO=(SELECT MAPNO FROM DBC.MAPS WHERE MAPNAME='"+map_v+"')" ):
                AlterTableCmd.append("alter table "+'.'.join(str(item) for item in row)+", map="+target_map)
        for AlterQuery in AlterTableCmd:
            session.execute(AlterQuery)

            
    def UserTblOnSourceMap(self):
        '''
        This method will alter all the tables from a source map to a target map.
        @return: a list of maps having user tables on it.
        '''
        AllMaps = []
        SourceMap = []
        RowInAllMaps = []
        udaexec = teradata.UdaExec(appName='MHM',version=1)
        session = udaexec.connect(method='odbc',username='dbc',password='dbc',system=self.__host,dbType='Teradata Database ODBC Driver 16.00')

        for row in session.execute("SEL MAPNAME FROM DBC.MAPS WHERE MAPNO >=1025 AND MAPKIND='C' ORDER BY MAPNAME" ):
            AllMaps.append(str(row[0]))
            RowInAllMaps.append("SEL COUNT (*) FROM DBC.TVM WHERE MAPNO=(SEL MAPNO FROM MAPS WHERE MAPNAME='"+str(row[0])+"')" )
        for index,qry in enumerate(RowInAllMaps):
            for i in session.execute(qry):
                if i[0] > 0:
                    if index != len(RowInAllMaps):
                        SourceMap.append(AllMaps[index])
                        break
            if index == len(RowInAllMaps):
                break
        
        return SourceMap
    
#    def CreatePossibleMaps(self, cluster_size):
#        '''
#        This method will be used to create all possible type of maps in the system.
#        '''
#        self.ChangeClusterWithFixedSize(cluster_size, "cc_log")
#        Reconfig.IsCommonReconfig("no", '', "rco_cc_Log)
               
    def CreateSpecificNoOfMaps(self, mapcount, CreateMap_Log):
        '''
        This method will be used to generate different conditions for CreateCommandForSpecificMaps method.
        @param mapcount: How many maps you want to create.(Logical maps)
        '''
        self.mapcount = mapcount
        self.NoOfMaps = 0
        self.CreateMap_out = ''
        if ((int(self.AMPCount)%int(self.ExistNoOfCluster) == 0) and int(self.GetNoOfAmpsInCluster("0"))%self.MinClusterSize ==0):
            '''
            All amps are distributed evenly across all clusters,all the clsuters are having even number of amps.
            '''
            AmpsInEachCluster = self.MinClusterSize
            NoOfCluster = int(self.AMPCount)/AmpsInEachCluster
        elif ((int(self.AMPCount)%int(self.ExistNoOfCluster) == 0) and int(self.GetNoOfAmpsInCluster("0"))%self.MinClusterSize !=0):
            '''
            This means all the clusters in the configuration is having 2amps/cluster.
            '''
            AmpsInEachCluster = int(self.GetNoOfAmpsInCluster("0"))
            NoOfCluster = int(self.AMPCount)/AmpsInEachCluster
        
        self.CreateCommandForSpecificMaps(AmpsInEachCluster, NoOfCluster)

        del_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,CreateMap_Log),"w")
        del_fd.write(self.CreateMap_out)
        del_fd.close()

        if re.search(".*TD_Map(.*)saved.*" ,self.CreateMap_out):
            return True
        else:
            return False    
                    
    def CreateCommandForSpecificMaps(self, AmpsInEachCluster, NoOfCluster):
        '''
        This method will be used to create all possible types of maps.
        '''
        '''
        Here we will create config command to delete amsp from lower end.
        '''
        CreateMapCmd = ''            
        i = 0
        StartAmp = 0
        TempAmp = 0
        while i<NoOfCluster:
            EndAmp = (TempAmp+AmpsInEachCluster)-1
            CreateMapCmd += "{bc} {da %d-%d} {ec} " %(StartAmp, EndAmp)
            TempAmp = EndAmp+1
            self.NoOfMaps+=1
            if (self.NoOfMaps < self.mapcount) and (EndAmp != int(self.AMPCount)-AmpsInEachCluster-1):
                i+=1
            else:
                break
        self.ExecuteCommandForSpecificMaps(CreateMapCmd)
        i = None    
        StartAmp = None
        TempAmp = None
        EndAmp = None
        '''
        Here we will create config command to delete amps from higher end
        '''
        CreateMapCmd = ''
        j = NoOfCluster
        EndAmp = int(self.EndOnlineAMP)
        TempAmp = EndAmp
        while j>0:
            StartAmp = (TempAmp-AmpsInEachCluster)+1
            CreateMapCmd += "{bc} {da %d-%d} {ec} {no} " %(StartAmp, EndAmp)
            TempAmp = StartAmp-1
            self.NoOfMaps+=1
            if (self.NoOfMaps < self.mapcount) and (StartAmp != AmpsInEachCluster):
                j-=1
            else:
                break
        self.ExecuteCommandForSpecificMaps(CreateMapCmd)            
        j = None    
        StartAmp = None
        TempAmp = None
        EndAmp = None
        '''
        Here we will create config command to delete amps in overlap manner. From both end it will delete
        same number of amps
        '''
        CreateMapCmd = ''
        i = 0
        j = NoOfCluster
        StartAmpTop = 0
        TempVarTop = 0
        EndAmpDwn = int(self.EndOnlineAMP)
        TempVarDwn = EndAmpDwn
        if (int(NoOfCluster)%2==0):
            while i<NoOfCluster and j>0:
                StartAmpDwn = (TempVarDwn-AmpsInEachCluster)+1    #This is the start amp of the delete from high
                EndAmpTop = (TempVarTop+AmpsInEachCluster)-1      #This is the end amp of the delete from top
                CreateMapCmd += "{bc} {da %d-%d} {da %d-%d} {ec} " %(StartAmpTop, EndAmpTop, StartAmpDwn, EndAmpDwn)
                TempVarDwn = StartAmpDwn-1                        #This is the last amp of the next cluster coming from end
                TempVarTop = EndAmpTop+1                          #This is the first amp of the next cluster coming from top.
                self.NoOfMaps+=1
                if self.NoOfMaps < self.mapcount and (TempVarTop != TempVarDwn-(2*(AmpsInEachCluster-1))-1):
                    i+=1
                    j-=1
                else:
                    break
        else:
            while i<NoOfCluster and j>0:
                StartAmpDwn = (TempVarDwn-AmpsInEachCluster)+1    #This is the start amp of the delete from high
                EndAmpTop = (TempVarTop+AmpsInEachCluster)-1      #This is the end amp of the delete from top
                CreateMapCmd += "{bc} {da %d-%d} {da %d-%d} {ec} " %(StartAmpTop, EndAmpTop, StartAmpDwn, EndAmpDwn)
                TempVarDwn = StartAmpDwn-1                        #This is the last amp of the next cluster coming from end
                TempVarTop = EndAmpTop+1                          #This is the first amp of the next cluster coming from top.
                self.NoOfMaps+=1
                if self.NoOfMaps < self.mapcount and (TempVarTop != TempVarDwn-AmpsInEachCluster+1):
                    i+=1
                    j-=1
                else:
                    break
        self.ExecuteCommandForSpecificMaps(CreateMapCmd)                        
        i = None
        j = None
        StartAmpTop = None
        TempVarTop = None
        EndAmpTop = None 
        StartAmpDwn= None 
        EndAmpDwn = None
        '''
        Here we will create overlap maps with with deleting unequally from both ends.
        '''
        CreateMapCmd = ''
        SysStartAmp = 0
        SysEndAmp = int(self.EndOnlineAMP)
        TempStartAmp = SysStartAmp
        TempEndAMp = SysEndAmp
        i = 0
        j = NoOfCluster
        count = 0
        while j>0:
            if count == 0:
                StartAmp = (TempEndAMp-2*AmpsInEachCluster)+1
            else:
                StartAmp = (TempEndAMp-AmpsInEachCluster)+1
            CreateMapCmd += "{bc} {da %d-%d} {da %d-%d} {ec} " %(SysStartAmp, (SysStartAmp+AmpsInEachCluster)-1, StartAmp, SysEndAmp)
            TempEndAMp = StartAmp-1
            self.NoOfMaps+=1
            if (self.NoOfMaps < self.mapcount) and (TempEndAMp-AmpsInEachCluster != SysStartAmp+AmpsInEachCluster-1):
                j-=1
                count+=1
            else:
                break
        self.ExecuteCommandForSpecificMaps(CreateMapCmd)                                            
        CreateMapCmd = ''
        count = 0
        while i<NoOfCluster:
            if count == 0:
                EndAmp = (TempStartAmp+2*AmpsInEachCluster)-1
            else:
                EndAmp = (TempStartAmp+AmpsInEachCluster)-1
            CreateMapCmd += "{bc} {da %d-%d} {da %d-%d} {ec} " %(SysStartAmp, EndAmp, (SysEndAmp-AmpsInEachCluster)+1, SysEndAmp)
            TempStartAmp = EndAmp+1
            self.NoOfMaps+=1
            if (self.NoOfMaps < self.mapcount) and (TempStartAmp+AmpsInEachCluster != SysEndAmp-AmpsInEachCluster+1):
                i+=1
                count+=1
            else:
                break
        self.ExecuteCommandForSpecificMaps(CreateMapCmd)                            

    
    def ExecuteCommandForSpecificMaps(self, CreateMapCmd):
        '''
        This method will be used to pass the command created in CreateSpecificNoOfMaps method.
        '''                                                   
        CreateMapCmd = CreateMapCmd+"{s}"
        cmd = "/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility config -output -force -commands \""+CreateMapCmd+"\"  -nostop'"
        self.CreateMap_out += os.popen(cmd).read()
            
    def DropMap(self, auto_select, map_name):
        '''
        This method will be used to drop a map
        @param map_name: the list containing all the map names to be deleted
        @param auto_select: if yes, it will select the maps which doesn't have any tables and will drop that map. Else user can
        can pass the map name which they want to delete.
        '''
        drop_cmd = []
        drop_map_list = []
        udaexec = teradata.UdaExec(appName='MHM',version=1)
        session = udaexec.connect(method='odbc',username='dbc',password='dbc',system=self.__host,dbType='Teradata Database ODBC Driver 16.00')
        if auto_select == "yes":
            for row in session.execute("SELECT MapName FROM DBC.MAPS WHERE MapNo NOT IN (SELECT MapNo FROM DBC.MAPS WHERE MapNo IN (SELECT DISTINCT MapNo FROM DBC.TVM)) AND MapSlot>1 AND SystemDefault='N' AND MAPKIND='C' ORDER BY MAPNAME"):
                drop_map_list.append(str(row[0]))
            for m in drop_map_list:
                drop_cmd.append("DROP MAP "+m)
            for drop_m in drop_cmd:
                session.execute(drop_m)
        else:
            if (isinstance(map_name, str)):
                drop_cmd = ("DROP MAP "+map_name)
                session.execute(drop_cmd)    


class Reconfig(object):
    '''
    class to run all type of reconfigs.
    '''
    
    def __init__(self, system, path):
        '''
        Constructor
        parameters:
        system    : Name of the system
        ampcount  : No of amps in the system
        path      : The Log Directory
        '''
        
        self.__host = system
        self.__LogDIR__ = path
        shutil.rmtree(os.path.join(os.getcwd(),self.__LogDIR__), ignore_errors=True)
    
        try:
            os.makedirs(self.__LogDIR__)
        except OSError:
            if not os.path.isdir(self.__LogDIR__):
                raise
        
        self.Flags_G = {105: 100};
            
    def ChangeGenralFlag(self, Flags, dbscntrl_Log):
        '''
        This method will be used to change the value of a general flag.
        '''
        dbscontrol_cmd = "/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility dbscontrol -output -force -commands \""
        cmd_list = ""
        for key in Flags.keys():
            cmd_list += " {MODIFY GENERAL "+str(key)+"="+str(Flags[key])+"}"
            cmd_list += " {write}"
        dbscontrol_cmd += cmd_list+" {quit} \" -nostop'"
        dbscontrol_Output = os.popen(dbscontrol_cmd).read()
        dbscontrol_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,dbscntrl_Log),"w")
        dbscontrol_fd.write(dbscontrol_Output)
        dbscontrol_fd.close()

        if re.search("Fatal error", dbscontrol_Output):
            print "Not able to set General flag \n\n"+dbscontrol_Output+" \n\n Check "+dbscntrl_Log
            return False
        return True

            
    def IsADDAmpReconfig(self, SPA_Change, NewMap_as_sys_default, sys_default_map, option, Add_R_Log):
        '''
        This method will be used to run ADD amp reconfig.
        '''
        reconfig_cmd = ''
        if NewMap_as_sys_default == "no":
            if SPA_Change == "yes":
                reconfig_cmd = "{yes} {no} "+sys_default_map+" "+option
            else:
                if self.ChangeGenralFlag(self.Flags_G, "SPA_FlagChng_Log") is True:
                    reconfig_cmd = "{no} {no} {no} "+sys_default_map+" "+option
        elif NewMap_as_sys_default == "yes":
            if SPA_Change == "yes":
                reconfig_cmd = "{yes} {yes} "+option
            else:
                if self.ChangeGenralFlag(self.Flags_G, "SPA_FlagChng_Log") is True:
                    reconfig_cmd = "{no} {no} {yes} "+option
        reconfig_cmd = "{r} "+reconfig_cmd

#        print "IsADDAmpReconfig: "+reconfig_cmd
        cmd = "/usr/bin/tdsh "+self.__host+" \""+CNSRUN+" -utility reconfig -prompt '>' -output -force -commands '"+reconfig_cmd+"'  -nostop\""
        reconfig_out = os.popen(cmd).read()
        aa_rco_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,Add_R_Log),"w")
        aa_rco_fd.write(reconfig_out)
        aa_rco_fd.close()

        if re.search(r"Restarting DBS due to completion of reconfiguration.",reconfig_out):
            return True
        else:
            return False

            
    def IsLogicalChngReconfig(self, dict_map_change, dict_map, sys_default_change, sys_default_map, OnlyLogicalRco_Log):
        '''
        This method will be used to run reconfig to change the Dictionary map or system default map
        '''
        reconfig_cmd = ''
        if dict_map_change == "yes":
            if sys_default_change == "yes":
                reconfig_cmd = "{yes} "+dict_map+" {yes} "+sys_default_map
            else:
                reconfig_cmd = "{yes} "+dict_map+" {no}"
        elif dict_map_change == "no":
            if sys_default_change == "yes":
                reconfig_cmd = "{no} {yes} "+sys_default_map
            else:
                print "\nNo changes to the current configuration selected. Aborting process"
                return False
        reconfig_cmd = "{r} "+reconfig_cmd
        cmd = "/usr/bin/tdsh "+self.__host+" \""+CNSRUN+" -utility reconfig -prompt '>' -output -force -commands '"+reconfig_cmd+"'  -nostop\""
        reconfig_out = os.popen(cmd).read()
        logical_rco_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,OnlyLogicalRco_Log),"w")
        logical_rco_fd.write(reconfig_out)
        logical_rco_fd.close()

        if re.search(r"Restarting DBS due to completion of reconfiguration.",reconfig_out):
            return True
        else:
            return False


    def IsCommonReconfig(self, sys_default_change, sys_default_map, CommonRco_Log):
        '''
        This method will be used to run Del/Del+Mod/Mod amp reconfigs.
        '''
        reconfig_cmd = ''
        if sys_default_change == "yes":
            reconfig_cmd = "{yes} "+sys_default_map
        elif sys_default_change == "no":
            reconfig_cmd = "{no} "
        reconfig_cmd = "{r} "+reconfig_cmd
        print reconfig_cmd
        cmd = "/usr/bin/tdsh "+self.__host+" \""+CNSRUN+" -utility reconfig -prompt '>' -output -force -commands '"+reconfig_cmd+"'  -nostop\""
        reconfig_out = os.popen(cmd).read()
        common_rco_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,CommonRco_Log),"w")
        common_rco_fd.write(reconfig_out)
        common_rco_fd.close()

        if re.search(r"Restarting DBS due to completion of reconfiguration.",reconfig_out):
            return True
        else:
            return False

    def IsAddCCReconfig(self, SPA_Change, sys_default_change, sys_default_map, AddaCCR_Log):
        '''
        This method will be used to run Add+ClusterChange amp reconfig.
        '''
        reconfig_cmd = ''
        if sys_default_change == "no":
            if SPA_Change == "yes":
                reconfig_cmd = "{yes} {no} "
            else:
                if self.ChangeGenralFlag(self.Flags_G, "SPA_FlagChng_Log") is True:
                    reconfig_cmd = "{no} {no} {no} "
        elif sys_default_change == "yes":
            if SPA_Change == "yes":
                reconfig_cmd = "{yes} {yes} "+sys_default_map
            else:
                if self.ChangeGenralFlag(self.Flags_G, "SPA_FlagChng_Log") is True:
                    reconfig_cmd = "{no} {no} {yes} "+sys_default_map
        reconfig_cmd = "{r} "+reconfig_cmd
        print "IsAddCCReconfig: "+reconfig_cmd
        cmd = "/usr/bin/tdsh "+self.__host+" \""+CNSRUN+" -utility reconfig -prompt '>' -output -force -commands '"+reconfig_cmd+"'  -nostop\""
        reconfig_out = os.popen(cmd).read()
        aaNcc_rco_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,AddaCCR_Log),"w")
        aaNcc_rco_fd.write(reconfig_out)
        aaNcc_rco_fd.close()

        if re.search(r"Restarting DBS due to completion of reconfiguration.",reconfig_out):
            return True
        else:
            return False

    
