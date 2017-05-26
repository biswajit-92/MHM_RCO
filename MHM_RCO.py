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
from collections import Counter
from random import shuffle
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


    def _check_dbs_state(self):
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
        
    def _is_system_quiescent(self):
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

    def _check_dbs_stateFor_sysinit(self):
        '''
        To check the state of DBS for _sysinit
        '''

        clock = 10
        while True:
            dbsstate = os.popen("/usr/bin/tdsh "+self.__host+" '/usr/pde/bin/pdestate -a'").read()
            if re.search(r"DBS is not running",dbsstate):
                return True
        time.sleep(10)
        clock += 10
        if clock >= 150:
            print "DBS is not in proper state to perform _sysinit, please check"
            return False
        
    def _restart_dbs(self, reason):
        '''
        To restart the database
        '''

        subprocess.call("/usr/bin/tdsh "+self.__host+" '/usr/pde/bin/tpareset -f -y"+" "+ reason +" ' ",shell=True)

    def _remove_GDO(self,reason="GDORemove"):
        '''
        To remove all the GDOs and recreate new ones.
        '''
        if self._check_dbs_state() != 1:
            print "Bringing Database down For GDO Removal"
            subprocess.call("/usr/bin/tdsh "+self.__host+" '/usr/pde/bin/tpareset -x -y"+" "+ reason +" ' ",shell=True)
            if self._check_dbs_state() == 1:
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
        if self._check_dbs_state() == 0:
            return
        else:
            print "\nThe system is not in a proper state to Perform _sysinit\nExiting"
            sys.exit(0)
            
        
    def _sysinit(self,out_file,IsGDORemove="YES"):
        '''
        To perform _sysinit, returns false if fail.
        '''

        if IsGDORemove == "YES" :
            self._remove_GDO()
        subprocess.call("/usr/bin/tdsh "+self.__host+" '/usr/pde/bin/ctl -first \"Start DBS=off; wr ;quit\"'",shell=True)
        self._restart_dbs("_sysinit")
        print "\n***********_sysinit Started***********"       
        time.sleep(10) 
        sys_output = os.popen("/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility _sysinit -output -force -commands \"{yes} {no} {yes} {1} {yes}\" -nostop'").read()
        sys_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,out_file),"w")
        sys_fd.write(sys_output)
        sys_fd.close()
        if re.search(r"_sysinit complete.",sys_output):
            return True
        else:
            return False


    def _dip(self,out_file):
        '''
        To perform _dip.
        '''

        if self._check_dbs_state() == 5:
            print "\n**************_dip Started************"
        _dip_output = os.popen("/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility _dip -output -force -commands \"{dbc} {_dipALL} {y} {_dipACC} {n}\" -nostop'").read()
        _dip_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,out_file),"w")
        _dip_fd.write(_dip_output)
        _dip_fd.close()
        if re.search(r"_dipACC is complete",_dip_output):
            return True
        else: 
            return False
        

    def _checktable(self,table,out_file):
        '''
        To run _checktable utility.
        '''
#        cmd = "/usr/bin/tdsh "+self.__host+'/usr/pde/bin/pdestate -a | grep " Logons are disabled - The system is quiescent"'
#        if subprocess.check_output(cmd, shell=True) == "" :
#            os.system("/usr/bin/tdsh "+self.__node+'echo "disable logons"| /usr/pde/bin/cnscons')
        dbsstate = os.popen("/usr/bin/tdsh "+self.__host+" '/usr/pde/bin/pdestate -a'").read()
        if re.search(r"Logons are enabled - The system is quiescent",dbsstate):
            os.system("/usr/bin/tdsh "+self.__node+'echo "disable logons"| /usr/pde/bin/cnscons')
        print "\n***********_checktable Started*************"
        chcktbl_out = os.popen("/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility _checktable -output -force -commands \"{CHECK "+table+" AT LEVEL THREE IN PARALLEL PRIORITY=H ERROR ONLY;} {QUIT;}\" -nostop'").read()
        chcktbl_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,out_file),"w")
        chcktbl_fd.write(chcktbl_out)
        chcktbl_fd.close()
        if re.search(r"0 table\(s\) failed the check",chcktbl_out):
            return True
        else:
            return False
            
            
    def _scandisk(self,out_file):
        '''
        To run _scandisk utility.
        '''
        print "\n**************_scandisk Started***********"
        _scandisk_out = os.popen("/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility ferret -output -force -prompt \"Ferret  ==>.*\" -commands \"{_scandisk/y} {quit}\" -nostop'").read()
        _scandisk_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,out_file),"w")
        _scandisk_fd.write(_scandisk_out)
        _scandisk_fd.close()
        if re.search(r"vprocs responded with no messages or errors",_scandisk_out):
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
        self.amps_in_system = ''
        self.map_count = ''
        self.online_ampcnt = ''
        self.t_amps_in_globalmap = ''
        self.t_down_amp = ''
        self.t_newR_amp = ''
        self.used_slot = ''
        self.end_onl_amp = ''
        self.max_clustersize_in_system = ''
        self.highest_clusterN = ''
        self.all_dist_cluster = ''
        self.min_cluster_size = 2
        self.max_cluster_size = 8
         
        
        self.__LogDIR__ = os.path.join(outdir,self.__LogDIR__)
        try:
            os.makedirs(self.__LogDIR__)
        except OSError:
            if not os.path.isdir(self.__LogDIR__):
                raise

        
        self.config_parameters()

    def config_parameters(self):
        '''
        This will be used to update the parameters used for creation of maps.
        '''
        self.amps_in_system = os.popen("/usr/bin/tdsh "+self.__host+" '/usr/pde/bin/vconfig -x | egrep vprtype\.*1  | wc -l' ").read()
        self.map_count = os.popen( "/usr/bin/tdsh " +self.__host+ " '/usr/tdbms/bin/dmpgdo mapinfo | grep TD_* | wc -l' " ).read()
#        self.MapInDefinedState = 
        self.online_ampcnt =  os.popen( "/usr/bin/tdsh " +self.__host+ " /usr/tdbms/bin/dmpgdo mapinfo | grep OnlineAmpCnt | head -n 1 | awk -F' ' '{print $3}'  " ).read()
        self.t_amps_in_globalmap = os.popen( "/usr/bin/tdsh " +self.__host+ " /usr/tdbms/bin/dmpgdo mapinfo | grep AmpCnt | head -n 1 | awk -F' ' '{print $3}'  " ).read()
        self.t_down_amp = int(self.t_amps_in_globalmap) - int(self.online_ampcnt)
        self.t_newR_amp = int(self.amps_in_system) - int(self.t_amps_in_globalmap)
        self.used_slot = int(self.map_count)
        self.end_onl_amp = os.popen( "/usr/bin/tdsh " +self.__host+ " /usr/tdbms/bin/dmpgdo dbsconfig 0| grep AMP | grep Online | tail -n 1| awk -F' ' '{print $1}' ").read()
        self.exist_num_cluster = os.popen( "/usr/bin/tdsh " +self.__host+ " /usr/tdbms/bin/dmpgdo dbsconfig 0 | grep AMP | grep Online | awk -F' ' '{print $4}' | sort -nu | wc -l ").read()
        self.max_clustersize_in_system = os.popen( "/usr/bin/tdsh " +self.__host+ " /usr/tdbms/bin/dmpgdo mapinfo | grep MaxClustersize | sort -n | tail -n 1 | awk -F ' ' '{print $3}' ").read()
        self.highest_clusterN = os.popen( "/usr/bin/tdsh " +self.__host+ " /usr/tdbms/bin/dmpgdo dbsconfig 0 | grep AMP | grep Online | awk -F' ' '{print $4}' | sort -nu | tail -n 1 ").read()
        self.all_dist_cluster = os.popen( "/usr/bin/tdsh "+self.__host+ " /usr/tdbms/bin/dmpgdo dbsconfig 0 | grep AMP | grep Online | awk -F' ' '{print $4}' | sort -nu ").read()
        self.all_dist_cluster = filter(None, list(map(str.strip,self.all_dist_cluster)))
        #Debug prints
#        print "\namps_in_system: "+self.amps_in_system
#        print "\nmap_count : "+self.map_count
#        print "\nDownt_amps_in_globalmap: "+str(self.t_down_amp)
#        print "\nonline_ampcnt: "+self.online_ampcnt
#        print "\nt_newR_amp: "+str(self.t_newR_amp)        

    
    def is_cluster_good(self):
        '''
        This function will be called to check whether the system is having good cluster arrangement or not
        @return: The function will return True if the clustering is good.
        '''
        check_cluster = os.popen("/usr/bin/tdsh "+self.__host+" /usr/tdbms/bin/dmpgdo dbsconfig 0 | grep AMP | grep Online |awk -F' ' '{print $4}' ").read()
        cluster_temp = check_cluster.split()
        print cluster_temp
        i = 0
        while i < len(cluster_temp)/2:
            if cluster_temp[i] < cluster_temp[i+1]:
                if (cluster_temp[i] == cluster_temp[i-1] and i!=0):
                    i+=1
                elif int(self.highest_clusterN) ==  (int(self.online_ampcnt) / 2)-1:
                    return False
                    break
            else:
                i+=1

        return True
    
    def _validate_and_modify_amps_and_cluster(self, amps_p_cluster):
        '''
        This method will be used to check if the amps_per_cluster value is valid or not.
        If valid then check the available amps and check is it possible to assign the amps evenly accross
        the clusters or not.
        Mainly called by _mod_amp and _mod_amp_with_shuffle_cluster methods.
        @param amps_per_cluster: how many amps should be there in each cluster. If value not 
        given then by default 2 amps/cluster.
        Allowed amps_per_cluster values are between 2 to 4.
        @return: available_amps: Final list of amps.  
        '''
        self.mod_amp_cmd = ''
        self.amps_per_cluster = amps_p_cluster
        available_amps = list(range(0,int(self.t_amps_in_globalmap)))
        '''For now let's not allow the amps_per_cluster value to be more than 4 and less than 2.'''
        if (self.amps_per_cluster > 4 or self.amps_per_cluster < 2):
            self.amps_per_cluster = random.randint(2,4)
        if self.amps_per_cluster == '':
            self.amps_per_cluster = self.min_cluster_size
            '''If the self.amps_per_cluster value is self.min_cluster_size then check if there can be an odd amp.
            if we get an odd amp then randomly pick a cluster and place the last amp in that cluster'''
            if len(available_amps)%self.min_cluster_size != 0:
                last_amp_in_this_cl = random.randint(0,int(self.exist_num_cluster)-1)
                self.mod_amp_cmd += " {ma %d,cn=%d}" %(int(available_amps[-1]), last_amp_in_this_cl)
                available_amps.remove(available_amps[-1])
        if len(available_amps)%self.amps_per_cluster != 0:
            odd_amps_cnt = len(available_amps)%self.amps_per_cluster
            '''If the odd amps count is less than the available cluster list then arrange those amps 
            in the available clusters'''
            if odd_amps_cnt <= len(self.all_dist_cluster):
                odd_amps_list = random.sample(range(0,int(self.t_amps_in_globalmap)),odd_amps_cnt)
                odd_amps_cluster = random.sample(range(0,len(self.all_dist_cluster)),odd_amps_cnt)
                for index,i in enumerate(odd_amps_list):
                    self.mod_amp_cmd += " {ma %d,cn=%d}" %(i, odd_amps_cluster[index])
                    available_amps.remove(i)     
        return available_amps    

    def _cr_config_modamp_cmd(self, mod_cmd, amp_grp, cluster_list, log):
        '''
        This method will be called to create and execute the commands for mod amp config.
        @param mod_cmd: If you already have some command string then you can pass it through this variable
        so that it can be appended to the final mod amp command in this method.
        @param amp_grp: Final list of amps going to be used in the command formation.
        @param cluster_list: List of clusters going to be used for clustering.
        @param log: Log file for this method.
        '''
        mod_amp_cmd = ''
        mod_amp_cmd += mod_cmd
        for index,ampgrp in enumerate(amp_grp):
            for each_amp in ampgrp:
                mod_amp_cmd += " {ma %d,cn=%d}" %(int(each_amp), int(cluster_list[index]))
        mod_amp_cmd = "{bc}"+mod_amp_cmd+" {ec} {s}"
        mod_amp_out = os.popen("/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility config -output -force -commands \""+mod_amp_cmd+"\"  -nostop'").read()
        mod_amp_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,log),"w")
        mod_amp_fd.write(mod_amp_out)
        mod_amp_fd.close()

        if re.search(".*TD_Map(.*)saved.*" ,mod_amp_out):
            return True
        else:
            return False         
    
    def _mod_amp(self, amps_per_cluster, mod_amp_log):
        '''
        This method will be called to perform mod amp config operation.
        @param amps_per_cluster: The cluster arrangement will be done as per this value.
        @param mod_amp_log: Log file for this method.
        '''
        available_amps = self._validate_and_modify_amps_and_cluster(amps_per_cluster)
        create_amp_grp = [available_amps[x:x+self.amps_per_cluster] for x in range(0,len(available_amps),self.amps_per_cluster)]
        cluster_list = list(range(0,len(create_amp_grp)))
        self._cr_config_modamp_cmd('', create_amp_grp, cluster_list, mod_amp_log)
        
    def _mod_amp_with_shuffle_cluster(self,amps_per_cluster, mod_amp_shuffle_log):
        '''
        This method will be called to make cluster change in a shufflig manner. if in the current system all the 
        cluster is having same no of amps then shuffle will maintain that cluster size.
        @param amps_per_cluster: During shuffling how many amps should be there in each cluster. If value not 
        given then by default 2 amps/cluster.
        Allowed amps_per_cluster values are between 2 to 4.  
        @param mod_amp_shuffle_log: Log file for this method.
        what's an odd amp here?
        A. if we divide the available amps with the available cluster list,the remainder we will get that's the
        odd amp.
        '''
        mod_amp_shuffle_cmd = ''
        available_amps = self._validate_and_modify_amps_and_cluster(amps_per_cluster)
        mod_amp_shuffle_cmd += self.mod_amp_cmd
        #let's shuffle the amps#
        shuffle(available_amps)
        create_amp_grp = [available_amps[x:x+self.amps_per_cluster] for x in range(0,len(available_amps),self.amps_per_cluster)]
        cluster_list = list(range(0,len(create_amp_grp)))
        self._cr_config_modamp_cmd(mod_amp_shuffle_cmd, create_amp_grp, cluster_list, mod_amp_shuffle_log)
          
    def _mod_amp_with_dc_cmd(self, dc_size, mod_log):
        '''
        This function will be called to make fixed cluster size arrangement in the configuration
        @param dc_size: The new cluster size
        '''
        '''
        Let's check whether the cluster size we are getting is valid or not.
        Check what is the most common cluster size of all available cluster and ignore that cluster in this method.
        '''
        possible_cluster_size = list(range(2,9))
        cluster_size = []
        for i in self.all_dist_cluster:
            cluster_size.append(self._get_ampIn_cluster(str(i)))
        count = Counter(cluster_size)
        most_cluster_size = count.most_common(1)[0][0]
        if int(most_cluster_size) == dc_size:
            possible_cluster_size.remove(int(most_cluster_size))
            dc_size = random.choice(possible_cluster_size)
        dc_cmd = "{bc} {dc %d} {ec} {s}" %(int(dc_size))
        dc_out = os.popen("/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility config -output -force -commands \""+dc_cmd+"\"  -nostop'").read()
        dc_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,mod_log),"w")
        dc_fd.write(dc_out)
        dc_fd.close()

        if re.search(".*TD_Map(.*)saved.*" ,dc_out):
            return True
        else:
            return False

    def _mod_amp_with_random_cluster(self, mod_amp_random_log):
        '''
        This method will be called to change the clustering of the current system with a random cluster size.
        @param mod_amp_random_log: Log file of this method.
        '''
        self._mod_amp_with_dc_cmd(random.randint(2,8), mod_amp_random_log)

    def _add_and_cc(self, cluster_size, add_cc_log):
        '''
        This function will be called to perform add amp along with cluster change.
        @param cluster_size: The new cluster size. 
        '''
        add_cc_cmd = ''
        new_amp_list = list(range(int(self.t_amps_in_globalmap),int(self.amps_in_system)))
        add_cc_cmd += "{aa %d-%d} " %(new_amp_list[0], new_amp_list[len(new_amp_list)-1])
        if (cluster_size != int(self.max_clustersize_in_system)):
            add_cc_cmd += "{dc %d} " %(cluster_size)
        else:
            add_cc_cmd += "{dc %d} " %(cluster_size+1)
        add_cc_cmd = "{bc} "+add_cc_cmd+"{ec} {s}"
        add_cc_out = os.popen("/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility config -output -force -commands \""+add_cc_cmd+"\"  -nostop'").read()
        add_cc_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,add_cc_log),"w")
        add_cc_fd.write(add_cc_out)
        add_cc_fd.close()
        
        if re.search(".*TD_Map(.*)saved.*" ,add_cc_out):
            return True
        else:
            return False    
        
    def _del_and_cc(self, percentage, cluster_size, del_cc_log):
        '''
        This function will be called to perform del amp along with clster change.
        @param percentage: The percentage of amps the user wants to delete.
        @param cluster_size:  The new cluster size.
        '''
        del_cc_cmd = ''
        del_amp_count = int(math.floor(percentage/100*int(self.online_ampcnt)))
        amp_from_end = int(self.end_onl_amp) - del_amp_count +1
        del_cc_cmd += "{da %s-%s} " %(amp_from_end,self.end_onl_amp)
        if (cluster_size != int(self.max_clustersize_in_system)):
            del_cc_cmd += "{dc %d} " %(cluster_size)
        else:
            del_cc_cmd += "{dc %d} " %(cluster_size-1)
        del_cc_cmd = "{bc} "+del_cc_cmd+"{ec} {s}"
        del_cc_out = os.popen("/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility config -output -force -commands \""+del_cc_cmd+"\"  -nostop'").read()
        del_cc_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,del_cc_log),"w")
        del_cc_fd.write(del_cc_out)
        del_cc_fd.close()
        
        if re.search(".*TD_Map(.*)saved.*" ,del_cc_out):
            return True
        else:
            return False    
                
        
    def _cr_config_aa_cmd(self, create_amp_grp, avail_cl_list):
        '''
        This method will be called to create a command which will be given to config utility to create a map for add amp operation.
        @param create_amp_grp: List having all the required amps.
        @param avail_cl_list: List of all the required clusters.
        @return: The config command which involves the amp_grp and avail_cl_list.
        '''
        config_cmd = ''
        for i in range(len(create_amp_grp)):
            if create_amp_grp[i][0] == create_amp_grp[i][len(create_amp_grp[i])-1] :
                config_cmd += " {aa %d,cn=%d}" %(create_amp_grp[i][0], int(avail_cl_list[i]))   #only a single amp is available to add in the command
            else:
                config_cmd += " {aa %d-%d,cn=%d}" %(create_amp_grp[i][0], create_amp_grp[i][len(create_amp_grp[i])-1], int(avail_cl_list[i])) #This creates contiguous amp command
                 
        return config_cmd
    
    def _get_ampIn_cluster(self, cluster_no):
        '''
        This method will be used to get the no of online amps in the given cluster.
        @return: The no of amps in the given cluster.
        '''
        return (os.popen( "/usr/bin/tdsh " +self.__host+ " /usr/tdbms/bin/dmpgdo dbsconfig 0 | grep AMP | grep Online | awk -F' ' '{print $4}' | sort -n | grep -w '"+cluster_no+"' | wc -l ").read()).strip()
        
    def _assign_ampTo_cluster(self, new_amps, sorted_cluster):
        '''
        This method will be called when we have to decide which group of amps will go to which cluster.
        We have all the amps which we have to add to the existing clusters, but it is very much necessary to know
        which cluster contains how many amps, so that we can add the groups of amps to correct cluster.
        Fucntion receives the actual copy of the AmpGroup,sorted_cluster

        @param new_amps:  all the NewReady amps.
        @param sorted_cluster: List contains valid cluster numbers for the amps.
        '''
        '''
        if all the NewReady amps can be accomodated to NoOfAvailableCluster*8 then check the ampgroup is proper or not.
        Ex:
        if cluster 0 is having 3 amps, then the ampgroup for cluster 0 must have 5 NewReady amps,
        if not then below if condition will arrange the ampgroups and return it to the calling method.

        '''
        get_cl_len = []
        can_get_amp = []
        amp_group = []
        cluster_copy = sorted_cluster[:]
        #assuming that we have all cluster and all online amp values#
        available_cl = list(range(0,int(self.end_onl_amp)+1))
        for i in cluster_copy:
            get_cl_len.append(int(self._get_ampIn_cluster(i))) #Get the no of amps present in all clusters serially.
        for i in get_cl_len:
            can_get_amp.append(8-int(i))  #This list contains how many amps can be adjusted in cluster_copy.
        for i in can_get_amp:
            amp_group.append(new_amps[:i])
            del new_amps[0:i]

        return amp_group
                      
    def _aa_to_exist_cluster(self, startf_this_amp, end_at_this_amp, add_exi_log):
        '''
        This function will be called to add NewReady amps to the existing clusters.
        @param startf_this_amp: If it's a valid amp no then start adding NewReady amps from this amp.
        @param end_at_this_amp: This is only valid with a startf_this_amp value. Consider NewReady amps between 
        startf_this_amp and end_at_this_amp boundary.
        if end_at_this_amp is empty string then add amps till the last NewReady amp.
        '''
        add_exi_cmd = ''
        if startf_this_amp != '':
            if end_at_this_amp != '':
                new_amp_list = list(range(int(startf_this_amp),int(end_at_this_amp)+1))
            else:
                new_amp_list = list(range(int(startf_this_amp),int(self.amps_in_system)))
        elif startf_this_amp == '':
            new_amp_list = list(range(int(self.t_amps_in_globalmap),int(self.amps_in_system)))
        avail_cl_list = list(self.all_dist_cluster)
        sorted_avail_cl = sorted(avail_cl_list)

        if int(self.exist_num_cluster) <= int(self.t_newR_amp):

            if (int(self.exist_num_cluster)*8-int(self.t_amps_in_globalmap)) >= int(self.t_newR_amp):
                '''
                The number of NewReady amps are less than the max number of amps that can accomodate the available cluster.
                '''
                boundary = int(math.ceil(float(len(new_amp_list))/float(self.exist_num_cluster)))
                create_amp_grp = [new_amp_list[x:x+boundary] for x in range(0,len(new_amp_list),boundary)]
                if (int(self.exist_num_cluster)*8-int(self.t_amps_in_globalmap)) == int(self.t_newR_amp):
                    arranged_amp_grp = self._assign_ampTo_cluster(new_amp_list, sorted_avail_cl)
                    add_exi_cmd += self._cr_config_aa_cmd(arranged_amp_grp, sorted_avail_cl)
                else:
                    add_exi_cmd += self._cr_config_aa_cmd(create_amp_grp, avail_cl_list)
            else:
                '''
                As the number of NewReady amps is much higher than the available cluster list and also every cluster
                can have max 8 amps,so we will add few of the NewReady amps to existing cluster and rest of them to new cluster.
                '''
                add_to_exi_amp = list(range(int(self.EndOnlineAMP)+1,int(self.EndOnlineAMP)+1+int(self.exist_num_cluster)*8-int(self.t_amps_in_globalmap)))
                get_last_amp = add_to_exi_amp[-1]
                arranged_amp_grp = self._assign_ampTo_cluster(add_to_exi_amp, sorted_avail_cl)
                add_exi_cmd += self._cr_config_aa_cmd(arranged_amp_grp, sorted_avail_cl)
                self._aa_to_new_cluster(get_last_amp,'', "Add_Amp_Log")

        else:
            '''
            The number of clusters are more than the number of NewReady amps, so add all the amps to the exisiting cluster.
            '''
            boundary = int(math.ceil(float(self.t_newR_amp)/float(self.min_cluster_size)))
            new_cluster_list = list(range(0,boundary+1))
            new_cluster_list.sort()
            create_amp_grp = [new_amp_list[x:x+boundary] for x in range(0,len(new_amp_list),boundary)]
            add_exi_cmd += self._cr_config_aa_cmd(create_amp_grp, new_cluster_list)

        add_exi_cmd = "{bc}"+add_exi_cmd+" {ec} {s}"
        add_exi_out = os.popen("/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility config -output -force -commands \""+add_exi_cmd+"\"  -nostop'").read()
        add_exi_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,add_exi_log),"w")
        add_exi_fd.write(add_exi_out)
        add_exi_fd.close()

        if re.search(".*TD_Map(.*)saved.*" ,add_exi_out):
            return True
        else:
            return False
        
    def _aa_to_existandnew_cluster(self, add_existandnew_log):
        '''
        This method will be used to add amps to both existing and new cluster.
        @param add_existandnew_log: It stores the logs.
        '''
        new_amp_list = list(range(int(self.t_amps_in_globalmap), int(self.amps_in_system)))
        ''' we want to divide the amps equally for existing and new clusters,if the no of new amps is odd then
        add the last amp to an existing cluster
        ''' 
        if (int(self.t_newR_amp)%self.min_cluster_size) != 0:
            last_new_amp = os.popen( "/usr/bin/tdsh "+self.__host+ " /usr/tdbms/bin/dmpgdo dbsconfig 0 | grep AMP | grep NewReady | tail -n 1| awk -F' ' '{print $1}' ").read()
            new_amp_list.remove(int(last_new_amp))
        add_to_existing_list = list(range(int(self.end_onl_amp)+1,int(self.end_onl_amp)+int(len(new_amp_list)/2)+1))
        add_to_new_list = list(range(int(add_to_existing_list[-1])+1, int(new_amp_list[-1])+1))
        self._aa_to_new_cluster(add_to_new_list[0],'', add_existandnew_log)                             
        self._aa_to_exist_cluster(add_to_existing_list[0], add_to_existing_list[-1], add_existandnew_log)
        
    def _aa_to_new_cluster(self, startf_this_amp, end_at_this_amp, add_new_log):
        '''
        This function will be called to add NewReady amps to New Cluster
        @param startf_this_amp: If it's a valid amp no then start adding NewReady amps from this amp.
        @param end_at_this_amp: This is only valid with a startf_this_amp value. Consider NewReady amps between 
        startf_this_amp and end_at_this_amp boundary.
        if end_at_this_amp is empty string then add amps till the last NewReady amp.
        '''
        add_new_cmd = ''
        if (startf_this_amp != ''):
            if end_at_this_amp != '':
                new_amp_list = list(range(int(startf_this_amp),int(end_at_this_amp)+1))
            else:
                new_amp_list = list(range(startf_this_amp, int(self.amps_in_system)))
        elif startf_this_amp == '':
            new_amp_list = list(range(int(self.t_amps_in_globalmap), int(self.amps_in_system)))

        if (len(new_amp_list)%self.min_cluster_size) != 0:
            boundary = int((self.t_newR_amp-1)/((self.t_newR_amp-1)/self.min_cluster_size))
            new_cluster_list = list(range(int(self.highest_clusterN)+1,int(self.highest_clusterN)+1+int((self.t_newR_amp-1)/self.min_cluster_size)))
            new_cluster_list.sort()
            last_new_amp = os.popen( "/usr/bin/tdsh "+self.__host+ " /usr/tdbms/bin/dmpgdo dbsconfig 0 | grep AMP | grep NewReady | tail -n 1| awk -F' ' '{print $1}' ").read()
            last_amp_in_this_cl = random.randint(0,int(self.exist_num_cluster)-1)
            add_new_cmd += " {aa %d,cn=%d}" %(int(last_new_amp), last_amp_in_this_cl)
            new_amp_list.remove(int(last_new_amp))
            if len(new_amp_list) == 2:
                add_new_cmd += " {aa %d-%d,cn=%d}" %(new_amp_list[0], new_amp_list[1], new_cluster_list[0])
            else:
                create_amp_grp = [new_amp_list[x:x+boundary] for x in range(0,len(new_amp_list),boundary)]
                for i in range(len(create_amp_grp)):
                    if create_amp_grp[i][0] == create_amp_grp[i][len(create_amp_grp[i])-1] :
                        add_new_cmd += " {aa %d,cn=%d}" %(create_amp_grp[i][0], new_cluster_list[i])
                    else:
                        add_new_cmd += " {aa %d-%d,cn=%d}" %(create_amp_grp[i][0], create_amp_grp[i][len(create_amp_grp[i])-1], new_cluster_list[i])

        else:
            no_of_new_cl = int(int(self.t_newR_amp)/self.min_cluster_size)
            boundary = int(int(self.t_newR_amp)/no_of_new_cl)
            new_cluster_list = list(range(int(self.highest_clusterN)+1,int(self.highest_clusterN)+no_of_new_cl+1))
            create_amp_grp = [new_amp_list[x:x+boundary] for x in range(0,len(new_amp_list),boundary)]
            for i in range(len(create_amp_grp)):
                if create_amp_grp[i][0] == create_amp_grp[i][len(create_amp_grp[i])-1] :
                    add_new_cmd += " {aa %d,cn=%d}" %(create_amp_grp[i][0], new_cluster_list[i])
                else:
                    add_new_cmd += " {aa %d-%d,cn=%d}" %(create_amp_grp[i][0], create_amp_grp[i][len(create_amp_grp[i])-1], new_cluster_list[i])

        add_new_cmd = "{bc}"+add_new_cmd+" {ec} {s}"
        add_new_out = os.popen("/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility config -output -force -commands \""+add_new_cmd+"\"  -nostop'").read()
        add_new_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,add_new_log),"w")
        add_new_fd.write(add_new_out)
        add_new_fd.close()

        if re.search(".*TD_Map(.*)saved.*" ,add_new_out):
            return True
        else:
            return False
            
        
    def _del_amp_map(self, percentage, op_type, del_amp_log, is_physical = 'no'):
        '''
        This will be used whenever we want to create a map with Delete amp operation
        
        @param op_type: 
        1: Delete from the higher End(From highest AMP Vproc)
        2: Delete from the lower End(From AMP 0)
        3: Overlap
        
        @param percentage:
        The no of AMP deletion depends on the percentage
        
        @param is_physical: 
        Whether you want to delete it physically or not
        '''
        del_amp_cmd = ''
        del_amp_count = int(math.floor(percentage/100*int(self.online_ampcnt)))
        amp_from_end = int(self.end_onl_amp) - del_amp_count +1
        amp_from_start = del_amp_count-1
        if (op_type == 1 and  (is_physical.lower() == 'no' or is_physical.lower() == 'n' )):
            del_amp_cmd = " {bc} {da %s-%s} {ec} {no} {s} " %(amp_from_end,self.end_onl_amp.strip())

        elif (op_type == 1 and  (is_physical.lower() == 'yes' or is_physical.lower() == 'y' )):
            del_amp_cmd = " {bc} {da %s-%s} {ec} {yes} {s}" %(amp_from_end,self.end_onl_amp.strip())

        elif op_type == 2:
            del_amp_cmd =  " {bc} {da 0-%s} {ec} {s}" %(amp_from_start)

        elif op_type == 3:
            del_amp_cmd = "{bc} {da 0-%s} {da %s-%s} {ec} {s}" %(amp_from_start,amp_from_end,self.end_onl_amp.strip())

        else:
            print "\nWrong config command.Please check the command"

        cmd = "/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility config -output -force -commands \""+del_amp_cmd+"\"  -nostop'"

        del_amp_out = os.popen(cmd).read()
        del_amp_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,del_amp_log),"w")
        del_amp_fd.write(del_amp_out)
        del_amp_fd.close()


        if re.search(".*TD_Map(.*)saved.*" ,del_amp_out):
            return True
        else:
            return False

            
    def _make_amp_down(self, map_name, down_amp_log):
        '''
        To make an AMP Down in the given MAP
        @param map_name: In this map the method will make one amp down.
        '''
        down_amp_cmd = ''
        mapslot_of_map = os.popen( "/usr/bin/tdsh "+self.__host+" /usr/tdbms/bin/dmpgdo mapinfo | grep -A 3 "+map_name+" | grep MapSlot | awk -F' ' '{print $3}' " ).read()
        mapslot_of_map = mapslot_of_map.strip()
        startamp_in_map = os.popen( "/usr/bin/tdsh "+self.__host+" /usr/tdbms/bin/dmpgdo dbsconfig "+mapslot_of_map+" | grep AMP | head -n 1 | awk -F' ' '{print $1}' ").read()
        startamp_in_map = startamp_in_map.strip()
        endamp_in_map = os.popen( "/usr/bin/tdsh " +self.__host+ " /usr/tdbms/bin/dmpgdo dbsconfig "+mapslot_of_map+" | grep AMP | tail -n 1 | awk -F' ' '{print $1}' ").read()
        endamp_in_map = endamp_in_map.strip()
        make_this_down = random.randint(int(startamp_in_map),int(endamp_in_map))
        down_amp_cmd += "{set %d offline}" %(make_this_down)
        down_amp_cmd += " {RESTART COLDWAIT DOWN} {YES}"

        down_amp_out = os.popen("/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility vprocmanager -output -force -commands \""+down_amp_cmd+"\"  -nostop'").read()
        down_amp_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,down_amp_log),"w")
        down_amp_fd.write(down_amp_out)
        down_amp_fd.close()   
        
        """TO DO:    CHECK DBS STATE"""
        if make_this_down == int(os.popen( "/usr/bin/tdsh " +self.__host+ " /usr/tdbms/bin/dmpgdo dbsconfig "+mapslot_of_map+" | grep AMP | grep Down | awk -F' ' '{print $1}' ").read()):
            return True
        else:
            return False
     
    def _alter_table_map(self, source_map, target_map):
        '''
        This method will alter all the tables from a source map to a target map
        @param source_map: List containing all the maps having user tables.
        @param target_map: The destination map to which all the tables will be moved.
        '''
        source_map = self._usertbl_on_maps()
        alter_tbl_cmd = []
        alter_hashindx_cmd = []
        alter_joinindx_cmd = []
        udaexec = teradata.UdaExec(appName='MHM',version=1,logConsole = True)
        session = udaexec.connect(method='odbc',username='dbc',password='dbc',system=self.__host,dbType='Teradata Database ODBC Driver 16.00')

        if isinstance(source_map, str):
            for row in session.execute("SELECT DISTINCT 'ALTER TABLE ' || trim(DataBaseName) || '.' || trim(TableName)||',MAP=' || '"+target_map+"' || ';' (title '') FROM  DBC.TablesV WHERE TableKind LIKE ANY ('T','O') and mapname='"+source_map+"'"):
                for item in row:
                    alter_tbl_cmd.append(item)
            for alter_qry in alter_tbl_cmd:
                session.execute(alter_qry)
            alter_tbl_cmd = []

            for row in session.execute("SELECT DISTINCT 'ALTER HASH INDEX ' || trim(DataBaseName) || '.' || trim(TableName)||',MAP=' || '"+target_map+"' || ';' (title '') FROM  DBC.TablesV WHERE TableKind='N' and mapname='"+source_map+"'"):
                for item in row:
                    alter_hashindx_cmd.append(item)
            for alter_qry in alter_hashindx_cmd:
                session.execute(alter_qry)
            alter_hashindx_cmd = []

            for row in session.execute("SELECT DISTINCT 'ALTER JOIN INDEX ' || trim(DataBaseName) || '.' || trim(TableName)||',MAP=' || '"+target_map+"' || ';' (title '') FROM  DBC.TablesV WHERE TableKind='I' and mapname='"+source_map+"'"):
                for item in row:
                    alter_joinindx_cmd.append(item)
            for alter_qry in alter_joinindx_cmd:
                session.execute(alter_qry)
            alter_joinindx_cmd = []

        elif isinstance(source_map, list):
            for map_name in source_map:
                for row in session.execute("SELECT DISTINCT 'ALTER TABLE ' || trim(DataBaseName) || '.' || trim(TableName)||',MAP=' || '"+target_map+"' || ';' (title '') FROM  DBC.TablesV WHERE TableKind LIKE ANY ('T','O') and mapname='"+map_name+"'"):
                    for item in row:
                        alter_tbl_cmd.append(item)
            for alter_qry in alter_tbl_cmd:
                session.execute(alter_qry)

            for map_name in source_map:
                for row in session.execute("SELECT DISTINCT 'ALTER HASH INDEX ' || trim(DataBaseName) || '.' || trim(TableName)||',MAP=' || '"+target_map+"' || ';' (title '') FROM  DBC.TablesV WHERE TableKind='N' and mapname='"+map_name+"'"):
                    for item in row:
                        alter_hashindx_cmd.append(item)
            for alter_qry in alter_hashindx_cmd:
                session.execute(alter_qry)

            for map_name in source_map:
                for row in session.execute("SELECT DISTINCT 'ALTER JOIN INDEX ' || trim(DataBaseName) || '.' || trim(TableName)||',MAP=' || '"+target_map+"' || ';' (title '') FROM  DBC.TablesV WHERE TableKind='I' and mapname='"+map_name+"'"):
                    for item in row:
                        alter_joinindx_cmd.append(item)
            for alter_qry in alter_joinindx_cmd:
                session.execute(alter_qry)

            
    def _usertbl_on_maps(self):
        '''
        This method will alter all the tables from a source map to a target map.
        @return: a list of maps having user tables on it.
        '''
        all_maps = []
        source_map = []
        row_inall_maps = []
        udaexec = teradata.UdaExec(appName='MHM',version=1)
        session = udaexec.connect(method='odbc',username='dbc',password='dbc',system=self.__host,dbType='Teradata Database ODBC Driver 16.00')

        for row in session.execute("SEL MAPNAME FROM DBC.MAPS WHERE MAPNO >=1025 AND MAPKIND='C' ORDER BY MAPNAME" ):
            all_maps.append(str(row[0]))
            row_inall_maps.append("SEL COUNT (*) FROM DBC.TVM WHERE MAPNO=(SEL MAPNO FROM MAPS WHERE MAPNAME='"+str(row[0])+"')" )
        for index,qry in enumerate(row_inall_maps):
            for i in session.execute(qry):
                if i[0] > 0:
                    if index != len(row_inall_maps):
                        source_map.append(all_maps[index])
                        break
            if index == len(row_inall_maps):
                break
        
        return source_map

               
    def _create_specific_maps(self, map_count, create_specific_log):
        '''
        This method will be used to generate different conditions for _cr_cmdfor_specific_maps method.
        @param map_count: How many maps you want to create.(Logical maps)
        '''
        self.map_count = map_count
        self.amps_in_eachcl = 0
        self.no_of_maps = 0        
        self.create_map_out = ''
        if ((int(self.t_amps_in_globalmap)%int(self.exist_num_cluster) == 0) and int(self._get_ampIn_cluster("0"))%self.min_cluster_size ==0):
            '''
            All amps are distributed evenly across all clusters,all the clsuters are having even number of amps.
            '''
            amps_in_eachcl = self.min_cluster_size
            no_of_cluster = int(self.t_amps_in_globalmap)/amps_in_eachcl
        elif ((int(self.t_amps_in_globalmap)%int(self.exist_num_cluster) == 0) and int(self._get_ampIn_cluster("0"))%self.min_cluster_size !=0):
            '''
            This means all the clusters in the configuration is having 2amps/cluster.
            '''
            amps_in_eachcl = int(self._get_ampIn_cluster("0"))
            no_of_cluster = int(self.t_amps_in_globalmap)/amps_in_eachcl
        
        self._cr_cmdfor_specific_maps(amps_in_eachcl, no_of_cluster)

        cr_specific_map_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,create_specific_log),"w")
        cr_specific_map_fd.write(self.create_map_out)
        cr_specific_map_fd.close()

        if re.search(".*TD_Map(.*)saved.*" ,self.create_map_out):
            return True
        else:
            return False    
                    
    def _cr_cmdfor_specific_maps(self, amps_in_eachcl, no_of_cluster):
        '''
        This method will be used to create all possible types of maps.
        '''
        '''
        Here we will create config command to delete amsp from lower end.
        '''
        create_map_cmd = ''            
        i = 0
        start_amp = 0
        temp_amp = 0
        while i<no_of_cluster:
            end_amp = (temp_amp+amps_in_eachcl)-1
            create_map_cmd += "{bc} {da %d-%d} {ec} " %(start_amp, end_amp)
            temp_amp = end_amp+1
            self.no_of_maps+=1
            if (self.no_of_maps < self.map_count) and (end_amp != int(self.t_amps_in_globalmap)-amps_in_eachcl-1):
                i+=1
            else:
                break
        self._exe_cmdfor_specificmap(create_map_cmd)
        i = None    
        start_amp = None
        temp_amp = None
        end_amp = None
        '''
        Here we will create config command to delete amps from higher end
        '''
        create_map_cmd = ''
        j = no_of_cluster
        end_amp = int(self.end_onl_amp)
        temp_amp = end_amp
        while j>0:
            start_amp = (temp_amp-amps_in_eachcl)+1
            create_map_cmd += "{bc} {da %d-%d} {ec} {no} " %(start_amp, end_amp)
            temp_amp = start_amp-1
            self.no_of_maps+=1
            if (self.no_of_maps < self.map_count) and (start_amp != amps_in_eachcl):
                j-=1
            else:
                break
        self._exe_cmdfor_specificmap(create_map_cmd)            
        j = None    
        start_amp = None
        temp_amp = None
        end_amp = None
        '''
        Here we will create config command to delete amps in overlap manner. From both end it will delete
        same number of amps
        '''
        create_map_cmd = ''
        i = 0
        j = no_of_cluster
        start_amp_top = 0
        temp_var_top = 0
        end_amp_down = int(self.end_onl_amp)
        temp_var_down = end_amp_down
        if (int(no_of_cluster)%2==0):
            while i<no_of_cluster and j>0:
                start_amp_down = (temp_var_down-amps_in_eachcl)+1    #This is the start amp of the delete from high
                end_amp_top = (temp_var_top+amps_in_eachcl)-1      #This is the end amp of the delete from top
                create_map_cmd += "{bc} {da %d-%d} {da %d-%d} {ec} " %(start_amp_top, end_amp_top, start_amp_down, end_amp_down)
                temp_var_down = start_amp_down-1                        #This is the last amp of the next cluster coming from end
                temp_var_top = end_amp_top+1                          #This is the first amp of the next cluster coming from top.
                self.no_of_maps+=1
                if self.no_of_maps < self.map_count and (temp_var_top != temp_var_down-(2*(amps_in_eachcl-1))-1):
                    i+=1
                    j-=1
                else:
                    break
        else:
            while i<no_of_cluster and j>0:
                start_amp_down = (temp_var_down-amps_in_eachcl)+1    #This is the start amp of the delete from high
                end_amp_top = (temp_var_top+amps_in_eachcl)-1      #This is the end amp of the delete from top
                create_map_cmd += "{bc} {da %d-%d} {da %d-%d} {ec} " %(start_amp_top, end_amp_top, start_amp_down, end_amp_down)
                temp_var_down = start_amp_down-1                        #This is the last amp of the next cluster coming from end
                temp_var_top = end_amp_top+1                          #This is the first amp of the next cluster coming from top.
                self.no_of_maps+=1
                if self.no_of_maps < self.map_count and (temp_var_top != temp_var_down-amps_in_eachcl+1):
                    i+=1
                    j-=1
                else:
                    break
        self._exe_cmdfor_specificmap(create_map_cmd)                        
        i = None
        j = None
        start_amp_top = None
        temp_var_top = None
        end_amp_top = None 
        start_amp_down= None 
        end_amp_down = None
        '''
        Here we will create overlap maps with with deleting unequally from both ends.
        '''
        create_map_cmd = ''
        sys_start_amp = 0
        sys_end_amp = int(self.end_onl_amp)
        temp_start_amp = sys_start_amp
        temp_end_amp = sys_end_amp
        i = 0
        j = no_of_cluster
        count = 0
        while j>0:
            if count == 0:
                start_amp = (temp_end_amp-2*amps_in_eachcl)+1
            else:
                start_amp = (temp_end_amp-amps_in_eachcl)+1
            create_map_cmd += "{bc} {da %d-%d} {da %d-%d} {ec} " %(sys_start_amp, (sys_start_amp+amps_in_eachcl)-1, start_amp, sys_end_amp)
            temp_end_amp = start_amp-1
            self.no_of_maps+=1
            if (self.no_of_maps < self.map_count) and (temp_end_amp-amps_in_eachcl != sys_start_amp+amps_in_eachcl-1):
                j-=1
                count+=1
            else:
                break
        self._exe_cmdfor_specificmap(create_map_cmd)                                            
        create_map_cmd = ''
        count = 0
        while i<no_of_cluster:
            if count == 0:
                end_amp = (temp_start_amp+2*amps_in_eachcl)-1
            else:
                end_amp = (temp_start_amp+amps_in_eachcl)-1
            create_map_cmd += "{bc} {da %d-%d} {da %d-%d} {ec} " %(sys_start_amp, end_amp, (sys_end_amp-amps_in_eachcl)+1, sys_end_amp)
            temp_start_amp = end_amp+1
            self.no_of_maps+=1
            if (self.no_of_maps < self.map_count) and (temp_start_amp+amps_in_eachcl != sys_end_amp-amps_in_eachcl+1):
                i+=1
                count+=1
            else:
                break
        self._exe_cmdfor_specificmap(create_map_cmd)                                      

    
    def _exe_cmdfor_specificmap(self, create_map_cmd):
        '''
        This method will be used to pass the command created in _create_specific_maps method.
        '''                                                   
        create_map_cmd = create_map_cmd+"{s}"
        cmd = "/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility config -output -force -commands \""+create_map_cmd+"\"  -nostop'"
        self.create_map_out += os.popen(cmd).read()
            
    def _drop_map(self, auto_select, map_name):
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
        t_amps_in_globalmap  : No of amps in the system
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
            
    def _change_general_flag(self, flags, dbscntrl_Log):
        '''
        This method will be used to change the value of a general flag.
        '''
        dbscontrol_cmd = "/usr/bin/tdsh "+self.__host+" '"+CNSRUN+" -utility dbscontrol -output -force -commands \""
        cmd_list = ""
        for key in flags.keys():
            cmd_list += " {MODIFY GENERAL "+str(key)+"="+str(flags[key])+"}"
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

            
    def _is_addamp_rco(self, spa_change, newmap_as_sysdef, sys_default_map, option, is_addamp_log):
        '''
        This method will be used to run ADD amp reconfig.
        '''
        reconfig_cmd = ''
        if newmap_as_sysdef == "no":
            if spa_change == "yes":
                reconfig_cmd = "{yes} {no} "+sys_default_map+" "+option
            else:
                if self._change_general_flag(self.Flags_G, "SPA_FlagChng_Log") is True:
                    reconfig_cmd = "{no} {no} {no} "+sys_default_map+" "+option
        elif newmap_as_sysdef == "yes":
            if spa_change == "yes":
                reconfig_cmd = "{yes} {yes} "+option
            else:
                if self._change_general_flag(self.Flags_G, "SPA_FlagChng_Log") is True:
                    reconfig_cmd = "{no} {no} {yes} "+option
        reconfig_cmd = "{r} "+reconfig_cmd

        cmd = "/usr/bin/tdsh "+self.__host+" \""+CNSRUN+" -utility reconfig -prompt '>' -output -force -commands '"+reconfig_cmd+"'  -nostop\""
        is_addamp_out = os.popen(cmd).read()
        is_addamp_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,is_addamp_log),"w")
        is_addamp_fd.write(is_addamp_out)
        is_addamp_fd.close()

        if re.search(r"Restarting DBS due to completion of reconfiguration.",is_addamp_out):
            return True
        else:
            return False

            
    def _is_logicalchng_rco(self, dict_map_change, dict_map, sys_default_change, sys_default_map, _is_logicalchng_log):
        '''
        This method will be used to run reconfig to change the Dictionary map or system default map
        '''
        is_logicalchng_cmd = ''
        if dict_map_change == "yes":
            if sys_default_change == "yes":
                is_logicalchng_cmd = "{yes} "+dict_map+" {yes} "+sys_default_map
            else:
                is_logicalchng_cmd = "{yes} "+dict_map+" {no}"
        elif dict_map_change == "no":
            if sys_default_change == "yes":
                is_logicalchng_cmd = "{no} {yes} "+sys_default_map
            else:
                print "\nNo changes to the current configuration selected. Aborting process"
                return False
        is_logicalchng_cmd = "{r} "+is_logicalchng_cmd
        cmd = "/usr/bin/tdsh "+self.__host+" \""+CNSRUN+" -utility reconfig -prompt '>' -output -force -commands '"+is_logicalchng_cmd+"'  -nostop\""
        is_logicalchng_out = os.popen(cmd).read()
        is_logicalchng_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,_is_logicalchng_log),"w")
        is_logicalchng_fd.write(is_logicalchng_out)
        is_logicalchng_fd.close()

        if re.search(r"Restarting DBS due to completion of reconfiguration.",is_logicalchng_out):
            return True
        else:
            return False

    def _is_common_rco(self, sys_default_change, sys_default_map, is_common_rco_log):
        '''
        This method will be used to run Del/Del+Mod/Mod amp reconfigs.
        '''
        is_common_rco_cmd = ''
        if sys_default_change == "yes":
            is_common_rco_cmd = "{yes} "+sys_default_map
        elif sys_default_change == "no":
            is_common_rco_cmd = "{no} "
        is_common_rco_cmd = "{r} "+is_common_rco_cmd
        print is_common_rco_cmd
        cmd = "/usr/bin/tdsh "+self.__host+" \""+CNSRUN+" -utility reconfig -prompt '>' -output -force -commands '"+is_common_rco_cmd+"'  -nostop\""
        is_common_rco_out = os.popen(cmd).read()
        is_common_rco_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,is_common_rco_log),"w")
        is_common_rco_fd.write(is_common_rco_out)
        is_common_rco_fd.close()

        if re.search(r"Restarting DBS due to completion of reconfiguration.",is_common_rco_out):
            return True
        else:
            return False

    def _is_add_cc_rco(self, spa_change, sys_default_change, sys_default_map, is_add_cc_rco_log):
        '''
        This method will be used to run Add+ClusterChange amp reconfig.
        '''
        is_add_cc_rco_cmd = ''
        if sys_default_change == "no":
            if spa_change == "yes":
                is_add_cc_rco_cmd = "{yes} {no} "
            else:
                if self._change_general_flag(self.Flags_G, "SPA_FlagChng_Log") is True:
                    is_add_cc_rco_cmd = "{no} {no} {no} "
        elif sys_default_change == "yes":
            if spa_change == "yes":
                is_add_cc_rco_cmd = "{yes} {yes} "+sys_default_map
            else:
                if self._change_general_flag(self.Flags_G, "SPA_FlagChng_Log") is True:
                    is_add_cc_rco_cmd = "{no} {no} {yes} "+sys_default_map
        is_add_cc_rco_cmd = "{r} "+is_add_cc_rco_cmd
        print "_is_add_cc_rco: "+is_add_cc_rco_cmd
        cmd = "/usr/bin/tdsh "+self.__host+" \""+CNSRUN+" -utility reconfig -prompt '>' -output -force -commands '"+is_add_cc_rco_cmd+"'  -nostop\""
        is_add_cc_rco_out = os.popen(cmd).read()
        is_add_cc_rco_fd = open(os.path.join(os.getcwd(),self.__LogDIR__,is_add_cc_rco_log),"w")
        is_add_cc_rco_fd.write(is_add_cc_rco_out)
        is_add_cc_rco_fd.close()

        if re.search(r"Restarting DBS due to completion of reconfiguration.",is_add_cc_rco_out):
            return True
        else:
            return False
