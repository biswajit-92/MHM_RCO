#!/usr/bin/python


'''
Define Dfault Location of teradata utility store here
'''
TDSH = "/usr/bin/tdsh"
DMPGDO = "/usr/tdbms/bin/dmpgdo"
PDEPATH = "/usr/pde/bin/pdepath -i"
PDESTATE="/usr/pde/bin/pdestate -a"
CNSRUN = "/usr/pde/bin/cnsrun"
VPROCMGR = "/usr/tdbms/bin/vprocmanager"
DBSCONTROL = "/usr/tdbms/bin"
FERRET = "/usr/tdbms/bin"
TPARESET = "/usr/pde/bin/tpareset"

'''
Define SQL query to know whether table is too large to be move on a map
'''
TBL_TOO_LARGE_FOR_MAP = """SELECT DatabaseName, TableName, AVG(CurrentPerm) 
FROM DBC.AllSpace 
WHERE (DatabaseName, TableName) IN 
  (SELECT DatabaseName, TableName FROM TablesV 
WHERE MapName IN (SELECT MapName FROM MapsV WHERE MapKind='S')) 
HAVING AVG(CurrentPerm) > 1024*1000 
GROUP BY 1,2 
ORDER BY 3 
"""

ANALYSE_TBLMOVE = "CALL TDMaps.AnalyzeSP('TD_Map',CURRENT_DATE - INTERVAL  '7' day, 'MyMoveTableList');"
ABORT_USER_SESSIONS = "select AbortSessions(HostId, UserName, SessionNo, 'Y', 'Y') from table (MonitorSession(-1, '*', 0)) AS ms where username like  'mhm_user%' ;);"

RANDOM_CONT = 1
RANDOM_SPARSE = 2

#Define Minumum and maximum calumn for creating a table
MIN_COL = 4
MAX_COL = 2048

# Define Max row size to be allowed  
MAX_ROWST = 1000 

'''
Position of coulmn for different kind of indexes supported
'''
UPI_INDEX = '1'
NUPI_INDEX = '3'
USI_INDEX = '2'
NUSI_INDEX = '4'
PPI_INDEX = UPI_INDEX
MLPPI_INDEX = UPI_INDEX
PA_INDEX = '1'

MIN_JI = 2
MIN_HI = 2
MIN_RI = 1
MAX_JI = 100
MAX_HI = 100
MAX_RI = 100

# Define temporal columy type and default value
temporal_column = ['PERIOD(DATE)',"period(date '2015-04-30', date '2016-04-30')"]

#Define character type and its default value 
char_column = [ 
                 ['CHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC',"'abcdefghij'"],
                 ['VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC',"'abcdefgh'"]
              ]
# Define LOB type and its default value
lobs = [
            ['BLOB(10)',"'1001000100'xb"],
            ['CLOB(10)',"'abcdefg'"],
            ['XML(100)',"'<abc> hello<abc>'"],
            ['JSON(40)',"'{ \"abc\" : \"01\"}'"]
        ]
# Define UDT and there default value
sys_udt = [
            ['SYSUDTLIB.ST_GEOMETRY(16776192) INLINE LENGTH 9920',"'POINT(1 2)'"],
            ['SYSUDTLIB.INTARRAY',"intarray(1,2,3)"],
            [ 'MBR','NEW MBR(0,0,4,4)']
           ]

#Define  basic type and terhe default value

COLUMS = [
              ['INTEGER',0],
              ['VARBYTE(5)',"'1011'xb"],
              ['BYTEINT',102],
              ['SMALLINT',32564],
              ['BYTE(5)',"'1011'xb"],
              ['BIGINT',4329875398726],
              ['DECIMAL(5,4)',5.456],
              ['DECIMAL(5,0)',5.456],
              ['FLOAT',5946.45],
              ['NUMBER',3627.36],
              ['NUMBER(5,0)',3654.4],
              ['NUMBER(*,4)',4753.7466],
              ["DATE FORMAT 'YY/MM/DD'","'2015-04-28'"],
              ['TIME(6)',"'12:23:25'"],
              ['TIME(0)',"'12:23:25'"],
              ['TIMESTAMP(6)',"'2015-04-28 12:23:25'"],
              ['TIME(6) WITH TIME ZONE',"'12:23:25+00:34'"],
              ['TIMESTAMP(6) WITH TIME ZONE',"'2015-04-28 12:23:25+00:34'"],
              ['INTERVAL YEAR(2)',12],
              ['INTERVAL YEAR(2) TO MONTH',"'34-10'"],
              ['INTERVAL MONTH(2)',45],
              ['INTERVAL DAY(2)',34],
              ['INTERVAL DAY(2) TO HOUR',"'45 26'"],
              ['INTERVAL DAY(2) TO MINUTE',"'45 24:12'"],
              ['INTERVAL DAY(2) TO SECOND(6)',"'99 23:59:59.999999'"],
              ['INTERVAL HOUR(2)',"'99'"],
              ['INTERVAL HOUR(2) TO MINUTE',"'99:59'"],
              ['INTERVAL HOUR(2) TO SECOND(6)',"'99:59:59.999999'"],
              ['INTERVAL MINUTE(2)',"'99'"],
              ['INTERVAL MINUTE(2) TO SECOND(6)',"'99:59.999999'"],
              ['INTERVAL SECOND(2,6)',"'99.999999'"],
              ['PERIOD(DATE)',"period(date '2015-04-30', date '2016-04-30')"],
              ['PERIOD(TIME(6))',"period(time '22:39:04.6532', time '23:24:49.6523')"],
              ['PERIOD(TIME(6) WITH TIME ZONE)',"period(time '22:39:04.6532+04:23', time '23:24:49.6523+03:34')"],
              ['PERIOD(TIMESTAMP(6))',"period(timestamp '2013-02-12 22:39:04.6532+09:34', timestamp '2014-12-31 23:24:49.6523+03:24')"],
              ['PERIOD(TIMESTAMP(6) WITH TIME ZONE)',"period(timestamp '2013-02-12 22:39:04.6532+09:34', timestamp '2014-12-31 23:24:49.6523+03:24')"],
          ]

#Define compression type for char/byte column used in test
COMPRESSION_TYPE = ['compress','compress using TD_LZ_COMPRESS decompress using TD_LZ_DECOMPRESS']
TAB_PROTECTION = ['FALLBACK']#,'NO FALLBACK']

# Define different option for creating table in teradata
TABLE_OPTION =['SET','MULTISET']
TABLE_TYPE = ['BASE','LDI','TEMPORAL']  #,'QUEUE']
P_I = ['UPI','NUPI','PA_CP','NOPI']
S_I = ['USI_NUSI','USI_NUSI_ORD','CP']
PP_I = ['PPI','MLPPI']   

#CONST = _Const()
