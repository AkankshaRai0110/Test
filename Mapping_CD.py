import cx_Oracle
import pandas as pd
import numpy as np
from configparser import ConfigParser

cx_Oracle.init_oracle_client(lib_dir=r"C:\temp\Instant Client\instantclient_19_13")
file='Config.ini'
config= ConfigParser()
config.read(file)
mdm_user=config['username']['mdm_prod']
mdm_pwd=config['password']['mdm']
dsn = cx_Oracle.makedsn("dbhostname", 1521, service_name="dbname")
mdm_prod_connect = cx_Oracle.connect(user=mdm_user, password=mdm_pwd, dsn=dsn,
                               encoding="UTF-8")
print("\u001b[31m MDM PROD Connected successfully \u001b[0m")
tail_num = input('Enter the tail: ')

#Mapping Query
Mapping_query=""" 
sql_query """
 
df_mapping_query =pd.read_sql_query(Mapping_query, mdm_prod_connect ,params=[tail_num])
##print (df_mapping_query)
mf1=df_mapping_query.loc[0:0]
print(mf1)

#IFS1
IFS_user=config['username']['fdm']
IFS_pwd=config['password']['fdm_pwd']

dsn = cx_Oracle.makedsn("db2hostname", 1521, service_name="db2name")
fdm_sage1_connect = cx_Oracle.connect(user=IFS_user, password=IFS_pwd, dsn=dsn,
                               encoding="UTF-8")
print("\u001b[31m FDM Sage1 Connected successfully \u001b[0m")
fdm_sage1_query=""" sql_query2 """
df_fdm_s1=pd.read_sql_query(fdm_sage1_query, fdm_sage1_connect ,params=[tail_num])
print (df_fdm_s1)
f1=df_fdm_s1[['AIRCRAFT_OPERATOR_CD','AIRCRAFT_OWNER_CD']]
print(f1)
new_row=['nan']
if f1.empty==True:
    f1=f1.append(pd.Series({'AIRCRAFT_OPERATOR_CD':'nan','AIRCRAFT_OWNER_CD':'nan'}, name=0))

opr_cmp1=np.where(mf1['OPERATOR'] == f1['AIRCRAFT_OPERATOR_CD'],'TRUE','FALSE')
own_cmp1=np.where(mf1['OWNER'] == f1['AIRCRAFT_OWNER_CD'],'TRUE','FALSE')

#IFS2
dsn = cx_Oracle.makedsn("db3hostname", 1521, service_name="db3name")
fdm_sage2_connect = cx_Oracle.connect(user=IFS_user, password=IFS_pwd, dsn=dsn,
                               encoding="UTF-8")
print("\u001b[31m FDM Sage2 Connected successfully \u001b[0m")
fdm_sage2_query=""" sql_query"""
df_fdm_s2=pd.read_sql_query(fdm_sage2_query, fdm_sage2_connect ,params=[tail_num])
print(df_fdm_s2)
f2=df_fdm_s2[['AIRCRAFT_OPERATOR_CD','AIRCRAFT_OWNER_CD']]
print(f2)
new_row=['nan']
if f2.empty==True:
    f2=f2.append(pd.Series({'AIRCRAFT_OPERATOR_CD':'nan','AIRCRAFT_OWNER_CD':'nan'}, name=0))
    
opr_cmp2=np.where(mf1['OPERATOR'] == f2['AIRCRAFT_OPERATOR_CD'],'TRUE','FALSE')
own_cmp2=np.where(mf1['OWNER'] == f2['AIRCRAFT_OWNER_CD'],'TRUE','FALSE')


if ((opr_cmp1=='TRUE' and own_cmp1=='TRUE') or (opr_cmp2=='TRUE' and own_cmp2=='TRUE')):
    print('\u001b[33mMDM & FDM tables are in Sync for\u001b[0m \033[92m' , tail_num,'\033[0m')
else:
   print("\u001b[33mMDM & FDM tables are Out of Sync for\u001b[0m \033[92m" , tail_num,'\033[0m')


#IDD Xref Verify
#IFS SRC
ifs_src_query=""" query """
df_ifs_tail=pd.read_sql_query(ifs_src_query, mdm_prod_connect,params=[tail_num])
print(df_ifs_tail)
f_ifs_tail=df_ifs_tail[['TAIL_NUM']]
print(f_ifs_tail)
new_row=['nan']
if f_ifs_tail.empty==True:
     f_ifs_tail=f_ifs_tail.append(pd.Series({'AIRCRAFT_OPERATOR_CD':'nan','AIRCRAFT_OWNER_CD':'nan'}, name=0))
     print(f_ifs_tail)

#DO SRC
do_src_query=""" SELECT * FROM C_BO_SERIALIZED_AIRCRAFT WHERE INCOMING_TAIL_NUM = :tail_num """
df_do_tail=pd.read_sql_query(do_src_query, mdm_prod_connect,params=[tail_num])
print(df_do_tail)
f_do_tail=df_do_tail[['INCOMING_TAIL_NUM']]
print(f_do_tail)
new_row=['nan']
if f_do_tail.empty==True:
    f_do_tail=f_do_tail.append(pd.Series({'AIRCRAFT_OPERATOR_CD':'nan','AIRCRAFT_OWNER_CD':'nan'}, name=0))
    print(f_do_tail)


#Compare
tail_cmp=np.where(f_ifs_tail['TAIL_NUM']==f_do_tail['INCOMING_TAIL_NUM'],'TRUE','FALSE')
if(tail_cmp=='TRUE'):
     print('\u001b[33mIFS tail & DO incoming Tail are in Sync for\u001b[0m \033[92m' , tail_num,'\033[0m')
else:
    print("\u001b[33mIFS tail & DO incoming Tail are Out of Sync for\u001b[0m \033[92m" , tail_num,'\033[0m')

#DO Query
DO_username=config['username']['do']
DO_password=config['password']['do_pwd']
do_db_connect = cx_Oracle.connect(DO_username , DO_password , "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=hostname(PORT=1523)))(CONNECT_DATA=(SID=EVNCTPP1)))")
print("\u001b[31m DO DB Connected successfully \u001b[0m")
ESN1=input('Enter the ESN1: ')
ESN2=input('Enter the ESN2: ')
DO_CD=input('Enter the DO code: ')
do_tail_num=input('Enter DO Tail: ')

#Check  engine Status
engine_status_query=""" query """
df_esn=pd.read_sql_query(engine_status_query, do_db_connect,params=[ESN1,ESN2])
print(df_esn)
f_esn=df_esn[['ENGINE_ID']]
print(f_esn)
if df_esn.empty==True:
     df_esn=df_esn.append(pd.Series({'ENGINE_ID':'nan','ENGINE_STATUS':'nan','SERIAL_NUMBER':'nan','DELETION_FLAG':'nan'}, name=0))
     print(df_esn)

#Comapre Engine_Id
esn_cmp=np.where(df_mapping_query['ENGINE_SERIAL_NUM'] == f_esn['ENGINE_ID'],'TRUE','FALSE')
if((esn_cmp=='TRUE','TRUE')):
     print('\u001b[33mDO Engine_Id & POS verified and IN Sync with MDM for\u001b[0m \033[92m', tail_num,'\033[0m')
else:
    print("\u001b[33mDO Engine_Id & POS Out of sync with MDM\u001b[0m")

#Check the Aircraft tail, position of the Engine 
aircraft_data_query=""" query """
df_asset=pd.read_sql_query(aircraft_data_query, do_db_connect, params=[ESN1,ESN2])
print(df_asset)
f_asset=df_asset[['AIRCRAFT_ID','ENGINE_POSITION','ENGINE_ID','AIRCRAFT_FAMILY']]
print(f_asset)
new_row=['nan']
if f_asset.empty==True:
    f_asset=f_asset.append(pd.Series({'AIRCRAFT_ID':'nan','ENGINE_POSITION':'nan','ENGINE_ID':'nan','AIRCRAFT_FAMILY':'nan'}, name=0))
    print(f_asset)

#Compare Aircraft tail, position of the Engine
do_mdm_tail_cmp=np.where(df_mapping_query['DO_TAIL'] == f_asset['AIRCRAFT_ID'],'TRUE','FALSE')
do_mdm_esn_pos_cmp=np.where(df_mapping_query['ENGINE_POSITION'] == f_asset['ENGINE_POSITION'],'TRUE','FALSE')
do_mdm_esn_cmp=np.where(df_mapping_query['ENGINE_SERIAL_NUM'] == f_asset['ENGINE_ID'],'TRUE','FALSE')
if((do_mdm_tail_cmp=='TRUE','TRUE') and (do_mdm_esn_pos_cmp=='TRUE','TRUE' ) and (do_mdm_esn_cmp=='TRUE','TRUE')):
     print('\u001b[33mAircraft_Tail is In sync with DO & MDM for\u001b[0m \033[92m', tail_num,'\033[0m')
else:
    print("\u001b[33mAircraft_Tail is Out of sync for DO & MDM\u001b[0m")

#Check  Engine level3, Engine Owner, N1 modifier, TCC Timer
engine_data_query="""query """
df_esn_lvl3=pd.read_sql_query(engine_data_query, do_db_connect, params=[ESN1,ESN2])
print(df_esn_lvl3)
f_esn_lvl3=df_esn_lvl3[['ENGINE_ID','ENGINE_TYPE','N1_MODIFIER']]
print(f_esn_lvl3)
new_row=['nan']
if f_esn_lvl3.empty==True:
    f_esn_lvl3=f_esn_lvl3.append(pd.Series({'ENGINE_ID':'nan','ENGINE_TYPE':'nan','N1_MODIFIER':'nan'}, name=0))
    print(f_esn_lvl3)

#Compare Engine_lvl3
do_mdm_etype_cmp=np.where(df_mapping_query['ENGINE_TYPE'] == f_esn_lvl3['ENGINE_TYPE'],'TRUE','FALSE')
do_mdm_N1mod_cmp=np.where(df_mapping_query['N1_MODIFIER'] == f_esn_lvl3['N1_MODIFIER'],'TRUE','FALSE')
if((do_mdm_etype_cmp=='TRUE','TRUE') and (do_mdm_N1mod_cmp=='TRUE','TRUE' )):
     print('\u001b[33mDO Engine_Type & N1 MOodifier is In sync with MDM for\u001b[0m \033[92m', tail_num,'\033[0m')
else:
    print("\u001b[33mDO & MDM Engine family data are Out of Sync\u001b[0m")

#Check the Aircraft tail, position of the Engine
aircraft_tail_data=""" query"""
df_acrft_tail_chk=pd.read_sql_query(aircraft_tail_data, do_db_connect , params=[do_tail_num])
print(df_acrft_tail_chk)
f_acrft_tail_chk=df_acrft_tail_chk[['AIRCRAFT_ID','ENGINE_POSITION','ENGINE_ID','AIRCRAFT_FAMILY']]
print(f_acrft_tail_chk)
new_row=['nan']
if f_acrft_tail_chk.empty==True:
    f_acrft_tail_chk=f_acrft_tail_chk.append(pd.Series({'AIRCRAFT_ID':'nan','ENGINE_POSITION':'nan','ENGINE_ID':'nan','AIRCRAFT_FAMILY':'nan'}, name=0))
    print(f_acrft_tail_chk)

#Compare Aircraft_DOTail & their engine position
do_mdm_tail_cmp2=np.where(df_mapping_query['DO_TAIL']==f_acrft_tail_chk['AIRCRAFT_ID'],'TRUE','FALSE')
do_mdm_esn_pos_cmp2=np.where(df_mapping_query['ENGINE_POSITION']==f_acrft_tail_chk['ENGINE_POSITION'],'TRUE','FALSE')
do_mdm_esn_cmp2=np.where(df_mapping_query['ENGINE_SERIAL_NUM']==f_acrft_tail_chk['ENGINE_ID'],'TRUE','FALSE')
if ((do_mdm_tail_cmp2=='TRUE','TRUE') and (do_mdm_esn_pos_cmp2=='TRUE','TRUE') and (do_mdm_esn_cmp2=='TRUE','TRUE')):
     print('\u001b[33mDO Tail & Engine_ID, POS is In sync with MDM for\u001b[0m \033[92m', tail_num,'\033[0m')
else:
    print("\u001b[33mDO & MDM are Out of sync for\u001b[0m \033[92m" , tail_num,'\033[0m')

#Check Aircraft Type
aircraft_type_data="""query"""
df_acrft_type=pd.read_sql_query(aircraft_type_data, do_db_connect , params=[do_tail_num])
print(df_acrft_type)
f_acrft_type=df_acrft_type[['AIRCRAFT_ID','AIRCRAFT_TYPE']]
print(f_acrft_type)
new_row=['nan']
if f_acrft_type.empty==True:
    f_acrft_type=f_acrft_type.append(pd.Series({'AIRCRAFT_ID':'nan','AIRCRAFT_TYPE':'nan'}, name=0))
    print(f_acrft_type)

#CompARE Aircraft_Type
acrft_type_cmp=np.where(mf1['AIRCRAFT_TYPE']==f_acrft_type['AIRCRAFT_TYPE'],'TRUE','FALSE')
if(acrft_type_cmp=='TRUE','TRUE'):
     print('\u001b[33mAircraft_Type is In sync with DO & MDM for\u001b[0m \033[92m', tail_num ,'\033[0m')
else:
    print("\u001b[33mAircraft_Type Data is Out of sync\u001b[0m")

#Check DO code
DO_cd_Query="""query """
df_do_code=pd.read_sql_query(DO_cd_Query, do_db_connect , params=[DO_CD])
print(df_do_code)
f_do_cd=df_do_code[['CUSTOMER_CODE']]
print(f_do_cd)
new_row=['nan']
if df_do_code.empty==True:
     do_cd=f_do_cd.append(pd.Series({'CUSTOMER_CODE':'nan'}, name=0))
     print(do_cd)
     print("\u001b[31mHi, \n",
        " \n"
         "Customer \u001b[0m'\033[94m",DO_CD,"\033[0m' \u001b[31mis enabled in MDM for below mentioned aircraft and engine. \n" ,
         " \n" 
         "DO Team, \n "
         " \n"
         "Kindly enable\u001b[0m '\033[94m",DO_CD,"\033[0m' \u001b[31min DO. \n" ,
         " \n"
         "Thanks,\u001b[0m \n"
         )
else:
    print("\u001b[31mHi, \n",
    " \n"
    "Customer\u001b[0m '\033[94m",DO_CD,"\033[0m' \u001b[31mis enabled in MDM for below mentioned aircraft and engine. \n",
    " \n"
    "Thanks,\u001b[0m \n"
    )




