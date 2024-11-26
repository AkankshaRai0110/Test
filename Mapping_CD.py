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
dsn = cx_Oracle.makedsn("admghp02-scan.ae.ge.com", 1521, service_name="annmdp03")
mdm_prod_connect = cx_Oracle.connect(user=mdm_user, password=mdm_pwd, dsn=dsn,
                               encoding="UTF-8")
print("\u001b[31m MDM PROD Connected successfully \u001b[0m")
tail_num = input('Enter the tail: ')

#Mapping Query
Mapping_query=""" Select
sa.tail_num as MDM_TAIL ,
sax1.tail_num as IFS_TAIL,
sax2.tail_num as DO_TAIL,
se.engine_serial_num as Engine_serial_num,
ae.engine_pos_num as Engine_position,
ecp.n1_mdfr as N1_modifier,
ps1.product_cd as Engine_type,
ps2.product_cd as Aircraft_type,
org1.icao_cd as operator,
org2.icao_cd as owner,
org3.icao_cd as Monitor    
from c_bo_serialized_aircraft sa
inner join c_bo_serialized_aircraft_xref sax1
on sa.rowid_object=sax1.rowid_object
and sax1.hub_state_ind=1
and sax1.rowid_system='IFS'
and sa.hub_state_ind=1
inner join c_bo_serialized_aircraft_xref sax2
on sa.rowid_object=sax2.rowid_object
and sax2.hub_state_ind=1
and sax2.rowid_system='DO'
and sa.hub_state_ind=1    
inner join c_rel_aircraft_engine ae
on sa.rowid_object=ae.serialized_aircraft_id
and ae.hub_state_ind=1
and ae.rel_end_date>sysdate
join c_bo_serialized_engine se
on ae.serialized_engine_id=se.rowid_object
and se.hub_state_ind=1
join c_rel_engine_family ref
on se.rowid_object= ref.serialized_engine_id
and ref.hub_state_ind=1
and ref.rel_end_date>sysdate
and ref.rel_type_code='ENGINE LVL3-ENGINE'
join c_bo_product_structure ps1
on ps1.rowid_object=ref.product_id
and ps1.hub_state_ind=1
and ps1.product_type='ENGINE-LVL3'
join c_rel_aircraft_family raf
on raf.serialized_aircraft_id=sa.rowid_object
and raf.hub_state_ind=1
and ae.rel_end_date>sysdate
join c_bo_product_structure ps2
on raf.product_id=ps2.rowid_object
and ps2.hub_state_ind=1
join c_bo_engine_config_param ecp
on ecp.serialized_engine_id=se.rowid_object
and ecp.hub_state_ind=1
join c_rel_org_srlzd_aircraft osa1
on osa1.serialized_aircraft_id=sa.rowid_object
and osa1.hub_state_ind=1
and osa1.rel_end_date>sysdate
and osa1.rel_type_code='AIRCRAFT-OPERATOR'
join c_bo_organization org1
on osa1.org_id=org1.rowid_object
and org1.hub_state_ind=1
join c_rel_org_srlzd_aircraft osa2
on osa2.serialized_aircraft_id=sa.rowid_object
and osa2.hub_state_ind=1
and osa2.rel_end_date>sysdate
and osa2.rel_type_code='AIRCRAFT-OWNER'
join c_bo_organization org2
on osa2.org_id=org2.rowid_object
and org2.hub_state_ind=1      
join c_rel_org_srlzd_aircraft osa3
on osa3.serialized_aircraft_id=sa.rowid_object
and osa3.hub_state_ind=1
and osa3.rel_end_date>sysdate
and osa3.rel_type_code='AIRCRAFT-MONITOR'
join c_bo_organization org3
on osa3.org_id=org3.rowid_object
and org3.hub_state_ind=1    
where sa.tail_num = :tail_num ORDER BY ENGINE_POSITION """
 
df_mapping_query =pd.read_sql_query(Mapping_query, mdm_prod_connect ,params=[tail_num])
##print (df_mapping_query)
mf1=df_mapping_query.loc[0:0]
print(mf1)

#IFS1
IFS_user=config['username']['fdm']
IFS_pwd=config['password']['fdm_pwd']

dsn = cx_Oracle.makedsn("ANNIFP01.ae.ge.com", 1521, service_name="ANNIFP01")
fdm_sage1_connect = cx_Oracle.connect(user=IFS_user, password=IFS_pwd, dsn=dsn,
                               encoding="UTF-8")
print("\u001b[31m FDM Sage1 Connected successfully \u001b[0m")
fdm_sage1_query=""" Select * from fms_stg_aircraft_serial_tab where tail_no = :tail_num and AIRCRAFT_STATUS_CD = 'IN_OPERATION' """
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
dsn = cx_Oracle.makedsn("ANNIFP02.ae.ge.com", 1521, service_name="ANNIFP02")
fdm_sage2_connect = cx_Oracle.connect(user=IFS_user, password=IFS_pwd, dsn=dsn,
                               encoding="UTF-8")
print("\u001b[31m FDM Sage2 Connected successfully \u001b[0m")
fdm_sage2_query=""" Select * from fms_stg_aircraft_serial_tab where tail_no = :tail_num and AIRCRAFT_STATUS_CD = 'IN_OPERATION' """
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
ifs_src_query=""" SELECT * FROM dq_C_L_IFS_SRLZD_AIRCRAFT WHERE TAIL_NUM = :tail_num """
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
do_db_connect = cx_Oracle.connect(DO_username , DO_password , "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=EVNCTPP1.ae.ge.com)(PORT=1523)))(CONNECT_DATA=(SID=EVNCTPP1)))")
print("\u001b[31m DO DB Connected successfully \u001b[0m")
ESN1=input('Enter the ESN1: ')
ESN2=input('Enter the ESN2: ')
DO_CD=input('Enter the DO code: ')
do_tail_num=input('Enter DO Tail: ')

#Check  engine Status
engine_status_query=""" Select * from sge1.engine where engine_id in (:ESN1,:ESN2) and ENGINE_STATUS='ONWING' UNION Select * from sge2.engine where engine_id in (:ESN1,:ESN2) and ENGINE_STATUS='ONWING' """
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
aircraft_data_query=""" Select * from sge1.onwing_engine where engine_id in (:ESN1,:ESN2) and removal_datetime is null UNION Select * from sge2.onwing_engine where engine_id in (:ESN1,:ESN2) and removal_datetime is null """
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
engine_data_query="""Select * from sge1.engine_config where engine_id in (:ESN1,:ESN2) and current_flag='YES' AND ENGINE_OWNER !='OBSOLETE' UNION Select * from sge2.engine_config where engine_id in (:ESN1,:ESN2)  and current_flag='YES' AND ENGINE_OWNER !='OBSOLETE' """
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
aircraft_tail_data=""" Select * from sge1.onwing_engine WHERE aircraft_id = :do_tail_num and removal_datetime is null UNION Select * from sge2.onwing_engine WHERE aircraft_id = :do_tail_num and removal_datetime is null"""
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
aircraft_type_data="""Select * from sge1.aircraft where aircraft_id = :do_tail_num UNION Select * from sge2.aircraft where aircraft_id = :do_tail_num """
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
DO_cd_Query="""select * from rdo_ecs_enabled_customers where customer_code= :DO_CD """
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




