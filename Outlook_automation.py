import decimal
import pyodbc
import time
import csv
import datetime
import os
import sys
import subprocess
import numpy
import email
import smtplib
import shutil

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import datetime, timedelta
from datetime import datetime as dt

os.chdir('C:/Users/tianxu/Documents/Outlook_automation')
pyodbc.pooling = False

#def main():
Login_info = open('C:/Work/LogInMozart_ts.txt', 'r')
server_name = Login_info.readline()
server_name = server_name[:server_name.index(';')+1]
UID = Login_info.readline()
UID = UID[:UID.index(';') + 1]
PWD = Login_info.readline()
PWD = PWD[:PWD.index(';') + 1]
Login_info.close()
#today_dt = datetime.date.today()
print 'Connecting Server to determine date info at: ' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '.'
conn = pyodbc.connect('DRIVER={Teradata};DBCNAME='+ server_name +'UID=' + UID + 'PWD=' + PWD)
curs = conn.cursor()

curs.execute('''

Create	volatile table Time_period as(
Select	max(Min_Week_Dt) as Min_Week_Dt, max(Max_Week_Dt) as Max_Week_Dt
from	
(
select	date '1969-12-31' as Min_Week_Dt, RETAIL_WK_END_DATE -7 as Max_Week_Dt
from	dw_cal_dt
where	cal_dt = date
union	
select	min(RETAIL_WK_END_DATE) as Min_Week_Dt,date '1969-12-31' as Max_Week_Dt
from	dw_cal_dt
where	cal_dt = date - 120
)a)
with	data primary index(Min_Week_Dt,Max_Week_Dt)
	on	commit preserve rows;

''')
conn.commit()

curs.execute('''
Drop	table p_ci_map_t.jsh_EPN_FCST_iAB_Output;


''')
conn.commit()

curs.execute('''

create    multiset table p_ci_map_t.jsh_EPN_FCST_iAB_Output  as (
sel           
dt.fsc_wk,
dt.fsc_mnth_num as fsc_mnth,
Case    
    when    dt.fsc_mnth_num = 1 then 'Jan'
    when    dt.fsc_mnth_num = 2 then 'Feb'
    when    dt.fsc_mnth_num = 3 then 'Mar'
    when    dt.fsc_mnth_num = 4 then 'Apr'
    when    dt.fsc_mnth_num = 5 then 'May'
    when    dt.fsc_mnth_num = 6 then 'Jun'
    when    dt.fsc_mnth_num = 7 then 'Jul'
    when    dt.fsc_mnth_num = 8 then 'Aug'
    when    dt.fsc_mnth_num = 9 then 'Sep'
    when    dt.fsc_mnth_num = 10 then 'Oct'
    when    dt.fsc_mnth_num = 11 then 'Nov'
    when    dt.fsc_mnth_num = 12 then 'Dec'
else    'Others'
end    as fsc_Mnth2,
dt.fsc_qtr_num as Fsc_Qtr,
dt.Fsc_Yr,
                   ------- dt.WEEK_OF_YEAR_ID, dt.retail_year,                                                                                               
                             CASE                    
                                           WHEN B.AMS_PRGRM_ID IN (1) THEN 'US'                            
                                           WHEN B.AMS_PRGRM_ID IN (7) THEN 'CA'           
                                           WHEN B.AMS_PRGRM_ID = 4 THEN 'AU'                              
                                           WHEN B.AMS_PRGRM_ID = 11 THEN 'DE'                              
                                           WHEN B.AMS_PRGRM_ID = 15 THEN 'UK'                              
                                           WHEN B.AMS_PRGRM_ID =10 THEN 'FR'                              
                                           WHEN B.AMS_PRGRM_ID=12 THEN 'IT'                
                                           WHEN B.AMS_PRGRM_ID=13 THEN 'ES'                              
                                           WHEN B.AMS_PRGRM_ID IN (2,3,5,14,16) THEN 'ROE'                              
                                           ELSE 'OTHERS'                  
                             END AS REGION,                            
                             case                     
                                                          when CLV_BUYER_TYPE_CD in (1,2) then 'Acquired'
                                                          when CLV_BUYER_TYPE_CD in (101) then 'Engaged'
                                                          when CLV_BUYER_TYPE_CD in (102) then 'Retained'
                                                          else 'Existing'
                             end as txn_type,
                             CASE
    WHEN region='US' THEN 0.65
        WHEN region='CA' THEN 0.69
    WHEN region='UK' THEN 0.54
    WHEN region='DE' THEN 0.55
    WHEN region='FR' THEN 0.73
    WHEN region='IT' THEN 0.65
    WHEN region='ES' THEN 0.70
    WHEN region='ROE' THEN 0.52

WHEN region='AU' THEN 0.52
    ELSE 0
  END AS RC_FCTR,
                             CLV_BUYER_TYPE_CD,                 
                             SUM(CAST(GMB_USD_AMT AS DECIMAL(24,6))) AS GMB_USD,                             
                             SUM(CAST(IGMB_USD_AMT  AS DECIMAL(24,6))) AS IGMB_USD,                      
                             sum(cast(IGMB_PLAN_RATE_AMT as DECIMAL(24,6))) AS iGMB_Plan,                      
                             SUM(CAST(DGMB_USD_AMT  AS DECIMAL(24,6))) AS DGMB_USD,                    
                             SUM(CAST(IGMB_USD_AMT AS DECIMAL(24,6)) - CAST(DGMB_USD_AMT AS DECIMAL(24,6))) AS ICAV_USD_Calc,               
                             sum(Cast( iCAV_USD_AMT as DEcimal (24,6))) as iCAV_USD_AMT,                           
                             sum(Cast( CAV_USD_AMT as DEcimal (24,6))) as CAV_USD_AMT,                            
                             SUM(CAST(IREV_USD_AMT  AS DECIMAL(24,6))) AS IREV_USD,                             
                             sum(cast(IREV_PLAN_RATE_AMT as DECIMAL(24,6))) AS iREV_Plan,         
                             SUM(CASE WHEN CLV_BUYER_TYPE_CD IN (1,2) THEN 1 ELSE 0 END) AS NORB_COUNT,               
                             SUM(CASE WHEN CLV_BUYER_TYPE_CD IN (1,2) THEN INCR_FCTR ELSE 0 END) AS INORB_COUNT, SUM(CASE WHEN CLV_BUYER_TYPE_CD IN (101) THEN 1 ELSE 0 END) AS CLVE_cnt, SUM(CASE WHEN CLV_BUYER_TYPE_CD IN (102) THEN 1 ELSE 0 END) AS CLVR_cnt,
                             SUM((CASE WHEN CLV_BUYER_TYPE_CD IN (102) THEN INCR_FCTR ELSE 0 END ) * RC_FCTR) AS IABR
FROM PRS_RESTRICTED_V.MH_IM_CORE_FAM2_FACT    FAM                                                
                             INNER JOIN p_ci_map_t.sh_fsc_base  DT ON FAM.CK_TRANS_DT = DT.CAL_DT            ----DW_CAL_DT DT           
                             JOIN PRS_AMS_V.AMS_PRGRM  B                          
                             ON FAM.CLIENT_ID=B.MPX_CLNT_ID                    
                             LEFT JOIN PRS_AMS_V.AMS_PBLSHR PBLSHR ON FAM.EPN_PBLSHR_ID = PBLSHR.AMS_PBLSHR_ID                           
                             LEFT JOIN PRS_AMS_V.AMS_PBLSHR_BSNS_MODEL PB_BM                              
                             ON PB_BM.PBLSHR_BSNS_MODEL_ID =COALESCE(PBLSHR.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, PBLSHR.PBLSHR_BSNS_MODEL_ID, -999)                            
WHERE                                            
             -- CK_TRANS_DT BETWEEN '2015-12-26' AND '2018-03-17'                
              
               CK_TRANS_DT >= '2015-12-16'
      and CK_TRANS_DT < (
select    week_beg_dt 
from    dw_cal_dt where cal_dt = current_date )
              AND MPX_CHNL_ID=6 --epn                                    
              AND FAM.CLIENT_ID IN (707, 709, 710, 724, 1185, 1346, 5282, 5221, 1553, 5222, 705, 711, 706)
              and epn_pblshr_id not in (5575245877,5575245881,5575245884,
                             5575245888,5575246818,5575245887) -- excluding eCG pubs, as they don't earn
GROUP BY 1,2,3,4,5,6,7,8,9)
with    data primary index (fsc_wk,  fsc_mnth, Fsc_Qtr, Fsc_Yr, region);


''')
conn.commit()

curs.execute('''
Drop	table p_ci_map_t.jsh_fcst_1 ;


''')
conn.commit() 
             
             
curs.execute('''             
Create	multiset table p_ci_map_t.jsh_fcst_1 as(
select	
MONTH_END_DT,
RETAIL_WK_END_DATE,
QTR_END_DT,
      TRANS_DT as CK_TRANS_DT,
      AMS_PRGRM_ID,
          case
            when coalesce(C.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, C.PBLSHR_BSNS_MODEL_ID,
		-999) = 1 then 'OCS'
            when coalesce(C.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, C.PBLSHR_BSNS_MODEL_ID,
		-999) in (2,3) then 'Content'
            when coalesce(C.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, C.PBLSHR_BSNS_MODEL_ID,
		-999) = 4 then 'Loyalty'
            else 'Other' 
end	as BM,
      --sum(ERNG_USD) as ERNG_USD,
      sum(ERNG_PRGRM_CRNCY) as ERNG_PRG
    from
      prs_ams_v.AMS_PBLSHR_ERNG a
      join dw_cal_dt cal
      on a.trans_dt = cal.cal_dt
      join prs_ams_v.AMS_PBLSHR c
      on a.AMS_PBLSHR_ID = c.AMS_PBLSHR_ID 


where 		  TRANS_DT > (
select	Min_Week_Dt 
from	Time_period)
      and trans_dt <= (
select	Max_Week_Dt 
from	Time_period)  
      
      and AMS_PRGRM_ID in (2,3,5,10,11,12,13,14,15,16,4,1,7)
      and a.ams_pblshr_id not in (5575245877,5575245881,5575245884,
		5575245888,5575246818,5575245887) -- excluding eCG pubs, as they don't earn
    group by 1,2,3,4,5,6)
    with data primary index(CK_TRANS_DT,AMS_PRGRM_ID,BM);
''')
conn.commit() 




curs.execute('''
    drop table p_ci_map_t.jsh_fcst_2;
''')
conn.commit() 
             
             
curs.execute('''   
Create multiset table p_ci_map_t.jsh_fcst_2 as(
select      
MONTH_END_DT,
RETAIL_WK_END_DATE,
QTR_END_DT,
      FAM2.CK_TRANS_DT,
      P.AMS_PRGRM_ID,
      case
            when coalesce(C.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, C.PBLSHR_BSNS_MODEL_ID,
                                -999) = 1 then 'OCS'
            when coalesce(C.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, C.PBLSHR_BSNS_MODEL_ID,
                                -999) in (2,3) then 'Content'
            when coalesce(C.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, C.PBLSHR_BSNS_MODEL_ID,
                                -999) = 4 then 'Loyalty'
            else 'Other' 
end         as BM,
      sum(FAM2.IREV_PLAN_RATE_AMT) as REV_USD   
      --sum(FAM2.IGMB_PLAN_AMT) as iGMB_USD,
      --sum(FAM2.IREV_PLAN_AMT) as iREV_USD
    from
      PRS_RESTRICTED_V.MH_IM_CORE_FAM2_FACT  as FAM2
      join prs_ams_v.AMS_PRGRM P
        on FAM2.CLIENT_ID=P.MPX_CLNT_ID
      join dw_cal_dt cal
      on FAM2.CK_TRANS_DT = cal.cal_dt  
        join prs_ams_v.AMS_PBLSHR C
            on FAM2.epn_PBLSHR_ID = C.AMS_PBLSHR_ID
    
    where  
	 CK_TRANS_DT   > (
select	Min_Week_Dt 
from	Time_period)
      and CK_TRANS_DT  <= (
select	Max_Week_Dt 
from	Time_period)  
      
      and P.AMS_PRGRM_ID in (2,3,5,10,11,12,13,14,15,16,4,1,7)
      and FAM2.CLIENT_ID in (707, 709, 710, 724, 1185, 1346, 5282,                     5221, 1553, 5222, 705, 711, 706)
      and FAM2.MPX_CHNL_ID=6
      and C.ams_pblshr_id not in (5575245877,5575245881,5575245884,  5575245888,5575246818,5575245887) -- excluding eCG pubs, as they don't earn
    group by 1,2,3,4,5,6)
    with data primary index(CK_TRANS_DT,AMS_PRGRM_ID,BM);
''')
conn.commit()  
    
    

  

curs.execute('''
	Drop	table p_ci_map_t.jsh_fcst_3;    
''')
conn.commit()	


curs.execute('''
Create	multiset table p_ci_map_t.jsh_fcst_3 as(
    select
MONTH_END_DT,
RETAIL_WK_END_DATE,
QTR_END_DT,
      FAM2.CK_TRANS_DT,
      P.AMS_PRGRM_ID,
       case
            when coalesce(C.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, C.PBLSHR_BSNS_MODEL_ID,
		-999) = 1 then 'OCS'
            when coalesce(C.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, C.PBLSHR_BSNS_MODEL_ID,
		-999) in (2,3) then 'Content'
            when coalesce(C.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, C.PBLSHR_BSNS_MODEL_ID,
		-999) = 4 then 'Loyalty'
            else 'Other' 
end	as BM,
      sum(FAM2.IGMB_PLAN_RATE_AMT) as iGMB_USD_Desktop,
      sum(FAM2.IREV_PLAN_RATE_AMT) as iREV_USD_Desktop
    from
      PRS_RESTRICTED_V.MH_IM_CORE_FAM2_FACT  as FAM2
      join prs_ams_v.AMS_PRGRM P
        on FAM2.CLIENT_ID=P.MPX_CLNT_ID
          join prs_ams_v.AMS_PBLSHR C
            on FAM2.EPN_PBLSHR_ID = C.AMS_PBLSHR_ID
     join dw_cal_dt cal
      on  FAM2.CK_TRANS_DT = cal.cal_dt  
    where 
    	 CK_TRANS_DT   >(
select	Min_Week_Dt 
from	Time_period)
      and CK_TRANS_DT  <= (
select	Max_Week_Dt 
from	Time_period)  
      and P.AMS_PRGRM_ID in (2,3,5,10,11,12,13,14,15,16,4,1,7)
      and FAM2.CLIENT_ID in (707, 709, 710, 724, 1185, 1346, 5282,
		5221, 1553, 5222, 705, 711, 706)
      and FAM2.MPX_CHNL_ID=6
      and FAM2.DEVICE_TYPE_ID=1
	    and FAM2.SAP_CATEGORY_ID NOT IN (5, 7, 41, 23, -999)
      and C.ams_pblshr_id not in (5575245877,5575245881,5575245884,
		5575245888,5575246818,5575245887) -- excluding eCG pubs, as they don't earn
    group by 1,2,3,4,5,6)
    with data primary index(CK_TRANS_DT,AMS_PRGRM_ID,BM);
''')
conn.commit()	
	
	
curs.execute('''	
Drop	table p_ci_map_t.jsh_fcst_4;  
''')
conn.commit()	
             
curs.execute('''
Create	multiset table p_ci_map_t.jsh_fcst_4 as(
    select
MONTH_END_DT,
RETAIL_WK_END_DATE,
QTR_END_DT,
      A.CK_TRANS_DT,
      B.AMS_PRGRM_ID,
       case
            when coalesce(C.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, C.PBLSHR_BSNS_MODEL_ID,
		-999) = 1 then 'OCS'
            when coalesce(C.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, C.PBLSHR_BSNS_MODEL_ID,
		-999) in (2,3) then 'Content'
            when coalesce(C.ADVRTSNG_PBLSHR_BSNS_MODEL_ID, C.PBLSHR_BSNS_MODEL_ID,
		-999) = 4 then 'Loyalty'
            else 'Other' 
end	as BM,
      sum(A.IGMB_PLAN_RATE_AMT) as iGMB_USD_Mobile,
      sum(A.IREV_PLAN_RATE_AMT) as iREV_USD_Mobile
    from
      PRS_RESTRICTED_V.MH_IM_CORE_FAM2_FACT  as A
      join prs_ams_v.AMS_PRGRM B
        on A.CLIENT_ID=B.MPX_CLNT_ID
      join dw_cal_dt cal
      on a.CK_TRANS_DT = cal.cal_dt  
      join prs_ams_v.AMS_PBLSHR C
            on A.EPN_PBLSHR_ID = C.AMS_PBLSHR_ID
    
     where 
     	 CK_TRANS_DT   >(
select	Min_Week_Dt 
from	Time_period)
      and CK_TRANS_DT  <= (
select	Max_Week_Dt 
from	Time_period)  
      and B.AMS_PRGRM_ID in (2,3,5,10,11,12,13,14,15,16,4,1,7)
      and A.CLIENT_ID in (707, 709, 710, 724, 1185, 1346, 5282,
		5221, 1553, 5222, 705, 711, 706)
      and A.MPX_CHNL_ID = 6   
      and A.DEVICE_TYPE_ID = 2
      and A.SAP_CATEGORY_ID NOT IN (5, 7, 41, 23, -999)     
      and C.ams_pblshr_id not in (5575245877,5575245881,5575245884,5575245888,5575246818,5575245887) -- excluding eCG pubs, as they don't earn
    group by 1,2,3,4,5,6)
    with data primary index(CK_TRANS_DT,AMS_PRGRM_ID,BM);
''')
conn.commit()	




curs.execute('''
Drop	table p_ci_map_t.jsh_fcst_5;

''')
conn.commit()
             
curs.execute('''            
Create	multiset table p_ci_map_t.jsh_fcst_5 as
(
select	
MONTH_END_DT,
RETAIL_WK_END_DATE,
QTR_END_DT,
CK_TRANS_DT,
prg_lkp as program,
BM,
sum(ERNG_PRG) as ERNG_PRG,
--sum(ERNG_USD) as ERNG_USD,
sum(REV_USD) as REV_USD,
sum(iGMB_USD) as iGMB_USD,
sum(coalesce(iRev_USD,0)) as iRev_USD,

sum(iGMB_USD_Desktop) as iGMB_USD_Desktop,
sum(iGMB_USD_Mobile) as iGMB_USD_Mobile,
sum(iRev_USD_Desktop) as iRev_USD_Desktop,
sum(iREV_USD_Mobile) as iREV_USD_Mobile

from	

(
Select	
a.*, 
Case	
	when	a.AMS_PRGRM_ID = 1 then 'US'
	when	a.ams_prgrm_id = 2 then 'ROE'
	when	a.ams_prgrm_id = 3 then 'ROE'
	when	a.ams_prgrm_id = 4 then 'AU'
	when	a.ams_prgrm_id = 5 then 'ROE'
	when	a.ams_prgrm_id = 7 then 'CA'
	when	a.ams_prgrm_id = 10 then 'FR'
	when	a.ams_prgrm_id = 11 then 'DE'
	when	a.ams_prgrm_id = 12 then 'IT'
	when	a.ams_prgrm_id = 13 then 'ES'
	when	a.ams_prgrm_id = 14 then 'ROE'
	when	a.ams_prgrm_id = 15 then 'UK'
	when	a.ams_prgrm_id = 16 then 'ROE'
	when	a.ams_prgrm_id = 17 then 'Half'
else	'Others'
end	as prg_lkp,
b.REV_USD, 
C.iGMB_USD_Desktop,
coalesce(d.iGMB_USD_Mobile,0) as iGMB_USD_Mobile,
C.iRev_USD_Desktop,
coalesce(D.iREV_USD_Mobile,0) as iREV_USD_Mobile,
C.iGMB_USD_Desktop+coalesce(d.iGMB_USD_Mobile,0) as iGMB_USD, 
C.iRev_USD_Desktop +coalesce(D.iREV_USD_Mobile,0) as iRev_USD

from	p_ci_map_t.jsh_fcst_1 a
join p_ci_map_t.jsh_fcst_2 b
	on	a.CK_TRANS_DT = b.CK_TRANS_DT
	and	a.AMS_PRGRM_ID = b.AMS_PRGRM_ID
	and	a.BM = b.BM
join p_ci_map_t.jsh_fcst_3 c
	on	a.CK_TRANS_DT = c.CK_TRANS_DT
	and	a.AMS_PRGRM_ID = c.AMS_PRGRM_ID
	and	a.BM = c.BM
left join p_ci_map_t.jsh_fcst_4 d
	on	a.CK_TRANS_DT = d.CK_TRANS_DT
	and	a.AMS_PRGRM_ID = d.AMS_PRGRM_ID
	and	a.BM = d.BM)a
group	by 1,2,3,4,5,6
)
with	data primary index(CK_TRANS_DT,program,BM);
''')
conn.commit()



curs.execute('''
Drop	table p_ci_map_t.jsh_fcst_6;
''')
conn.commit()

curs.execute('''

Create	multiset table p_ci_map_t.jsh_fcst_6 as
(
select	
a.*,
WEEK_OF_YEAR_ID,
YEAR_ID,
case	
	when	program = 'AU' then (ERNG_PRG*1.0000/0.76) --not updated
	when	program = 'UK' then (ERNG_PRG*1.0000/1.33) 
	when	program = 'US' then (ERNG_PRG*1.0000/1) 
	when	program = 'CA' then (ERNG_PRG*1.0000/1) 
else	 ERNG_PRG
end	as ERNG_PRG_lc,
case	
	when	program = 'AU' then (ERNG_PRG * 0.76) -- AUD
	when	program = 'UK' then (ERNG_PRG * 1.33) -- Pound Sterling
	when	program = 'US' then (ERNG_PRG * 1) 
	when	program = 'CA' then (ERNG_PRG * 1) 
	when	program = 'DE' then (ERNG_PRG * 1.18) -- Euro
	when	program = 'FR' then (ERNG_PRG * 1.18) -- Euro
	when	program = 'IT' then (ERNG_PRG * 1.18) -- Euro
	when	program = 'ES' then (ERNG_PRG * 1.18) -- Euro
	when	program = 'ROE' then (ERNG_PRG * 1.18) -- Euro
else	 ERNG_PRG
end	as ERNG_USD,
case	
	when	program = 'AU' then (REV_USD*1.0000/0.76) --not updated
	when	program = 'UK' then (REV_USD*1.0000/1.33) 
	when	program = 'US' then (REV_USD*1.0000/1) 
	when	program = 'CA' then (REV_USD*1.0000/1) 
else	 REV_USD
end	as REV_USD_lc,
case	
	when	program = 'AU' then (iGMB_USD*1.0000/0.76) --not updated
	when	program = 'UK' then (iGMB_USD*1.0000/1.33) 
	when	program = 'US' then (iGMB_USD*1.0000/1) 
	when	program = 'CA' then (iGMB_USD*1.0000/1) 
else	 iGMB_USD
end	as iGMB_USD_lc,
case	
	when	program = 'AU' then (iRev_USD*1.0000/0.76) --not updated
	when	program = 'UK' then (iRev_USD*1.0000/1.33) 
	when	program = 'US' then (iRev_USD*1.0000/1) 
	when	program = 'CA' then (iRev_USD*1.0000/1) 
else	 iRev_USD
end	as iRev_USD_lc

from	p_ci_map_t.jsh_fcst_5 a
join dw_cal_dt cal
	on	a.CK_TRANS_DT = cal.cal_dt)
with	data primary index(CK_TRANS_DT,program,BM);
''')
conn.commit()





curs.execute('''
create	volatile table Test_mult as (
Select	
a.*,
b.*
From	
(
Select	
MONTH_END_DT,                   
RETAIL_WK_END_DATE,             
QTR_END_DT,                     
program ,                       
BM  ,                           
--sum(ERNG_PRG_lc) as ERNG_PRG,
sum(ERNG_USD) as ERNG_USD,
sum(REV_USD) as REV_USD,
sum(iGMB_USD) as iGMB_USD,
sum(iRev_USD) as iRev_USD,
sum(iGMB_USD_Desktop) as iGMB_USD_Desktop,
sum(iGMB_USD_Mobile) as iGMB_USD_Mobile,
sum(iRev_USD_Desktop) as iRev_USD_Desktop,
sum(iREV_USD_Mobile) as iREV_USD_Mobile
--sum(WEEK_OF_YEAR_ID) as WEEK_OF_YEAR_ID,
--sum(YEAR_ID) as YEAR_ID
--sum(ERNG_PRG_lc) as ERNG_PRG_lc,
--sum(REV_USD_lc) as REV_USD_lc,
--sum(iGMB_USD_lc) as iGMB_USD_lc,
--sum(iRev_USD_lc) as iRev_USD_lc
from	p_ci_map_t.jsh_fcst_6
group	by 1,2,3,4,5) a
left join 
( 
Select	wk_end_dt,
fsc_wk,
fsc_mnth_num as fsc_mnth,
Case	
	when	fsc_mnth_num = 1 then 'Jan'
	when	fsc_mnth_num = 2 then 'Feb'
	when	fsc_mnth_num = 3 then 'Mar'
	when	fsc_mnth_num = 4 then 'Apr'
	when	fsc_mnth_num = 5 then 'May'
	when	fsc_mnth_num = 6 then 'Jun'
	when	fsc_mnth_num = 7 then 'Jul'
	when	fsc_mnth_num = 8 then 'Aug'
	when	fsc_mnth_num = 9 then 'Sep'
	when	fsc_mnth_num = 10 then 'Oct'
	when	fsc_mnth_num = 11 then 'Nov'
	when	fsc_mnth_num = 12 then 'Dec'
else	'Others'
end	as fsc_Mnth2,
fsc_qtr_num as Fsc_Qtr,
Fsc_Yr
from	
p_ci_map_t.sh_fsc_base
group	by 1,2,3,4,5,6) b
	on	a.RETAIL_WK_END_DATE = b.wk_end_dt
)
with	data primary index(MONTH_END_DT,RETAIL_WK_END_DATE,QTR_END_DT,
		program ,BM) 
	on	commit preserve rows;
''')
conn.commit()



curs.execute('''
create	volatile table test as(
select	* 
from	p_ci_map_t.jsh_EPN_FCST_Output a 
where	RETAIL_WK_END_DATE < (
select	min(RETAIL_WK_END_DATE) 
from	p_ci_map_t.jsh_fcst_6)
)
with	data 
	on	commit preserve rows;
''')
conn.commit()



curs.execute('''
Drop	table p_ci_map_t.jsh_EPN_FCST_Output;

''')
conn.commit()             

curs.execute('''
create	multiset table p_ci_map_t.jsh_EPN_FCST_Output  as (
Select	a.* 
from	test a
union	
select	b.* 
from	Test_mult b

)
with	data primary index(MONTH_END_DT,RETAIL_WK_END_DATE,QTR_END_DT,
		program ,BM  );
''')
conn.commit()


curs.execute('''
sel	*
from	p_ci_map_t.jsh_EPN_FCST_Output;
''')
conn.commit()

print 'Send eMail'

execfile('EmailSender_OUTLOOK.py')
conn.close()
exit(0)