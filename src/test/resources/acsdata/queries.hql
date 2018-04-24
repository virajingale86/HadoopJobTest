CREATE DATABASE mplus;

CREATE TABLE mplus.acs_data ( value string) partitioned by (eventdate string) row format delimited fields terminated by '\n' stored as textfile;

alter table mplus.acs_data add if not exists partition(eventdate="20180422") location '/acsdata/input/20180422/';

alter table mplus.acs_data add if not exists partition(eventdate="20180423") location '/acsdata/input/20180423/';

CREATE TABLE  mplus.acs_extract_daily (
     
     msisdn string,
     
     templatefilename string,
     
     provisioningupdatedatetime string,
     
     clientversion string,
     
     mnoid string,
     
     clientvendor string,
     
     terminalvendor string,
     
     terminalversion string,
     
     terminalmodel string,
     
     rcsversion string
          
     )
     partitioned by (eventdate string) row format delimited fields terminated by ',' stored as textfile;
     
alter table mplus.acs_extract_daily add if not exists partition(eventdate="2018/04/22") location '/acsdata/out/list/2018/04/22/acs_extract_1_day/';

alter table mplus.acs_extract_daily add if not exists partition(eventdate="2018/04/23") location '/acsdata/out/list/2018/04/23/acs_extract_1_day/';

insert overwrite table mplus.acs_extract_daily partition(eventdate="2018/04/22")
SELECT msisdn, templatefilename, if(isnull(pudt), pudt1,pudt) as provisioningupdatedatetime, clientversion,mnoid,clientvendor,
terminalvendor, terminalversion,terminalmodel,rcsversion from mplus.acs_data a LATERAL VIEW 
json_tuple(a.value, 'MSISDN','TemplateFileName','ProvisioningUpdateTimeDate', 
                    'ProvisioningUdateTimeDate','ClientVersion','MNOID','ClientVendor',
                    'TerminalVendor','TerminalSWVersion','TerminalModel','RCSVersion')
                    b as msisdn,templatefilename,pudt,pudt1,clientversion, mnoid,clientvendor, terminalvendor, terminalversion,terminalmodel,rcsversion
where a.eventdate="20180422";

insert overwrite table mplus.acs_extract_daily partition(eventdate="2018/04/23")
SELECT msisdn, templatefilename, if(isnull(pudt), pudt1,pudt) as provisioningupdatedatetime, clientversion,mnoid,clientvendor,
terminalvendor, terminalversion,terminalmodel,rcsversion from mplus.acs_data a LATERAL VIEW 
json_tuple(a.value, 'MSISDN','TemplateFileName','ProvisioningUpdateTimeDate', 
                    'ProvisioningUdateTimeDate','ClientVersion','MNOID','ClientVendor',
                    'TerminalVendor','TerminalSWVersion','TerminalModel','RCSVersion')
                    b as msisdn,templatefilename,pudt,pudt1,clientversion, mnoid,clientvendor, terminalvendor, terminalversion,terminalmodel,rcsversion
where a.eventdate="20180423";


select * from mplus.acs_extract_daily;
     
     