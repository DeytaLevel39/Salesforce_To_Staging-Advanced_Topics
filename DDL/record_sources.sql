create schema if not exists data_vault;

create or replace table data_vault.ref_record_source
(id string,
 name string,
 system string,
 country_code string
 );

 insert into data_vault.ref_record_source values ('1','Customer Event Messages','SALESFORCE','UK');
 insert into data_vault.ref_record_source values ('2','Order Event Messages','SALESFORCE','UK');
 insert into data_vault.ref_record_source values ('101','Customer Event Messages','SALESFORCE','US');
 insert into data_vault.ref_record_source values ('102','Order Event Messages','SALESFORCE','UK');