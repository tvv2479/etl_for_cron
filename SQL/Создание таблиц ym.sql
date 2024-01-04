create table devices (
  id int primary KEY,
  devises_name varchar (20)
  );
  
select *from devices d ;

insert into devices(id, devises_name) 
values
(1, 'десктоп'), (2, 'мобильные телефоны'), (3, 'планшеты'), (4, 'TV');

-----------------------------------------

create table ym_hits_stranitsi2 (
  id serial PRIMARY KEY,
  counter_user_id_hash varchar(50),
  client_id varchar(50),
  watch_id varchar(50),
  date_event date,
  datetime_event timestamp,
  not_bounce smallint,
  link smallint,
  title varchar (300),
  URL varchar (4000)
  );
 

select * from ym_hits_stranitsi2;
SELECT count(*) FROM ym_hits_stranitsi; -- количество строк
SELECT pg_size_pretty( pg_total_relation_size( 'ym_hits_stranitsi' ) ); -- размер таблицы
delete from ym_hits_stranitsi_new; -- удалить строки
drop table ym_hits_stranitsi_new;

TRUNCATE ym_hits_stranitsi2 RESTART IDENTITY;
----------------------------------------

create table ym_hits_obshee2 (
  id serial,
  counter_user_id_hash varchar(50),
  client_id varchar(50),
  watch_id varchar(50),
  date_event date,
  datetime_event timestamp,
  counter_id numeric,
  device_category smallint,
  region_city varchar (50),
  region_city_id smallint,
  region_country varchar (50),
  region_country_id smallint,
  last_traffic_source varchar (30),
  referer varchar (4000),
  ip_address varchar (100),
  is_page_view smallint,
  artificial smallint,
  primary key (id),
  constraint fk_devises
  FOREIGN key (device_category) references devices(id)
  on delete set null
  );
 
 select * from ym_hits_obshee2;
delete from ym_hits_obshee_new;
SELECT count(*) FROM ym_hits_obshee; -- ���������� �������
SELECT pg_size_pretty( pg_total_relation_size( 'ym_hits_obshee' ) ); -- ������ �������
drop table ym_hits_obshee_new;

TRUNCATE ym_hits_obshee2 RESTART IDENTITY;

----------------------------------------------
 

create table ym_hits_deystviya2 (
  id serial PRIMARY KEY,
  counter_user_id_hash varchar(50),
  client_id varchar(50),
  watch_id varchar(50),
  date_event date,
  datetime_event timestamp,
  download smallint,
  goals_id varchar (300),
  last_social_network varchar (150),
  last_search_engine varchar (150),
  last_search_engine_root varchar (150),
  share_service int,
  share_URL varchar (2000),
  share_title varchar (600),
  last_social_network_profile varchar (600)
  );
 
 
select * from ym_hits_deystviya2;
 SELECT count(*) FROM ym_hits_deystviya; -- ���������� �������
SELECT pg_size_pretty( pg_total_relation_size( 'ym_hits_deystviya' ) ); -- ������ ������� 
delete from ym_hits_deystviya_new;
drop table ym_hits_deystviya_new;

TRUNCATE ym_hits_deystviya2 RESTART IDENTITY;
--------------------------------------------

 
 create table ym_hits_ad2(
  id serial PRIMARY KEY,
  counter_user_id_hash varchar(50),
  client_id varchar(50),
  watch_id varchar(50),
  date_event date,
  datetime_event timestamp,
  utm_campaign varchar(2000),
  utm_content varchar(2000),
  utm_medium varchar(2000),
  utm_source varchar(2000),
  utm_term varchar(2000),
  openstat_ad varchar(2000),
  openstat_campaign varchar(2000),
  openstat_service varchar(2000),
  openstat_source varchar(2000),
  last_adv_engine varchar(2000)
  );
 
   select * from ym_hits_ad2;
 SELECT count(*) FROM ym_hits_prosmotri; -- ���������� �������
SELECT pg_size_pretty( pg_total_relation_size( 'ym_hits_prosmotri' ) ); -- ������ �������
delete from ym_hits_prosmotri_new;

TRUNCATE ym_hits_ad2 RESTART IDENTITY;
------------------------------------------------


 create table ym_visits_obshee2(
    id serial,
    counter_user_id_hash varchar(50),
    client_id varchar(50),
    visit_id varchar(50),
    date_event date,
    datetime_event timestamp,
    counter_id numeric,
    is_new_user smallint,
    start_url varchar (1000),
    end_url varchar (1000),
    page_views smallint,
    visit_duration smallint,
    device_category smallint,
    region_country varchar (100),
    region_country_id int,
    region_city varchar (100),
    region_city_id int,
    bounce smallint,
    ip_address varchar(50),
    primary key (id),
    constraint fk_devises_2
    FOREIGN key (device_category) references devices(id)
    on delete set null
    );
   
    select * from ym_visits_obshee2;
  SELECT count(*) FROM ym_visits_obshee; -- ���������� �������
SELECT pg_size_pretty( pg_total_relation_size( 'ym_visits_obshee' ) ); -- ������ ������� 
delete from ym_visits_obshee;
drop table ym_visits_obshee_new;

TRUNCATE ym_visits_obshee2 RESTART IDENTITY;
------------------------------------------------

 
create table ym_visits_detali2(
  id serial primary KEY,
  counter_user_id_hash varchar(50),
  client_id varchar(50),
  visit_id varchar(50),
  date_event date,
  datetime_event timestamp,
  last_traffic_source varchar (50),
  last_adv_engine varchar (200),
  last_referal_source varchar (200),
  last_search_engine_root varchar (200),
  last_search_engine varchar (150),
  last_social_network varchar (150),
  last_social_network_profile varchar (600),
  referer varchar (2000),
  impressions_product_coupon text
  );
 
 select * from ym_visits_detali2;
SELECT count(*) FROM ym_visits_zakazi; -- ���������� �������
SELECT pg_size_pretty( pg_total_relation_size( 'ym_visits_zakazi' ) ); -- ������ ������� 

TRUNCATE ym_visits_detali2 RESTART IDENTITY;




-- ТЕХНИЧЕСКАЯ ЧАСТЬ


drop table ym_hits_stranitsi1, ym_hits_obshee1, 
ym_hits_obshee1, ym_hits_deystviya1, 
ym_hits_ad1, ym_visits_obshee1, ym_visits_detali1 ;
