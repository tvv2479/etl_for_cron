-- Orders
select bso.id, user_id, bso.status_id, bso.account_number, bso.additional_info, bso.affiliate_id, bso.allow_delivery, bso.bx_user_id, bso.canceled, bso."comments", bso.company_id, bso.created_by, bso.currency, 
       bso.date_allow_delivery, bso.date_bill, bso.date_canceled, bso.date_deducted, bso.date_insert, bso.date_lock, bso.date_marked, bso.date_pay_before, bso.date_payed, bso.date_status, bso.date_update, 
       bso.deducted, bso.delivery_doc_date, bso.delivery_doc_num, bso.delivery_id, bso.discount_value, bso.emp_allow_delivery_id, bso.emp_canceled_id, bso.emp_deducted_id, bso.emp_marked_id, bso.emp_payed_id, 
       bso.emp_status_id, bso.external_order, bso.id_1c, bso.is_recurring, bso.is_sync_b24, bso.lid, bso.locked_by, bso.marked, bso.order_topic, bso.pay_system_id, bso.pay_voucher_date, bso.pay_voucher_num, 
       bso.payed, bso.person_type_id, bso.price, bso.price_delivery, bso.price_payment, bso.ps_currency, bso.ps_response_date, bso.ps_status, bso.ps_status_code, bso.ps_status_description, bso.ps_status_message,
       bso.ps_sum, bso.reason_canceled, bso.reason_marked, bso.reason_uno_deducted, bso.recount_flag, bso.recurring_id, bso.reserved, bso.responsible_id, bso.running, bso.search_content, bso.stat_gid,
       bso.store_id, bso.sum_paid, bso.tax_value, bso.tracking_number, bso.updated_1c, bso.user_description, bso."version", bso.version_1c,
       bsopv.value as promocode
  from b_sale_order bso
       join (select order_id,
                    value
               from b_sale_order_props_value  
              where name = 'Промокод') as bsopv
             on bso.id = bsopv.order_id
 where date(bso.date_insert) BETWEEN '{date_start}' AND date_add(CURDATE(), INTERVAL -1 day)
              
-- Users
SELECT id, date_register, timestamp_x, active, admin_notes, auto_time_zone, "blocked", bx_user_id, checkword, checkword_time, confirm_code, email, external_auth_id, language_id, last_activity_date, last_login,
       "name", last_name, second_name, lid, login, login_attempts, "password", password_expired, personal_birthdate, personal_birthday, personal_city, personal_country, personal_fax, personal_gender,
       personal_icq, personal_mailbox, personal_mobile, personal_notes, personal_pager, personal_phone, personal_photo, personal_profession, personal_state, personal_street, personal_www, personal_zip,
       stored_hash, time_zone, time_zone_offset, title, work_city, work_company, work_country, work_department, work_fax, work_logo, work_mailbox, work_notes, work_pager, work_phone, work_position,
       work_profile, work_state, work_street, work_www, work_zip
  FROM b_user
 where date(date_register) BETWEEN date('{date_start}') AND date_add(CURDATE(), INTERVAL -1 day)
 
-- Fuser 
SELECT id, date_insert, date_update, user_id, code
  FROM b_sale_fuser
 where date(date_insert) BETWEEN '{date_start}' AND date_add(CURDATE(), INTERVAL -1 DAY)
 
 
-- Basket
SELECT id, order_id, fuser_id, barcode_multi, base_price, callback_func, can_buy, cancel_callback_func, catalog_xml_id, currency, custom_price, date_insert, date_refresh, date_update, deducted, delay, detail_page_url,
       demensions, discount_coupon, discount_name, discount_price, discount_value, lid, marking_code_group, measure_code, measure_name, "module", "name", notes, order_callback_func, pay_callback_func,
       price, price_type_id, product_id, priduct_price_id, product_provider_class, product_xml_id, quantity, recommendation, reserve_quantity, reserved, set_parent_id, sort, subscribe, "type", vat_included,
       vat_rate, weight
  FROM b_sale_basket
 where date(date_insert) BETWEEN '{date_start}' AND date_add(CURDATE(), INTERVAL -1 day)
  
-- Guest
SELECT id, timestamp_x, favorites, c_events, sessions, hits, repair, first_session_id, first_date, first_url_from, first_url_to, first_url_to_404, first_site_id, first_adv_id, first_referer1, first_referer2, 
       first_referer3, last_session_id, last_date, last_user_id, last_user_auth, last_url_last, last_url_last_404, last_user_agent, last_ip, last_cookie, last_language, last_adv_id, last_adv_back, last_referer1, 
       last_referer2, last_referer3, last_site_id, last_country_id, LAST_city_id, last_city_info
  FROM b_stat_guest
 where date(first_date) BETWEEN date('{date_start}') AND date_add(CURDATE(), INTERVAL -1 day)
 
 
-- Session
SELECT id, guest_id, new_guest, user_id, user_auth, c_event, hits, favorites, url_from, url_to, url_to_404, url_last, url_last_404, user_agent, date_stat, date_first, date_last, ip_first, 
       ip_first_number, ip_last, ip_last_number, first_hit_id, last_hit_id, adv_id, adv_back, referer1, referer2, referer3, stop_list_id, country_id, city_id, first_site_id, last_site_id
   FROM b_stat_session
  where date(date_stat) BETWEEN date('{date_start}') AND date_add(CURDATE(), INTERVAL -1 day)
  
  
-- Order_props_value
select bsopv.order_id,
       bso.date_insert,
       bso.user_id,
       max(case when order_props_id = 2 then value end) as city,
       max(case when order_props_id = 6 then value end) as email,
       max(case when order_props_id = 17 then value end) as company,
       max(case when order_props_id = 19 then value end) as first_name,
       max(case when order_props_id = 18 then value end) as last_name,
       max(case when order_props_id = 20 then value end) as phone,
       max(case when order_props_id = 23 then value end) as promocode,
       max(case when order_props_id = 24 then value end) as customer_type_shop,
       max(case when order_props_id = 25 then value end) as customer_type_org,
       max(case when order_props_id = 26 then value end) as customer_type_compsny,
       max(case when order_props_id = 27 then value end) as customer_type_personal,
       min(entity_type)  as entity_type 
  from b_sale_order_props_value as bsopv
       join b_sale_order bso
            on bsopv.order_id = bso.id
 where date(bso.date_insert) BETWEEN date('{date_start}') AND date_add(CURDATE(), INTERVAL -1 day)
 group by order_id, user_id
 
-- Status
SELECT id, sort, "type", "notify", color
  FROM b_sale_status
  
-- Transact
select id, timestamp_x, transact_date, order_id, user_id, amount, currency, current_budget, debit, description, employee_id, notes, payment_id
  from b_sale_user_transact
  
-- Order change
select id, order_id, user_id, "data", date_create, date_modify, entity, entity_id, "type"
  from b_sale_order_change
  
-- City
 select id, "name", region_id, short_name
   from b_sale_location_city
   
-- Country
select id,"name", short_name
  from b_sale_location_country
  
  
 
 
 
 
 
 