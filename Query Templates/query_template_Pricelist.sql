SELECT  
	final.NR_KALKULACJI,
	final.WERSJA_KALKULACJI,
	final.STATUS_OPIS,
	final.USLUGAID,
	final.NAZWA_KLIENTA,
	final.TYP_WYCENY,
	final.KLIENT_OPIS,
	final.COMMENT_KALKULACJI,
	final.USER_FULL_NAME,
	final.USER_COUNTRY,
	final.BU,
	final.CONTRACT_PERIOD,
	final.RENEWAL_DECISION,
	final.NAZWA,
	final.LABEL,
	final.ATTR_ORDER,
	final.data_wpr,
	final.wu_identyfikator,
	lob.LOB,
	case 
		when attr_type='dictionary' then wartS.COL1
		when attr_datatype = 'Currency' and isnumeric(attr_value) <> 0 then cast(cast(attr_value as float) * rate.day_rate as nvarchar(100))
        when attr_datatype = 'Currency' and isnumeric(attr_value) = 0 then 'Wrong currency value :'+attr_value+'!'
	    else attr_value
	end as VALUE
FROM (
SELECT
      k.CL_COUNTRY_CODE as country_code,
	  k.DATA_WPR as data_wpr,
	  au.TYP as attr_type,
	  u.ID as USLUGAID,
	  k.NR_KALKULACJI,
	  k.WERSJA_KALKULACJI,
	  sk.OPIS as STATUS_OPIS,
	  k.OPIS as COMMENT_KALKULACJI,
	  k.NAZWA_KLIENTA,
	  k.TYP_WYCENY,
	  tk.OPIS as KLIENT_OPIS,
	  (users.NAME +' '+users.SECOND_NAME) as USER_FULL_NAME,
	  users.USER_COUNTRY,
	  '${BU}' as BU,
	  wk.NDATA13 as CONTRACT_PERIOD,
	  wk.VDATA22 as RENEWAL_DECISION,
	  wu.NAZWA,
      case au.IDENTYFIKATOR
		when 'recurring_reve_ui' then 'Recurring revenue Final Price'
		when 'result_sgst_price' then 'Recurring revenue Suggested Price'
		when 'nrc_ui' then 'Non-recurring revenue Final Price'
		when 'nrc_sgst_result' then 'Non-recurring revenue Suggested Price'
		when 'result_mrc_discount' then 'MRC discount'
		when 'result_nrc_discount' then 'NRC discount'
		when 'country_ui' then 'Local BU Price List'
		when 'RNL_prev_rev_result' then 'Previous recurring revenue'
		else au.LABEL 
	  end as LABEL,
	  case au.IDENTYFIKATOR
		when 'country_ui' then 5
		when 'location_city_b_ui' then 10
		when 'speed_ui' then 15
		when 'result_sgst_price' then 20
		when 'recurring_reve_ui' then 25
		when 'result_mrc_discount' then 30
		when 'result_decision_mkt' then 33
		when 'nrc_sgst_result' then 35
		when 'nrc_ui' then 40
		when 'result_nrc_discount' then 45
		when 'result_decision_nrc' then 50
		when 'RNL_prev_rev_result' then 51
		when 'RNL_price_change_result' then 55
		else 100
	  end as ATTR_ORDER,
      case when au.KOD='DUAL' then au.SELECTED else au.BVALUE end attr_value,
      case when au.KOD='DUAL' then au.VALUE else au.DICT_NAME end attr_dict_name,
	  au.BVALUE,
	  au.DATATYPE as attr_datatype,
	  wu.IDENTYFIKATOR as wu_identyfikator
  FROM DSA.${BU}_CBCT.KALKULACJE k
  join DSA.${BU}_CBCT.STATUSY_KALKULACJI sk on sk.ID = k.STATUS_ID
  join DSA.${BU}_CBCT.TYPY_KLIENTOW tk on k.TYP_KLIENTA_ID = tk.ID
  join DSA.${BU}_CBCT.WINIKY_KALKULACJI wk on wk.KALKULACJA_ID = k.ID
  join DSA.${BU}_CBCT.USERS users on k.OPR_WPR = users.ID
  join DSA.${BU}_CBCT.USLUGI u on u.KALKULACJA_ID = k.ID
  join DSA.${BU}_CBCT.ATRYBUTY_USLUGI au on au.USLUGA_ID = u.ID
  join DSA.${BU}_CBCT.WZORCE_USLUG wu on u.WZORZEC_USLUGI_ID = wu.ID
  left outer join ( select hk.NR_KALKULACJI, hk.WERSJA_KALKULACJI, Min(hk.DATA_MOD) as min_date
           from DSA.${BU}_CBCT.HISTORIA_KALKULACJI hk
           where hk.STATUS_ID = 15
           group by hk.NR_KALKULACJI, hk.WERSJA_KALKULACJI
    ) as h1 on k.NR_KALKULACJI = h1.NR_KALKULACJI and k.WERSJA_KALKULACJI = h1.WERSJA_KALKULACJI
  WHERE wu.IDENTYFIKATOR LIKE '%pricelist%'
  AND au.IDENTYFIKATOR in 
  ('result_sgst_price','recurring_reve_ui', 'result_mrc_discount','result_decision_mkt',
   'nrc_sgst_result','nrc_ui','result_nrc_discount','result_decision_nrc',
   'country_ui','location_city_b_ui','speed_ui','RNL_prev_rev_result')--,'RNL_price_change_result')
  and k.DATA_WPR >= @date_from_cre
  and k.DATA_WPR < dateadd(day, 1, @date_to_cre)
  and (@date_from_mod is null or k.DATA_MOD >= @date_from_mod )
  and (@date_to_mod is null or k.DATA_MOD < dateadd(day, 1, @date_to_mod))
  and (@date_from_appr is null or h1.min_date >= @date_from_appr)
     and (@date_to_appr is null or h1.min_date < dateadd(day, 1, @date_to_appr))
  and (@calc_num is null or k.NR_KALKULACJI = @calc_num)
  and (@loa = 0 or wk.VDATA2 = @loa)
  and (@client is null or lower(k.NAZWA_KLIENTA) like '%' + lower(@client) + '%')
  and (@contract_len is null or cast(wk.NDATA13 as int) = @contract_len)
  and (@customer_type is null or k.TYP_KLIENTA_ID = @customer_type)
  and (@mrc_low is null or cast(wk.VDATA5 as float) >= @mrc_low)
  and (@mrc_high is null or cast(wk.VDATA5 as float) <= @mrc_high)
  and (@status = 0 or k.STATUS_ID = @status)
  and (
       (@date_from_status is null or @date_to_status is null )
       or
       exists (
         select hk.ID
         from DSA.${BU}_CBCT.HISTORIA_KALKULACJI hk
         WHERE
               hk.NR_KALKULACJI = k.NR_KALKULACJI
               and hk.WERSJA_KALKULACJI = k.WERSJA_KALKULACJI
               and hk.DATA_MOD >= @date_from_status and hk.DATA_MOD < dateadd(day, 1, @date_to_status)
               and hk.STATUS_ID in (@status_changed)
       )
     )
   )final
  left join DSA.${BU}_CBCT.WZORCE_SLOWNIKOW ws
	on ws.NAZWA = final.attr_dict_name
	and ws.COUNTRY_CODE in (final.country_code, 'ALL')
	and ws.WAZNE_OD <= final.data_wpr
	and (ws.WAZNE_DO is null or ws.WAZNE_DO >= final.data_wpr)
  left join DSA.${BU}_CBCT.WARTOSCI_SLOWNIKOW wartS
	on wartS.WZORCE_SLOWNIKOW_ID = ws.ID
	and cast(wartS.CODE as varchar(100)) = cast(final.attr_value as varchar(100))
  outer apply (
	-- convert currency using BCT com_exchange_rates dictionary
	(select top 1
		isnull(1/cast(
			case '${BU}'
			  when s.NAME2 then ws.COL2 
			  when s.NAME3 then ws.COL3
			  when s.NAME4 then ws.COL4
			  when s.NAME5 then ws.COL5
			  when s.NAME6 then ws.COL6
			  when s.NAME7 then ws.COL7    
			end as float
		),0.0) as day_rate
	from DSA.${BU}_CBCT.WZORCE_SLOWNIKOW s 
	JOIN DSA.${BU}_CBCT.WARTOSCI_SLOWNIKOW ws on ws.[WZORCE_SLOWNIKOW_ID]=s.[ID]
	where s.NAZWA = 'com_exchange_rates'
	  -- filter only version of selected dictionary which was valid at calculation creation date
	  and s.WAZNE_OD <= final.data_wpr
	  and (s.WAZNE_DO is null or s.WAZNE_DO >= final.data_wpr)
	  and ws.COL1 = @currency)  
	) as rate
	join (
   SELECT 
      ws.COL1,
    case 1
      when ws.COL3 then s.NAME3
      when ws.COL4 then s.NAME4
      when ws.COL5 then s.NAME5
      when ws.COL6 then s.NAME6
      when ws.COL7 then s.NAME7
      else 'Service not corresponding to LOB - error in BCT com_lobs dictionary' 
    end as LOB
   FROM DSA.PL_CBCT.WZORCE_SLOWNIKOW s
    JOIN DSA.PL_CBCT.WARTOSCI_SLOWNIKOW ws on ws.WZORCE_SLOWNIKOW_ID=s.ID
    where s.NAZWA = 'com_lobs' and s.WAZNE_DO is null
     ) as lob on lob.COL1 = final.wu_identyfikator
	 and (lob.LOB in (@lob)
          or ('0' in (@lob) and lob.LOB is null ))
  union all
  select 
	0, null, null, null, null, null, null, null, null, null, null,
	null, null, null, tmp.atrr_label, tmp.atrr_order, null, null, null, null
	FROM (VALUES ('Local BU Price List', 5),('City', 10),('Bandwidth', 15),--('Price change at renewal', 55)
				 ('Recurring revenue Suggested Price', 20),('Recurring revenue Final Price', 25),
				 ('MRC discount', 30),('Price List MRC Decision', 33),('Non-recurring revenue Suggested Price', 35),
				 ('Non-recurring revenue Final Price', 40),('NRC discount', 45),			 			 
				 ('NRC Decision', 50),('Previous recurring revenue',51)) as tmp(atrr_label,atrr_order)
ORDER BY NR_KALKULACJI DESC, WERSJA_KALKULACJI, USLUGAID, ATTR_ORDER