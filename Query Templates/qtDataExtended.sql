select
  step1.calculation_number,
  step1.calculation_version,
  step1.company_name,
  step1.client_id,
  step1.contract_period,
  step1.status,
  step1.creation_date,
  step1.decision_calculation,
  step1.SUB_LOA_DISCOUNT,
  step1.SUB_LOA_NRC,
  step1.SUB_LOA_CAPEX,
  step1.SUB_LOA_PROCESSING,
  cast(isnull(step1.VDATA26, 0.0) as float) * rate.day_rate as RESULT_RPP_GM,
  cast(isnull(step1.VDATA25, 0.0) as float) * rate.day_rate as RESULT_RPP_PB,
  step1.Reason_Lost__c, 
  step1.Reason_Lost_Comments__c, 
  step1.Stage_Lost__c,
  step1.CUST_TYPE,
  step1.item_id,
  step1.USER_FULL_NAME,
  step1.ACCEPTED_DATE,
  case when step1.renewal = 'T' then '*' else ' ' end renewal,
  step1.attr_descr,
  case	step1.attr_id 
		when 'mb_price_result' then 5
		when 'result_nrc' then 10
		when 'result_reve' then 15
		when 'RNL_prev_rev_result' then 17
		when 'RNL_price_change_result' then 18
		when 'location_city_ui' then 20
		when 'location_city_b_ui' then 23
		when 'location_street_b_ui' then 25
		when 'speed_ui' then 30
		when 'access_ui' then 45 -- IP Transit Only
		when 'access_type_ui' then 40
		when 'access_type_b_ui' then 45
		when 'sla_ui' then 50
		else 100
  end as attr_order,
  step1.attr_id,
  case 
        when attr_type='dictionary' then dict_values.COL1 
        -- Conversion from float to nvarchar is necessary 
        -- otherwise text values from other records are forced 
        -- to be convert to numbers too and it raises exception.
        when attr_datatype = 'Currency' and isnumeric(attr_value) <> 0 then cast(cast(attr_value as float) * rate.day_rate as nvarchar(100))
        -- Below case should never happend
        when attr_datatype = 'Currency' and isnumeric(attr_value) = 0 then 'Wrong currency value :'+attr_value+'!'
        else attr_value
  end 
  as value
from
(
  -- **************************************************************************
  select
    calculations.*,
    services.item_id,
	services.renewal,
    services.attr_descr,
    services.attr_id,
    services.attr_type,
    services.attr_value,
    services.attr_dict_name,
    services.attr_datatype
  from
  (
    -- **************************************************************************
    select
      -- *** Calculation info
      calc.CL_COUNTRY_CODE cl_country_code,
      calc.ID calculation_id,
      calc.NR_KALKULACJI as calculation_number,
      calc.WERSJA_KALKULACJI as calculation_version,
      calc.NAZWA_KLIENTA as company_name,
      calc.NIP as client_id,
      calc_result.NDATA13 as contract_period,
      calc_statuses_defs.OPIS as status,
      calc.DATA_WPR as creation_date,
	  --DATA_MOD only for STATUS_ID 5 = Accepted for Realization
	  case calc.STATUS_ID
		when 5 then calc.DATA_MOD
		else null
	  end as ACCEPTED_DATE,
      calc_result.VDATA2 as decision_calculation,
	  calc_result.VDATA12 as SUB_LOA_DISCOUNT,
	  calc_result.VDATA21 as SUB_LOA_NRC,
	  calc_result.VDATA22 as SUB_LOA_RENEWAL,
	  calc_result.VDATA4 as SUB_LOA_CAPEX,
	  calc_result.VDATA10 as SUB_LOA_PROCESSING,
	  calc_result.VDATA26,
	  calc_result.VDATA25,
	  cust_type.OPIS as CUST_TYPE,
      sfdc_opp.Reason_Lost__c, 
      sfdc_opp.Reason_Lost_Comments__c, 
      sfdc_opp.Stage_Lost__c,
	  (users.NAME +' '+users.SECOND_NAME) as USER_FULL_NAME
    from
      DSA.${BU}_CBCT.KALKULACJE calc
	  join DSA.${BU}_CBCT.TYPY_KLIENTOW cust_type on calc.TYP_KLIENTA_ID = cust_type.ID
      join DSA.${BU}_CBCT.WINIKY_KALKULACJI calc_result on calc_result.KALKULACJA_ID = calc.ID
      join DSA.${BU}_CBCT.STATUSY_KALKULACJI calc_statuses_defs on calc_statuses_defs.ID = calc.STATUS_ID
	  join DSA.${BU}_CBCT.USERS users on calc.OPR_WPR = users.ID
      left join DSA.SFDC.Opportunity sfdc_opp on calc.SFDC_OPPORTUNITY_NO = sfdc_opp.Opportunity_No__c  collate Czech_CI_AS
	where
     calc.DATA_WPR >= @date_from_cre
     and calc.DATA_WPR < dateadd(day, 1, @date_to_cre)

     and (@date_from_mod is null or calc.DATA_MOD >= @date_from_mod )
     and (@date_to_mod is null or calc.DATA_MOD < dateadd(day, 1, @date_to_mod))	  
	 and (@status = 0 or calc.STATUS_ID = @status)
  ) calculations
    -- **************************************************************************
  join
  (
    select
      -- *** ethernet info
      u1.KALKULACJA_ID calculation_id,
      u1.ID item_id,
	  u1.RENEWAL renewal,
      --au1.LABEL attr_descr,
	  case au1.IDENTYFIKATOR
		when 'mb_price_result' then 'Marketing Price'
		when 'result_nrc' then 'Non recurring revenue'
		when 'result_reve' then 'Recurring revenue'
		when 'location_city_ui' then 'Client location: City (A-end)'
		when 'location_city_b_ui' then 'Client location: City (B-end)'
		when 'location_street_b_ui' then 'Client location'
		when 'access_type_ui' then 'Access Type (A-end)'
		when 'access_type_b_ui' then 'Access Type (B-end)'
		when 'access_ui' then 'Access Type (B-end)' --only for IP Transit
		when 'RNL_prev_rev_result' then 'Previous recurring revenue'
		when 'RNL_price_change_result' then 'Price change at renewal'
		else au1.LABEL 
	  end attr_descr,
      au1.IDENTYFIKATOR attr_id,
      au1.TYP attr_type,
	  au1.DATATYPE attr_datatype,
      -- KOD='DUAL' for 2 level dictionaries, NULL otherwise
      case when au1.KOD='DUAL' then au1.SELECTED else au1.BVALUE end attr_value,
      case when au1.KOD='DUAL' then au1.VALUE else au1.DICT_NAME end attr_dict_name
    from
      DSA.${BU}_CBCT.USLUGI u1
      join DSA.${BU}_CBCT.ATRYBUTY_USLUGI au1 on au1.USLUGA_ID = u1.ID
      join DSA.${BU}_CBCT.WZORCE_USLUG wu1 on u1.WZORZEC_USLUGI_ID = wu1.ID
    where
      -- *** Filters
      -- and k.status_id = 5 -- STATUS: Contract is signed - Ready for instaltion
      u1.LICZ = 'T'
      and wu1.IDENTYFIKATOR in (@product)
      and au1.IDENTYFIKATOR in (
			'location_city_ui',
			'location_city_b_ui',
			'location_street_b_ui',
			'speed_ui',
			'access_ui',
			'access_type_ui',
			'access_type_b_ui',
			'sla_ui',
			'mb_price_result',
			'result_nrc',
			'result_reve',
			'RNL_prev_rev_result'
			--'RNL_price_change_result'
		)
         		
	  
  ) services on calculations.calculation_id = services.calculation_id
    -- **************************************************************************
where
     (
        --neni nektery ze 3 filtru
       (@date_from_status is null or @date_to_status is null )
       or
	   --mam je vsechny	
       exists (
         select hk.ID --, hk.NR_KALKULACJI, hk.WERSJA_KALKULACJI
         from DSA.${BU}_CBCT.HISTORIA_KALKULACJI hk
         WHERE -- reference to calculation
               hk.NR_KALKULACJI = calculations.calculation_number
               and hk.WERSJA_KALKULACJI = calculations.calculation_version
               -- filters
               and hk.DATA_MOD >= @date_from_status and hk.DATA_MOD < dateadd(day, 1, @date_to_status)
               and hk.STATUS_ID in (@status_changed)
       )
     )	
) step1

-- dictionary values decoding procedure below
left join DSA.${BU}_CBCT.WZORCE_SLOWNIKOW dict_def
  on dict_def.NAZWA = step1.attr_dict_name -- associate dictionary with given name
  and dict_def.COUNTRY_CODE in (step1.cl_country_code, 'ALL') -- associate only dictionary for calculation country or global
  -- filter only version of selected dictionary which was valid at calculation creation date
  and dict_def.WAZNE_OD <= step1.creation_date
  and (dict_def.WAZNE_DO is null or dict_def.WAZNE_DO >= step1.creation_date)
left join DSA.${BU}_CBCT.WARTOSCI_SLOWNIKOW dict_values
  on dict_values.WZORCE_SLOWNIKOW_ID = dict_def.ID
  and cast(dict_values.CODE as varchar(100)) = cast(step1.attr_value as varchar(100)) -- WARNING: In this line I have use a little trick to avoide text to number conversion error.
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
	from DSA.PL_CBCT.WZORCE_SLOWNIKOW s 
	JOIN DSA.PL_CBCT.WARTOSCI_SLOWNIKOW ws on ws.[WZORCE_SLOWNIKOW_ID]=s.[ID]
	where s.NAZWA = 'com_exchange_rates'
	  -- filter only version of selected dictionary which was valid at calculation creation date
	  and s.WAZNE_OD <= step1.creation_date
	  and (s.WAZNE_DO is null or s.WAZNE_DO >= step1.creation_date)
	  and ws.COL1 = @currency)  
	) as rate
	
	union all
	select 
	0, null, null, null, null, null, null, null, null, null,
	null, null, null, null, null, null, null, null, null, null,
	null, null, tmp.atrr_label, tmp.atrr_order, null, null
	FROM (VALUES ('Marketing Price', 5),('Non recurring revenue', 10),('Recurring revenue', 15),
				('Previous recurring revenue',17),--('Price change at renewal', 18),
				 ('Client location: City (A-end)', 20),('Client location: City (B-end)', 23),('Client location', 25),
				 ('Speed', 30),('Access Type (A-end)', 40),('Access Type (B-end)', 45),('SLA', 50)) as tmp(atrr_label,atrr_order)
order by calculation_number, calculation_version, item_id, attr_order, attr_id