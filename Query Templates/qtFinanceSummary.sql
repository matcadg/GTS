select * from (
select distinct
      k.ID,
      k.NR_KALKULACJI as CALC_NUMBER,
      k.WERSJA_KALKULACJI as VERSION,
	  case when k.COSTING_UPDATE = 'N' then 'No'
           when k.COSTING_UPDATE = 'Y' then 'Yes'
      end as COSTING_UPDATE,
      k.STATUS_ID, s.OPIS as STATUS,
      k.NAZWA_KLIENTA as CUST_NAME,
      k.NIP,
      k.TYP_WYCENY as BID_APPR_TYPE,
	  k.OPIS as COMMENT,
      tk.OPIS as CUST_TYPE,
      (u.SECOND_NAME + ' ' + u.NAME) as SALESMAN_NAME,
      u.USER_COUNTRY,
      u.BUSSINESS_UNIT as USER_BU,
      '${BU}' as BU,

      cast(wk.NDATA1 as float)  * rate.day_rate  as ONE_OFF_OPEX,
      cast(wk.NDATA2 as float)  * rate.day_rate  as TOT_CAPEX,
      cast(wk.NDATA4 as float)  * rate.day_rate  as NDATA4,
      isnull(wk.NDATA7, 0.0) as NDATA7,
      cast(wk.NDATA10 as float)  * rate.day_rate  as NPV,
      cast(wk.NDATA11 as float) as GROSS_MARGIN,
      cast(wk.NDATA13 as int) as CONTRACT_PERIOD,
      cast(wk.NDATA16 as float)  * rate.day_rate  as TOT_REC_COST,
      cast(wk.NDATA17 as float)  * rate.day_rate  as TOT_REC_REVE,
	  cast(wk.NDATA23 as float) * rate.day_rate as TOT_PREV_REVE, -- CBCTPL-913
	  -- current MRC - (TOT_PREV_REVE or OLD_MON_RECUR_REVE or 0)
	  (cast(wk.VDATA5 as float) - cast(isnull(wk.NDATA23, isnull(old_rec_reve.val,0)) as float)) * rate.day_rate  as TOT_PREV_REVE_CHANGE, -- CBCTPL-913
	  cast(wk.VDATA23 as float)  * rate.day_rate  as REVE_CHANGE_TEST,
	  (wk.VDATA1) as PAYBACK,	
      cast(wk.VDATA2 as int) as LOA,
      wk.VDATA12 as SUB_LOA_DISCOUNT,
	  wk.VDATA20 as SUB_LOA_PRICELIST,
	  wk.VDATA21 as SUB_LOA_NRC,
	  wk.VDATA9 as SUB_LOA_VOICE_FRACTION,
	  wk.VDATA10 as SUB_LOA_PROCESSING,
	  wk.VDATA22 as SUB_LOA_RENEWAL,
	  cast(wk.NDATA20 as float) * rate.day_rate as RESULT_SUGST_PRICE,
	  --cast(wk.VDATA17 as float) * rate.day_rate as RESULT_RPP, -- cast(wk.VDATA17 as float) as RESULT_RPP,
	  cast(isnull(wk.VDATA26, 0.0) as float) * rate.day_rate as RESULT_RPP_GM,
	  cast(isnull(wk.VDATA25, 0.0) as float) * rate.day_rate as RESULT_RPP_PB,
	  cast(wk.NDATA18 as float) * rate.day_rate as RESULT_MBP,
	  case 
		when VDATA11 in ('-') then NULL
		WHEN RIGHT(VDATA11, 1) = '%' then CAST(LEFT(VDATA11, len(VDATA11)-1) AS float) / 100
		ELSE CAST(VDATA11 as float)
		end as RESULT_MRC_DISCOUNT, -- cast(wk.VDATA11 as float) as RESULT_MRC_DISCOUNT,
      wk.VDATA4 as SUB_LOA_CAPEX,
      cast(wk.VDATA5 as float)  * rate.day_rate  as TOT_MON_RECUR_REVE,
      
      k.DATA_WPR as CREATED,
      k.DATA_MOD as MODIFIED,

      h1.min_date APPROVED,
      h2.min_date APPROVED_OE,
      h3.min_date APPROVAL_FILTER,

      case when k.ZMIANA_WARUNKOW_UMOWY = 'N' then 'No'
           when k.ZMIANA_WARUNKOW_UMOWY = 'Y' then 'Yes'
      end as CHANGE,

      lob.LOB, rate.day_rate * lob.val as LOB_VAL,

      cast(wk.NDATA12 as float) * rate.day_rate as NON_RECUR_REVE,
	  -- till 2011.11.03 was: rate.day_rate * reve.val as NON_RECUR_REVE,
	  
      rate.day_rate * old_rec_reve.val as OLD_MON_RECUR_REVE,
      rate.day_rate * opex.val as MON_RECUR_OPEX,
      rate.day_rate * old_rec_opex.val as OLD_MON_RECUR_OPEX,
      rate.day_rate * capex.val as REUS_CAPEX,
      rate.day_rate * net_capex.val as NETWORK_CAPEX,
	  
	  new_renewal_count.renewed_items as renewed_items_count,
	  new_renewal_count.all_items as all_items_count

from
    DSA.${BU}_CBCT.KALKULACJE k
    join DSA.${BU}_CBCT.WINIKY_KALKULACJI wk on wk.KALKULACJA_ID = k.ID
    join DSA.${BU}_CBCT.STATUSY_KALKULACJI s on k.STATUS_ID = s.ID
    join DSA.${BU}_CBCT.TYPY_KLIENTOW tk on k.TYP_KLIENTA_ID = tk.ID
    join DSA.${BU}_CBCT.USERS u on k.OPR_WPR = u.ID
    left outer join ( select hk.NR_KALKULACJI, hk.WERSJA_KALKULACJI, Min(hk.DATA_MOD) as min_date
           from DSA.${BU}_CBCT.HISTORIA_KALKULACJI hk
           where hk.STATUS_ID = 15
           group by hk.NR_KALKULACJI, hk.WERSJA_KALKULACJI
    ) as h1 on k.NR_KALKULACJI = h1.NR_KALKULACJI and k.WERSJA_KALKULACJI = h1.WERSJA_KALKULACJI
    left outer join ( select hk.NR_KALKULACJI, hk.WERSJA_KALKULACJI, Min(hk.DATA_MOD) as min_date
           from DSA.${BU}_CBCT.HISTORIA_KALKULACJI hk
           where hk.STATUS_ID = 5
           group by hk.NR_KALKULACJI, hk.WERSJA_KALKULACJI
    ) as h2 on k.NR_KALKULACJI = h2.NR_KALKULACJI and k.WERSJA_KALKULACJI = h2.WERSJA_KALKULACJI
    left outer join ( select hk.NR_KALKULACJI, /*hk.WERSJA_KALKULACJI,*/ Min(hk.DATA_MOD) as min_date
           from DSA.${BU}_CBCT.HISTORIA_KALKULACJI hk
           where hk.STATUS_ID = 8
           group by hk.NR_KALKULACJI --, hk.WERSJA_KALKULACJI
    ) as h3 on k.NR_KALKULACJI = h3.NR_KALKULACJI --and k.WERSJA_KALKULACJI = h3.WERSJA_KALKULACJI
--    join DSA.${BU}_CBCT.USLUGI us on us.KALKULACJA_ID = k.ID

    left outer join
    (select /*us.ID,*/ us.KALKULACJA_ID, /*us.WZORZEC_USLUGI_ID, pl.IDENTYFIKATOR,*/ pl.LOB,
       sum(cast( isnull(au."BVALUE", 0.0) as float)) val
       from  DSA.${BU}_CBCT.USLUGI us
             join DSA.${BU}_CBCT.ATRYBUTY_USLUGI au on au.USLUGA_ID = us.ID and au.IDENTYFIKATOR = 'result_reve'
             join DSA.${BU}_CBCT.WZORCE_USLUG wu on wu.ID = us.WZORZEC_USLUGI_ID
             left outer join DSA.${BU}_CBCT.PRODUCT2LOB_T pl on pl.IDENTYFIKATOR = wu.IDENTYFIKATOR
     where us.LICZ = 'T' and wu.PODRZEDNA = 'N'
			 and pl.LOB in (@lob)
       group by /*us.ID,*/ us.KALKULACJA_ID, /*us.WZORZEC_USLUGI_ID, pl.IDENTYFIKATOR,*/ pl.LOB
    ) as lob on lob.KALKULACJA_ID = k.ID

    left outer join
    (
     select us.KALKULACJA_ID, sum(cast( isnull(au."BVALUE", 0.0) as float)) val
     from  DSA.${BU}_CBCT.USLUGI us
           join DSA.${BU}_CBCT.ATRYBUTY_USLUGI au on au.USLUGA_ID = us.ID and au.IDENTYFIKATOR in ('nrc_ui', 'non_recurring_revenue_ui')
           join DSA.${BU}_CBCT.WZORCE_USLUG wu on wu.ID = us.WZORZEC_USLUGI_ID
     where us.LICZ = 'T' and wu.PODRZEDNA = 'N'
     group by us.KALKULACJA_ID
    ) as reve on reve.KALKULACJA_ID = k.ID

    left outer join
    (
     select us.KALKULACJA_ID, sum(cast( isnull(au."BVALUE", 0.0) as float)) val
     from  DSA.${BU}_CBCT.USLUGI us
           join DSA.${BU}_CBCT.ATRYBUTY_USLUGI au on au.USLUGA_ID = us.ID and au.IDENTYFIKATOR = 'PT_recurring_reve_ui'
           join DSA.${BU}_CBCT.WZORCE_USLUG wu on wu.ID = us.WZORZEC_USLUGI_ID
     where us.LICZ = 'T' and wu.PODRZEDNA = 'N'
     group by us.KALKULACJA_ID
    ) as old_rec_reve on old_rec_reve.KALKULACJA_ID = k.ID

    left outer join
    (
     select us.KALKULACJA_ID, sum(cast( isnull(au."BVALUE", 0.0) as float)) val
     from  DSA.${BU}_CBCT.USLUGI us
           join DSA.${BU}_CBCT.ATRYBUTY_USLUGI au on au.USLUGA_ID = us.ID and au.IDENTYFIKATOR = 'result_cost'
           join DSA.${BU}_CBCT.WZORCE_USLUG wu on wu.ID = us.WZORZEC_USLUGI_ID
     where us.LICZ = 'T' and wu.PODRZEDNA = 'N'
     group by us.KALKULACJA_ID
    ) as opex on opex.KALKULACJA_ID = k.ID

    left outer join
    (
     select us.KALKULACJA_ID, sum(cast( isnull(au."BVALUE", 0.0) as float)) val
     from  DSA.${BU}_CBCT.USLUGI us
           join DSA.${BU}_CBCT.ATRYBUTY_USLUGI au on au.USLUGA_ID = us.ID and au.IDENTYFIKATOR = 'PT_monthly_opex_ui'
           join DSA.${BU}_CBCT.WZORCE_USLUG wu on wu.ID = us.WZORZEC_USLUGI_ID
     where us.LICZ = 'T' and wu.PODRZEDNA = 'N'
     group by us.KALKULACJA_ID
    ) as old_rec_opex on old_rec_opex.KALKULACJA_ID = k.ID

    left outer join
    (
     select us.KALKULACJA_ID, sum(cast( isnull(cast(au."BVALUE" as float), 0.0) as float)) val
     from  DSA.${BU}_CBCT.USLUGI us
           join DSA.${BU}_CBCT.ATRYBUTY_USLUGI au on au.USLUGA_ID = us.ID and au.IDENTYFIKATOR = 'result_capex'
     where us.LICZ = 'T'
     group by us.KALKULACJA_ID
    ) as capex on capex.KALKULACJA_ID = k.ID

    left outer join
    (
     select us.KALKULACJA_ID, sum(cast( isnull(au."BVALUE", 0.0) as float)) val
     from  DSA.${BU}_CBCT.USLUGI us
           join DSA.${BU}_CBCT.ATRYBUTY_USLUGI au on au.USLUGA_ID = us.ID 
					and au.IDENTYFIKATOR in ('capex_network_ui', 'capex_network_ui1','capex_network_ui2','network_capex_ui')
           join DSA.${BU}_CBCT.WZORCE_USLUG wu on wu.ID = us.WZORZEC_USLUGI_ID
     where us.LICZ = 'T' and wu.PODRZEDNA = 'N'
     group by us.KALKULACJA_ID
    ) as net_capex on net_capex.KALKULACJA_ID = k.ID

	-- Count calculation items with info about 'new' or 'renewal'
    left outer join
    (
     select us.KALKULACJA_ID, sum(case when us.RENEWAL='T' then 1 else 0 end) as renewed_items, count(*) as all_items
     from  DSA.${BU}_CBCT.USLUGI us
           join DSA.${BU}_CBCT.WZORCE_USLUG wu on wu.ID = us.WZORZEC_USLUGI_ID
     where us.LICZ = 'T' and wu.PODRZEDNA = 'N'
     group by us.KALKULACJA_ID
    ) as new_renewal_count on new_renewal_count.KALKULACJA_ID = k.ID
	
	outer apply (
/*       select
         case when @currency = 'EUR' then
          (select top 1
                 isnull( cast((rate_from.RATE/rate_from.AMOUNT) / (rate_to.RATE/rate_to.AMOUNT) as decimal(38,8)) ,
                         0.0) --as day_rate
          from DSA.DUR.PLIST_CURRENCY_RATE rate_from
               join DSA.DUR.PLIST_CURRENCY_RATE rate_to on rate_from.DT = rate_to.DT and rate_from.CURRENCY_ID <> rate_to.CURRENCY_ID
          where convert(varchar(8), rate_from.DT, 112) <= convert(varchar(8), k.DATA_WPR, 112)
		        and rate_from.CURRENCY_ID = 'PLN' --@CurrFrom
				and rate_to.CURRENCY_ID = 'EUR' --@CurrTo
          order by rate_from.DT desc)
          
             else 1.0
                     end as day_rate
*/
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
	  and s.WAZNE_OD <= k.DATA_WPR
	  and (s.WAZNE_DO is null or s.WAZNE_DO >= k.DATA_WPR)
	  and ws.COL1 = @currency)  
	) as rate
where
     k.DATA_WPR >= @date_from_cre
     and k.DATA_WPR < dateadd(day, 1, @date_to_cre)

     and (@date_from_mod is null or k.DATA_MOD >= @date_from_mod )
     and (@date_to_mod is null or k.DATA_MOD < dateadd(day, 1, @date_to_mod))

     and (@date_from_appr is null or h1.min_date >= @date_from_appr)
     and (@date_to_appr is null or h1.min_date < dateadd(day, 1, @date_to_appr))

     and (@calc_num is null or k.NR_KALKULACJI = @calc_num)
     and (@status = 0 or k.STATUS_ID = @status)
     and (@loa = 0 or wk.VDATA2 = @loa)
     and (@client is null or lower(k.NAZWA_KLIENTA) like '%' + lower(@client) + '%')

     and (@contract_len is null or cast(wk.NDATA13 as int) = @contract_len)
     and (@customer_type is null or k.TYP_KLIENTA_ID = @customer_type)

     and (@capex_low is null or cast(wk.NDATA2 as float) >= @capex_low)
     and (@capex_high is null or cast(wk.NDATA2 as float) <= @capex_high)
     and (@mrc_low is null or cast(wk.VDATA5 as float) >= @mrc_low)
     and (@mrc_high is null or cast(wk.VDATA5 as float) <= @mrc_high)

     and (lob.LOB in (@lob)
                 or ('0' in (@lob) and lob.LOB is null ))

     and
     (
        --neni nektery ze 3 filtru
       (@date_from_status is null or @date_to_status is null )
       or
	   --mam je vsechny	
       exists (
         select hk.ID --, hk.NR_KALKULACJI, hk.WERSJA_KALKULACJI
         from DSA.${BU}_CBCT.HISTORIA_KALKULACJI hk
         WHERE -- reference to calculation
               hk.NR_KALKULACJI = k.NR_KALKULACJI
               and hk.WERSJA_KALKULACJI = k.WERSJA_KALKULACJI
               -- filters
               and hk.DATA_MOD >= @date_from_status and hk.DATA_MOD < dateadd(day, 1, @date_to_status)
               and hk.STATUS_ID in (@status_changed)
       )
     )

union all

select 
      0,0,null,
      null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 
      null, null, null, null, null, null, null,
	  'Internet', 0,
      null, null, null, null, null, null, null, null

union all

select 
      0,0,null,
      null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 
      null, null, null, null, null, null, null,
	  'Data', 0,
      null, null, null, null, null, null, null, null

union all

select 
      0,0,null,
      null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 
      null, null, null, null, null, null, null,
	  'Voice', 0,
      null, null, null, null, null, null, null, null

union all

select 
      0,0,null,
      null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 
      null, null, null, null, null, null, null,
	  'Other', 0,
      null, null, null, null, null, null, null, null
	  
union all

select 
      0,0,null,
      null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 
      null, null, null, null, null, null, null,
	  'Colo', 0,
      null, null, null, null, null, null, null, null
    ) sq            
order by 
	sq.CALC_NUMBER, 
	CASE sq.LOB
		WHEN 'Internet' then 2
		WHEN 'Data' then 1
		WHEN 'Voice' then 4
		WHEN 'Other' then 3
		WHEN 'Colo' then 5
		ELSE 6
	END,
	sq.LOB