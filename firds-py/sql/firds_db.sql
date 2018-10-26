CREATE OR REPLACE VIEW public.firds_db AS
 SELECT f.isin,
    f.inst_name,
    f.cfi_code,
    f.deriv_ind,
    f.issuer_id,
    f.tv_mic,
    f.inst_short_name,
    f.issuer_admiss_request,
    f.admiss_request_date,
    f.first_trade_date,
    f.currency,
    f.expiry_date,
    f.price_multiplier,
    f.underlying_isins,
    f.underlying_leis,
    f.delivery_type,
    f.comp_auth_country,
    f.termination_date,
    f.baseproduct,
    f.subproduct,
    f.further_subproduct,
    f.transaction_type,
    f.publication_date,
    f.fin_price_type
   FROM fulins f
  WHERE NOT (f.hash IN ( SELECT d.hash
           FROM dltins d
          WHERE d.operation_type = 'TermntdRcrd'::text OR 
d.operation_type = 'ModfdRcrd'::text))
UNION ALL
 SELECT d.isin,
    d.inst_name,
    d.cfi_code,
    d.deriv_ind,
    d.issuer_id,
    d.tv_mic,
    d.inst_short_name,
    d.issuer_admiss_request,
    d.admiss_request_date,
    d.first_trade_date,
    d.currency,
    d.expiry_date,
    d.price_multiplier,
    d.underlying_isins,
    d.underlying_leis,
    d.delivery_type,
    d.comp_auth_country,
    d.termination_date,
    d.baseproduct,
    d.subproduct,
    d.further_subproduct,
    d.transaction_type,
    d.publication_date,
    d.fin_price_type
   FROM dltins d
  WHERE d.operation_type = 'NewRcrd'::text OR d.operation_type = 
'ModfdRcrd'::text;

