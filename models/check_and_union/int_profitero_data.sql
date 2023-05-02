-- 1/3/23: MJB - remove non-ASCII from actual_content_txt
-- 1/5/23: MJB - Replace '-' with empty string in description
-- 2/6/23: MJB - pull product name from all_retailers instead of profitero

--first option: name this model with int_ preffix, so that it does not populate profitero_data model. 

select TRIM(prof.retailer_desc) as retailer_desc,
        p.material_nbr,
        p.material_status_cd,
        p.material_type_cd,
        p.upc_cd as upc_cd,
        prof.rpc as rpc,
        p.upc_cd as global_trade_item_number_cd, -- Use UPC Code for GTIN (leading zeros? checksum?)
        sls.sls_hier_division_cd,
        sls.sls_hier_division_desc,
        sls.sls_hier_division_status_cd,
        'US' as country_of_origin_cd, -- Default, Is there a way to determine this in Profitero?
        'usRetail' as business_group_cd, -- Default
        cast(NULL as string) as active_version_txt, -- Cannot determine Versionds
        p.brand_cd,
        p.brand_name_txt as brand_nm,
        cast(NULL as string) as brand_story_txt, -- Cannot determine Brand Story
        COALESCE(p.marketing_name_txt, prof.profitero_product_name) as product_nm,
        NULL as product_description_txt, -- Use Profitero clean up title as Product Description
        'Profitero' as attribute_source_txt, -- Attribute source
        TRIM(prof.profitero_content_field) as attribute_cd, -- Attribute value
        TRIM(prof.profitero_content_field) as content_field,
        TRIM(prof.profitero_actual_content_value) as actual_content,
        prof.prof_date,
        case when top_sales.gtin is not null then 'Y' else 'N' end as top_sales_fg,  -- Top sales product,
        prof.prof_url as prof_url,
        'PIC' as created_by,
        current_datetime() as created_datetime,
        p.market_first_ship_dt
from (select retailer_desc,
             pim_upc_cd as pim_upc,
             rpc_cd as rpc,
             brand_nm as pim_brand_name,
             product_name as profitero_product_name,
             profitero_content_field_txt as profitero_content_field,
             IF(actual_content_txt = '-','',REGEXP_REPLACE(actual_content_txt, r'[^[:ascii:]]', '')) AS profitero_actual_content_value,
             profitero_url_txt as prof_url,
             prof_date
      from transient.pim_profitero_product_content
      ) AS prof
  inner join `edw-prd-e567f9.pic.dim_pim_product_all_retailers` p on INSTR(prof.pim_upc,p.upc_cd)>=1
    and p.material_type_cd = 'CNPK' and p.material_status_cd = 'A'
    and p.retailer = 'Enterprise' 
  inner join `edw-prd-e567f9.enterprise.dim_product_sls_hier_horiz` sls on LTRIM(sls.hier_cd,'0') = p.material_nbr
    and sls.hier_level_cd = 'MATL'
    and sls.language_cd = 'EN'
    and sls.version_cd = 0
    and sls.current_flg = TRUE
  left outer join `input.priority_skus` top_sales on INSTR(top_sales.gtin,p.upc_cd)>=1
