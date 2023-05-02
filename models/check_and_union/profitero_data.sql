WITH 

RETAILER_UPC_FIELDS AS
(SELECT distinct retailer_desc, upc_cd, rpc, product_nm, attribute_cd, prof_date, prof_url
   FROM {{ ref('int_profitero_data') }} pd
)

select distinct
       prof.retailer_desc,
       prof.material_nbr,
       prof.material_status_cd,
       prof.material_type_cd,
       prof.upc_cd as upc_cd,
       prof.rpc as rpc,
       prof.global_trade_item_number_cd, 
       prof.sls_hier_division_cd,
       prof.sls_hier_division_desc,
       prof.sls_hier_division_status_cd,
       prof.country_of_origin_cd, -- Default
       prof.business_group_cd, -- Default
       prof.active_version_txt, 
       prof.brand_cd,
       prof.brand_nm,
       prof.brand_story_txt,
       prof.product_nm,
       cast(NULL as INT64) as product_description_txt, -- Use Profitero clean up title as Product Description
       prof.attribute_source_txt, -- Attribute source
       'Total Product Carousel Image Count' as attribute_cd, -- Attribute value
       'Total Product Carousel Image Count' as content_field,
       '0' as actual_content,  --
       prof.prof_date,
       prof.top_sales_fg,  -- Top sales product,
       prof.prof_url,
       'PIC' as created_by,
       current_datetime() as created_datetime,
       prof.market_first_ship_dt
FROM {{ ref('int_profitero_data') }} prof
  where not exists (select 1 from RETAILER_UPC_FIELDS f2
                                where prof.retailer_desc = f2.retailer_desc
                                  and prof.upc_cd = f2.upc_cd
                                  and prof.rpc = f2.rpc
                                  and attribute_cd = 'Total Product Carousel Image Count'
                    ) 

union all 

select * from {{ref('int_profitero_data')}}