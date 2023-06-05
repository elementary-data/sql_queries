/*
Note that by default stl_scans data has a retention of only 7 days
https://docs.aws.amazon.com/redshift/latest/dg/c_intro_STL_tables.html
*/

-- Change days_back in the interval here
with days_back as (
    select (GETDATE() - interval '90 day') as days_back
),

table_scans as (

    select distinct 
        scans.userid as user_id,
        scans.query as query_id,
        users.usename as user_name,
        scans.tbl as table_id,
        queries.querytxt as query_text,
        lower(tables.database) || '.' || lower(tables.schema) || '.' || lower(tables.table) as full_table_name,
        lower(tables.database) as database_name,
        lower(tables.schema) as schema_name,
        lower(tables.table) as table_name,
        queries.starttime as start_time,
        queries.endtime as end_time
    from stl_scan as scans
    join svv_table_info as tables on (scans.tbl = tables.table_id)
    join stl_query as queries on (scans.query = queries.query)
    join svl_user_info as users on (queries.userid = users.usesysid)
    where 
        queries.aborted = 0
        and queries.endtime > (select * from days_back)
    order by scans.endtime desc

),

scans_with_row_num as(

    select *,
        count(query_id) over (partition by full_table_name) as queries_count,
        row_number() over (partition by full_table_name order by end_time desc) as row_number
    from table_scans

),

table_scans_stats as (

    select
        full_table_name,
        end_time as latest_query_time,
        queries_count,
        query_id as latest_query_id,
        query_text as latest_query_text,
        user_name as latest_scan_user_name,
        database_name,
        schema_name,
        table_name
    from scans_with_row_num
    where row_number = 1 

),

all_tables as (

    select 
        lower(database_name) || '.' || lower(schema_name) || '.' || lower(table_name) as full_table_name,
        lower(database_name) as database_name,
        lower(schema_name) as schema_name,
        lower(table_name) as table_name, 
        table_type
    from (
    select 
        table_catalog as database_name,
        table_schema as schema_name,
        table_name as table_name,
        table_type
    from
        pg_catalog.svv_tables
    where
        table_schema not in ('information_schema', 'pg_catalog', 'pg_internal')
    union
    select distinct 
        redshift_database_name as database_name,
        schemaname as schema_name,
        tablename as table_name,
        'EXTERNAL TABLE' as table_type
    from
        svv_external_tables
    )

)

select 
    full_table_name,
    table_type,
    case when 
        latest_query_time is not null then true
        else false
    end as is_active,        
    latest_query_time,
    queries_count,
    latest_query_id,
    latest_query_text,
    latest_scan_user_name,
    tables.database_name,
    tables.schema_name,
    tables.table_name
from all_tables as tables left join 
    table_scans_stats as scans using (full_table_name)
order by is_active, latest_query_time, queries_count desc     
