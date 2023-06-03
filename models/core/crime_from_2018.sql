{{ config(materialized="view") }}

with
    crime_from_2018_0 as (select * from {{ source("core", "crime_part_0")}} where Year >=2018),
    crime_from_2018_1 as (select * from {{ source("core", "crime_part_1")}}where Year >=2018),
    crime_from_2018_2 as (select * from {{ source("core", "crime_part_2")}}where Year >=2018),
    crime_from_2018_3 as (select * from {{ source("core", "crime_part_3")}}where Year >=2018),
    crime_from_2018_4 as (select * from {{ source("core", "crime_part_4")}}where Year >=2018),
    crime_from_2018_5 as (select * from {{ source("core", "crime_part_5")}}where Year >=2018),
    crime_from_2018_6 as (select * from {{ source("core", "crime_part_6")}}where Year >=2018),
    crime_from_2018_7 as (select * from {{ source("core", "crime_part_7")}}where Year >=2018),
    crime_from_2018_8 as (select * from {{ source("core", "crime_part_8")}}where Year >=2018),
    crime_from_2018_9 as (select * from {{ source("core", "crime_part_9")}}where Year >=2018),
    crime_from_2018_10 as (select * from {{ source("core", "crime_part_10")}}where Year >=2018),
    crime_from_2018_11 as (select * from {{ source("core", "crime_part_11")}}where Year >=2018),
    crime_from_2018_12 as (select * from {{ source("core", "crime_part_12")}}where Year >=2018),
    crime_from_2018_13 as (select * from {{ source("core", "crime_part_13")}}where Year >=2018),
    crime_from_2018_14 as (select * from {{ source("core", "crime_part_14")}}where Year >=2018),
    crime_from_2018_15 as (select * from {{ source("core", "crime_part_15")}}where Year >=2018),
    crime_from_2018_joined as (
        select * from crime_from_2018_0
        union all
        select * from crime_from_2018_1
        union all
        select * from crime_from_2018_2
        union all
        select * from crime_from_2018_3
        union all
        select * from crime_from_2018_4
        union all
        select * from crime_from_2018_5
        union all
        select * from crime_from_2018_6
        union all
        select * from crime_from_2018_7
        union all
        select * from crime_from_2018_8
        union all
        select * from crime_from_2018_9
        union all
        select * from crime_from_2018_10
        union all
        select * from crime_from_2018_11
        union all
        select * from crime_from_2018_12
        union all
        select * from crime_from_2018_13
        union all
        select * from crime_from_2018_14
        union all
        select * from crime_from_2018_15
    )
select 
    cast(ID as integer) as id,
    cast(Description as string) description,
    cast(Arrest as boolean) arrest,
    cast(Domestic as boolean) domestic,
    cast(Beat as integer) beat,
    cast(District as integer) district,
    cast(Ward as integer) ward,
    cast(Year as integer) year_of_crime,
    cast(Location as string) location
from crime_from_2018_joined

