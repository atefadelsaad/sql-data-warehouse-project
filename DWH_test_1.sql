
use master
create database DataWarehouse
use DataWarehouse
go 
create schema bronze
go
create schema silver
go
create schema gold
go



if OBJECT_ID('bronze.UnitMeasure','U') is not null
drop table bronze.UnitMeasure
CREATE TABLE [bronze].[UnitMeasure](
	[UnitMeasureCode] [nchar](3) NOT NULL,
	[Name] nvarchar(50) NOT NULL,
	[ModifiedDate] [datetime] NOT NULL
)


create or alter procedure bronze.load_bronze as
begin
truncate table bronze.UnitMeasure
insert into DataWarehouse.bronze.UnitMeasure(
	[UnitMeasureCode],
	[Name],
	[ModifiedDate]
) 
select 
	[UnitMeasureCode] ,
	[Name] ,
	[ModifiedDate] 
from AdventureWorks2022.production.UnitMeasure
end
if OBJECT_ID('gold.dim_UnitMeasure','U') is not null
drop table gold.dim_UnitMeasure 
create table gold.dim_UnitMeasure(
	[UnitMeasureCode] [nchar](3) NOT NULL,
	[Name] nvarchar(50) NOT NULL,
	[ModifiedDate] date NOT NULL
)

create or alter procedure  gold.load_gold as
begin
truncate table gold.dim_UnitMeasure
insert into  gold.dim_UnitMeasure(
	UnitMeasureCode,
	[Name],
	[ModifiedDate]
)
select 
	UnitMeasureCode,
	[Name],
	[ModifiedDate]
	from silver.cln_UnitMeasure
end


if OBJECT_ID('silver.cln_UnitMeasure','U') is not null
drop table silver.cln_UnitMeasure
create table silver.cln_UnitMeasure
(
	[UnitMeasureCode] [nchar](3) NOT NULL,
	[Name] nvarchar(50) NOT NULL,
	[ModifiedDate] date NOT NULL
)

create or alter procedure silver.load_silver as
begin
truncate table silver.cln_UnitMeasure
insert into silver.cln_UnitMeasure(
UnitMeasureCode,
[Name],
[ModifiedDate]
)


select 
UPPER(trim(UnitMeasureCode))  as UnitMeasureCode,
trim([Name]) as [Name],
cast([ModifiedDate] as date) as [ModifiedDate]
from bronze.UnitMeasure
end



exec silver.load_silver
exec gold.load_gold
exec bronze.load_bronze
select * from bronze.UnitMeasure