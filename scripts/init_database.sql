/* 
	script details:
		creating database with the name of "madhandatawarehouse" and if exists then it is dropped and recreated 
		and schemas are created namely bronze,silver,gold.
	warning:
		running this script will delete the entire the database eith datasets inside the database if its exists,
		so proceed with caution.
*/
use master;
go

if exists (select 1 from sys.databases where name ='madhandatawarehouse')
begin
	alter database madhandatawarehouse set single_user with rollback immediate;
	drop database madhandatawarehouse;
end;
go

create database madhandatawarehouse;
go

use madhandatawarehouse;
go

create schema bronze;
go

create schema silver;
go

create schema gold;
go
