/*
To check whether the database exist if yes then drop it 
warnings: running this script will drop the entire database 
'datawarehouse' if it exists all the data in the database will
delete parmanently.*/
if exists ( select 1 from sys.databases where name='datawarehouse')
begin 
     alter database datawarehouse set single_user with rollback immediate;
     drop database datawarehouse;
end 
go

create database datawarehouse;
use datawarehouse;

/*create schemas as we have defined in the objective 
and will also use separator */

create schema bronze;
go
create schema silver;
go
create schema gold;
go