use master
go
set nocount on
go
use WealthBench
go

/*
alter database WealthBench set single_user with rollback immediate
go
use master
go
drop database WealthBench
go
*/

create database [WealthBench]
    on primary ( name = N'WealthBench'    , filename = N'D:\SQLData\WealthBench.mdf'   , size = 8192kb, filegrowth = 65536kb)
log on         ( name = N'WealthBench_log', filename = N'D:\SQLLog\WealthBench_log.ldf', size = 4196mb, filegrowth = 65536kb)
go
alter database WealthBench add filegroup FGInMem contains memory_optimized_data;
go
alter database WealthBench add file (name = N'FGInMem', filename = N'D:\SQLData\WealthBench_InMem.xdf') to filegroup FGInMem;
go

use WealthBench
go
if not exists (select * from sys.dm_resource_governor_resource_pools where name = 'IMResPool')
create resource pool IMResPool with (max_memory_percent = 80);
go
alter resource governor reconfigure; 
go
exec sp_xtp_bind_db_resource_pool 'WealthBench', 'IMResPool' 
go
select d.database_id, d.name, d.resource_pool_id, rp.name from sys.databases d join sys.dm_resource_governor_resource_pools rp on d.resource_pool_id = rp.pool_id
go
use master
go
alter database WealthBench set offline
go
alter database WealthBench set online
go

backup database WealthBench to disk = 'D:\SQLBackup\WealthBench_FULL_INIT.fbak' with stats = 1, compression, init
backup log WealthBench to disk = 'D:\SQLBackup\WealthBench_LOG_INIT.lbak' with stats = 1, compression
alter database WealthBench set recovery simple

/*
ALTER DATABASE WealthBench SET QUERY_STORE CLEAR;
ALTER DATABASE WealthBench SET QUERY_STORE = ON (OPERATION_MODE = READ_WRITE);
ALTER DATABASE WealthBench SET QUERY_STORE = OFF;

sp_configure 'max degree of parallelism', 1
reconfigure with override
*/

