use master
go
set nocount on
go
use WealthBench
go


/* Build options */
declare @inmemtyp        tinyint = 1
      , @inmemtbl        tinyint = 1
      , @inmemtbldurable tinyint = 0
      , @fks             tinyint = 1
	  , @prnt            tinyint = 0
	  , @exec            tinyint = 1
	  , @dropobjs        tinyint = 1

declare @s nvarchar(max), @dur nvarchar(max)

if @inmemtbldurable = 0 select @dur = N', durability = SCHEMA_ONLY' else select @dur = N''

if db_name() = 'WealthBench'
begin

if not exists (select * from sys.schemas where name = 'Fund')   exec('create schema Fund');
if not exists (select * from sys.schemas where name = 'Accnt')  exec('create schema Accnt');
if not exists (select * from sys.schemas where name = 'Trans')  exec('create schema Trans');
if not exists (select * from sys.schemas where name = 'Asset')  exec('create schema Asset');
if not exists (select * from sys.schemas where name = 'Rebal')  exec('create schema Rebal');
if not exists (select * from sys.schemas where name = 'Thread') exec('create schema Thread');
if not exists (select * from sys.schemas where name = 'Archive') exec('create schema Archive');

if @dropobjs = 1
 begin
 
  if exists (select * from sys.procedures where name = 'Deposit') drop procedure Deposit
  if exists (select * from sys.procedures where name = 'Withdrawal') drop procedure Withdrawal
  if exists (select * from sys.procedures where name = 'PriceUpdateWrap') drop procedure PriceUpdateWrap
  if exists (select * from sys.procedures where name = 'PriceUpdate') drop procedure PriceUpdate
  if exists (select * from sys.procedures where name = 'Rebalance') drop procedure Rebalance
  if exists (select * from sys.procedures where name = 'RebalanceAcct') drop procedure RebalanceAcct
  if exists (select * from sys.procedures where name = 'CloseRebalance') drop procedure CloseRebalance
  if exists (select * from sys.procedures where name = 'GetPriceSnapshot') drop procedure GetPriceSnapshot
  if exists (select * from sys.procedures where name = 'InsPriceSnapshotMutex') drop procedure InsPriceSnapshotMutex
  if exists (select * from sys.procedures where name = 'ArchiveHistory') drop procedure ArchiveHistory
  
  if exists (select * from sys.procedures where name = 'AccountAsset') drop procedure Archive.AccountAsset
  if exists (select * from sys.procedures where name = 'TransTrade') drop procedure Archive.TransTrade
  if exists (select * from sys.procedures where name = 'FundRebalance') drop procedure Archive.FundRebalance

  if exists (select * from sys.types where is_table_type = 1 and name = 'HoldingsTarget') drop type HoldingsTarget
  if exists (select * from sys.types where is_table_type = 1 and name = 'HoldingsCurrent') drop type HoldingsCurrent
  if exists (select * from sys.types where is_table_type = 1 and name = 'Transact') drop type Transact
  if exists (select * from sys.types where is_table_type = 1 and name = 'AssetList') drop type AssetList
  if exists (select * from sys.types where is_table_type = 1 and name = 'RebalanceAsset') drop type RebalanceAsset

  if object_id('Rebal.DebugLog') is not null            drop table Rebal.DebugLog
  if object_id('Rebal.PriceSnapshotMutex') is not null  drop table Rebal.PriceSnapshotMutex
  if object_id('Rebal.PriceSnapshotGate') is not null   drop table Rebal.PriceSnapshotGate
  if object_id('Rebal.ErrorLog') is not null            drop table Rebal.ErrorLog
  if object_id('Trans.TransactQueue') is not null       drop table Trans.TransactQueue
  if object_id('Trans.Transact') is not null            drop table Trans.Transact
  if object_id('Trans.TransactType') is not null        drop table Trans.TransactType
  if object_id('Trans.Trade') is not null               drop table Trans.Trade
  if object_id('Trans.TradeType') is not null           drop table Trans.TradeType
  if object_id('Rebal.PriceSnapshot') is not null       drop table Rebal.PriceSnapshot
  if object_id('Rebal.AccountAssetArchive') is not null drop table Rebal.AccountAssetArchive
  if object_id('Rebal.AccountAsset') is not null        drop table Rebal.AccountAsset
  if object_id('Rebal.AccountQueue') is not null        drop table Rebal.AccountQueue
  if object_id('Rebal.AccountAudit') is not null        drop table Rebal.AccountAudit
  if object_id('Thread.StateAudit') is not null         drop table Thread.StateAudit
  if object_id('Thread.State') is not null              drop table Thread.State
  if object_id('Rebal.Account') is not null             drop table Rebal.Account
  if object_id('Accnt.HoldingHistory') is not null      drop table Accnt.HoldingHistory
  if object_id('Accnt.Holding') is not null             drop table Accnt.Holding
  if object_id('Accnt.BalanceHistory') is not null      drop table Accnt.BalanceHistory
  if object_id('Accnt.Balance') is not null             drop table Accnt.Balance
  if object_id('Accnt.Account') is not null             drop table Accnt.Account
  if object_id('Accnt.Investor') is not null            drop table Accnt.Investor
  if object_id('Asset.ModelAsset') is not null          drop table Asset.ModelAsset
  if object_id('Asset.Model') is not null               drop table Asset.Model
  if object_id('Accnt.Advisor') is not null             drop table Accnt.Advisor
  if object_id('Asset.ListingPriceHistory') is not null drop table Asset.ListingPriceHistory
  if object_id('Asset.ListingPrice') is not null        drop table Asset.ListingPrice
  if object_id('Asset.Listing') is not null             drop table Asset.Listing
  if object_id('Asset.Asset') is not null               drop table Asset.Asset
  if object_id('Asset.Class') is not null               drop table Asset.Class
  if object_id('Asset.Exchange') is not null            drop table Asset.Exchange
  if object_id('Rebal.FundMutex') is not null           drop table Rebal.FundMutex
  if object_id('Fund.BalanceHistory') is not null       drop table Fund.BalanceHistory
  if object_id('Fund.Balance') is not null              drop table Fund.Balance
  if object_id('Rebal.Fund') is not null                drop table Rebal.Fund
  if object_id('Fund.Fund')  is not null                drop table Fund.Fund
  if object_id('dbo.Currency')  is not null             drop table dbo.Currency
 end


/*****************************
* Table Types
******************************/


select @s = N'
create type dbo.AssetList as table (
   Asset_id bigint not null
 , Exch_code varchar(10) not null
 , Curr_code char(3) not null
 , Weighting decimal(9, 6) null
 , Units decimal(20, 6) null
 , Price decimal(20, 6) null'
if @inmemtyp = 1 select @s += N' 
 , primary key nonclustered (Asset_id, Exch_code, Curr_code)
)  with (memory_optimized = on);
'
else select @s += N'
 , primary key clustered (Asset_id, Exch_code, Curr_code)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create type dbo.RebalanceAsset as table (
   Rebal_id uniqueidentifier not null
 , Accnt_id bigint not null
 , Asset_id bigint not null
 , Model_id bigint not null
 , Exch_code varchar(10) not null
 , Fund_id bigint not null
 , Curr_code char(3) not null
 , Price decimal(20, 6) null
 , Units_current decimal(20, 6) null
 , Value_current as Units_current * Price
 , Units_target decimal(20, 6) null
 , Value_target decimal(20, 6) null
 , Units_rounded decimal(20, 6) null
 , Value_rounded decimal(20, 6) null
 , Units_adjusted decimal(20, 6) null
 , Value_adjusted decimal(20, 6) null'
if @inmemtyp = 1 select @s += N' 
 , primary key nonclustered (Rebal_id, Accnt_id, Asset_id)
)  with (memory_optimized = on);
'
else select @s += N'
 , primary key nonclustered (Rebal_id, Accnt_id, Asset_id)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s

select @s = N'
create type dbo.HoldingsCurrent as table (
   Asset_id bigint not null
 , Exch_code varchar(10) not null
 , Curr_code char(3) not null
 , Weighting decimal(9, 6) null
 , Units decimal(20, 6) not null
 , Price decimal(20, 6) not null
 , Value as Units * Price
'
if @inmemtyp = 1 select @s += N' , primary key nonclustered (Asset_id)
 , index ix01 (Price, Asset_id)
)  with (memory_optimized = on);
'
else select @s += N' , primary key clustered (Asset_id)
  , index ix01 (Price, Asset_id)
); 
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create type dbo.HoldingsTarget as table (
   Asset_id bigint not null
 , Exch_code varchar(10) not null
 , Curr_code char(3) not null
 , Weighting decimal(9, 6) null
 , Units decimal(20, 6) not null
 , Price decimal(20, 6) not null
 , Value as Units * Price
 , UnitsRounded decimal(20, 6) null default(0)
 , UnitsAdjusted decimal(20, 6) null default(0)
 , Deleting tinyint null default(0)
 , Inserting tinyint null default(0)
 , Updating tinyint null default(0)'
if @inmemtyp = 1 select @s += N' , primary key nonclustered (Asset_id)
 , index ix01 (Price, Asset_id)
)  with (memory_optimized = on);
'
else select @s += N' , primary key clustered (Asset_id)
  , index ix01 (Price, Asset_id)
); 
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create type dbo.Transact as table (
   Tran_id uniqueidentifier not null
 , Accnt_id bigint not null
 , TxTyp_id bigint not null
 , Rebal_id uniqueidentifier null
 , Amount decimal(20, 6) not null
 , Curr_code char(3) not null
 , TranQueued_dt datetime not null'
if @inmemtyp = 1 select @s += N' , primary key nonclustered (Tran_id)
)  with (memory_optimized = on);
'
else select @s += N' , primary key clustered (Tran_id)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s




/*****************************
* Tables
******************************/


select @s = N'
create table dbo.Currency (
   Curr_code char(3) not null
 , Curr_name nvarchar(256) not null'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_Currency primary key nonclustered (Curr_code)
 , constraint uncix_Currency_name unique nonclustered (Curr_name)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_Currency primary key clustered (Curr_code)
 , constraint uncix_Currency_name unique nonclustered (Curr_name)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create table Fund.Fund (
   Fund_id bigint not null
 , Fund_name nvarchar(256) not null
 , Curr_code char(3) not null'
if @fks = 1 select @s += N'
 , constraint fk_Fund_Curr_code foreign key (Curr_code) references dbo.Currency (Curr_code)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_Fund primary key nonclustered (Fund_id, Curr_code)
 , constraint ak_Fund_id_Curr_code unique nonclustered (Fund_id)
 , constraint uncix_Fund_name unique nonclustered (Fund_name)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_Fund primary key clustered (Fund_id, Curr_code)
 , constraint ak_Fund_id_Curr_code unique nonclustered (Fund_id)
 , constraint uncix_Fund_name unique nonclustered (Fund_name)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s

select @s = N'
create table Fund.Balance (
   Fund_id bigint not null
 , Curr_code char(3) not null
 , Balance decimal(20, 6) not null default (0)'
if @fks = 1 select @s += N'
 , constraint fk_FundBalance_Fund_id foreign key (Fund_id, Curr_code) references Fund.Fund (Fund_id, Curr_code)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_FundBalance primary key nonclustered (Fund_id)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_FundBalance primary key clustered (Fund_id)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create table Accnt.Investor (
   Inv_id bigint not null
 , Inv_name nvarchar(256) not null'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_Investor primary key nonclustered (Inv_id)
 , constraint uncix_Investor_name unique nonclustered (Inv_name)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_Investor primary key clustered (Inv_id)
 , constraint uncix_Investor_name unique nonclustered (Inv_name)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create table Asset.Exchange (
   Exch_code varchar(10) not null
 , Exch_name nvarchar(256) not null
 , ISIN_Prefix char(2) not null
 , Curr_code char(3) not null
 , List_count bigint not null'
if @fks = 1 select @s += N'
 , constraint fk_Exchange_Curr_code foreign key (Curr_code) references dbo.Currency (Curr_code)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_Exchange primary key nonclustered (Exch_code, Curr_code)
 , constraint ak_Exchange_Curr_code unique nonclustered (Exch_code)
 , constraint uncix_Exchange_name unique nonclustered (Exch_name)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_Exchange primary key clustered (Exch_code, Curr_code)
 , constraint ak_Exchange_Curr_code unique nonclustered (Exch_code)
 , constraint uncix_Exchange_name unique nonclustered (Exch_name)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create table Asset.Class (
   Class_code varchar(64) not null
 , Class_name nvarchar(256) not null'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_AssetClass primary key nonclustered (Class_code)
 , constraint uncix_AssetClass unique nonclustered (Class_name)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_AssetClass primary key clustered (Class_code)
 , constraint uncix_AssetClass unique nonclustered (Class_name)
);'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create table Asset.Asset (
   Asset_id bigint not null
 , ISIN_code char(12)  null
 , Asset_name nvarchar(256) not null
 , Class_code varchar(64) not null'
if @fks = 1 select @s += N'
 , constraint fk_Asset_Class_code foreign key (Class_code) references Asset.Class (Class_code)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_Asset primary key nonclustered (Asset_id)
 , constraint uncix_Asset unique nonclustered (Asset_name)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_Asset primary key nonclustered (Asset_id)
 , constraint uncix_Asset unique nonclustered (Asset_name)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create table Asset.Listing (
   Asset_id bigint not null
 , Exch_code varchar(10) not null
 , ISIN_Code char(12) not null
 , Curr_code char(3) not null
 , List_No bigint not null'
if @fks = 1 select @s += N'
 , constraint fk_Listing_Asset_id foreign key (Asset_id) references Asset.Asset (Asset_id)
 , constraint fk_Listing_Exch_code foreign key (Exch_code, Curr_code) references Asset.Exchange (Exch_code, Curr_code)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_Listing primary key nonclustered (Asset_id, Exch_code, Curr_code)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_Listing primary key clustered (Asset_id, Exch_code, Curr_code)
);'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create table Asset.ListingPrice (
   Asset_id bigint not null
 , Exch_code varchar(10) not null
 , Curr_code char(3) not null
 , Price decimal(20, 6) not null'
if @fks = 1 select @s += N'
 , constraint fk_ListingPrice_Key foreign key (Asset_id, Exch_code, Curr_code) references Asset.Listing (Asset_id, Exch_code, Curr_code)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_ListingPrice primary key nonclustered (Asset_id, Exch_code, Curr_code)
 , index ix_ListingPrice_Curr_code nonclustered (Curr_code) /* supports GetPriceSnapshot range query */
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_ListingPrice primary key clustered (Asset_id, Exch_code, Curr_code)
 , index ix_ListingPrice_Curr_code nonclustered (Curr_code,Asset_id, Exch_code, Price) /* supports GetPriceSnapshot range query */
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create table Rebal.PriceSnapshotGate (
   Curr_code char(3) not null
 , GateStatus smallint not null'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_PriceSnapshotGate primary key nonclustered (Curr_code)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_PriceSnapshotGate primary key clustered (Curr_code)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s



select @s = N'
create table Accnt.Advisor (
   Advisor_id bigint not null
 , Advisor_name nvarchar(256) not null'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_AccntAdvisor primary key nonclustered (Advisor_id)
 , constraint uncix_AccntAdvisor_name unique nonclustered (Advisor_name)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_AccntAdvisor primary key clustered (Advisor_id)
 , constraint uncix_AccntAdvisor_name unique nonclustered (Advisor_name)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create table Asset.Model (
   Model_id bigint not null
 , Model_name nvarchar(256) not null
 , Advisor_id bigint not null
 , Curr_code char(3) not null
 , AssetsCount int null'
if @fks = 1 select @s += N'
 , constraint fk_AssetModel_Advisor_id foreign key (Advisor_id) references Accnt.Advisor (Advisor_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_AssetModel_Curr_code primary key nonclustered (Model_id, Curr_code)
 , constraint ak_AssetModel unique nonclustered (Model_id)
 , constraint uncix_AssetModel_name unique nonclustered (Model_name)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_AssetModel_Curr_code primary key clustered (Model_id, Curr_code)
 , constraint ak_AssetModel unique nonclustered (Model_id)
 , constraint uncix_AssetModel_name unique nonclustered (Model_name)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create table Asset.ModelAsset (
   Model_id bigint not null
 , Asset_id bigint not null
 , Exch_code varchar(10) not null
 , Curr_code char(3) not null
 , Weighting decimal(9, 6) not null'
if @fks = 1 select @s += N'
 , constraint fk_AssetModelAsset_Model_Curr_code foreign key (Model_id, Curr_code) references Asset.Model (Model_id, Curr_code)
 , constraint fk_AssetModelAsset_Asset_Listing foreign key (Asset_id, Exch_code, Curr_code) references Asset.Listing (Asset_id, Exch_code, Curr_code)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_AssetModelAsset primary key nonclustered (Model_id, Asset_id, Curr_code)
 , constraint ak_AssetModelAsset unique nonclustered (Model_id, Asset_id)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_AssetModelAsset primary key clustered (Model_id, Asset_id, Curr_code)
 , constraint ak_AssetModelAsset unique nonclustered (Model_id, Asset_id)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create table Accnt.Account (
   Accnt_id bigint not null
 , Inv_id bigint not null
 , Curr_code char(3) not null
 , Fund_id bigint not null
 , Model_id bigint not null'
if @fks = 1 select @s += N'
 , constraint fk_Account_Inv_Id foreign key (Inv_id) references Accnt.Investor (Inv_id)
 , constraint fk_Account_Curr_code foreign key (Curr_code) references dbo.Currency (Curr_code)
 , constraint fk_Account_Model_id_Curr_code foreign key (Model_id, Curr_code) references Asset.Model (Model_id, Curr_code)
 , constraint fk_Account_Fund_id foreign key (Fund_id, Curr_code) references Fund.Fund (Fund_id, Curr_code)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_Account primary key nonclustered (Accnt_id, Fund_id, Curr_code)
 , constraint ak_Account unique nonclustered (Accnt_id)
 , index ix_Account_Curr_code nonclustered (Curr_code, Accnt_id)
)  with (memory_optimized = on'+@dur+N');
alter table Accnt.Account add index ix_Account_Fund_id nonclustered (Fund_id, Accnt_id, Curr_code);
'
else select @s += N'
 , constraint pk_Account primary key clustered (Accnt_id, Fund_id, Curr_code)
 , constraint ak_Account unique nonclustered (Accnt_id)
 , index ix_Account_Curr_code nonclustered (Curr_code, Accnt_id)
);
create nonclustered index ix_Account_Fund_id on Accnt.Account (Fund_id, Accnt_id, Curr_code);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create table Accnt.Balance (
   Accnt_id bigint not null
 , Inv_id bigint not null
 , Curr_code char(3) not null
 , Fund_id bigint not null
 , Model_id bigint not null
 , InvBalance decimal(20, 6) not null default (0)
 , CashBalance decimal(20, 6) not null default (0)'
if @fks = 1 select @s += N'
 , constraint fk_AccountBalance_Account foreign key (Accnt_id, Fund_id, Curr_code) references Accnt.Account (Accnt_id, Fund_id, Curr_code)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_AccountBalance primary key nonclustered hash (Accnt_id) with (bucket_count = 30000)
 , index ix_AccountBalance_Fund_id nonclustered (Fund_id)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_AccountBalance primary key clustered (Fund_id, Accnt_id)
-- , index ix_AccountBalance_Fund_id nonclustered (Fund_id)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s

select @s = N'
create table Accnt.BalanceHistory (
   Id bigint not null identity (1, 1)
 , Accnt_id bigint not null
 , Curr_code char(3) not null
 , Rebal_id uniqueidentifier not null
 , Fund_id bigint not null
 , InvBalance decimal(20, 6) not null default (0)
 , CashBalance decimal(20, 6) not null default (0)'
if @fks = 1 select @s += N'
 , constraint fk_AccountBalanceHistory_Account foreign key (Accnt_id, Fund_id, Curr_code) references Accnt.Account (Accnt_id, Fund_id, Curr_code)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_AccountBalanceHistory primary key nonclustered (Accnt_id, Fund_id, Curr_code, Rebal_id)
 , constraint ak_AccountBalanceHistory unique nonclustered (Accnt_id, Rebal_id)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_AccountBalanceHistory primary key clustered (Accnt_id, Fund_id, Curr_code, Rebal_id)
 , constraint ak_AccountBalanceHistory unique nonclustered (Accnt_id, Rebal_id)
) '
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'

create table Rebal.Fund (
   Rebal_id uniqueidentifier not null
 , Fund_id bigint not null
 , Curr_code char(3) not null
 , AccntsCount bigint not null default(-1)
 , Start_dt datetime not null
 , End_dt datetime not null default(''01-Jan-1900'')'
if @fks = 1 select @s += N'
 , constraint fk_RebalFund_Fund_id foreign key (Fund_id, Curr_code) references Fund.Fund (Fund_id, Curr_code)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_RebalFund_Rebal_Fund_Curr_code primary key nonclustered (Rebal_id, Fund_id, Curr_code)
 , constraint ak_RebalFund_1 unique nonclustered (Rebal_id)
 , constraint ak_RebalFund_2 unique nonclustered (Fund_id, End_dt) /* Funds should only have one Rebalance "open" at once */
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_RebalFund_Rebal_Fund_Curr_code primary key clustered (Rebal_id, Fund_id, Curr_code)
 , constraint ak_RebalFund_1 unique nonclustered (Rebal_id)
 , constraint ak_RebalFund_2 unique nonclustered (Fund_id, End_dt) /* Funds should only have one Rebalance "open" at once */
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create table Rebal.FundMutex (
   Fund_id bigint not null
 , Rebal_id uniqueidentifier not null
 , Mutex_id uniqueidentifier not null'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_RebalFundMutex primary key nonclustered (Fund_id)
 , constraint ak_RebalFundMutex unique (Mutex_id)
)  with (memory_optimized = on, durability = SCHEMA_ONLY);
'
else select @s += N'
 , constraint pk_RebalFundMutex primary key clustered (Fund_id)
 , constraint ak_RebalFundMutex unique (Mutex_id)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s

select @s = N'
create table Rebal.PriceSnapshotMutex (
   Curr_code char(3) not null
 , Mutex_id uniqueidentifier not null'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_RebalPriceSnapshotMutex primary key nonclustered (Curr_code)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_RebalPriceSnapshotMutex primary key clustered (Curr_code)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s

select @s = N'
create table Rebal.PriceSnapshot (
   Rebal_id uniqueidentifier not null
 , Fund_id bigint not null
 , Asset_id bigint not null
 , Exch_code varchar(10) not null
 , Curr_code char(3) not null
 , Price decimal(20, 6) not null'
if @fks = 1 select @s += N'
 , constraint fk_RebalPriceSnapshot_Fund foreign key (Rebal_id, Fund_id, Curr_code) references Rebal.Fund (Rebal_id, Fund_id, Curr_code)
 , constraint fk_RebalPriceSnapshot_Asset foreign key (Asset_id, Exch_code, Curr_code) references Asset.Listing (Asset_id, Exch_code, Curr_code)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_RebalPriceSnapshot primary key nonclustered (Rebal_id, Asset_id, Exch_code, Curr_code)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_RebalPriceSnapshot primary key clustered (Rebal_id, Asset_id, Exch_code, Curr_code)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create table Fund.BalanceHistory (
   Id bigint not null identity(1, 1)
 , Fund_id bigint not null
 , Rebal_id uniqueidentifier not null
 , Curr_code char(3) not null
 , LogDt datetime not null
 , Balance decimal(20, 6) not null'
if @fks = 1 select @s += N'
 , constraint fk_FundBalanceHistory_Fund_id foreign key (Fund_id, Curr_code) references Fund.Fund (Fund_id, Curr_code)
 , constraint fk_FundBalanceHistory_Rebal_id foreign key (Rebal_id, Fund_id, Curr_code) references Rebal.Fund (Rebal_id, Fund_id, Curr_code)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_FundBalanceHistory primary key nonclustered (Fund_id, Rebal_id)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_FundBalanceHistory primary key clustered (Fund_id, Rebal_id)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s



select @s = N'
create table Thread.State (
   ThreadId bigint not null
 , Fund_id bigint not null
 --, Curr_code char(3) not null
 , QueueNoFrom bigint not null
 , QueueNoTo bigint not null
 , NextQueueNo bigint not null'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_ThreadState primary key nonclustered hash (ThreadId) with (bucket_count = 60)
 , index ix_ThreadState_Fund_id nonclustered (Fund_id)
)  with (memory_optimized = on, durability = SCHEMA_ONLY);
'
else select @s += N'
 , constraint pk_ThreadState_GBP primary key clustered (ThreadId)
 , index ix_ThreadState_Fund_id nonclustered (Fund_id)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create table Thread.StateAudit (
   ThreadId bigint not null
 , MaxThreadId bigint not null
 , Fund_id bigint not null
 --, Curr_code char(3) not null
 , NoOfFunds bigint not null
 , ThreadMod bigint not null
 , ThreadsPerFund bigint not null
 , NthThread bigint not null
 , AccntsCount bigint not null
 , AccntsPerThread bigint not null
 , QueueNoFrom bigint not null
 , QueueNoTo bigint not null
 , NextQueueNo bigint not null'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_ThreadStateAudit primary key nonclustered (ThreadId)
)  with (memory_optimized = on, durability = SCHEMA_ONLY);
'
else select @s += N'
 , constraint pk_ThreadStateAudit primary key clustered (ThreadId)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s



select @s = N'
create table Rebal.AccountQueue (
   Queue_No bigint not null
 , Fund_id bigint not null
 , Curr_code char(3) not null
 , Accnt_id bigint not null'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_RebalAccountQueue primary key nonclustered (Fund_id, Accnt_id)
)  with (memory_optimized = on, durability = SCHEMA_ONLY);
alter table Rebal.AccountQueue add index ix_RebalAccountQueueNo_01 unique nonclustered (Fund_id, Queue_No);
'
else select @s += N'
 , constraint pk_RebalAccountQueue primary key clustered (Fund_id, Accnt_id)
);
create unique nonclustered index ix_RebalAccountQueueNo_01 on Rebal.AccountQueue (Fund_id, Queue_No);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s




select @s = N'
create table Rebal.Account (
   Rebal_id uniqueidentifier not null
 , Item_no bigint not null
 , Fund_id bigint not null
 , Curr_code char(3) not null
 , Accnt_id bigint not null
 , Model_id bigint not null
 , Sql_Process_id bigint null
 , ThreadId bigint not null '
if @fks = 1 select @s += N'
 , constraint fk_RebalAccount_Rebal_Fund_Curr_code foreign key (Rebal_id, Fund_id, Curr_code) references Rebal.Fund (Rebal_id, Fund_id, Curr_code)
 , constraint fk_RebalAccount_Fund_Curr_code foreign key (Accnt_id, Fund_id, Curr_code) references Accnt.Account (Accnt_id, Fund_id, Curr_code)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_RebalAccount primary key nonclustered (Rebal_id, Accnt_id, Fund_id, Curr_code)
 --, constraint ak_RebalAccount unique nonclustered (Rebal_id, Accnt_id)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_RebalAccount primary key clustered (Rebal_id, Accnt_id, Fund_id, Curr_code)
 --, constraint ak_RebalAccount unique nonclustered (Rebal_id, Accnt_id)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create table Rebal.AccountAudit (
   Audit_Id uniqueidentifier not null
 , Rebal_id uniqueidentifier not null
 , Fund_id bigint not null
 , Curr_code char(3) not null
 , Accnt_id bigint not null
 , Start_dt datetime2(6) not null
 , CurrentInvBalance decimal(20, 6) not null
 , CurrentHoldingsCount bigint null
 , CurrentHoldingsValue decimal(20, 6) not null
 , CurrentCashBalance decimal(20, 6) not null
 , NewDeposits decimal(20, 6) not null
 , NewWithdrawals decimal(20, 6) not null
 , TargetInvestValue decimal(20, 6) not null
 , RoundedTargetInvestValue decimal(20, 6) not null
 , RebalancedInvestValue decimal(20, 6) not null
 , RebalancedCashBalance decimal(20, 6) not null
 , End_dt datetime2(6) null'
if @fks = 1 select @s += N'
 , constraint fk_RebalAccountAudit_RebalAccount foreign key (Rebal_id, Accnt_id, Fund_id, Curr_code) references Rebal.Account (Rebal_id, Accnt_id, Fund_id, Curr_code)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_RebalAccountAudit primary key nonclustered hash (Rebal_id, Accnt_id) with (bucket_count = 40000)
)  with (memory_optimized = on, durability = SCHEMA_ONLY);
'
else select @s += N'
 , constraint pk_RebalAccountAudit primary key clustered (Rebal_id, Accnt_id)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s



select @s = N'
create table Rebal.AccountAsset (
   RebalHistory_Id bigint not null identity(1, 1)
 , Rebal_id uniqueidentifier not null
 , Accnt_id bigint not null
 , Asset_id bigint not null
 , Model_id bigint not null
 , Exch_code varchar(10) not null
 , Fund_id bigint not null
 , Curr_code char(3) not null
 , Price decimal(20, 6) null
 , Units_current decimal(20, 6) null
 , Units_target decimal(20, 6) null
 , Value_target decimal(20, 6) null
 , Units_rounded decimal(20, 6) null
 , Value_rounded decimal(20, 6) null
 , Units_adjusted decimal(20, 6) null
 , Value_adjusted decimal(20, 6) null'
if @fks = 1 select @s += N'
 , constraint fk_RebalAccountAsset_Rebal_Accnt_id foreign key (Rebal_id, Accnt_id, Fund_id, Curr_code) references Rebal.Account (Rebal_id, Accnt_id, Fund_id, Curr_code)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_RebalAccountAsset primary key nonclustered (Rebal_id, Accnt_id, Asset_id)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_RebalAccountAsset primary key clustered (Rebal_id, Accnt_id, Asset_id)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create table Rebal.AccountAssetArchive (
   RebalHistory_Id bigint not null
 , Rebal_id uniqueidentifier not null
 , Accnt_id bigint not null
 , Asset_id bigint not null
 , Model_id bigint not null
 , Exch_code varchar(10) not null
 , Fund_id bigint not null
 , Curr_code char(3) not null
 , Price decimal(20, 6) null
 , Units_current decimal(20, 6) null
 , Asset_id_target bigint null
 , Units_target decimal(20, 6) null
 , Value_target decimal(20, 6) null
 , Units_rounded decimal(20, 6) null
 , Value_rounded decimal(20, 6) null
 , Units_adjusted decimal(20, 6) null
 , Value_adjusted decimal(20, 6) null'
select @s += N'
 , constraint pk_RebalAccountAssetArchive primary key clustered (RebalHistory_id) with (ignore_dup_key = on)
 , constraint ak_RebalAccountAssetArchive unique nonclustered (Rebal_id, Accnt_id, Asset_id)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create table Accnt.HoldingHistory (
   Rebal_id uniqueidentifier not null
 , Accnt_id bigint not null
 , Asset_id bigint not null
 , Fund_id bigint not null
 , Exch_code varchar(10) not null
 , Curr_code char(3) not null
 , Units decimal(20, 6) not null'
if @fks = 1 select @s += N'
 --, constraint fk_HoldingHistory_Accnt_Curr_code foreign key (Accnt_id, Fund_id, Curr_code) references Accnt.Account (Accnt_id, Fund_id, Curr_code)
 --, constraint fk_HoldingHistory_Listing foreign key (Asset_id, Exch_code, Curr_code) references Asset.Listing (Asset_id, Exch_code, Curr_code)
'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_HoldingHistory primary key nonclustered (Rebal_id, Accnt_id, Asset_id)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_HoldingHistory primary key clustered (Rebal_id, Accnt_id, Asset_id)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create table Accnt.Holding (
   Accnt_id bigint not null
 , Asset_id bigint not null
 , Fund_id bigint not null
 , Exch_code varchar(10) not null
 , Curr_code char(3) not null
 , Units decimal(20, 6) not null'
if @fks = 1 select @s += N'
 , constraint fk_Holding_Accnt_Curr_code foreign key (Accnt_id, Fund_id, Curr_code) references Accnt.Account (Accnt_id, Fund_id, Curr_code)
 , constraint fk_Holding_Listing foreign key (Asset_id, Exch_code, Curr_code) references Asset.Listing (Asset_id, Exch_code, Curr_code)
'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_Holding primary key nonclustered (Accnt_id, Asset_id)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_Holding primary key clustered (Accnt_id, Asset_id)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create table Asset.ListingPriceHistory (
   ListingPriceHistory_id bigint not null identity(1, 1)
 , Asset_id bigint not null
 , Exch_code varchar(10) not null
 , Price_dt datetime not null
 , PricePre decimal(20, 6) not null
 , PricePost decimal(20, 6) not null
 , Curr_code char(3) not null'
if @fks = 1 select @s += N'
 , constraint fk_ListingPriceHistory_Asset_id foreign key (Asset_id, Exch_code, Curr_code) references Asset.Listing (Asset_id, Exch_code, Curr_code)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_ListingPriceHistory primary key nonclustered (ListingPriceHistory_id)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_ListingPriceHistory primary key clustered  (ListingPriceHistory_id)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create table Trans.TradeType (
   TrTyp_id bigint not null
 , TrTyp_name nvarchar(256) not null
 , constraint uncix_TransTradeType_name unique nonclustered (TrTyp_name)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_TransTradeType primary key nonclustered (TrTyp_id)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_TransTradeType primary key clustered (TrTyp_id)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create table Trans.Trade (
   Rebal_id uniqueidentifier not null
 , Accnt_id bigint not null
 , Asset_id bigint not null
 , TrTyp_id bigint not null --constraint chk_Trade_TrTyp_id check (TrTyp_id in (1, 2))
 , Exch_code varchar(10) not null
 , Fund_id bigint not null
 , Curr_code char(3) not null
 , Units decimal(20, 6) not null
 , Price decimal(20, 6) not null
 , TradeTime datetime not null default(getdate())'
if @fks = 1 select @s += N'
 , constraint fk_TransTrade_PriceSnapshot foreign key (Rebal_id, Asset_id, Exch_code, Curr_code) references Rebal.PriceSnapshot (Rebal_id, Asset_id, Exch_code, Curr_code)
 , constraint fk_TransTrade_TrTyp_id foreign key (TrTyp_id) references Trans.TradeType (TrTyp_id)
 , constraint fk_TransTrade_Rebal_id_Accnt_id foreign key (Rebal_id, Accnt_id, Fund_id, Curr_code) references Rebal.Account (Rebal_id, Accnt_id, Fund_id, Curr_code)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_TransTrade primary key nonclustered (Rebal_id, Accnt_id, Asset_id, TrTyp_id)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_TransTrade primary key clustered (Rebal_id, Accnt_id, Asset_id, TrTyp_id)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create table Trans.TransactType (
   TxTyp_id bigint not null
 , TxTyp_name nvarchar(256) not null
 , index uncix_TransTransactType_name unique nonclustered (TxTyp_name)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_TransTransactType primary key nonclustered (TxTyp_id)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_TransTransactType primary key clustered (TxTyp_id)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s

select @s = N'
create table Trans.TransactQueue (
   Tran_id uniqueidentifier not null
 , Accnt_id bigint not null
 , Fund_id bigint not null
 , Curr_code char(3) not null
 , TxTyp_id bigint not null
 , Amount decimal(20, 6) not null
 , TranQueued_dt datetime not null default(getdate())'
if @fks = 1 select @s += N'
 , constraint fk_TransTransactQueue_Accnt_Curr_Code foreign key (Accnt_id, Fund_id, Curr_code) references Accnt.Account (Accnt_id, Fund_id, Curr_code)
 , constraint fk_TransTransactQueue_TxTyp_id foreign key (TxTyp_id) references Trans.TransactType (TxTyp_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_TransTransactQueue primary key nonclustered (Tran_id)
 , index ix_TransTransactQueue_Accnt_id_TxTyp_id nonclustered (Accnt_id, TxTyp_id, Tran_id)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_TransTransactQueue primary key clustered (Tran_id)
 , index ix_TransTransact_Accnt_id_TxTyp_id nonclustered (Accnt_id, TxTyp_id, Tran_id)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create table Trans.Transact (
   Tran_id uniqueidentifier not null
 , Accnt_id bigint not null
 , Fund_id bigint not null
 , Curr_code char(3) not null
 , TxTyp_id bigint not null
 , Rebal_id uniqueidentifier null
 , Amount decimal(20, 6) not null
 , TranQueued_dt datetime not null
 , TranProcessed_dt datetime not null default(getdate())'
if @fks = 1 select @s += N'
 , constraint fk_TransTransact_Accnt_Curr_Code foreign key (Accnt_id, Fund_id, Curr_code) references Accnt.Account (Accnt_id, Fund_id, Curr_code)
 , constraint fk_TransTransact_Rebal_id_Accnt_id foreign key (Rebal_id, Accnt_id, Fund_id, Curr_code) references Rebal.Account (Rebal_id, Accnt_id, Fund_id, Curr_code)
 , constraint fk_TransTransact_TxTyp_id foreign key (TxTyp_id) references Trans.TransactType (TxTyp_id)'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_TransTransact primary key nonclustered (Tran_id)
 , index ix_TransTransact_Accnt_id_TxTyp_id nonclustered (Accnt_id, Rebal_id, TxTyp_id, Tran_id)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_TransTransact primary key clustered (Tran_id)
 , index ix_TransTransact_Accnt_id_TxTyp_id nonclustered (Accnt_id, TxTyp_id, Tran_id)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create table Rebal.ErrorLog (
   Error_id uniqueidentifier not null
 , Error_number bigint null
 , Error_severity int null
 , Error_state int null
 , Error_procedure nvarchar(128) null
 , Error_line int null
 , Error_message nvarchar(max) null
 , Error_dt datetime null
 , Sql_Process_id bigint null
 , Line_no int null
 , Log_point int null
 , rebal_accts_line_no bigint null
 , rebal_acct_line_no bigint null
 , RetryNo bigint null
 , MaxRetries bigint null
 , Rebal_id uniqueidentifier null
 , ThreadId bigint not null
 , MaxThreadId bigint not null'
if @inmemtbl = 1 select @s += N' 
 , constraint pk_RebalErrorLog primary key nonclustered (Error_id)
)  with (memory_optimized = on'+@dur+N');
'
else select @s += N'
 , constraint pk_RebalErrorLog primary key clustered (Error_id)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s


select @s = N'
create table Rebal.DebugLog (
   LogId bigint not null identity(1, 1)
 , LogUid uniqueidentifier not null
 , LogStep smallint not null
 , Logdt datetime2 not null default(getdate())
 , ThreadId bigint not null
 , MaxThreadId bigint not null
 , spid int not null
 , Curr_code char(3) null
 , Rebal_id uniqueidentifier null
 , Fund_id bigint null
 , Accnt_id bigint null
 , QueueNo bigint null
 , QueueNoFrom bigint null
 , QueueNoTo bigint null
 , ThreadsToComplete bigint null
 , Mutex_id uniqueidentifier null
 , Mutex_id_check uniqueidentifier null
 , RetryCount bigint null
 , Err_id uniqueidentifier null
 , Err_num bigint null
 , Err_id2 uniqueidentifier null
 , Err_num2 bigint null
 , AccntsCompleted bigint null
 , AccntRetryNo smallint null
 , AccntMaxRetries smallint null'
if @inmemtyp = 1 select @s += N' 
 , constraint pk_RebalDebugLog primary key nonclustered (LogUId, LogStep)
)  with (memory_optimized = on, durability = SCHEMA_ONLY);
'
else select @s += N'
 , constraint pk_RebalDebugLog primary key clustered (LogUId, LogStep)
);
'
if @exec = 1 exec(@s)
if @prnt = 1 print @s

end
go

