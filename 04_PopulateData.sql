use master
go
set nocount on
go
use WealthBench
go

delete from Rebal.ErrorLog
delete from Rebal.FundMutex
delete from Trans.TransactQueue
delete from Trans.Transact
delete from Trans.TransactType
delete from Trans.Trade
delete from Trans.TradeType
delete from Rebal.AccountAsset
delete from Rebal.AccountQueue
delete from Rebal.AccountAudit
delete from Rebal.Account
delete from Fund.BalanceHistory
delete from Rebal.Fund
delete from Accnt.HoldingHistory
delete from Accnt.Holding
delete from Accnt.BalanceHistory
delete from Accnt.Balance
--alter table Accnt.Account drop constraint fk_Account_Inv_Id
delete from Accnt.Account
delete from Accnt.Investor
--alter table Accnt.Account  add constraint fk_Account_Inv_Id foreign key (Inv_id) references Accnt.Investor (Inv_id)
delete from Asset.ListingPriceHistory
delete from Asset.ModelAsset
delete from Asset.Model
delete from Accnt.Advisor
delete from Asset.ListingPrice
delete from Asset.Listing
delete from Asset.Asset
delete from Asset.Class
delete from Asset.Exchange
delete from Fund.Balance
delete from Fund.Fund
delete from Currency
delete from Rebal.PriceSnapshotGate



/*****************************
*
* Populate DB with domain data
*
******************************/

--select * from Thread.StateAudit order by ThreadId


insert into dbo.Currency (Curr_code, Curr_name) values ('GBP', 'Great British Pound'),('USD', 'United States Dollar'),('AUD', 'Australian Dollar')

insert into Rebal.PriceSnapshotGate (Curr_code, GateStatus) values ('GBP', 1),('USD', 1),('AUD', 1)

insert into Fund.Fund (Fund_id, Fund_name, Curr_code) values
(1, 'Fund 1 UK Members', 'GBP'),(2, 'Fund 2 US Members', 'USD'),(3, 'Fund 3 AU Members', 'AUD'),(4, 'Fund 4 GBP Growth', 'GBP'),(5, 'Fund 5 USD Growth', 'USD'),(6, 'Fund 6 AUD Growth', 'AUD')

insert into Fund.Balance (Fund_id, Curr_code, Balance) values
(1, 'GBP', 0.00),(2, 'USD', 0.00),(3, 'AUD', 0.00),(4, 'GBP', 0.00),(5, 'USD', 0.00),(6, 'AUD', 0.00)

insert into Asset.Exchange (Exch_code, Exch_name, ISIN_Prefix, Curr_code, List_count) values 
 ('FTSE', 'Financial Times Stock Exchange', 'UK', 'GBP', 0)
,('NYSE', 'New York Stock Exchange', 'US', 'USD', 0)
,('ASX', 'Australian Securities Exchange', 'AU', 'AUD', 0)
--,('NASDAQ', 'National Association of Securities Dealers Automated Quotations Stock Market', 'US', 'USD', 0)

insert into Asset.Class (Class_code, Class_name) values 
('ST', 'Stock'),('BO', 'Bonds'),('CO', 'Commodities'),('PR', 'Property')

insert into Trans.TradeType (TrTyp_id, TrTyp_name) values
(1, 'Buy'),(2, 'Sell')

insert into Trans.TransactType (TxTyp_id, TxTyp_name) values
(1, 'Deposit'),(2, 'Withdrawal')--,(3, 'Rollforward Adjustment'),(4, 'Rebalance Adjustment')
go






/*****************************
*
* Create Numbers table
*
* Copyright: Erland Sommarskog
* URL: https://www.sommarskog.se/Short%20Stories/table-of-numbers.html
*
******************************/
go
if not exists (select * from tempdb.sys.tables where name = 'Numbers')
begin
CREATE TABLE tempdb.dbo.Numbers (n int NOT NULL PRIMARY KEY);
WITH L0   AS (SELECT 1 AS c UNION ALL SELECT 1),
     L1   AS (SELECT 1 AS c FROM L0 AS A, L0 AS B),
     L2   AS (SELECT 1 AS c FROM L1 AS A, L1 AS B),
     L3   AS (SELECT 1 AS c FROM L2 AS A, L2 AS B),
     L4   AS (SELECT 1 AS c FROM L3 AS A, L3 AS B),
     L5   AS (SELECT 1 AS c FROM L4 AS A, L4 AS B),
     Nums AS (SELECT row_number() OVER(ORDER BY c) AS n FROM L5)
INSERT tempdb.dbo.Numbers SELECT n FROM Nums WHERE n <= 1000000;
SELECT MIN(n) AS "min", MAX(n) AS "max", COUNT(*) AS "count" FROM tempdb.dbo.Numbers;
end
go


/*****************************
*
* Populate DB with random data
*
******************************/

declare @NoOfAssetsPerExchange bigint = 200
      , @NoOfAdvisors bigint = 10
      , @NoOfInvestorsPerFund bigint = 10000
	  , @NoOfModelsPerAdvisorPerCurrency bigint = 10
	  , @MinimumAssetsPerModel bigint = 5
	  , @MaximumAssetsPerModel bigint = 20

/* Assets & Listings */
declare @Exch_code varchar(10), @ISIN_Prefix char(2), @Class_code varchar(64), @Asset_id bigint, @ISIN_code char(12), @Asset_name nvarchar(256), @Asset_count bigint, @Price decimal(20, 6), @Curr_code char(3), @List_no bigint
select @Asset_id = 1
select @Exch_code = min(Exch_code) from Asset.Exchange
while @Exch_code is not null
 begin
  select @Class_code = null, @ISIN_Prefix = null, @Curr_code = null, @List_no = 0
  select @ISIN_Prefix = ISIN_Prefix, @Curr_code = Curr_code from Asset.Exchange where Exch_code = @Exch_code
  select @Class_code = min(Class_code) from Asset.Class
  while @Class_code is not null
   begin
    select @Asset_count = 1
    while @Asset_count <= @NoOfAssetsPerExchange
     begin
      select @ISIN_code = null, @Asset_name = null, @Price = null
	  select @ISIN_Code = @ISIN_Prefix+left(('000000000'+convert(varchar(64), @Asset_id)), 12), @Asset_name = @Class_code+N'_'+convert(nvarchar(256), @Asset_id)+N'_'+convert(nvarchar(256), @ISIN_code)
	  insert into Asset.Asset (Asset_id, ISIN_code, Asset_name, Class_code) values (@Asset_id, @ISIN_code, @Asset_name, @Class_code)
	  select @Price = round((rand()*(5-1)), 0)+1
	  select @List_no += 1
	  insert into Asset.Listing (Asset_id, Exch_code, ISIN_Code, Curr_code, List_no) values (@Asset_id, @Exch_code, @ISIN_Code, @Curr_code, @List_no)
	  update Asset.Exchange set List_count = @List_no where Exch_code = @Exch_code
	  insert into Asset.ListingPrice (Asset_id, Exch_code, Curr_code, Price) values (@Asset_id, @Exch_code, @Curr_code, @Price)
      select @Asset_count += 1, @Asset_id += 1
     end
     select @Class_code = min(Class_code) from Asset.Class where Class_code > @Class_code
   end
  select @Exch_code = min(Exch_code) from Asset.Exchange where Exch_code > @Exch_code
 end

/* Advisors */
declare @Advisor_id bigint, @Advisor_name nvarchar(256)
select @Advisor_id = 1
while @Advisor_id <= @NoOfAdvisors
 begin
  select @Advisor_name = N'Advisor'+convert(nvarchar(256), @Advisor_id)
  insert into Accnt.Advisor (Advisor_id, Advisor_name) values (@Advisor_id, @Advisor_name)
  select @Advisor_id += 1
 end

/* Models */
declare @Model_id bigint, @Model_name nvarchar(256), @Adv_Model_count bigint, @Max_Advisor_id bigint
select @Max_Advisor_id = max(Advisor_id) from Accnt.Advisor
select @Advisor_id = 1, @Model_id = 1
while @Advisor_id <= @Max_Advisor_id
 begin
  select @Advisor_name = null, @Curr_code = null
  select @Advisor_name = Advisor_name from Accnt.Advisor where Advisor_id = @Advisor_id
  select @Curr_code = min(Curr_code) from dbo.Currency
  while @Curr_code is not null
   begin
    select @Adv_Model_count = 1
    while @Adv_Model_count <= @NoOfModelsPerAdvisorPerCurrency
     begin
      select @Adv_Model_count += 1
      select @Model_name = N'Model'+convert(nvarchar(256), @Model_id)
      insert into Asset.Model (Model_id, Model_name, Advisor_id, Curr_code) values (@Model_id, @Model_name, @Advisor_id, @Curr_code)  
      select @Model_id += 1
     end
    select @Curr_code = min(Curr_code) from dbo.Currency where Curr_code > @Curr_code
   end
  select @Advisor_id += 1
 end

/* Model Assets */
declare @ThisAllocationsCount int, @AssetNo int, @Weighting decimal(9, 6), @WeightRemaining decimal(9, 6), @MaxListingRowNo bigint, @RandListingNo bigint
select @Model_id = null, @Asset_id = 0, @Weighting = 0
select @Model_id = min(Model_id) from Asset.Model
while @Model_id is not null
 begin
  select @ThisAllocationsCount = null, @WeightRemaining = 100, @Curr_code = null
  select @Curr_code = Curr_Code from Asset.Model where Model_id = @Model_id
  select @ThisAllocationsCount = round((rand() * (@MaximumAssetsPerModel-@MinimumAssetsPerModel)), 0)+@MinimumAssetsPerModel
  update Asset.Model set AssetsCount = @ThisAllocationsCount where Model_id = @Model_id
  select @AssetNo = 1
  while @AssetNo <= @ThisAllocationsCount
   begin
    select @Asset_id = null, @RandListingNo = null, @Exch_code = null
	select @MaxListingRowNo = count(*) from Asset.Listing where Curr_code = @Curr_Code and Asset_id not in (select Asset_id from Asset.ModelAsset where Model_id = @Model_id)
	select @RandListingNo = (round((rand() * (@MaxListingRowNo - 1)), 0))+1

	select @Asset_id = Asset_id, @Exch_code = Exch_code
	from  (select row_number() over (order by Asset_id) as RowNo, Asset_id, Exch_code 
           from Asset.Listing where Curr_code = @Curr_Code and Asset_id not in (select Asset_id from Asset.ModelAsset where Model_id = @Model_id) ) a
	where  RowNo = @RandListingNo

	if @AssetNo = @ThisAllocationsCount select @Weighting = @WeightRemaining
     else select @Weighting = (round((rand() * (@WeightRemaining - (@ThisAllocationsCount - @AssetNo + 1))), 0))+1
	select @WeightRemaining = @WeightRemaining - @Weighting
    --if @WeightRemaining > 20 and @Weighting > 20 select @Weighting = 20 

    insert into Asset.ModelAsset (Model_id, Asset_id, Exch_code, Curr_code, Weighting) values (@Model_id, @Asset_id, @Exch_code, @Curr_code, @Weighting/100.000000)
	select @AssetNo += 1
   end
  select @Model_id = min(Model_id) from Asset.Model where Model_id > @Model_id
 end


/* Investors & Accounts */
declare @Fund_id bigint, @MaxModelRowNo bigint, @MaxInvId bigint

select @Fund_id = min(Fund_id) from Fund.Fund
while @Fund_id is not null
 begin
  select @Curr_code = null, @MaxInvId = null, @MaxModelRowNo = null
  select @Curr_code = Curr_code from Fund.Fund where Fund_id = @Fund_id

  if object_id('tempdb..#Models') is not null drop table #Models
  create table #Models (Id bigint not null identity(1, 1), Model_id bigint not null primary key, Curr_code char(3) not null)
  insert into #Models (Model_id, Curr_code) select Model_id, Curr_code from Asset.Model where Curr_code = @Curr_code order by Model_id
  select @MaxModelRowNo = max(Id) from #Models
  
  select @MaxInvId = max(Inv_id) from Accnt.Investor
  select @MaxInvId = isnull(@MaxInvId, 0)
  --select @Fund_id as "@Fund_id", @Curr_code as "@Curr_code", @MaxModelRowNo as "@MaxModelRowNo", @MaxInvId as "@MaxInvId", @NoOfInvestorsPerFund as "@NoOfInvestorsPerFund"

  insert Accnt.Investor (Inv_id, Inv_name)
  select n as Inv_id, N'Investor'+convert(nvarchar(256), n) as Inv_name
  from   tempdb..Numbers n
  where  n > @MaxInvId and n <= @MaxInvId + @NoOfInvestorsPerFund
  
  insert Accnt.Account (Accnt_id, Inv_id, Curr_code, Fund_id, Model_id)
  select i.Inv_id as Accnt_id, i.Inv_id, @Curr_code as Curr_code, @Fund_id as Fund_id, (select Model_id from #Models where Id = (round((rand() * (@MaxModelRowNo - 1)), 0))+1) as Model_id
  from   Accnt.Investor i
  where  Inv_id > @MaxInvId and Inv_id <= @MaxInvId + @NoOfInvestorsPerFund

  insert Accnt.Balance (Accnt_id, Inv_id, Curr_code, Fund_id, Model_id)
  select a.Inv_id as Accnt_id, a.Inv_id, a.Curr_code, a.Fund_id, a.Model_id
  from   Accnt.Account a
  where  Fund_id = @Fund_id

  insert Trans.TransactQueue (Tran_id, Accnt_id, TxTyp_id, Amount, Fund_id, Curr_Code)
  select newid() as Tran_id, a.Inv_id as Accnt_id, 1 as TxTyp_id, convert(decimal(20, 6), (((round((rand() * (100 - 1)), 0))+1)*1000)) as Amount, a.Fund_id, a.Curr_code
  from   Accnt.Account a
  where  Fund_id = @Fund_id

  insert Rebal.AccountQueue (Queue_No, Fund_id, Curr_Code, Accnt_id)
  select row_number() over (order by Accnt_id) as Queue_no, Fund_id, Curr_Code, Accnt_id
  from   Accnt.Account
  where  Fund_id = @Fund_id

  declare @Rebal_id uniqueidentifier = newid()
  insert Rebal.Fund (Rebal_id, Fund_id, Curr_Code, Start_dt, End_dt)
  select @Rebal_id, Fund_id, Curr_Code, getdate(), '01-Jan-1900'
  from   Fund.Fund where Fund_id = @Fund_id

  insert Rebal.PriceSnapshot (Rebal_id, Fund_id, Asset_id, Exch_code, Curr_code, Price)
  select @Rebal_id, @Fund_id, Asset_id, Exch_code, Curr_code, Price 
  from   Asset.ListingPrice where Curr_code = @Curr_Code

  select @Fund_id = min(Fund_id) from Fund.Fund where Fund_id > @Fund_id
 end
go


declare @NoOfFunds bigint, @MaxThreadId bigint = 60
select @NoOfFunds = count(*) from Fund.Fund

delete Thread.StateAudit
insert Thread.StateAudit (
        ThreadId, MaxThreadId,   Fund_id, NoOfFunds, ThreadMod, ThreadsPerFund, NthThread, AccntsCount, AccntsPerThread, QueueNoFrom,QueueNoTo,NextQueueNo)
select  ThreadId, MaxThreadId, c.Fund_id, NoOfFunds, ThreadMod, ThreadsPerFund, NthThread, AccntsCount, AccntsPerThread, QueueNoFrom
      , case when (ThreadId + NoOfFunds) > MaxThreadId then AccntsCount else QueueNoFrom + AccntsPerThread - 1 end as QueueNoTo, QueueNoFrom as NextQueueNo
from   (select ThreadId, MaxThreadId, b.Fund_id, NoOfFunds, ThreadMod, ThreadsPerFund, NthThread, AccntsCount, AccntsCount / ThreadsPerFund as AccntsPerThread
             , ((NthThread-1) * (AccntsCount / ThreadsPerFund))+1 as QueueNoFrom
        from  (select ThreadId, MaxThreadId, a.Fund_id, NoOfFunds, ThreadMod
                    , case when ThreadMod != 0 then case when ((ThreadsPerFund * NoOfFunds) + ThreadMod <= MaxThreadId) then ThreadsPerFund + 1 else ThreadsPerFund end else ThreadsPerFund end as ThreadsPerFund
                    , ((ThreadId-1) / @NoOfFunds)+1 as NthThread, AccntsCount
               from   (select ThreadId, @MaxThreadId as MaxThreadId
                            , (ThreadId % @NoOfFunds)+1 as Fund_id
                            , @NoOfFunds as NoOfFunds
                            , (ThreadId % @NoOfFunds)+1 as ThreadMod
                            , @MaxThreadId / @NoOfFunds as ThreadsPerFund
                       from   (select n as ThreadId from tempdb.dbo.Numbers where n <= @MaxThreadId) n
                      ) a
                join   (select Fund_id, count(*) as AccntsCount from Accnt.Account group by Fund_id) c on a.Fund_id = c.Fund_id
		        ) b
        ) c
order by ThreadId
delete from Thread.State
insert into Thread.State (ThreadId, Fund_id, QueueNoFrom, QueueNoTo, NextQueueNo)
select ThreadId, Fund_id, QueueNoFrom, QueueNoTo, NextQueueNo from Thread.StateAudit

select * from Thread.StateAudit where Fund_id = 1 order by ThreadId
select * from Thread.State where Fund_id = 1 order by ThreadId


go
