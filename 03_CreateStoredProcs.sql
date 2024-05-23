set nocount on
go
use master
go
use WealthBench
go
if exists (select * from sys.procedures where name = 'CloseRebalance') drop procedure CloseRebalance
if exists (select * from sys.procedures where name = 'GetPriceSnapshot') drop procedure GetPriceSnapshot
if exists (select * from sys.procedures where name = 'InsPriceSnapshotMutex') drop procedure InsPriceSnapshotMutex
if exists (select * from sys.procedures where name = 'RebalanceAcct') drop procedure RebalanceAcct
if exists (select * from sys.procedures where name = 'Rebalance') drop procedure Rebalance
if exists (select * from sys.procedures where name = 'PriceUpdate') drop procedure PriceUpdate
if exists (select * from sys.procedures where name = 'Deposit') drop procedure Deposit
if exists (select * from sys.procedures where name = 'Withdrawal') drop procedure Withdrawal
if exists (select * from sys.procedures where name = 'ArchiveHistory') drop procedure ArchiveHistory
if exists (select * from sys.procedures where name = 'PriceUpdate') drop procedure PriceUpdate
if exists (select * from sys.procedures where name = 'PriceUpdateWrap') drop procedure PriceUpdateWrap

if exists (select * from sys.procedures where name = 'AccountAsset') drop procedure Archive.AccountAsset
if exists (select * from sys.procedures where name = 'TransTrade') drop procedure Archive.TransTrade
if exists (select * from sys.procedures where name = 'FundRebalance') drop procedure Archive.FundRebalance

go

/*****************************
* CloseRebalance
******************************/
go
create or alter procedure dbo.CloseRebalance
   @Curr_code char(3)
 , @Fund_id bigint
 , @Rebal_id uniqueidentifier
 , @QueueNo bigint
 , @QueueNoTo bigint
 , @RebalancePriceSnapshot dbo.AssetList readonly
with native_compilation, schemabinding, execute as owner as begin atomic with (transaction isolation level = repeatable read, language = N'us_english')
--as begin

declare @Rebal_id_New uniqueidentifier = newid()
      , @Mutex_id_insert uniqueidentifier = newid()
      , @Mutex_id_exists uniqueidentifier

declare @FundBalance decimal(20, 6), @AccntsCompleted bigint, @Err_num bigint

if @Rebal_id is not null and @Fund_id is not null and @QueueNo >= @QueueNoTo
 begin

     select @Mutex_id_exists = Mutex_id from Rebal.FundMutex where Fund_id = @Fund_id
     if @Mutex_id_exists is null
      begin
       insert Rebal.FundMutex (Fund_id, Rebal_id, Mutex_id) values (@Fund_id, @Rebal_id, @Mutex_id_insert)
        begin
         select @FundBalance = sum(IsNull(InvBalance, 0)+Isnull(CashBalance, 0)) from Accnt.Balance where Fund_id = @Fund_id
         
         update Fund.Balance set Balance += 1 where Fund_id = @Fund_id

         insert into Fund.BalanceHistory (Fund_id, Rebal_id, Curr_code, LogDt, Balance) values (@Fund_id, @Rebal_id, @Curr_code, getdate(), @FundBalance)

         update Rebal.Fund set End_dt = getdate() where Rebal_id = @Rebal_id

         insert Rebal.Fund (Rebal_id, Fund_id, Curr_Code, Start_dt, End_dt)
         values (@Rebal_id_New, @Fund_id, @Curr_Code, getdate(), '01-Jan-1900')

		 insert Rebal.PriceSnapshot (Rebal_id, Fund_id, Asset_id, Exch_code, Curr_code, Price)
		 select @Rebal_id_New, @Fund_id, Asset_id, Exch_code, Curr_code, Price
		 from   @RebalancePriceSnapshot

         update Thread.State set NextQueueNo = QueueNoFrom where Fund_id = @Fund_id

        end

       delete Rebal.FundMutex where Fund_id = @Fund_id
      end
 end
end
go
/*****************************
* GetPriceSnapshot
******************************/
go
create or alter procedure dbo.GetPriceSnapshot
   @Curr_Code char(3)
with native_compilation, schemabinding, execute as owner as begin atomic with (transaction isolation level = repeatable read, language = N'us_english')
--as begin
 select Asset_id, Exch_code, Curr_code, Price
 from   Asset.ListingPrice where Curr_code = @Curr_Code
end
go
/*****************************
* InsPriceSnapshotMutex
******************************/
go
create or alter procedure dbo.InsPriceSnapshotMutex
   @Curr_Code char(3)
 , @Mutex_id uniqueidentifier
with native_compilation, schemabinding, execute as owner as begin atomic with (transaction isolation level = repeatable read, language = N'us_english')
--as begin
begin try
insert Rebal.PriceSnapshotMutex (Curr_code, Mutex_id) values (@Curr_code, @Mutex_id)
end try
begin catch
end catch
end
go
/*****************************
* RebalanceAcct
******************************/
create or alter procedure RebalanceAcct
   @Rebal_id uniqueidentifier
 , @Accnt_id bigint
 , @Curr_code char(3)
 , @Fund_id bigint
 , @Model_id bigint
 , @Audit_Id uniqueidentifier
 , @ThreadId bigint
with native_compilation, schemabinding, execute as owner as begin atomic with (transaction isolation level = repeatable read, language = N'us_english')
--as begin

declare @HoldingCurrent           dbo.HoldingsCurrent
      , @HoldingTarget            dbo.HoldingsTarget
      , @Transacts                dbo.Transact
      , @AccountAsset             dbo.RebalanceAsset
      , @HoldingAssetList         dbo.AssetList
      , @ModelAssetList           dbo.AssetList

declare @Start_dt                 datetime2 = sysdatetime()
      , @Exch_code                varchar(10)

declare @CurrentInvBalance        decimal(20, 6)
      , @CurrentCashBalance       decimal(20, 6)
      , @CurrentHoldingsCount     bigint
      , @CurrentHoldingsValue     decimal(20, 6)

declare @NewDeposits              decimal(20, 6)
      , @NewWithdrawals           decimal(20, 6)

declare @TargetInvestValue        decimal(20, 6)
      , @RoundedTargetInvestValue decimal(20, 6)
      , @RebalancedInvestValue    decimal(20, 6)
      , @RebalancedCashBalance    decimal(20, 6)

declare @AdjAsset_id              bigint
      , @AdjAsset_id_Price        decimal(20, 6)

/* Get current Account balance and key attributes */
select @Curr_code = Curr_code, @Model_id = Model_id, @CurrentInvBalance = InvBalance, @CurrentCashBalance = CashBalance 
from   Accnt.Balance where Accnt_id = @Accnt_id

insert Rebal.Account (Rebal_id, Item_no, Fund_id, Curr_Code, Accnt_id, Model_id, Sql_Process_id, ThreadId)
values (@Rebal_id, 1, @Fund_id, @Curr_Code, @Accnt_id, @Model_id, @@spid, @ThreadId)

insert @HoldingAssetList (Asset_id, Exch_code, Curr_code, Units)
select Asset_id, Exch_code, Curr_code, Units 
from   Accnt.Holding
where  Accnt_id = @Accnt_id

insert @ModelAssetList (Asset_id, Exch_code, Curr_code, Weighting)
select Asset_id, Exch_code, Curr_code, Weighting 
from   Asset.ModelAsset
where  Model_id = @Model_id

/* Get queued Deposits and Withdrawals */
insert @Transacts (Tran_id, Accnt_id, TxTyp_id, Rebal_id, Amount, Curr_Code, TranQueued_dt)
select Tran_id, Accnt_id, TxTyp_id, @Rebal_id, Amount, Curr_Code, TranQueued_dt
from   Trans.TransactQueue where Accnt_id = @Accnt_id and TxTyp_id in (1, 2)
select @NewDeposits    = sum(Amount) from @Transacts where TxTyp_id = 1
select @NewWithdrawals = sum(Amount) from @Transacts where TxTyp_id = 2
insert Trans.Transact(Tran_id, Accnt_id, Fund_id, Curr_Code, TxTyp_id, Rebal_id, Amount, TranQueued_dt)
select Tran_id, Accnt_id, @Fund_id, Curr_Code, TxTyp_id, Rebal_id, Amount, TranQueued_dt
from   @Transacts
delete Trans.TransactQueue where Accnt_id = @Accnt_id and TxTyp_id in (1, 2)

/* Get current Holdings */
insert @HoldingCurrent (Asset_id, Exch_code, Curr_code, Units, Price)
select h.Asset_id, h.Exch_code, h.Curr_code, h.Units, rps.Price
from   @HoldingAssetList h
join   Rebal.PriceSnapshot rps on rps.Rebal_id = @Rebal_id and h.Asset_id = rps.Asset_id and h.Exch_code = rps.Exch_code and h.Curr_code = rps.Curr_code

/* Calculate Holdings value using current listing prices */
select @CurrentHoldingsValue = sum(Units * Price), @CurrentHoldingsCount = count(*) from @HoldingCurrent

select @CurrentHoldingsValue = isnull(@CurrentHoldingsValue, 0), @CurrentCashBalance = isnull(@CurrentCashBalance, 0), @NewDeposits = isnull(@NewDeposits, 0), @NewWithdrawals = isnull(@NewWithdrawals, 0)
     , @CurrentInvBalance = isnull(@CurrentInvBalance, 0), @CurrentHoldingsCount = isnull(@CurrentHoldingsCount, 0)

/* Set the Target Balance (Existing Holdings + Cash + Deposits - Withdrawals) */
select @TargetInvestValue  = @CurrentHoldingsValue + @CurrentCashBalance + @NewDeposits - @NewWithdrawals
select @TargetInvestValue  = isnull(@TargetInvestValue, 0)

/* Get Model Allocation assets & weighting, with current listing prices */
insert @HoldingTarget (Asset_id, Exch_code, Curr_code, Weighting, Price, Units)
select a.Asset_id, a.Exch_code, a.Curr_code, a.Weighting, rps.Price, (@TargetInvestValue * a.Weighting / rps.Price) /* <-- Allocate assets based upon model weighting */
from   @ModelAssetList a
join   Rebal.PriceSnapshot rps on rps.Rebal_id = @Rebal_id and a.Asset_id = rps.Asset_id and a.Exch_code = rps.Exch_code and a.Curr_code = rps.Curr_code

/* Round units */
update @HoldingTarget set UnitsRounded = Round(Units, 0), UnitsAdjusted = Round(Units, 0)

--select @RoundedTargetInvestValue = sum(t.UnitsAdjusted * t.Price) from @HoldingTarget t where t.UnitsAdjusted >= 1
select @RoundedTargetInvestValue = sum(t.Value) from @HoldingTarget t where t.UnitsAdjusted > 0
select @RoundedTargetInvestValue = isnull(@RoundedTargetInvestValue, 0)

/* If model allocation exceeds investment balance, adjust down until <= balance */
if @RoundedTargetInvestValue > @TargetInvestValue
 begin
  select @AdjAsset_id = null
  select @AdjAsset_id = (select top 1 t.Asset_id from @HoldingTarget t where UnitsAdjusted >= 1 order by Price asc, Asset_id asc)
  while @AdjAsset_id is not null
   begin
    select @AdjAsset_id_Price = Price from @HoldingTarget t where Asset_id = @AdjAsset_id
    update @HoldingTarget set UnitsAdjusted = UnitsAdjusted - 1 where Asset_id = @AdjAsset_id
	select @RoundedTargetInvestValue = sum(isnull(t.UnitsAdjusted, 0) * isnull(t.Price, 0)) from @HoldingTarget t where t.UnitsAdjusted >= 1
	select @RoundedTargetInvestValue = isnull(@RoundedTargetInvestValue, 0)
    select @AdjAsset_id = null
    if @RoundedTargetInvestValue > @TargetInvestValue
    select @AdjAsset_id = (select top 1 t.Asset_id from @HoldingTarget t where UnitsAdjusted >= 1 order by Price asc, Asset_id asc)
   end
 end

update @HoldingTarget set UnitsAdjusted = UnitsRounded where UnitsAdjusted = 0

select @RebalancedInvestValue = sum(isnull(t.UnitsAdjusted, 0) * isnull(t.Price, 0)) from @HoldingTarget t
select @RebalancedCashBalance = isnull(@TargetInvestValue, 0) - isnull(@RebalancedInvestValue, 0)

/* Create Buy & Sell trades from delta of Current / Target holdings */
insert Trans.Trade (Rebal_id, Accnt_id, Asset_id, TrTyp_id, Exch_code, Fund_id, Curr_code, Units, Price) 
select @Rebal_id as Rebal_id, @Accnt_id as Accnt_id, Asset_id, case when isnull(Units_target, 0) > isnull(Units_current, 0) then 1 when isnull(Units_target, 0) < isnull(Units_current, 0) then 2 else 0 end as TrTyp_id
     , Exch_code, @Fund_id as Fund_id, Curr_code, (isnull(Units_target, 0) - isnull(Units_current, 0)) as Units, Price
from  (select     c.Exch_code, c.Curr_code, t.Price, c.Asset_id, c.Units as "Units_current", t.UnitsAdjusted as "Units_target"
       from       @HoldingCurrent c
       left join  @HoldingTarget t on c.Asset_id = t.Asset_id
       union
       select     t.Exch_code, t.Curr_code, t.Price, t.Asset_id, c.Units as "Units_current", t.UnitsAdjusted as "Units_target"
       from       @HoldingTarget t
       left join  @HoldingCurrent c on c.Asset_id = t.Asset_id) ra
where (isnull(Units_target, 0) > isnull(Units_current, 0)) or (isnull(Units_target, 0) < isnull(Units_current, 0))

/* Replace Holdings */
delete from Accnt.Holding where Accnt_id = @Accnt_id
insert Accnt.Holding (Accnt_id,Asset_id,Fund_id,Exch_code,Curr_code,Units)
select @Accnt_id,Asset_id,@Fund_id,Exch_code,@Curr_code,UnitsAdjusted
from   @HoldingTarget
where  UnitsAdjusted > 0

/* Log Holdings in History */
insert Accnt.HoldingHistory (Accnt_id,Rebal_id,Asset_id,Fund_id,Exch_code,Curr_code,Units)
select @Accnt_id,@Rebal_id,Asset_id,@Fund_id,Exch_code,@Curr_code,UnitsAdjusted
from   @HoldingTarget
where  UnitsAdjusted > 0

/* Update Account Balance */
select @RebalancedInvestValue = isnull(@RebalancedInvestValue, 0), @RebalancedCashBalance = isnull(@RebalancedCashBalance, 0)
update Accnt.Balance set InvBalance = @RebalancedInvestValue, CashBalance = @RebalancedCashBalance where Accnt_id = @Accnt_id

/* Log Account Balance in History */
insert Accnt.BalanceHistory (Rebal_id,Accnt_id,Curr_code,Fund_id,InvBalance,CashBalance)
values (@Rebal_id,@Accnt_id,@Curr_code,@Fund_id,@RebalancedInvestValue,@RebalancedCashBalance)

/* Log Rebalance Assets for audit */
--insert Rebal.AccountAsset
--      (Rebal_id, Accnt_id, Asset_id, Model_id, Exch_code, Fund_id, Curr_code, Price, Units_current, Units_target, Value_target, Units_rounded, Value_rounded, Units_adjusted, Value_adjusted)
--select Rebal_id, Accnt_id, Asset_id, Model_id, Exch_code, Fund_id, Curr_code, Price, Units_current, Units_target, Value_target, Units_rounded, Value_rounded, Units_adjusted, Value_adjusted
--from   @AccountAsset

select @CurrentHoldingsCount = isnull(@CurrentHoldingsCount, 0), @CurrentHoldingsValue = isnull(@CurrentHoldingsValue, 0), @CurrentInvBalance = isnull(@CurrentInvBalance, 0), @CurrentCashBalance = isnull(@CurrentCashBalance, 0)
, @NewDeposits = isnull(@NewDeposits, 0), @NewWithdrawals = isnull(@NewWithdrawals, 0), @TargetInvestValue = isnull(@TargetInvestValue, 0), @RoundedTargetInvestValue = isnull(@RoundedTargetInvestValue, 0)
/* Log Rebalance Account State Variables for audit */
insert Rebal.AccountAudit 
       ( Audit_id, Rebal_id, Fund_id, Curr_code, Accnt_id, Start_dt, CurrentHoldingsCount, CurrentHoldingsValue, CurrentInvBalance, CurrentCashBalance, NewDeposits, NewWithdrawals, 
         TargetInvestValue, RoundedTargetInvestValue, RebalancedInvestValue, RebalancedCashBalance,End_dt)
values (@Audit_id,@Rebal_id,@Fund_id,@Curr_code,@Accnt_id,@Start_dt,@CurrentHoldingsCount,@CurrentHoldingsValue,@CurrentInvBalance,@CurrentCashBalance,@NewDeposits,@NewWithdrawals,
        @TargetInvestValue,@RoundedTargetInvestValue,@RebalancedInvestValue,@RebalancedCashBalance,null)

update Thread.State set NextQueueNo += 1 where ThreadId = @ThreadId

end
go
/*****************
* Rebalance
*****************/
create or alter procedure dbo.Rebalance @ThreadId bigint, @MaxThreadId bigint, @logstate smallint = 0 as
set nocount on
declare @Err_id uniqueidentifier, @Err_num bigint, @Err_sev int, @Err_st int, @Err_prc nvarchar(128), @Err_ln int, @Err_msg nvarchar(max), @Err_id2 uniqueidentifier, @Err_num2 bigint 
declare @rebal_accts_line_no bigint, @rebal_acct_line_no bigint, @Audit_Id uniqueidentifier
declare @Rebal_id uniqueidentifier, @RetryNo bigint, @MaxRetries bigint, @RetryCount bigint
declare @Accnt_id bigint, @Curr_code char(3), @Fund_id bigint, @Model_id bigint

select @Fund_id = Fund_id from Thread.State where ThreadId = @ThreadId

declare @QueueNo bigint, @QueueNoFrom bigint, @QueueNoTo bigint
select @Curr_code = Curr_code from Fund.Fund where Fund_id = @Fund_id
declare @LogUid uniqueidentifier = newid()

declare @AccntRetryNo smallint = 1, @AccntMaxRetries smallint = 5, @GateStatus smallint

if @logstate = 1
 begin
  insert Rebal.DebugLog (LogUid,LogStep,ThreadId,MaxThreadId,spid,Curr_code,Rebal_id,Fund_id,Accnt_id,QueueNo,QueueNoFrom,QueueNoTo,RetryCount,Err_id,Err_num,Err_id2,Err_num2,AccntRetryNo,AccntMaxRetries)
  values (@LogUid,1,@ThreadId,@MaxThreadId,@@spid,@Curr_code,@Rebal_id,@Fund_id,@Accnt_id,@QueueNo,@QueueNoFrom,@QueueNoTo,@RetryCount,@Err_id,@Err_num,@Err_id2,@Err_num2,@AccntRetryNo,@AccntMaxRetries)
 end

/*
* Rebalance an Account in this Fund, with up to 5 retries.
*/
begin
begin try

while @AccntRetryNo <= @AccntMaxRetries
 begin
  select @Rebal_id = null
  select @Rebal_id = Rebal_id from Rebal.Fund rf where rf.Fund_id = @Fund_id and rf.End_dt = '01-Jan-1900'
  select @GateStatus = GateStatus from Rebal.PriceSnapshotGate where Curr_code = @Curr_code
  select @Err_id = null, @Err_id2 = null, @Err_num = null, @Err_num2 = null, @RetryCount = 0
  if @Rebal_id is not null and @Fund_id is not null and @GateStatus = 1
   begin
    select @QueueNo = NextQueueNo, @QueueNoFrom = QueueNoFrom, @QueueNoTo = QueueNoTo from Thread.State where ThreadId = @ThreadId
    if @QueueNo between @QueueNoFrom and @QueueNoTo
     begin
      select @Accnt_id = Accnt_id from Rebal.AccountQueue where Fund_id = @Fund_id and Queue_No = @QueueNo
 	  select @Model_id = Model_id from Accnt.Account where Accnt_id = @Accnt_id
      select @RetryNo = 1, @MaxRetries = 5, @Audit_Id = newid()
      while @RetryNo <= @MaxRetries
       begin
        begin try
         exec dbo.RebalanceAcct @Rebal_id, @Accnt_id, @Curr_code, @Fund_id, @Model_id, @Audit_Id, @ThreadId
         select @RetryNo = @MaxRetries
         begin try
          update Rebal.AccountAudit set End_dt = sysdatetime() where Rebal_Id = @Rebal_Id and Accnt_id = @Accnt_id
         end try
         begin catch
         end catch
        end try
        begin catch
         select @Err_num = error_number(), @Err_sev = error_severity(), @Err_st = error_severity(), @Err_prc = error_procedure(), @Err_ln = error_line(), @Err_msg = error_message(), @Err_id = newid()
         if @Err_num in (2627, 41325) 
           begin
            insert into Rebal.ErrorLog(Error_id,Error_number,Error_severity,Error_state,Error_procedure,Error_line,Error_message,Error_dt,Sql_Process_id,rebal_accts_line_no,rebal_acct_line_no,Log_point,Rebal_id,RetryNo,MaxRetries,ThreadId,MaxThreadId)
            values (@Err_id,@Err_num,@Err_sev,@Err_st,@Err_prc,@Err_ln,@Err_msg,getdate(),@@spid,@rebal_accts_line_no,@rebal_acct_line_no,210,@Rebal_id,@RetryNo,@MaxRetries,@ThreadId,@MaxThreadId);
            select @RetryNo = @MaxRetries
           end
          else
           begin
            insert into Rebal.ErrorLog(Error_id,Error_number,Error_severity,Error_state,Error_procedure,Error_line,Error_message,Error_dt,Sql_Process_id,rebal_accts_line_no,rebal_acct_line_no,Log_point,Rebal_id,RetryNo,MaxRetries,ThreadId,MaxThreadId)
            values (@Err_id,@Err_num,@Err_sev,@Err_st,@Err_prc,@Err_ln,@Err_msg,getdate(),@@spid,@rebal_accts_line_no,@rebal_acct_line_no,220,@Rebal_id,@RetryNo,@MaxRetries,@ThreadId,@MaxThreadId);
            throw
           end
        end catch
        select @RetryNo += 1, @RetryCount += 1
       end
     end
   end
  if @Accnt_Id is null
   waitfor delay '00:00:00.200'; 
  else
   select @AccntRetryNo = @AccntMaxRetries

  select @AccntRetryNo += 1
 end

end try
begin catch
 select @Err_num2 = error_number(), @Err_sev = error_severity(), @Err_st = error_severity(), @Err_prc = error_procedure(), @Err_ln = error_line(), @Err_msg = error_message(), @Err_id2 = newid()
 insert into Rebal.ErrorLog(Error_id,Error_number,Error_severity,Error_state,Error_procedure,Error_line,Error_message,Error_dt,Sql_Process_id,rebal_accts_line_no,rebal_acct_line_no,Log_point,ThreadId,MaxThreadId)
 values (@Err_id2,@Err_num,@Err_sev,@Err_st,@Err_prc,@Err_ln,@Err_msg,getdate(),@@spid,@rebal_accts_line_no,@rebal_acct_line_no,200,@ThreadId,@MaxThreadId);
 --Throw
end catch

if @logstate = 1
 begin
  insert Rebal.DebugLog (LogUid,LogStep,ThreadId,MaxThreadId,spid,Curr_code,Rebal_id,Fund_id,Accnt_id,QueueNo,QueueNoFrom,QueueNoTo,RetryCount,Err_id,Err_num,Err_id2,Err_num2,AccntRetryNo,AccntMaxRetries)
  values (@LogUid,2,@ThreadId,@MaxThreadId,@@spid,@Curr_code,@Rebal_id,@Fund_id,@Accnt_id,@QueueNo,@QueueNoFrom,@QueueNoTo,@RetryCount,@Err_id,@Err_num,@Err_id2,@Err_num2,@AccntRetryNo,@AccntMaxRetries)
 end
end

/*
* Close this Fund Rebalance if the last Account has been rebalanced, and open a new Fund Rebalance
*/
begin
begin try
declare @Mutex_id uniqueidentifier, @Mutex_id_check uniqueidentifier
select @QueueNo = NextQueueNo, @QueueNoFrom = QueueNoFrom, @QueueNoTo = QueueNoTo from Thread.State where ThreadId = @ThreadId
select @RetryCount = 0
 if @QueueNo >= @QueueNoTo
  begin
   declare @RebalancePriceSnapshot dbo.AssetList
   declare @ThreadsToComplete bigint
   select @ThreadsToComplete = count(*) from Thread.State where Fund_id = @Fund_id and NextQueueNo <= QueueNoTo
   if @ThreadsToComplete = 0
    begin

     select @Mutex_id = newid()
     begin try
      exec dbo.InsPriceSnapshotMutex @Curr_Code, @Mutex_id
     end try
     begin catch
     end catch
     select @Mutex_id_check = Mutex_id from Rebal.PriceSnapshotMutex where Curr_code = @Curr_code and Mutex_id = @Mutex_id
     if @Mutex_id_check is not null
      begin
       update Rebal.PriceSnapshotGate set GateStatus = 0 where Curr_code = @Curr_code
       select @RetryNo = 1, @MaxRetries = 5
       while @RetryNo <= @MaxRetries
        begin
         begin try
	      insert into @RebalancePriceSnapshot (Asset_id, Exch_code, Curr_code, Price) exec dbo.GetPriceSnapshot @Curr_Code
          select @RetryNo = @MaxRetries
         end try
         begin catch
          select @Err_num = error_number();
         end catch
         select @RetryNo += 1, @RetryCount += 1
        end

       exec dbo.CloseRebalance @Curr_code, @Fund_id, @Rebal_id, @QueueNo, @QueueNoTo, @RebalancePriceSnapshot

       if @Mutex_id_check is not null update Rebal.PriceSnapshotGate set GateStatus = 1 where Curr_code = @Curr_code
       delete Rebal.PriceSnapshotMutex where Curr_code = @Curr_code and Mutex_id = @Mutex_id
      end
    end
  end
end try
begin catch
 select @Err_num = error_number(), @Err_sev = error_severity(), @Err_st = error_severity(), @Err_prc = error_procedure(), @Err_ln = error_line(), @Err_msg = error_message()
 select @Err_id = newid()
 insert into Rebal.ErrorLog(Error_id,Error_number,Error_severity,Error_state,Error_procedure,Error_line,Error_message,Error_dt,Sql_Process_id,rebal_accts_line_no,rebal_acct_line_no,Log_point,ThreadId,MaxThreadId)
 values (@Err_id,@Err_num,@Err_sev,@Err_st,@Err_prc,@Err_ln,@Err_msg,getdate(),@@spid,@rebal_accts_line_no,@rebal_acct_line_no,400,@ThreadId,@MaxThreadId);
 delete Rebal.PriceSnapshotMutex where Curr_code = @Curr_code and Mutex_id = @Mutex_id
 if @Mutex_id_check is not null update Rebal.PriceSnapshotGate set GateStatus = 1 where Curr_code = @Curr_code
 --Throw
end catch

if @Accnt_Id is null waitfor delay '00:00:00.200'; 

if @logstate = 1
 begin
  insert Rebal.DebugLog (LogUid,LogStep,ThreadId,MaxThreadId,spid,Curr_code,Rebal_id,Fund_id,Accnt_id,QueueNo,QueueNoFrom,QueueNoTo,ThreadsToComplete,Mutex_id,Mutex_id_check,RetryCount,Err_id,Err_num,Err_id2,Err_num2)
  values (@LogUid,3,@ThreadId,@MaxThreadId,@@spid,@Curr_code,@Rebal_id,@Fund_id,@Accnt_id,@QueueNo,@QueueNoFrom,@QueueNoTo,@ThreadsToComplete,@Mutex_id,@Mutex_id_check,@RetryCount,@Err_id,@Err_num,@Err_id2,@Err_num2)
 end
end

go

/*****************
* PriceUpdate
*****************/
go
create or alter procedure PriceUpdate @Asset_id bigint, @Curr_code char(3), @Exch_code varchar(10), @Variance bigint
with native_compilation, schemabinding, execute as owner as begin atomic with (transaction isolation level = repeatable read, language = N'us_english')
--as begin
declare @PricePre decimal(20, 6), @PricePost decimal(20, 6)

declare @GateStatus smallint
select @GateStatus = GateStatus from Rebal.PriceSnapshotGate where Curr_code = @Curr_code
if @GateStatus = 1
 begin

  /* Get the asset's current Price & Price adjusted by @Variance */
  select @PricePre  = Price, @PricePost = Price + (Price * (convert(decimal(20, 6), @Variance)/100))
  from   Asset.ListingPrice 
  where  Asset_id = @Asset_id and Exch_code = @Exch_code and Curr_code = @Curr_code

  /* Update ListingPrice */
  update Asset.ListingPrice set Price = @PricePost where Asset_id = @Asset_id and Exch_code = @Exch_code and Curr_code = @Curr_code

  /* Log update to ListingPriceHistory */
  insert into Asset.ListingPriceHistory (Asset_id, Exch_code, Price_dt, PricePre, PricePost, Curr_code) values (@Asset_id, @Exch_code, getdate(), @PricePre, @PricePost, @Curr_code)

 end
end
go
/*****************
* PriceUpdateWrap
*****************/
go
create or alter procedure PriceUpdateWrap @Asset_id bigint, @Curr_code char(3), @Exch_code varchar(10), @Variance bigint
as begin
declare @Err_id uniqueidentifier, @RetryNo bigint, @MaxRetries bigint, @RetryCount bigint
select @RetryNo = 1, @MaxRetries = 5

while @RetryNo <= @MaxRetries
 begin
  begin try
   exec dbo.PriceUpdate @Asset_id, @Curr_code, @Exch_code, @Variance
   select @RetryNo = @MaxRetries
  end try
  begin catch
  end catch
  select @RetryNo += 1
 end
end
go
/*****************
* Deposit
*****************/
create or alter procedure Deposit @Accnt_id bigint, @Curr_code char(3), @Fund_id bigint, @Amount decimal(20, 6)
with native_compilation, schemabinding, execute as owner as begin atomic with (transaction isolation level = repeatable read, language = N'us_english')
--as begin
insert into Trans.TransactQueue (Tran_id,Accnt_id,TxTyp_id,Amount,Fund_id,Curr_Code,TranQueued_dt)
values (newid(),@Accnt_id,1,@Amount,@Fund_id,@Curr_Code,getdate())
end
go
/*****************
* Withdrawal
*****************/
create or alter procedure Withdrawal @Accnt_id bigint, @Curr_code char(3), @Fund_id bigint, @Amount decimal(20, 6)
with native_compilation, schemabinding, execute as owner as begin atomic with (transaction isolation level = repeatable read, language = N'us_english')
--as begin
insert into Trans.Transact (Tran_id,Accnt_id,TxTyp_id,Amount,Fund_id,Curr_Code,TranQueued_dt)
values (newid(),@Accnt_id,2,@Amount,@Fund_id,@Curr_Code,getdate())
end
go
/*****************
* Archive History
*****************/
create or alter procedure ArchiveHistory as
set nocount on

declare @MaxRebalHistory_id bigint, @ArchiveToRebalHistory_id bigint
select  @MaxRebalHistory_id = max(RebalHistory_id) from Rebal.AccountAsset
if @MaxRebalHistory_id > 1000
 begin
  select @ArchiveToRebalHistory_id = @MaxRebalHistory_id - 1000
  insert  Rebal.AccountAssetArchive (RebalHistory_id,Rebal_id,Accnt_id,Asset_id,Model_id,Exch_code,Fund_id,Curr_code,Price,Units_current,Units_target,Value_target,Units_rounded,Value_rounded,Units_adjusted,Value_adjusted)
  select  RebalHistory_id,Rebal_id,Accnt_id,Asset_id,Model_id,Exch_code,Fund_id,Curr_code,Price,Units_current,Units_target,Value_target,Units_rounded,Value_rounded,Units_adjusted,Value_adjusted
  from    Rebal.AccountAsset
  where   RebalHistory_id <= @ArchiveToRebalHistory_id

  delete 
  from    Rebal.AccountAsset
  where   RebalHistory_id <= @ArchiveToRebalHistory_id
 end
go



create or alter procedure Archive.AccountAsset
as
set nocount on
select 'AccountAsset'
go

create or alter procedure Archive.TransTrade
as
set nocount on
select 'TransTrade'
go


create or alter procedure Archive.FundRebalance
as
set nocount on
select 'FundRebalance'
go

