## Feature Store Example

This project demonstrates a basic machine learning pipeline utilizing a feature store for managing data, model training and evaluation, and tracking ML experiments.

# Setup
1. Create and activate a Python 3.6+ virtual environment:

```
python3 -m venv venv
source venv/bin/activate

```

2. Install requirements:

```
pip install -r requirements.txt
```

3. Install the project in editable mode:

```
pip install -e .

```

## Contents

* data_exploration.py: Functions for analyzing and visualizing the features data

* data_generation.py: Simulates getting features data and outputs to a CSV

* model_training.py: Trains a model on the features data

* model_saving.py: Saves trained models and outputs like predictions

* main.py: Orchestrates the ML pipeline steps

* src/: Source code

* feature_store/: Directory to store the features data
* model_runs/: Directory to store trained models and experiment outputs

## Running

1. Activate the virtual env:

```
source venv/bin/activate

```

2. Run main module:

```
python src/main.py
```

3. Model outputs in model_runs/

4. Deactivate virtual env when done:
```
deactivate
```

USE [IDB_Billing]
GO

/****** Object:  StoredProcedure [dbo].[Billing_CreateInvoices]    Script Date: 5/7/2024 12:40:44 PM ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Billing_CreateInvoices]') AND type IN (N'P', N'PC'))
	DROP PROCEDURE [dbo].[Billing_CreateInvoices]
GO

/****** Object:  StoredProcedure [dbo].[Billing_CreateInvoices]    Script Date: 5/7/2024 12:40:44 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[Billing_CreateInvoices]
( 
	@date1 DATETIME = NULL,  
	@date2 DATETIME = NULL, 
	@InvDate DATETIME = NULL,
	@BillingCode VARCHAR(16) = NULL,
	@ProductGroup VARCHAR(8) = NULL,
	@InvoiceType VARCHAR(32) = NULL,
	@ServiceType VARCHAR(32) = NULL,
	@UserName VARCHAR(32) = NULL,
	@ReportMode INT = 0,
	@SummaryType VARCHAR(16) = NULL,
	@Owner VARCHAR(10) = NULL,
	
	@UseTieredBilling BIT = 0,
	@NAVXrun BIT = 0,
	@EODrun BIT = 0,
	/* IDBBC-292: Flag to skip the EOD R8FIN Match comm calculations */
	@ExcludeMATCH BIT = 0,
	@Debug BIT = 0
)

AS

	SET NOCOUNT ON  

	--Defauly @EODrun to 0
	SET @EODrun = ISNULL(@EODrun,0)

	--IDBBC-292: Default @ExcludeMatch to 0
	SET @ExcludeMATCH = ISNULL(@ExcludeMATCH,0)

	DECLARE @timestamp1 DATETIME, @timestamp2 DATETIME, @Message VARCHAR(100),@logCount INT = 1
	SET @timestamp1 = GETDATE()
	DECLARE @today DATE = GETDATE() -- IDB-18425

	DECLARE @RowsAffected int,
		@Error int,
		@Msg varchar(500),
		@InvTypeId int,
		@BillCodeCount Int,
		@DbID Int,
		@NextInvNum Int,

		@User varchar(32),
		@ServiceTypeId Int,
		@PeriodId Int,
		@VolUnitMultiplier float,
		@TierRowsAffected int

	DECLARE @AMSWP_ME_Fixed_CommRate float
	
	DECLARE @CurrMaxActiveBillingId BigInt,
		@CurrMaxActiveBillingScheduleId BigInt,
		@CurrMaxActiveBranchId BigInt,
		@CurrMaxInvoiceDealDetailId BigInt,
		@CurrMaxInvoiceFailChargeDetailId BigInt

	DECLARE @Curr_d_Deal_Id varchar(255), 
		@Curr_d_BILLING_CODE varchar(16),
		@Curr_d_ProductGroup varchar(8),
		@Curr_d_TradeDate varchar(8),
		@Curr_d_DEAL_DAYS_TO_MATURITY int,
		@Curr_d_SWSecType varchar(16),
		@Curr_d_Leg varchar(10),
		@Curr_d_SOURCE varchar(8)

	
	/* SHIRISH 12/16/2014 - DECLARING VARIABLES TO BE USED BY CURSORS FOR GetTradeInfo, DealDetails and FailCharges */
	DECLARE @CursorStr VARCHAR(500)
	CREATE TABLE #ProductsToProcess (Product VARCHAR(8), RowNum INT)
	DECLARE @RowCounter INT, @MaxRows INT
	DECLARE @CursorProduct VARCHAR(8)


	/* NILESH 08/01/2013 -- Tiered Billing */
	-- To be enabled when running end of month reports
	-- or Invoice Mode
	SET @UseTieredBilling = ISNULL(@UseTieredBilling, 0)
	IF		(@ReportMode = 0  /*Invoice Mode */
			/* Summary Mode running End of Day */
			OR (@SummaryType IN ('YTD', 'MTD'))
			/* Daily Type Summary with explicit request to user tiered billing */
			OR (@SummaryType IN ('D','PBD') AND @UseTieredBilling = 1)
		)
		SET @UseTieredBilling = 1
	ELSE
		SET @UseTieredBilling = 0

			
	/* Added */
	DECLARE @InvoiceCountry varchar(5)
	IF @Owner = 'US'
		SET @InvoiceCountry = 'USA'
	ELSE
		SET @InvoiceCountry = @Owner
		
	CREATE TABLE #Id
	(
		RowNum Int, 
		Id BigInt
	)

	CREATE TABLE #InvoiceInventory
	(
		InvNum int NOT NULL,
		InvDbId int NOT NULL,
		DetailNum int NULL,
		logon_id varchar(32) NULL,
		ProductGroup varchar(8) NULL,
		Source varchar(8) NULL, 
		Billing_Plan_Id bigint NULL,
		instrument_type varchar(32) NULL, -- IDBBC-120
		ItemAmount float NULL,
		ItemAmountForRebate float NULL,
		InvInvTypeId int NOT NULL,
		who varchar(32) NULL,
		created datetime NULL ,
		periodid int NULL,
		discount_group varchar(8) NULL,
		ChargeId Int NULL
	) 


	CREATE TABLE #ClearingTrades
	(
		InvNum INT NOT NULL,
		InvDbId INT NOT NULL,
		Dealer VARCHAR(8) NULL,
		Trader VARCHAR(128) NULL,
		BILLING_CODE VARCHAR(16) NOT NULL,
		PeriodId INT NOT NULL,
		Trd_Dt DATETIME NULL,
		SettleDate DATETIME NULL,
		Clearing_Destination VARCHAR(16) NULL,
		ProductGroup VARCHAR(8) NULL,
		Quantity FLOAT NULL,
		NetMoney FLOAT NULL,
		Cancelled INT NULL,
		Source VARCHAR(8) NULL,
		DEAL_ID VARCHAR(32) NULL, 
		Side CHAR(2) NULL,
		DecPrice FLOAT NULL,
		ACT_MEMBER CHAR(1),
		IsActiveStream BIT NULL,
		Deal_Trd_ID VARCHAR(255) NULL,
		SECSuffix VARCHAR(5) NULL, -- New columns for DIT-10124
		ClearingID VARCHAR(255) NULL, -- New columns for DIT-10124
		ContraClearingID VARCHAR(255) NULL, -- New columns for DIT-10124
		SwSecType VARCHAR(50) NULL, -- New column for DIT-10123
		DEAL_SECURITY_ID VARCHAR(255),
		Trd_Deal_Id VARCHAR(255) NULL, -- New column IDBBC-41
		ContraDealer VARCHAR(8) NULL, -- IDBBC-108
		InvDetEnrichSource VARCHAR(8) -- IDBBC-216
	)

	-- SHIRISH 05/21/2019 - DIT-11041 This table is used to calculate clearing trades for NAVX commission calculation
	CREATE TABLE #CTForNAVXCommission
	(
		Dealer varchar(8) NULL,
		Trader varchar(128) NULL,
		BILLING_CODE varchar(16) NOT NULL,
		PeriodId int NOT NULL,
		Trd_Dt datetime NULL,
		ProductGroup varchar(8) NULL,
		Quantity float NULL,
		NetMoney float NULL,
		Source varchar(8) NULL,
		DEAL_ID varchar(32) NULL, 
		DecPrice float NULL,
		IsActiveStream bit NULL,
		Deal_Trd_ID varchar(255) NULL,
		ContraDealer VARCHAR(8) NULL,
		ContraUserId VARCHAR(128) NULL
	)
	
	
	CREATE TABLE #Failed_TRSY_CTs
	(
		RowNum bigint IDENTITY(1, 1),
		InvNum int NOT NULL,
		InvDbId int NOT NULL,
		BILLING_CODE varchar(16) NOT NULL,
		Dealer varchar(8) NULL,
		PeriodId int NOT NULL,
		Source varchar(8) NULL, 
		Deal_Negotiation_Id varchar(255) NULL,
		Trader_ID varchar(255) NULL,
		Side_Dealer_Perspective varchar(5) NULL,
		Instrument varchar(64) NULL, 
		Deal_Security_Name varchar(255) NULL,
		Price_MC varchar(16) NULL, 
		DecPrice float NULL,
		Principal varchar(255) NULL,
		AccruedInt varchar(255) NULL,
		Clearing_Destination varchar(16) NULL,
		CTCreator varchar(16) NULL,
		Trd_Dt datetime NULL,
		CTSettled_Date datetime NULL,
		Original_Settle_Date datetime NULL,
		Product varchar(9) NULL,
		ProductGroup varchar(8) NULL,
		Quantity float NULL,
		Deal_Proceeds varchar(255) NULL,
		FailCharge float NULL,
		ChargeId int NULL
	)


	CREATE TABLE #InvoiceInventory_Staging
	(
		InvNum int NOT NULL,
		InvDbId int NOT NULL,
		BILLING_CODE varchar(16) NOT NULL,
		PeriodId int NOT NULL,
		Logon_Id varchar(255) NULL, 
		ProductGroup varchar(8) NULL, 
		Source varchar(8) NULL, 
		BILLING_PLAN_ID bigint NULL,
		INSTRUMENT_TYPE varchar(32) , -- IDBBC-120
		BILLING_TYPE varchar(20) ,
		AggressiveVolume float NULL,
		PassiveVolume float NULL,
		TotalVolume float NULL,
		AggressiveTrades int NULL,
		PassiveTrades int NULL,
		TotalTrades int NULL,
		CHARGE_RATE_AGGRESSIVE float NULL,
		CHARGE_RATE_PASSIVE float NULL,
		CHARGE_FLOOR float NULL,
		CHARGE_CAP float NULL,
		CommissionOwed_PreCap float NULL,
		CommissionOwed_PreCap_Override float NULL,
		CommissionOwed float NULL,
		TIER_DAILY_CHARGE_CAP float NULL,
		TIER_DAILY_CHARGE_FLOOR float NULL,
		TIER_BILLING_PLAN_ID bigint NULL,
		TIER_CHARGE_RATE_AGGRESSIVE float NULL,
		TIER_CHARGE_RATE_PASSIVE float NULL,
		/* NILESH 12/29/2914 - IDB 13217 */
		CommissionCreditAmount float NULL,
		IsActiveStream tinyint NULL,
		Security_Currency VARCHAR(4) NULL,
		RepoTicketFees FLOAT NULL
	)

	CREATE TABLE #CommissionOwed
	(
		InvNum int NOT NULL,
		InvDbId int NOT NULL,
		BILLING_CODE varchar(16) NOT NULL,
		ProductGroup varchar(8) NULL, 
		PeriodId int NOT NULL,
		Source varchar(8) NULL, 
		ChargeId int NULL,
		Volume float NULL,
		CommissionOwed float NULL,
		CommissionOwedForRebate float NULL,
		IsActiveStream tinyint NULL,
		Security_Currency VARCHAR(4) NULL,
		RepoTicketFees FLOAT NULL
	)

	
	/* NILESH -- Tiered Billing */
	-- Used for regenerating the commission summary data
	-- using tiered rates.
	CREATE TABLE #CommissionOwed_Tier
	(
		InvNum int NOT NULL,
		InvDbId int NOT NULL,
		BILLING_CODE varchar(16) NOT NULL,
		ProductGroup varchar(8) NULL, 
		PeriodId int NOT NULL,
		TradeDate DateTime NOT NULL,
		Source varchar(8) NULL, 
		ChargeId int NULL,
		Volume float NULL,
		CommissionOwed float NULL
	)

	/* SHIRISH 03/09/2017 - Create a table to be used for cursors so this can be reused */
	CREATE TABLE #Range_Cursor
	(
		CurrentStartDate datetime NULL,
		CurrentEndDate datetime NULL,
		ProductGroup varchar(10) NULL,
		ETraceBillable char(1) NULL,
		VTraceBillable char(1) NULL,
		TraceEligibleStartDate datetime NULL,
		RowNum int
	)

	CREATE TABLE #TraceDeals
	(
		Billing_Code  varchar(16) NOT NULL,
		PeriodId int NOT NULL,
		TradeDate datetime NOT null,
		Deal_Negotiation_Id varchar(255) null,
		Dealer varchar(8) null,
		Trader_Id varchar(255) null,
		ProductGroup varchar(8) null,
		Side varchar(5) null,
		Trace_Status varchar(255) null,
		TraceSubmissionFees float null,
		TracePassThruFees float null,
		EffectiveStartDate datetime null,
		EffectiveEndDate datetime null,
		TracePassThruFlag char(1) null,
		TraceSource  varchar(8) NULL
	)

	/* NILESH-02/14/2012: New table to implement the changes to allow commission rates override */
	CREATE TABLE #CommissionScheduleOverride
	(
		BillingCode varchar(20), 
		ProductGroup varchar(8), 
		Source char(2) NULL, 
		TradeType varchar(15) NULL, 
		Billing_Type varchar(20) NULL, 
		Charge_Rate float, 
		Range_From float NULL, 
		Range_To float NULL, 
		EffectiveStartDate datetime, 
		EffectiveEndDate datetime
	)

	/* NILESH 12/29/2914 - IDB 13217 */
	/* New table to capture the Commission Credits */
	CREATE TABLE #CommissionCredits
	(
		InvNum int NOT NULL,
		InvDbId int NOT NULL,
		Dealer varchar(8) NULL,
		BILLING_CODE varchar(16) NOT NULL,
		PeriodId int NOT NULL,
		ProductGroup varchar(8) NULL,
		ChargeId int NULL,
		CommissionCredit float NULL,
		Source varchar(8) NULL
	)

	/* IDBBC-310 
	 * Table to store EUREPO ticket fees by billing code/Invoice for invoice generation
	 */
	CREATE TABLE #RepoTicketFeesByInvoice
	(
		InvNum INT,
        InvDbId INT,
		Billing_Code VARCHAR(15),
		PeriodId INT,
		ProductGroup VARCHAR(15),
		Source VARCHAR(5),
		ChargeId INT,
		Volume INT,
		TicketFees FLOAT
	)

	CREATE TABLE #REBATE
	(
		InvNum INT,
		InvDbId INT,
		InvInvTypeId INT,
		Billing_Code VARCHAR(15),
		ProductGroup VARCHAR(10),
		AvailableRebate DECIMAL(16,2),
		Commission DECIMAL(16,2),
		CommissionOwed DECIMAL(16,2),
		CommissionCollected DECIMAL(16,2),
		RemainingRebate DECIMAL(16,2),
		StartBillingPeriod DATETIME, -- IDBBC-75 Currently NAVX rebates are stored for next billing period. For OTR rebate we need to get start of current billing period
		EndBillingPeriod DATETIME,
		InvDate DATETIME,
		Who VARCHAR(30)
	)

	-- IDBBC-7 table is used to store volume based commission/rebate
	CREATE TABLE #VolumeBasedCommission 
	(
		InvNum int,
		InvDbId int,
		BillingCode VARCHAR(16),
		ProductGroup VARCHAR(16),
		PlatformTotal int,
		PlatFormVolumeDescription VARCHAR(10),
		AggrVol int,
		PassVol int,
		PassiveComm DECIMAL(10,2),
		TotalComm DECIMAL(10,2),
		DWASVol int,
		OperatorVol INT,
		BilateralVol INT, -- IDBBC-103
		InsertUpdate BIT
	)


	/* GET THE TIERED BILLING FOR APPLICABLE PRODUCTS */
	CREATE TABLE #TieredSchedule
	(
		BILLING_PLAN_ID bigint NULL,
		BILLING_CODE varchar(16) NOT NULL,
		COMPANY_ACRONYM varchar(8) NULL,
		PRODUCT_GROUP varchar(8) NOT NULL,
		INSTRUMENT_TYPE varchar(32) NULL, -- IDBBC-120
		SOURCE varchar(8) NULL, 
		LEG varchar(10) NULL,
		BILLING_TYPE varchar(20) NULL, 
		CHARGE_RATE_PASSIVE float NULL,
		CHARGE_RATE_AGGRESSIVE float NULL,
		SETTLE_RATE_PASSIVE float NULL,
		SETTLE_RATE_AGGRESSIVE float NULL,
		DAILY_CHARGE_FLOOR float NULL,
		DAILY_CHARGE_CAP float NULL,
		CHARGE_FLOOR float NULL,
		CHARGE_CAP float NULL,
		EFFECTIVE_DATE datetime NULL,
		EXPIRATION_DATE datetime NULL,
		PLAN_FREQ varchar(255) NULL,
		SUB_INSTRUMENT_TYPE VARCHAR(64) NULL	/* DIT-11312 */
	)
			
	CREATE TABLE #TieredBillingSchedule
	(
		BILLING_PLAN_ID bigint NULL,
		BILLING_CODE varchar(16) NOT NULL,
		COMPANY_ACRONYM varchar(8) NULL,
		PRODUCT_GROUP varchar(8) NOT NULL,
		INSTRUMENT_TYPE varchar(32) NULL, --IDBBC-120
		SOURCE varchar(8) NULL, 
		LEG varchar(10) NULL,
		BILLING_TYPE varchar(20) NULL, 
		CHARGE_RATE_PASSIVE float NULL,
		CHARGE_RATE_AGGRESSIVE float NULL,
		SETTLE_RATE_PASSIVE float NULL,
		SETTLE_RATE_AGGRESSIVE float NULL,
		DAILY_CHARGE_FLOOR float NULL,
		DAILY_CHARGE_CAP float NULL,
		CHARGE_FLOOR float NULL,
		CHARGE_CAP float NULL,
		EFFECTIVE_DATE datetime NULL,
		EXPIRATION_DATE datetime NULL,
		PLAN_FREQ varchar(255) NULL,
		SUB_INSTRUMENT_TYPE VARCHAR(64) NULL	/* DIT-11312 */
	)

	DECLARE @CurrentStartDate DateTime,@CurrentEndDate DateTime

	/* NILESH 07/29/2013 : New table to hold the tiered billing information based on total notional value */
	CREATE TABLE #NOTIONAL_TIER_INFO(Dealer varchar(255), Quantity float, ProductGroup varchar(8), InstrumentType varchar(255),PeriodId int NOT NULL, TIER_CHARGE_RATE_PASSIVE DECIMAL(36,18) NULL, 
								TIER_CHARGE_RATE_AGGRESSIVE DECIMAL(36,18) NULL, Tier_EffectiveDate datetime NULL, Tier_ExpirationDate datetime NULL, Tier_Billing_Plan_Id bigint NULL)


	/* NILESH 09/16/2013 : New table to hold the tiered billing information based on weighted notional value as a % of total volume */
	CREATE TABLE #WGT_NOTIONAL_TIER_INFO(Dealer varchar(255), WgtQuantity float, PctTotalVolume float, ProductGroup varchar(8), PeriodId int NOT NULL, TIER_CHARGE_RATE_PASSIVE DECIMAL(36,18) NULL, 
								TIER_CHARGE_RATE_AGGRESSIVE DECIMAL(36,18) NULL, Tier_EffectiveDate datetime NULL, Tier_ExpirationDate datetime NULL, Tier_Billing_Plan_Id bigint NULL)

	--SET INVOICE DATE
	--If invoice date is not passed, default to 1st of the month
	--Invoices generated by the batch will always have date set to 1st of the month in which the invoice is generated
	SET @InvDate = ISNULL(@InvDate, CAST(MONTH(GETDATE()) AS Varchar(2)) + '/01/' + CAST(YEAR(GETDATE()) AS Varchar(4)))


	--SET REPORT MODE IF IT IS NULL
	SELECT @ReportMode = ISNULL(@ReportMode, 0) -- 0 is invoice create mode


	--SET VOLUME UNIT MULTIPLIER 
	/*	Volume is stored (in TW_DEAL table) in units of million. 
		When calculating DRate in BILL, actual value (in millions) of the volume will have to be used
		Set the constant value here so the variable @VolUnitMultiplier can be used when calculating DRate
	*/
	SELECT @VolUnitMultiplier = 1000000.00

	/* NILESH 05/16/2014 SET FIXED AMSWP ME Deals Comm Rate */
	SELECT @AMSWP_ME_Fixed_CommRate = 0.0125
	
	/* SHIRISH 02/09/2016 SET MAX DV01 CREDIT AMOUNT */
	/* SHIRISH 1/11/2017 - Setting MAX DV01 Credit Amount to $5000 
	 *                     Credit amount was 20000 uptil 11/30/2016.  From 12/1/2016 it has been updated to 5000
	 */
	/* SHIRISH 1/13/2017 - No need for this variable as this logic has been moved to GetDealerCommissionCredits proc */
	--DECLARE @MaxDV01CreditAmount FLOAT = 5000

	/* SHIRISH 04/11/2016 - Active Stream Charge Rate  */
	--DECLARE @ActiveStreamAggressiveChargeRate FLOAT = 0.0
	--DECLARE @ActiveStreamPassiveChargeRate FLOAT = 2.0

	--CHECK USERNAME
	IF @UserName IS NOT NULL
	BEGIN
		SELECT @User = RIGHT(@UserName, LEN(@UserName) - CHARINDEX('\', @UserName))
	END
	ELSE
	BEGIN
		SELECT @User = RIGHT(SUSER_SNAME(), LEN(SUSER_SNAME()) - CHARINDEX('\', SUSER_SNAME()))
	END

	--CHECK IF REPORT MODE AND CALLING USER
	--The proc can be called in invoice generate mode only by BlgUser 
	IF @ReportMode = 0 AND @User <> 'BlgUser'
	BEGIN
		SET @Msg = @User + ' does not have permission to create invoices' 
		RAISERROR(@msg,16,-1)
		RETURN @@Error
	END

	--RD: 01/06/2010 - CHECK IF @SummaryType HAS A VALUE WHEN ALGO IS CALLED IN SUMMARIZATION MODE  
	IF @ReportMode = 1 AND @SummaryType IS NULL
	BEGIN
		SET @Msg = 'Summary Type is mandatory when the algo is called in summarization mode' 
		RAISERROR(@msg,16,-1)
		RETURN @@Error
	END

	/* SHIRISH 10/30/2014 -- Generating Process ID
	-- Generating random number so that we can track records in common tables
	-- this ID will be inserted in all common tables
	*/
	DECLARE @ProcessID int	
	SET @ProcessID = ROUND(100000*RAND(), 0)
	
	BEGIN TRY
			
		--SET @date1 and @date2 IF NULL IS PASSED. @date1 and @date2 WILL BE NULL WHEN CALLED FOR COMMISSION SUMMARY AND COULD BE NULL WHEN CALLED BY FRONT END
		IF (@ReportMode > 0)
		BEGIN
			
			SELECT @date1 = CASE @SummaryType 
					--Default to 1st of the year if @SummaryType is YTD and @date1 is NULL
					WHEN 'YTD' THEN ISNULL(@date1, CONVERT(Varchar(10),IDB_CodeBase.dbo.fnYearStartDate(GETDATE()), 112))
					--Default to 1st of the month if @SummaryType is MTD and @date1 is NULL
					WHEN 'MTD' THEN ISNULL(@date1, CONVERT(Varchar(10),IDB_CodeBase.dbo.fnMonthStartDate(GETDATE()), 112))
					--Default to current date if @SummaryType is PBD and @date1 is NULL
					--Although the summary type is PBD(previous business day), set @date1 to current date because 
					--the summarization job runs each night and loads YTD, MTD and current day's summary data
					--The term PBD is really for the commission summary report which runs at 6 AM each day and reports
					--commissions and fees for YTD, MTD and previous business day.
					WHEN 'PBD' THEN ISNULL(@date1, CONVERT(Varchar(8), GETDATE(), 112))
					--Default to current date if @SummaryType is D.
					WHEN 'D' THEN ISNULL(@date1, CONVERT(Varchar(8), GETDATE(), 112))
					--Default to current date if @SummaryType is NULL. It could be null when called from front-end
					ELSE ISNULL(@date1, CONVERT(Varchar(8), GETDATE(), 112))
				END


			--Default to current date if @date2 is NULL
			SELECT @date2 = ISNULL(@date2, CONVERT(VARCHAR(8), GETDATE(), 112))

		END

		--IDBBC-26 setting up period start and end dates for volume based commission calculations
		--IDBBC-106 Moving below date assignments here as we need to make sure Date1 is not null.  When Date1 is passed as NULL above code will assign appropriate value to Date1
		-- If Date1 is null then below dates will also be null and Commission/Rebate won't get calculated correctly
		DECLARE @PeriodStartDate DATE = IDB_Codebase.dbo.fnMonthStartDate(@Date1)
		DECLARE @PeriodEndDate DATE = IDB_Codebase.dbo.fnMonthEndDate(@Date1)
		DECLARE @EndDateForVADV DATE = CASE WHEN @PeriodEndDate < @today THEN @PeriodEndDate ELSE @today END
		DECLARE @NextPeriodStartDate DATE = IDB_Codebase.dbo.fnMonthStartDate(DATEADD(MONTH,1,@date1))
		DECLARE @NextPeriodEndDate DATE = IDB_Codebase.dbo.fnMonthEndDate(DATEADD(MONTH,1,@date1))

		-- SHIRISH 07/22/2019 - IDB-18425 Adding below insert to keep track of Billing runs
		-- if a particular type of billing record (D,MTD,YTD) exists for a particular day then delete old record and keep the latest record 
		DELETE	BS
		FROM	IDB_Billing.dbo.BillingSteps BS (NOLOCK)
		JOIN	IDB_Billing.dbo.BillingRuns BR (NOLOCK) ON BS.RunDate = BR.RunDate
											  AND BS.ProcessID = BR.ProcessID
		WHERE	BR.RunDate = @today
		AND		BR.Date1 = @date1
		AND		BR.Date2 = @date2
		AND		ISNULL(BR.BillingCode,'') = ISNULL(@BillingCode,'')
		AND		BR.ReportMode = @ReportMode
		AND		ISNULL(BR.SummaryType,'') = ISNULL(@SummaryType,'')
		AND		ISNULL(BR.NavxRun,0) = ISNULL(@NavxRun,0)
		AND		ISNULL(BR.Owner,'') = ISNULL(@Owner,'')
		AND		ISNULL(@ProductGroup,'') = ISNULL(BR.ProductGroup,'')
		AND		BR.EODRun = @EODrun

		DELETE	IDB_Billing.dbo.BillingRuns
		WHERE	RunDate = @today
		AND		Date1 = @date1
		AND		Date2 = @date2
		AND		ISNULL(BillingCode,'') = ISNULL(@BillingCode,'')
		AND		ReportMode = @ReportMode
		AND		ISNULL(SummaryType,'') = ISNULL(@SummaryType,'')
		AND		ISNULL(NavxRun,0) = ISNULL(@NavxRun,0) 
		AND		ISNULL([Owner],'') = ISNULL(@Owner,'')
		AND		ISNULL(@ProductGroup,'') = ISNULL(ProductGroup,'')
		AND		EODRun = @EODrun

		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'Initial Declaration Block',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		INSERT INTO IDB_Billing.dbo.BillingRuns
		(
		    RunDate,
		    ProcessID,
		    Date1,
		    Date2,
		    InvoicedDate,
		    BillingCode,
		    ReportMode,
		    SummaryType,
		    NavxRun,
		    InsertedOn,
			Owner,
			ProductGroup,
			EODRun
		)
		VALUES (@today, @ProcessID, @date1, @date2, @InvDate, @BillingCode, @ReportMode, @SummaryType,@NAVXrun, GETDATE(),@Owner, @ProductGroup, @EODRun)

		/* Vipin 04/04/2017 : For extra discount for TRSY */
		CREATE TABLE #IDB_OFTRDiscountRate(Source VARCHAR(5) NOT NULL, Dealer VARCHAR(8) NOT NULL, Billing_Code VARCHAR(16), 
							ProductGroup VARCHAR(8), SWSecType VARCHAR(50),
							Effective_Startdate DATETIME NOT NULL, Effective_Enddate DATETIME NOT NULL,
							MaturityFrom INT, MaturityTo INT, Discount_Amount FLOAT)	

		INSERT INTO #IDB_OFTRDiscountRate (Source, Dealer, Billing_Code, ProductGroup, SWSecType, Effective_Startdate, Effective_Enddate, MaturityFrom, MaturityTo, Discount_Amount)
		SELECT	Source, Dealer, Billing_Code, ProductGroup, SWSecType, Effective_Startdate, Effective_Enddate, MaturityFrom, MaturityTo, Discount_Amount
		FROM	IDB_Billing.dbo.IDB_OFTRDiscountRate
		WHERE	((@ProductGroup IS NULL) OR (@ProductGroup = ProductGroup))
		AND		((@BillingCode IS NULL) OR (@BillingCode = Billing_Code))

		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'IDB_OFTRDiscountRate',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2


		/* SHIRISH 11/4/2014 --  Updating this section to use a permanent table */
		--GET PERIODIDs FOR THE DATES PASSED
		INSERT INTO IDB_Billing.dbo.wPeriodIDs
		(
			PROCESS_ID,
			PeriodId,
			PeriodDate
		)
		SELECT 
			PROCESS_ID,
			PeriodId,
			PeriodDate
		FROM IDB_Reporting.dbo.fnGetPeriodIDs(@ProcessID, @date1, @date2, @ReportMode)

		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'fnGetPeriodIDs',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2


		--GET DbID
		SELECT	@DbID = DbID
		FROM	IDB_Customers.dbo.IdLookUp (NOLOCK)
		WHERE	TableName = 'InvoiceHistory' 

		--GET INVOICE TYPE
		SELECT	@InvTypeId = InvTypeId 
		FROM	IDB_Billing.dbo.InvoiceType (NOLOCK) 
		WHERE	InvoiceType = ISNULL(@InvoiceType,'Dealerweb')

		--GET SERVICE TYPE
		SELECT	@ServiceTypeId = ServiceTypeId 
		FROM	IDB_Billing.dbo.ServiceType (NOLOCK) 
		WHERE	ServiceType = ISNULL(@ServiceType,'Trading')


	--GET CHARGE TYPE
	/* NILESH 12/29/2914 - IDB 13217 */
	/* Added a new charge type : DV01CommissionCredit &  StreamingCommissionCredit */
	SELECT 
		ChargeID,
		ChargeType,
		ChargeDescription = Description,
		ChargeCat
	INTO #ChargeType
	FROM IDB_Billing.dbo.ChargeType (NOLOCK)
	-- 2019/12/16 IDBBC-14 - #ChargeType is always joined using a specific charge Type. So there is no need to use below filter
	--WHERE ChargeType IN ('Commissions','Clearing','Trade Fail','TRACE','DV01CommissionCredit','StreamingCommissionCredit','OTR Rebate','AMSWPFIXBASISCOM')  

		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'Initial Declaration Block',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		--GET SUBMISSION AND NETTING FEES
		SELECT	ClearingSchedule_Id,
			ProductGroup,
			SubmissionFee,
			FixedNettingFee,
			VariableNettingFee,
			EffectiveStartDate = CONVERT(varchar(8),EffectiveStartDate,112),
			EffectiveEndDate = CONVERT(varchar(8),EffectiveEndDate,112),
			TradeType,
			RepoCarryCharge
			
		INTO	#ClearingSchedule

		FROM	IDB_Billing.dbo.ClearingSchedule (NOLOCK)
		WHERE	ClearingScheduleAcronym = (CASE WHEN ProductGroup IN ('EFP','NAVX','BTIC') THEN 'SEC' WHEN ProductGroup = 'EUREPO' THEN 'LCH' ELSE 'GSD' END) --GDB-99 IDBBC-277
			AND ((@ProductGroup IS NULL) OR (ProductGroup = @ProductGroup))

		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After ClearingSchedule',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		-- GET CAT FEE SCHEDULE
		SELECT	ClearingSchedule_Id,
				ClearingScheduleDesc,
				ProductGroup,
				CATFee,
				EffectiveStartDate = CONVERT(varchar(8),EffectiveStartDate,112),
				EffectiveEndDate = CONVERT(varchar(8),EffectiveEndDate,112)
			
		INTO	#CATFeeSchedule

		FROM	IDB_Billing.dbo.ClearingSchedule (NOLOCK)
		WHERE	ClearingScheduleAcronym = 'CAT' 
		AND		((@ProductGroup IS NULL) OR (ProductGroup = @ProductGroup))

		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After CAT Fee Schedule',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		--GET TRACE SUBMISSION FEES /* NILESH 02-23-2010 */
		SELECT	ClearingSchedule_Id,
			ProductGroup,
			SubmissionFee,
			EffectiveStartDate = CONVERT(varchar(8),EffectiveStartDate,112),
			EffectiveEndDate = CONVERT(varchar(8),EffectiveEndDate,112)

		INTO	#TraceSchedule

		FROM	IDB_Billing.dbo.ClearingSchedule (NOLOCK)
		WHERE	ClearingScheduleAcronym = 'TRACE'
		AND	((@ProductGroup IS NULL) OR (ProductGroup = @ProductGroup))

		--GET FAIL CHARGE SCHEDULE
		SELECT
			ProductGroup,
			TMPGReferenceRate,
			FailChargeMinThreshold = MinimumThreshold,
			Effective_Date = CONVERT(varchar(8),Effective_Date,112),
			Expiration_Date = CONVERT(varchar(8),Expiration_Date,112)

		INTO	#FailChargeSchedule

		FROM	IDB_Billing.dbo.FailChargeSchedule (NOLOCK)
		WHERE	ScheduleCode = 'Delivery_Fail_Charge'
			AND ((@ProductGroup IS NULL) OR (ProductGroup = @ProductGroup))

		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After FailChargeSchedule',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		--GET TRACE ELIGIBLE PRODUCTS	/*NILESH 04/27/2011*/
		SELECT	Product,
			TraceEligibleBillCode,
			TraceEligibleStartDate,
			ETraceBillable,
			VTraceBillable
		
		INTO	#TraceEligibleProducts
		
		FROM	IDB_CodeBase.dbo.fnProductType()
		WHERE	TraceEligible = 1
		/* NILESH:
		-- Added following condition to prevent any non billable products being included in this list
		-- e.g. CORP which was recently added for Trace Submission Report.
		-- Currently we have only two AGCY and MBS.
		*/
		AND		(ISNULL(ETraceBillable, 'N') = 'Y' OR ISNULL(VTraceBillable, 'N') = 'Y')
		AND	((@productgroup IS NULL) OR (@productgroup = Product))


		-- Commission Override Schedule 
		INSERT #CommissionScheduleOverride
		(
			BillingCode, 
			ProductGroup, 
			Source, 
			TradeType, 
			Billing_Type, 
			Charge_Rate, 
			Range_From, 
			Range_To, 
			EffectiveStartDate, 
			EffectiveEndDate
		)
		SELECT	BillingCode, 
				ProductGroup, 
				Source, 
				TradeType, 
				Billing_Type, 
				Charge_Rate, 
				Range_From, 
				Range_To, 
				EffectiveStartDate, 
				EffectiveEndDate
		
		FROM	IDB_Billing.dbo.fnGetCommissionOverrideSchedule() fco

		WHERE	((@ProductGroup IS NULL) OR (fco.ProductGroup = @ProductGroup))

		AND		((@BillingCode IS NULL) OR (fco.BillingCode = @BillingCode))
		AND		(
					(@date1 BETWEEN CONVERT(Varchar(8), fco.EffectiveStartDate, 112) AND CONVERT(Varchar(8), fco.EffectiveEndDate, 112))
					OR
					(@date2 BETWEEN CONVERT(Varchar(8), fco.EffectiveStartDate, 112) AND CONVERT(Varchar(8), fco.EffectiveEndDate, 112))
					OR
					(CONVERT(Varchar(8), fco.EffectiveStartDate, 112) >= @date1 AND CONVERT(Varchar(8), fco.EffectiveEndDate, 112) <= IDB_CodeBase.dbo.fnMonthEndDate(@date2))
				)
	
		IF @Debug = 1
			SELECT '#CommissionScheduleOverride----->', * FROM #CommissionScheduleOverride ORDER BY BillingCode,ProductGroup,Source

		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After fnCommissionOverrideSchedule',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/* SHIRISH 10/30/2014 --  Updating this section to use a permanent table */
		--GET ACTIVE BILLING
		INSERT INTO IDB_Billing.dbo.wActiveBilling
		(
			PROCESS_ID, 
			COMPANY_NAME,
			COMPANY_ACRONYM,
			COMPANY_LEGAL_NAME,
			COMPANY_TYPE,
			COMPANY_MBSCCIDS,
			COMPANY_AUTHORIZED,
			BILL_CONTACT_ID,
			BILLING_CODE,
			BILLING_CONTACT_FIRST_NAME,
			BILLING_CONTACT_MIDDLE_INITIAL,
			BILLING_CONTACT_LAST_NAME,
			BILLING_CONTACT_PHONE,
			BILLING_CONTACT_FAX,
			BILLING_CONTACT_EMAIL,
			BILLING_CONTACT_ADDRESS_1, 
			BILLING_CONTACT_ADDRESS_2,
			BILLING_CONTACT_CITY,
			BILLING_CONTACT_STATE,
			BILLING_CONTACT_ZIP,
			BILLING_CONTACT_COUNTRY_CODE,
			BILLING_CONTACT_TIMESTAMP,
			BILLING_CONTACT_WHO,
			BILLING_CONTACT_BILLING_COMPANY,
			BILLING_CONTACT_CURRENCY_CODE,
			BILLING_CONTACT_PURCHASE_ORDER,
			BILLING_CONTACT_OWNER,
			DELIVERY_METHOD_DESCRIPTION,
			DELIVERY_METHOD_ACRONYM,
			INVOICE_CC_EMAIL_1,
			INVOICE_CC_EMAIL_2,
			INVOICE_CC_EMAIL_3,
			INVOICE_CC_EMAIL_4,
			INVOICE_CC_EMAIL_5,
			CURRENCY_CODE,
			SourceDB,
			SECF_BillingCode,
			ProductSpecificInvoice,
			SECSuffix -- New columns for DIT-10124, Indicates data associated with LEK/ABN Billing Codes
		)
		SELECT	ID, 
				COMPANY_NAME,
				COMPANY_ACRONYM,
				COMPANY_LEGAL_NAME,
				COMPANY_TYPE,
				COMPANY_MBSCCIDS,
				COMPANY_AUTHORIZED,
				BILL_CONTACT_ID,
				BILLING_CODE,
				BILLING_CONTACT_FIRST_NAME,
				BILLING_CONTACT_MIDDLE_INITIAL,
				BILLING_CONTACT_LAST_NAME,
				BILLING_CONTACT_PHONE,
				BILLING_CONTACT_FAX,
				BILLING_CONTACT_EMAIL,
				BILLING_CONTACT_ADDRESS_1, 
				BILLING_CONTACT_ADDRESS_2,
				BILLING_CONTACT_CITY,
				BILLING_CONTACT_STATE,
				BILLING_CONTACT_ZIP,
				BILLING_CONTACT_COUNTRY_CODE,
				BILLING_CONTACT_TIMESTAMP,
				BILLING_CONTACT_WHO,
				BILLING_CONTACT_BILLING_COMPANY,
				BILLING_CONTACT_CURRENCY_CODE,
				BILLING_CONTACT_PURCHASE_ORDER,
				BILLING_CONTACT_OWNER,
				DELIVERY_METHOD_DESCRIPTION,
				DELIVERY_METHOD_ACRONYM,
				INVOICE_CC_EMAIL_1,
				INVOICE_CC_EMAIL_2,
				INVOICE_CC_EMAIL_3,
				INVOICE_CC_EMAIL_4,
				INVOICE_CC_EMAIL_5,
				CURRENCY_CODE,
				SourceDB,
				SECF_BillingCode,
				ProductSpecificInvoice,
				SECSuffix -- New columns for DIT-10124, Indicates data associated with LEK/ABN Billing Codes
		FROM	IDB_Reporting.dbo.fnGetActiveBilling(@ProcessID, @date1, @date2, @ReportMode, @BillingCode, @Owner, @NAVXrun)
		/* 
		-- NILESH 08/06/2015 : DWN1 is an internal account and should not be invoiced. All other
		-- billing codes are for the customers who trade with Rafferty but have no contractual 
		-- agreement with Dealerweb so need not generate the invoice.
		*/
		/* SHIRISH 09/08/2017 - Removing ROSE1 from below list as they now have contract with Dealerweb */
		/* SHIRISH 10/05/2017 - Removing CIBC1 from below list as they now have contract with Dealerweb 
								Adding BARCAP23V to below list as it was introduced to provide Barcap with additional trade detail but there should be no additional invoice*/
		/* SHIRISH 09/06/2018 - Removing EDFM1 as they have been enabled of 8/1/2018 */
		/* SHIRISH 07/06/2020 - IDBBC-76 Enabling CF1 invoice generation starting June 2020 billing period (Invoie Date = '2020-07-01) */
		WHERE (		(@ReportMode > 0) OR 
					((@ReportMode = 0 OR @Debug = 1) AND (
															BILLING_CODE NOT IN ('ASG1', 'BREAN1', 'BWT1','CF1','CM1','FTN1','HSBCBK1','HTG1','NBC1','NE1','DWN1','BARCAP23V')
															OR
															(BILLING_CODE = 'CF1' AND @InvDate >= '20200701') -- IDBBC-76
														 )
					)

			  )
		/* SHIRISH 04/02/2019 - DIT-10744 Removing SECF25 from ActiveBilling as we already have SECF59 billing code set for EFP METALS trades */
		AND		BILLING_CODE <> 'SECF25'
		/* IDBBC-292 : Exclude MATCH billing codes if requested */
		AND		((@ExcludeMATCH = 0) OR (@ExcludeMATCH = 1 AND RIGHT(BILLING_CODE,3) <> '_R8'))

		--IF @Debug = 1
		--	SELECT 'After wActiveBilling load', * FROM IDB_Billing.dbo.wActiveBilling ORDER BY BILLING_CODE

		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After fnGetActiveBilling',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

				
		/* SHIRISH 07/14/2016: If Billing_Code is passed then get Dealer.  This will be used to calculating Submission/Netting fees for USREPO */
		DECLARE @Dealer Varchar(15) = NULL
		IF ((@ProductGroup IS NULL OR @ProductGroup IN ('USREPO','EUREPO')) AND @BillingCode IS NOT NULL)
			SELECT @Dealer = Company_Acronym 
			FROM IDB_Billing.dbo.wActiveBilling
			WHERE PROCESS_ID = @ProcessID
		
		
		--GET ACTIVE BRANCHS
		INSERT INTO IDB_Billing.dbo.wActiveBranch
		(
			PROCESS_ID,
			COMPANY_NAME,
			COMPANY_ACRONYM,
			BILL_CONTACT_ID,
			BILLING_CODE,
			BRANCH_TRACE_PASSTHRU,
			BRANCH_ID,
			BRANCH_NAME,
			BRANCH_LEGAL_NAME,
			BRANCH_TYPE,
			BRANCH_ADDRESS,
			BRANCH_CITY,
			BRANCH_STATE,
			BRANCH_ZIP,
			BRANCH_COUNTRY,
			BRANCH_PHONE_NUMBER,
			BRANCH_IPS,
			BRANCH_AUTHORIZED,
			CURRENCY_CODE,
			SourceDB,
			FALCON_BRANCH_ID,
			SECSuffix -- New columns for DIT-10124, Indicates data associated with LEK/ABN Billing Codes
		)
		SELECT 	PROCESS_ID,
				COMPANY_NAME,
				COMPANY_ACRONYM,
				BILL_CONTACT_ID,
				BILLING_CODE,
				BRANCH_TRACE_PASSTHRU,
				BRANCH_ID,
				BRANCH_NAME,
				BRANCH_LEGAL_NAME,
				BRANCH_TYPE,
				BRANCH_ADDRESS,
				BRANCH_CITY,
				BRANCH_STATE,
				BRANCH_ZIP,
				BRANCH_COUNTRY,
				BRANCH_PHONE_NUMBER,
				BRANCH_IPS,
				BRANCH_AUTHORIZED,
				CURRENCY_CODE,
				SourceDB,
				FALCON_BRANCH_ID,
				SECSuffix
		FROM	IDB_Reporting.dbo.fnGetActiveBranch(@ProcessID)
		/* IDBBC-292 */
		WHERE	((@ExcludeMATCH = 0) OR (@ExcludeMATCH = 1 AND RIGHT(BILLING_CODE,3) <> '_R8'))		
		
		/* 
		-- NILESH 03/25/2014
		-- Commented the following code as this would prevent the trades done by a non Invoice Country
		-- branch mapped to a contact with its Owner set to the other country.
		-- e.g. A Billing Contact from US is mapped to a branch in UK. So the idea is any US product trades 
		-- done by UK branch needs to be accounted for. The protection will be the @Owner which is used
		-- to identify the valid contacts.
		*/
		--AND			((@InvoiceCountry IS NULL) OR (B.BRANCH_COUNTRY = @InvoiceCountry))

		/* NILESH 07/12/2012
		-- The invoice need to be addressed to Branch Legal Name instead of Company Name
		-- so at this point we will update the Company_Legal_Name to Branch_Legal_Name 
		-- and later use this legal name instead of company name.
		*/
		/* SHIRISH 09/23/2015
		-- Need to update Company Legal Name for NSI4 and NSI4V to Nomura International PLC.  This can not be done in STRADE tables.
		-- SHIRISH 03/28/2017: As Goldman Sachs pays SEC fees for billing codes SECF10,SECF12,SECF13,SECF14,SECF15,SECF40
		-- we need to update legal name on invoice to "Goldman Sachs & Co"
		*/
		/* SHIRISH 11/20/2019: IDBBC-9
		-- Updating below section to use Legal Name override table instead of using a case statement.
		*/
		IF (@ReportMode = 0 OR @Debug = 1)
		BEGIN
			UPDATE	AB
			SET		AB.COMPANY_LEGAL_NAME = ISNULL(O.LegalName, BR.BRANCH_LEGAL_NAME) 
			FROM	IDB_Billing.dbo.wActiveBilling AB (NOLOCK) -- SHIRISH 11/4/2014 -- updating query to use permanent table
			JOIN	IDB_Billing.dbo.wActiveBranch BR (NOLOCK) ON AB.BILLING_CODE = BR.BILLING_CODE AND AB.PROCESS_ID = BR.PROCESS_ID -- SHIRISH 11/4/2014 -- updating query to use permanent table
			LEFT JOIN IDB_Billing.dbo.Billing_LegalName_Override O (NOLOCK) ON AB.BILLING_CODE = O.BillingCode
			WHERE	AB.PROCESS_ID = @ProcessID
		END

		--IF @Debug = 1
		--	SELECT 'After wActiveBranch load', * from IDB_Billing.dbo.wActiveBranch 

		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After fnActiveBranch',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		-- SHIRISH 01/24/2019: DIT-10124, For Invoice mode we need to update Branch_ID for all LEK/ABN billing codes with branch id for original SEC Billing code as GetTradeInfo uses branchid to match trades with billing codes.
		-- This way correct trades can be matched with correct billing codes
		-- GDB-1236 EFP/NAVX migration from SmartCC to Falcon

		if @ReportMode <> 1
		BEGIN
			UPDATE	ABLib
			SET		ABLib.BRANCH_ID = BCB.BranchID
			FROM	IDB_Billing.dbo.wActiveBranch ABLib (NOLOCK)
			JOIN	Falcon.dbo.BillingContact BC (nolock) on BC.BillingCode = SUBSTRING(ABLib.Billing_Code,1,Len(ABLib.Billing_Code)-4)
			JOIN	Falcon.dbo.BillingContactBranch bcb (nolock) on BC.BillContactId = BCB.BillContactId --AND BCB.DELETED_IND = 0
			WHERE	ABLib.PROCESS_ID = @ProcessID
			AND		ABLib.SECSuffix <> ''

			-- IDBBC-153 
			-- When an SEC fee billing code has more than one branch associated to it, above step will only update one of them for the curresponding LIB/ABN billing code.  
			-- We need to insert remaining branches into wActiveBranch to make sure LIB/ABN billing code will pick up all the trades
			INSERT INTO IDB_Billing.dbo.wActiveBranch
			(
			    PROCESS_ID,
			    COMPANY_NAME,
			    COMPANY_ACRONYM,
			    BILL_CONTACT_ID,
			    BILLING_CODE,
			    BRANCH_TRACE_PASSTHRU,
			    BRANCH_ID,
			    BRANCH_NAME,
			    BRANCH_LEGAL_NAME,
			    BRANCH_TYPE,
			    BRANCH_ADDRESS,
			    BRANCH_CITY,
			    BRANCH_STATE,
			    BRANCH_ZIP,
			    BRANCH_COUNTRY,
			    BRANCH_PHONE_NUMBER,
			    BRANCH_IPS,
			    BRANCH_AUTHORIZED,
			    CURRENCY_CODE,
			    SourceDB,
			    FALCON_BRANCH_ID,
			    SECSuffix
			)
			SELECT	ABLib.PROCESS_ID,
					ABLib.COMPANY_NAME,
					ABLib.COMPANY_ACRONYM,
					ABLib.BILL_CONTACT_ID,
					ABLib.BILLING_CODE,
					ABLib.BRANCH_TRACE_PASSTHRU,
					BCB.BranchID,
					B.Name,
					B.LegalName,
					ABLib.BRANCH_TYPE,
					ABLIb.BRANCH_ADDRESS,
					ABLib.BRANCH_CITY,
					ABLib.BRANCH_STATE,
					ABLib.BRANCH_ZIP,
					ABLib.BRANCH_COUNTRY,
					ABLib.BRANCH_PHONE_NUMBER,
					ABLib.BRANCH_IPS,
					ABLib.BRANCH_AUTHORIZED,
					ABLib.CURRENCY_CODE,
					ABLib.SourceDB,
					B.CompanyID,
					ABLib.SECSuffix
			FROM	IDB_Billing.dbo.wActiveBranch ABLib
			JOIN	Falcon.dbo.BillingContact BC (NOLOCK) ON BC.BillingCode = SUBSTRING(ABLib.Billing_Code,1,LEN(ABLib.Billing_Code)-4)
			JOIN	Falcon.dbo.BillingContactBranch bcb (NOLOCK) ON BC.BillContactId = BCB.BillContactId AND BCB.BranchID <> ABLib.BRANCH_ID
			JOIN	Falcon.dbo.Company B WITH (NOLOCK) ON BCB.BranchID = B.CompanyID AND B.CompanyLevel = 2
			WHERE	ABLib.PROCESS_ID = @ProcessID
			AND		ABLib.SECSuffix <> ''

		END

		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After fnActiveBranch 22222',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/* SHIRISH 11/4/2014 --  Updating this section to use a permanent table */
		--GET BILLING SCHEDULE
		INSERT INTO IDB_Billing.dbo.wBillingSchedule
		(
			PROCESS_ID,
			COMPANY_NAME,
			COMPANY_ACRONYM,
			BILL_CONTACT_ID,
			BILLING_CODE,
			PRODUCT_GROUP,
			INSTRUMENT_TYPE,
			BILLING_PLAN_ID,
			BILLING_TYPE,
			CHARGE_RATE_PASSIVE,
			CHARGE_RATE_AGGRESSIVE,
			CHARGE_FLOOR,
			CHARGE_CAP,
			SETTLE_RATE_PASSIVE,
			SETTLE_RATE_AGGRESSIVE,
			EFFECTIVE_DATE,
			EXPIRATION_DATE,
			OVERWRITE,
			TRD_TYPE,
			SOURCE,
			LEG,
			MTY_START,
			MTY_END,
			TIER_BILLING_PLAN_ID,
			TIER_CHARGE_RATE_AGGRESSIVE,
			TIER_CHARGE_RATE_PASSIVE,
			INSTRUMENT,
			SECSuffix,  -- New columns for DIT-10124, Indicates data associated with LEK/ABN Billing Codes
			SUB_INSTRUMENT_TYPE,	/*DIT-11312*/
			UseGap --IDBBC-132
		)
		SELECT	PROCESS_ID,
				COMPANY_NAME,
				COMPANY_ACRONYM,
				BILL_CONTACT_ID,
				BILLING_CODE,
				PRODUCT_GROUP,
				INSTRUMENT_TYPE,
				BILLING_PLAN_ID,
				BILLING_TYPE,
				CHARGE_RATE_PASSIVE,
				CHARGE_RATE_AGGRESSIVE,
				CHARGE_FLOOR,
				CHARGE_CAP,
				SETTLE_RATE_PASSIVE,
				SETTLE_RATE_AGGRESSIVE,
				EFFECTIVE_DATE,
				EXPIRATION_DATE,
				OVERWRITE,
				TRD_TYPE,
				SOURCE,
				LEG,
				MTY_START,
				MTY_END,
				TIER_BILLING_PLAN_ID,
				TIER_CHARGE_RATE_AGGRESSIVE,
				TIER_CHARGE_RATE_PASSIVE,
				INSTRUMENT,
				SECSuffix,
				SUB_INSTRUMENT_TYPE,	/*DIT-11312*/
				UseGap -- IDBBC-132
		FROM IDB_Reporting.dbo.fnGetBillingSchedule(@ProcessID, @date1, @date2, @ProductGroup)
		/* IDBBC-292 : Exclude MATCH schedules if requested */
		WHERE ((@ExcludeMATCH = 0) OR (@ExcludeMATCH = 1 AND RIGHT(BILLING_CODE,3) <> '_R8'))

		/* NILESH 01/30/2017 : A new billing schedule with source ME_V was created to implement a separate rates for V & manually entered voice trades */
		/* SHIRISH 08/05/2019: GDB-59 Need to translate falcon billing schedule source VME to RC */
		UPDATE IDB_Billing.dbo.wBillingSchedule
		SET SOURCE = 'RC'
		WHERE SOURCE IN ('ME','VME')
		AND PRODUCT_GROUP = 'AMSWP'
		AND PROCESS_ID = @ProcessID

		-- IDBBC-229 For OTR falcon schedule (OTRNOTE/OTRBOND) rates to 0 as we will be using GDB override schedules to calculate commission
		UPDATE	IDB_Billing.dbo.wBillingSchedule
		SET		CHARGE_RATE_PASSIVE = 0.0,
				CHARGE_RATE_AGGRESSIVE = 0.0
		WHERE	PROCESS_ID = @ProcessID
		AND		PRODUCT_GROUP = 'OTR'
		AND		INSTRUMENT_TYPE IN ('OTRNOTE','OTRBOND')

		--IF @debug = 1 
		--	SELECT 'After wBillingSchedule load', * FROM IDB_Billing.dbo.wBillingSchedule ORDER BY PRODUCT_GROUP,BILLING_CODE

		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After fnGetBillingSchedule',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

				
		INSERT INTO #TieredBillingSchedule
		(
			BILLING_PLAN_ID,
			BILLING_CODE,
			COMPANY_ACRONYM,
			PRODUCT_GROUP,
			INSTRUMENT_TYPE, 
			SOURCE, 
			LEG,
			BILLING_TYPE, 
			CHARGE_RATE_PASSIVE,
			CHARGE_RATE_AGGRESSIVE,
			SETTLE_RATE_PASSIVE,
			SETTLE_RATE_AGGRESSIVE,
			DAILY_CHARGE_FLOOR,
			DAILY_CHARGE_CAP,
			CHARGE_FLOOR,
			CHARGE_CAP,
			EFFECTIVE_DATE,
			EXPIRATION_DATE,
			PLAN_FREQ,
			SUB_INSTRUMENT_TYPE	/* DIT-11312 */
		)
		SELECT DISTINCT 
				B.BILLING_PLAN_ID,
				A.BILLING_CODE, 
				A.COMPANY_ACRONYM,
				B.PRODUCT_GROUP,
				B.INSTRUMENT_TYPE,
				B.SOURCE,
				B.LEG,
				B.BILLING_TYPE,
				B.CHARGE_RATE_PASSIVE,
				B.CHARGE_RATE_AGGRESSIVE,
				B.SETTLE_RATE_PASSIVE,
				B.SETTLE_RATE_AGGRESSIVE,
				B.DAILY_CHARGE_FLOOR,
				B.DAILY_CHARGE_CAP,
				B.CHARGE_FLOOR,
				B.CHARGE_CAP,
				B.EFFECTIVE_DATE,
				B.EXPIRATION_DATE,
				PLAN_FREQ = CASE WHEN B.BILLING_CODE IN ('CDX1','CDX2','CDX3','CDX4') THEN 'MONTHLY' ELSE CAST(NULL AS VARCHAR) END,
				SUB_INSTRUMENT_TYPE = A.SUB_INSTRUMENT_TYPE	/* DIT-11312 */

		FROM	IDB_Billing.dbo.wBillingSchedule A (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table
		JOIN		IDB_CodeBase.dbo.fnProductType() fp ON A.PRODUCT_GROUP = fp.Product
		CROSS JOIN	 #TieredSchedule	 B		--#BillingSchedule B

		WHERE	A.BILLING_CODE NOT IN ('CDX1','CDX2','CDX3','CDX4','CDXRC')
		AND		fp.ProductInvoiceUsesTieredBilling = 'Y'
		AND		A.PROCESS_ID = @ProcessID	

		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, '#TieredBillingSchedule',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/* Generate the list of distinct products that are used by the billing schedule for later use */
		DECLARE @BILLINGPRODUCTS AS TABLE(Product_Group  varchar(8))
		
		INSERT INTO @BILLINGPRODUCTS
		SELECT DISTINCT  PRODUCT_GROUP 
		FROM	IDB_Billing.dbo.wBillingSchedule (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table
		WHERE	PROCESS_ID = @ProcessID
		UNION
		SELECT Product
		FROM #TraceEligibleProducts
		/* NILESH - Since the trace eligible products are all US based we want to make sure they do not get included for the UK run */
		WHERE ((@Owner IS NULL) OR (@Owner = 'US'))

		SELECT	Billing_Code = B.Billing_Code,
			ProductGroup = t.Product,
			Branch_Id = B.Branch_Id
		
		INTO	#ActiveBranchTraceInfo
		
		FROM	IDB_Billing.dbo.wActiveBranch B (NOLOCK)
		OUTER APPLY IDB_Codebase.dbo.fnParseString(B.BRANCH_TRACE_PASSTHRU, ',') r
		JOIN	#TraceEligibleProducts t ON r.Result = t.TraceEligibleBillCode	

		WHERE	B.PROCESS_ID = @ProcessID -- condition to track records in a permanent table

		/* 
		-- NILESH 03/04/2013
		-- Currently the Trace Setup is done at the branch level and not at the billing code level.
		-- This creates an issue when the same branch uses multiple billing codes. A new Billing
		-- code ending in number '2' was created for EFP product since a seperate invoice had
		-- to be generated. We want to eliminate these billing codes as EFP is not a TRACE
		-- eligible product.
		*/
		DELETE ABT
		FROM	#ActiveBranchTraceInfo ABT
		WHERE Billing_Code LIKE '%2'
		
		/* 
		-- This table will be used to create a list of all the billing codes which have not been
		-- setup in the table or there is a change in the status of TracePassThru charges for a
		-- product within a branch.
		*/
		CREATE TABLE #TraceUpdateList(Billing_Code varchar(16), ProductGroup varchar(8), Active char(1), TracePassThruFlag char(1), New char(1))
		
		-- Get trace setup records		
		SELECT	DISTINCT
			Billing_Code,
			ProductGroup,
			EffectiveStartDate,
			EffectiveEndDate,
			TracePassThruFlag,
			Active
		
		INTO	#TracePassThruSetup
		FROM	IDB_Billing.dbo.TracePassThruSetup
		JOIN	#TraceEligibleProducts tp ON ProductGroup = tp.Product
		
		WHERE	(
				((EffectiveStartDate BETWEEN @date1 AND @date2) OR (EffectiveStartDate <@date1))
				OR
				((EffectiveEndDate BETWEEN @date1 AND @date2) OR (EffectiveEndDate > @date2))
			)		
		AND	((@BillingCode IS NULL) OR (BILLING_CODE = @BillingCode))
		AND ((@productgroup IS NULL) OR (@productgroup = ProductGroup))
		AND Active = 'Y' 

		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, '#TracePassThruSetup',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/*************************** END BLOCK FOR TRACE SETUP DATA ************************************/
		
		--GET BILLING CODES WITH ACTIVE BRANCH AND ACTIVE BILLING SCHEDULE
		/*THIS INFO WILL BE USED TO GENERATE INVOICE NUMBERS. INVOICES SHOULD BE GENERATED
			ONLY FOR BILLING CODES THAT HAVE ACTIVE BRANCHES AND ACTIVE BILLING SCHEDULE

		*/

		/* SHIRISH 11/4/2014 --  Updating this section to use a permanent table */
		-- GET ACTIVE BILLING CODES
		INSERT INTO IDB_Billing.dbo.wActiveBillingCodes
		(
			PROCESS_ID,
			InvNum,
			InvDbId,
			Billing_Code,
			ProductGroup,
			Start_Billing_Period,
			End_Billing_Period,
			MasterBillingCode,
			SECSuffix,
			InvDetEnrichSource -- IDBBC-165
		)
		SELECT 
			PROCESS_ID,
			InvNum,
			InvDbId,
			Billing_Code,
			ProductGroup,
			Start_Billing_Period,
			End_Billing_Period,
			MasterBillingCode,
			SECSuffix,
			InvDetEnrichSource -- IDBBC-165
		FROM IDB_Reporting.dbo.fnGetActiveBillingCodes(@ProcessID, @DbID, @date1, @date2, @ReportMode)

		
		;WITH CTE_MyTrace
		(
			Billing_Code,
			ProductGroup
		)
		AS
		(
			SELECT	DISTINCT
				Billing_Code,
				ProductGroup
			
			FROM	#TracePassThruSetup
			
			WHERE	TracePassThruFlag = 'Y'
			
			EXCEPT
				
			SELECT	DISTINCT
				Billing_Code,
				ProductGroup
			
			FROM	IDB_Billing.dbo.wActiveBillingCodes (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table
			WHERE	PROCESS_ID = @ProcessID
		)
		INSERT INTO IDB_Billing.dbo.wActiveBillingCodes -- SHIRISH 11/5/2014 -- updating query to use permanent table
		(
			PROCESS_ID,
			InvNum,
			InvDbId,
			Billing_Code,
			ProductGroup,
			Start_Billing_Period,
			End_Billing_Period,
			MasterBillingCode,
			SECSuffix,
			InvDetEnrichSource -- IDBBC-165
		)
		Select	
			PROCESS_ID = @ProcessID,
			InvNum = NULL,
			InvDbId = @DbID,
			Billing_Code,
			ProductGroup,
			Start_Billing_Period = @date1,
			End_Billing_Period = @date2,
			MasterBillingCode = NULL,
			SECSuffix = '',
			InvDetEnrichSource = 'DW' -- IDBBC-165
			
		From	CTE_MyTrace

		/*GET BILLING CODE COUNT. 
			EACH BILLING CODE SHOULD HAVE ONE INVOICE NUMBER. DO A DISTINCT TO ELIMINATE DUPLICATION DUE TO PRODUCT GROUP
		*/
		SELECT	@BillCodeCount = COUNT(DISTINCT ISNULL(MasterBillingCode, Billing_Code))
		FROM	IDB_Billing.dbo.wActiveBillingCodes (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table
		WHERE	PROCESS_ID = @ProcessID

		/*GENERATE IDs*/
		INSERT INTO #Id
		EXEC IDB_CodeBase.dbo.GetNextLookUpId 'InvoiceHistory', @Increment = @BillCodeCount, @DbID = @DbID, @NextID = @NextInvNum OUTPUT, @ReportMode = @ReportMode 


		/*	SHIRISH 10/31/2014 -- updating section to use permanent table
			ASSIGN A NUMBER TO EACH BILLING CODE 
			EACH BILLING CODE SHOULD HAVE ONE INVOICE NUMBER. DO A DENSE_RANK TO ELIMINATE DUPLICATION DUE TO PRODUCT GROUP
			ActiveBillingCodes will have multiple rows (by product) for each billing code. All rows of each billing code
			should have the same RowNum, which in turn will be used to assign InvNum. 
		*/
		UPDATE	ABC
		SET		InvNum = I.Id
		FROM	IDB_Billing.dbo.wActiveBillingCodes ABC (NOLOCK)
		JOIN	(SELECT DISTINCT 
						DENSE_RANK() OVER (ORDER BY ISNULL(MasterBillingCode, Billing_Code)) 'RowNum', 
						Billing_Code = ISNULL(MasterBillingCode, Billing_Code)
				FROM IDB_Billing.dbo.wActiveBillingCodes ABCI
				WHERE PROCESS_ID = @ProcessID) A ON ISNULL(ABC.MasterBillingCode, ABC.Billing_Code) = A.Billing_Code
		JOIN	#Id I ON A.RowNum = I.rowNum
		WHERE	ABC.PROCESS_ID = @ProcessID

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'wActiveBillingCodes',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2
				
		--------------- COMMISSION SUMMARY REPORT SUPPORT BLOCK ----------------------------
		/*
		THE TEMP TABLES IN THIS BLOCK ARE FOR THE COMMISSION SUMMARY REPORT. THEY SHOULD NOT BE USED FOR INVOICES
		*/

		/* THE #Source TABLE IS FOR THE COMMISSION SUMMARY REPORT. IT SHOULD NOT BE USED FOR INVOICES 
			Need to create separate E and V line items in #ActiveBillingCodesWithPeriodIDs, which inturn
			will be used to create E and V line items in the CommissionSummary table

		*/
		CREATE TABLE #Source
		(
			Source varchar(8)
		)

		INSERT INTO #Source
		SELECT Source = 'E'
		UNION
		SELECT Source = 'V'
		UNION
		SELECT Source = 'RC'
		/* IDBBC-302 : New source HFV */
		UNION
		SELECT Source = 'HFV'

		/* RD: 01/06/2010: CREATE TEMP TABLE WITH COMBINATION OF ALL ACTIVE BILLING CODES AND PERIODIDs
		THIS IS FOR THE COMMISSION SUMMARY REPORT. IT SHOULD NOT BE USED FOR INVOICES */

		SELECT	DISTINCT
			InvNum = ABC.InvNum,
			InvDbId = ABC.InvDbId,
			Billing_Code = ABC.Billing_Code,
			ProductGroup = ABC.ProductGroup, 
			Start_Billing_Period = ABC.Start_Billing_Period,
			End_Billing_Period = ABC.End_Billing_Period,
			PeriodId = P.PeriodId,
			Source = S.Source --See the comment on #Source table

		INTO	#ActiveBillingCodesWithPeriodIDs
			
		FROM	IDB_Billing.dbo.wActiveBillingCodes ABC (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table
		JOIN	@BILLINGPRODUCTS BP ON ABC.ProductGroup = BP.Product_Group
		
		--CROSS JOIN #BillableProducts BP
		CROSS JOIN IDB_Billing.dbo.wPeriodIDs P (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table
		CROSS JOIN #Source S

		WHERE	P.PROCESS_ID = @ProcessID
		AND		ABC.PROCESS_ID = @ProcessID
		AND		RIGHT(ABC.Billing_Code,3) <> '_R8'	/* IDBBC-292: Exclude R8FIN records as CROSS JOIN to source is not necessary */

		/* IDBBC-292 : Below for R8FIN */
		UNION

		SELECT	DISTINCT
			InvNum = ABC.InvNum,
			InvDbId = ABC.InvDbId,
			Billing_Code = ABC.Billing_Code,
			ProductGroup = ABC.ProductGroup, 
			Start_Billing_Period = ABC.Start_Billing_Period,
			End_Billing_Period = ABC.End_Billing_Period,
			PeriodId = P.PeriodId,
			Source = 'R8FIN'

		FROM	IDB_Billing.dbo.wActiveBillingCodes ABC (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table
		JOIN	@BILLINGPRODUCTS BP ON ABC.ProductGroup = BP.Product_Group
		
		--CROSS JOIN #BillableProducts BP
		CROSS JOIN IDB_Billing.dbo.wPeriodIDs P (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table

		WHERE	P.PROCESS_ID = @ProcessID
		AND		ABC.PROCESS_ID = @ProcessID
		AND		RIGHT(ABC.Billing_Code,3) = '_R8'
		AND		@ExcludeMATCH = 0

		--IF @Debug = 1
		--	SELECT 'After #ActiveBillingCodesWithPeriodIDs load', * FROM #ActiveBillingCodesWithPeriodIDs

		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, '#ActiveBillingCodesWithPeriodIDs',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/* NILESH -- Tiered Billing */
		-- Table used for re-generating the CommissionSummary data
		-- for MTD summary type and UseTieredBilling = 1
		SELECT		DISTINCT
						InvNum = ABC.InvNum,
						InvDbId = ABC.InvDbId,
						Billing_Code = ABC.Billing_Code,
						ProductGroup = ABC.ProductGroup, 
						Start_Billing_Period = TD.TradeDate,
						End_Billing_Period = TD.TradeDate,
						PeriodId = P.PeriodId,
						Source = S.Source --See the comment on #Source table

		INTO	#ActiveBillingCodesWithPeriodIDs_Tier
			
		FROM	IDB_Billing.dbo.wActiveBillingCodes ABC (NOLOCK)
		JOIN		@BILLINGPRODUCTS BP ON ABC.ProductGroup = BP.Product_Group
		
		CROSS JOIN IDB_Billing.dbo.wPeriodIDs P (NOLOCK)
		CROSS JOIN #Source S
		CROSS JOIN IDB_Reporting.dbo.TradeDate TD
		
		WHERE	TradeDate Between @Date1 and @Date2 
		AND		P.PROCESS_ID = @ProcessID
		AND		ABC.PROCESS_ID = @ProcessID
		AND		ABC.ProductGroup NOT IN ('MATCH')	/* IDBBC-292 : Exclude MATCH as we don't need a SOURCE cross apply. */

		/* IDBBC-292: Block for R8FIN */
		UNION

		SELECT		DISTINCT
						InvNum = ABC.InvNum, InvDbId = ABC.InvDbId, Billing_Code = ABC.Billing_Code, ProductGroup = ABC.ProductGroup, Start_Billing_Period = TD.TradeDate, End_Billing_Period = TD.TradeDate, PeriodId = P.PeriodId, Source = 'R8FIN'

		FROM	IDB_Billing.dbo.wActiveBillingCodes ABC (NOLOCK)
		JOIN		@BILLINGPRODUCTS BP ON ABC.ProductGroup = BP.Product_Group
		CROSS JOIN IDB_Billing.dbo.wPeriodIDs P (NOLOCK)
		CROSS JOIN IDB_Reporting.dbo.TradeDate TD
		
		WHERE	TD.TradeDate BETWEEN (CASE WHEN @date1 < '2024-01-22' AND @date2>= '2024-01-22' THEN '2024-01-22' ELSE @Date1 END) and @Date2 
		AND		P.PROCESS_ID = @ProcessID
		AND		ABC.PROCESS_ID = @ProcessID
		AND		ABC.ProductGroup IN ('MATCH')
		AND		@ExcludeMATCH = 0

		--IF @Debug = 1
		--	SELECT 'After #ActiveBillingCodesWithPeriodIDs_Tier load', * FROM #ActiveBillingCodesWithPeriodIDs_Tier

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, '#ActiveBillingCodesWithPeriodIDs_Tier',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

	-------------------------------------------------------------------------------------------
	IF @Owner = 'US'
	BEGIN
		IF (@productgroup IS NULL OR @productgroup = 'AGCY')
			EXEC IDB_Reporting.dbo.GetTradeInfo_AGCY @ProcessID, @ReportMode, @date1, @date2 -- IDBBC-144

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'GetTradeInfo_AGCY',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2
				
		IF (@productgroup IS NULL OR @productgroup = 'BILL')
			EXEC IDB_Reporting.dbo.GetTradeInfo_BILL @ProcessID, @date1, @date2 -- IDBBC-144

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'GetTradeInfo_BILL',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2
				
		--IF (@productgroup IS NULL OR @productgroup = 'IOS')
		--	EXEC IDB_Reporting.dbo.GetTradeInfo_IOS @ProcessID
			
		IF (@productgroup IS NULL OR @productgroup = 'TRSY')
			EXEC IDB_Reporting.dbo.GetTradeInfo_TRSY @ProcessId, @date1, @date2 -- IDBBC-144

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'GetTradeInfo_TRSY',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		IF (@productgroup IS NULL OR @productgroup = 'TIPS')
			EXEC IDB_Reporting.dbo.GetTradeInfo_TIPS @ProcessID, @date1, @date2 -- IDBBC-144

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'GetTradeInfo_TIPS',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2
					
		IF (@productgroup IS NULL OR @productgroup = 'EFP')
			EXEC IDB_Reporting.dbo.GetTradeInfo_EFP @ProcessID, @date1, @date2 -- IDBBC-144


		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'GetTradeInfo_EFP',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		--IF (@productgroup IS NULL OR @productgroup = 'EQSWP')
		--	EXEC IDB_Reporting.dbo.GetTradeInfo_EQSWP @ProcessID
			
		IF (@productgroup IS NULL OR @productgroup = 'AMSWP')
			EXEC IDB_Reporting.dbo.GetTradeInfo_AMSWP @ProcessID, @date1, @date2 -- IDBBC-144

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'GetTradeInfo_AMSWP',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2
					
		IF (@productgroup IS NULL OR @productgroup = 'CAD')
			EXEC IDB_Reporting.dbo.GetTradeInfo_CAD @ProcessID,@ReportMode, @date1, @date2 -- IDBBC-144

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'GetTradeInfo_CAD',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2
					
		--IF (@productgroup IS NULL OR @productgroup = 'UCDS')
		--	EXEC IDB_Reporting.dbo.GetTradeInfo_UCDS @ProcessID
			
		--IF (@productgroup IS NULL OR @productgroup = 'CDXEM')
		--	EXEC IDB_Reporting.dbo.GetTradeInfo_CDXEM @ProcessID
			
		IF (@productgroup IS NULL OR @productgroup = 'USFRN')
			EXEC IDB_Reporting.dbo.GetTradeInfo_USFRN @ProcessID, @date1, @date2 -- IDBBC-144

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'GetTradeInfo_USFRN',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2
					
		IF (@productgroup IS NULL OR @productgroup = 'NAVX')
			EXEC IDB_Reporting.dbo.GetTradeInfo_NAVX @ProcessID, @date1, @date2 -- IDBBC-144

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'GetTradeInfo_NAVX',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2
					
		IF (@productgroup IS NULL OR @productgroup = 'OTR')
			EXEC IDB_Reporting.dbo.GetTradeInfo_OTR @ProcessID,@ReportMode, @date1, @date2 -- IDBBC-144

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'GetTradeInfo_OTR',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2
        
		/* IDBBC-292: R8FIN Revenue */
		IF (@productgroup IS NULL OR @productgroup = 'MATCH') AND (@ExcludeMATCH = 0)
		BEGIN
			EXEC IDB_Reporting.dbo.GetTradeInfo_R8FIN @ProcessID,@ReportMode, @date1 , @date2

			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'GetTradeInfo_R8FIN',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2
		END

		
		IF (@productgroup IS NULL OR @productgroup = 'USREPO')
			EXEC IDB_Reporting.dbo.GetTradeInfo_USREPO @ProcessID, @date1, @date2 -- IDBBC-144		

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'GetTradeInfo_USREPO',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		-- VK 08/29/2019 GDB-99:  Getting BTIC trades for billing
		IF (@productgroup IS NULL OR @productgroup = 'BTIC')
			EXEC IDB_Reporting.dbo.GetTradeInfo_BTIC @ProcessID, @date1, @date2 -- IDBBC-144

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'GetTradeInfo_BTIC',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		-- SHIRISH 05/02/2023:  Getting BOX trades for billing
		IF (@productgroup IS NULL OR @productgroup = 'BOX')
			EXEC IDB_Reporting.dbo.GetTradeInfo_BOX @ProcessID, @date1, @date2 -- IDBBC-144

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'GetTradeInfo_BOX',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		-- SHIRISH 10/04/2024:  Getting REVCON trades for billing
		IF (@productgroup IS NULL OR @productgroup = 'REVCON')
			EXEC IDB_Reporting.dbo.GetTradeInfo_REVCON @ProcessID, @date1, @date2 -- IDBBC-344

		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'GetTradeInfo_REVCON',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		-- SHIRISH 10/08/2024:  IDBBC-344 Getting COMBO trades for billing
		IF (@productgroup IS NULL OR @productgroup = 'COMBO')
			EXEC IDB_Reporting.dbo.GetTradeInfo_COMBO @ProcessID, @date1, @date2 -- IDBBC-344

		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'GetTradeInfo_COMBO',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

	END


	IF @Owner = 'UK'
	BEGIN

		--IF (@productgroup IS NULL OR @productgroup = 'ECDS')
		--	EXEC IDB_Reporting.dbo.GetTradeInfo_ECDS @ProcessID, @ReportMode
		--  SHIRISH 07/22/2019 DIT-18425
		--SET @timestamp2 = GETDATE()
		--INSERT INTO IDB_Billing.dbo.BillingSteps 
		--VALUES (@today, @ProcessID, 2, 'GetTradeInfo_ECDS',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		--SET @timestamp1 = @timestamp2

		-- SM 03/13/2019 DIT-10488 Getting GILTS trades for billing
		IF (@productgroup IS NULL OR @productgroup = 'GILTS')
			EXEC IDB_Reporting.dbo.GetTradeInfo_GILTS @ProcessID, @ReportMode,@date1, @date2 -- IDBBC-144

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'GetTradeInfo_GILTS',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		-- SHIRISH 05/02/2023:  IDBBC-238 Getting EUREPO trades for billing
		IF (@productgroup IS NULL OR @productgroup = 'EUREPO')
			EXEC IDB_Reporting.dbo.GetTradeInfo_EUREPO @ProcessID, @date1, @date2 -- IDBBC-238

		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'GetTradeInfo_EUREPO',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

	END

		IF ((@ReportMode = 0 OR @Debug = 1) OR @SummaryType in ('MTD','YTD')) 
		BEGIN


			;WITH BillingCode_WithoutDeals_AllProd (InvNum, InvDbId, BILLING_CODE, Branch_Id, ProductGroup, PeriodId) AS
			(
				SELECT	InvNum = ABC.InvNum,
					InvDbId= ABC.InvDbId,
					BILLING_CODE = ABC.BILLING_CODE,
					Branch_Id = B.Branch_Id,
					ProductGroup = ABC.ProductGroup,
					PeriodId = P.PeriodId
					
				FROM	IDB_Billing.dbo.wActiveBranch B (NOLOCK) -- SHIRISH 11/4/2014 -- updating query to use permanent table
				JOIN	IDB_Billing.dbo.wActiveBillingCodes ABC (NOLOCK) ON B.BILLING_CODE = ABC.BILLING_CODE -- SHIRISH 11/4/2014 -- updating query to use permanent table
				JOIN	IDB_Codebase.dbo.fnProductType() PT ON ABC.ProductGroup = PT.Product AND B.SourceDB = PT.SourceDB -- SHIRISH 06/14/2017: join to select data from correct source (FALCON/SMARTCC)
				JOIN	IDB_Billing.dbo.wPeriodIDs P (NOLOCK) ON P.PeriodDate BETWEEN ABC.Start_Billing_Period AND ABC.End_Billing_Period -- SHIRISH 11/4/2014 -- updating query to use permanent table
				
				WHERE	ABC.ProductGroup NOT IN ('AGCY')
				AND		B.PROCESS_ID = @ProcessID
				AND		ABC.PROCESS_ID = @ProcessID
				AND		P.PROCESS_ID = @ProcessID
				
				EXCEPT
				
				SELECT	DISTINCT 
					InvNum,
					InvDbId,
					Billing_Code,
					Branch_Id,
					ProductGroup,
					PeriodId
					
				FROM	IDB_Billing.dbo.wDeals_AllProd (NOLOCK) -- SHIRISH 11/4/2014 -- updating query to use permanent table
				
				WHERE	PROCESS_ID = @ProcessID
				-- SHIRISH 09/07/2016 - Changed below condition to make sure AMSWP E invoices with no trades will have floor charges applied
				--                      Currently as AMSWP E invoices pick up RC/V trades, floor charges do not get applied.
				--                      We also need to make sure this block is not executed for AGCY
				--AND		ProductGroup <> 'AGCY' -- SHIRISH  11/10/2014 -- condition to remove AGCY records as tables merge #Deals and #Deals_AllProd
				AND		CASE ProductGroup WHEN 'AMSWP' THEN Source WHEN 'AGCY' THEN '' ELSE 'E' END = 'E'
			)
			/* SHIRISH 2017/08/17: Removed unused columns */
			INSERT INTO IDB_Billing.dbo.wDeals_AllProd -- SHIRISH 11/4/2014 -- updating query to use permanent table
			(
				PROCESS_ID,
				InvNum,
				InvDbId,
				BILLING_CODE,
				PeriodId,
				Leg,
				Source,
				BRANCH_ID,
				DEAL_PRICE,
				TradeDate,
				SWSecType,
				ProductGroup,
				DEAL_DAYS_TO_MATURITY,
				IsActiveStream,
				DEAL_REPO_START_DATE, -- IDBBC-53 Need this field for USREPO Cap/Floor fees. For USREPO we use Repo Start date instead of trade date
				DEAL_ID -- IDBBC-86
			)
			SELECT	
				@ProcessID,
				InvNum, 
				InvDbId, 
				BILLING_CODE, 
				PeriodId = BWD.PeriodId,
				Leg = 'PRI',
				Source = 'E',
				Branch_Id, 
				-- SHIRISH 02/01/2017: For USREPO setting Deal_Price to 100 so commission is calculated as 0 instead of NULL
				DEAL_PRICE = CASE WHEN ProductGroup IN ('USREPO','EUREPO') THEN 100 ELSE NULL END, --IDBBC-238
				TradeDate = PeriodDate,
				/* 
				-- NILESH 06/06/2013
				-- Following is an additional temporary fix code to allow the
				-- deals to be matched up to one of the billing schedule for 
				-- AMSWP as there are no 'ALL' instrument type record setup
				-- and would require additional testing after creating this 
				-- schedule.
				-- SHIRISH 02/01/2017
				-- Adding INSTRUMENT TYPE USREPOGC for USREPO so a schedule can be matched to this dummy trade record
				*/
				SWSecType = CASE ProductGroup WHEN 'AMSWP' THEN 'AMSWPBSPRD' WHEN 'CAD' THEN 'CADBSPRD' WHEN 'USREPO' THEN 'USREPOGC' WHEN 'EUREPO' THEN 'EUREPOGC' ELSE NULL END, -- IDBBC-238
				ProductGroup,
				-- SHIRISH 02/01/2017: Updating below column for USREPO so it matched a one specific billing schedule
				DEAL_DAYS_TO_MATURITY = 1, -- IDBBC-96 Updating to 1 as NULL gets interpreted as 0 and does not match any schedule as schedules have maturity starts at 1 day
				IsActiveStream = CASE ProductGroup WHEN 'OTR' THEN 0 ELSE NULL END,
				DEAL_REPO_START_DATE = CASE WHEN ProductGroup IN ('USREPO') THEN P.PeriodDate ELSE NULL END, -- IDBBC-53 Need this field for USREPO Cap/Floor fees -- IDBBC-305
				/* IDBBC-86 If Deal_ID is not set and if there are multiple billing codes without trade then all these trades will be split into a single partition and only one 
					of them will get rank 1. In this case we will miss trades for rest of the billing codes and if there are any fixed fees for that billing code we will not calculate 
					those as well. To avoid this scenario we are adding a dummy Deal_ID specific to each invoice so that when there are no trades for multiple billing codes with same 
					instrument type then these trades can be partitioned correctly and assigned appropreate weight. */
				Deal_ID = 'X' + CAST(BWD.InvNum AS VARCHAR) + CAST(BWD.InvDBId AS VARCHAR) + BWD.BILLING_CODE + CONVERT(VARCHAR(8),@InvDate,112)

			FROM	BillingCode_WithoutDeals_AllProd as BWD
			JOIN	IDB_Billing.dbo.wPeriodIDs P (NOLOCK) ON P.PeriodId = BWD.PeriodId AND P.PROCESS_ID = @ProcessID -- SHIRISH 11/5/2014 -- updating query to use permanent table

		END

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'wDeals_AllProd',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/*********** Get Clearing Trades ***********
		This block is moved up to calculate commission for EQSWP.
		This is used to calculate 'FICC GAD Subm8ission and Netting Fees'
		-- IDBBC-144 Remvoing cursor loop as we are not using IsDateRangeinTradeHistory table as all the data is now availble in one table and there is no need to check for historical tables
		
		*/
		;WITH CTE_CT
		AS
		(
			/* NAVX */
			SELECT
				Dealer = ct.Dealer, --cth.Dealer, -- GDB-1236
				Trader = ct.TraderId, --cth.Trader, -- GDB-1236
				Trd_Dt = CASE ct.RPGRP WHEN 'NAVX' THEN ct.Reportdate ELSE ct.TRADEDATE END, -- CASE cth.product WHEN 'NAVX' THEN CONVERT(DATE,ct.reportdt) ELSE cth.Trd_Dt END,
				ct.BranchId,
				SettleDate = NULL, --CONVERT(DATE,trd_msg.STLDT), -- Need to check if used anywhere
				Clearing_Destination = ct.CT_Destination, -- cth.dest, -- GDB-1236
				ProductGroup = ct.RPGRP, --cth.product, -- GDB-1236
				Quantity = ABS(ct.QUANTITY), --ABS(ct.qty), -- GDB-1236
				NetMoney = ISNULL(CAST(ct.DEAL_O_NET AS FLOAT),0), -- ISNULL(CAST(trd_msg.NET as float),0), -- GDB-1236
				Cancelled = CT.Cancelled,
				Source = CASE WHEN ct.BrokerId IS NULL THEN 'E' ELSE 'V' END, --CASE WHEN  cth.Broker IS NULL THEN 'E' ELSE 'V' END,
				CT_Source = ct.Source, -- IDBBC-392: USED FOR OTR FICC pass through charge calculation to match  source DW/DWC with InvDetEnrichSource
				Deal_ID = ct.DEAL_NEGOTIATION_ID, -- cth.Deal_ID, -- GDB-1236
				Side = ct.SIDE, --cth.side, -- GDB-1236
				/* DIT-10159 Modified the code below to use Estimated Price for IWMSQ_NAVX security for the month of December 2018. */
				-- GDb-1236 - After EQUITY migration to Falcon, BMPRC is coming up as 0.0 instead of NULL.  IN either case we need to take TRADE_PRICE
				DecPrice = CASE WHEN ct.SECURITY_GRP = 'NAVXSQ' THEN CASE WHEN CONVERT(DATE,ct.Reportdate) BETWEEN '20181201' AND '20181231' AND ct.DEAL_SECURITY_ID = 'IWMSQ_NAVX' THEN CASE WHEN ct.BMPRC IS NULL OR ct.BMPRC = 0.0 THEN ct.TRADE_PRICE ELSE ct.BMPRC END /*ISNULL(ct.BMPRC,ct.TRADE_PRICE)*/ ELSE NULL END 
																ELSE CASE 
																		-- As per Jason there was an issue on 4/4/2018 with rates and operations had to enter corrected rates
																		-- so we need to pull corrected rate
																		-- Operations was not able to update below trade for price. So hard coading this to 263.8282
																		--WHEN CONVERT(DATE,ct.reportdt) = '20180404' AND cth.Deal_ID = 'D20180404SB00000037' THEN 263.8282
																		-- SHIRISH 05/23/2018: As per Jason estimated rate for SPY on 4/4 should be same (263.8282) for all trades
																		WHEN CONVERT(DATE,ct.Reportdate) = '20180404' AND ct.INSTRUMENT = 'SPY' THEN CAST(263.8282 AS float) -- GDB-301
																		WHEN CONVERT(DATE,ct.Reportdate) = '20180404' AND ct.RPGRP = 'NAVX' THEN ct.TRADE_PRICE
																		WHEN CONVERT(DATE,ct.Reportdate) = '20190813' AND ct.INSTRUMENT = 'SPY' THEN CAST(292.3785 AS float) -- GDB-131 -- GDB-301
																		WHEN CONVERT(Varchar(8), ct.TRADEDATE, 112) = '20190813' AND ct.DEAL_NEGOTIATION_ID = 'D20190813SB00000022' THEN CAST(292.3785 AS float) --GDB-131 -- GDB-301
																		ELSE CASE WHEN ct.BMPRC IS NULL OR ct.BMPRC = 0.0 THEN ct.TRADE_PRICE ELSE ct.BMPRC END -- GDb-1236 ISNULL(ct.BMPRC,ct.TRADE_PRICE)
																		END 
																END,
				--GDB-99
				--GDB-1236
				ACT_MEMBER = CASE WHEN CASE WHEN ct.RPGRP  IN ('EFP','BTIC') THEN CONVERT(varchar(8),ct.TRADEDATE,112) WHEN ct.RPGRP = 'NAVX' THEN  CONVERT(DATE,ct.Reportdate) END >= '20150330' THEN ct.ACT_MEMBER ELSE 'N' END,
				IsActiveStream = CAST(NULL as bit),
				Deal_Trd_ID = ct.Deal_Trd_Did, --trd_did,  --GDB-1236
				ClearingID = ct.CLEARING_ID, -- trd_msg.CLEARINGID, -- GDB-1236
				ContraClearingID = ct.CONTRA_CLEARING_ID, -- trd_msg.CONTRA_CLEARINGID, -- GDB-1236
				SwSecType = ct.TradeType2, --ct.Deal_RSGRP, -- trd_msg.RSGRP, -- GDB-1236 --Need to confirm
				ct.DEAL_SECURITY_ID, -- cth.idbinstr_id, -- GDB-1236
				ct.Trd_Deal_Id, -- = Trd_msg.DID, -- IDBBC-41 -- GDB-1236
				ContraDealer = ct.CONTRA_DLR, -- trd_msg.CONTRA_DLR -- IDBBC-108 --GDB-1236
				InvDetEnrichSource = NULL -- IDBBC-216  only used for OTR

			FROM	IDB_Reporting.dbo.IDB_Falcon_CT ct (NOLOCK)
			WHERE	CT.Reportdate BETWEEN @date1 AND @date2
			AND		ct.RPGRP IN ('NAVX')
			AND		(@ProductGroup IS NULL OR ct.PGRP = @ProductGroup)
			AND		'KILL' <> ISNULL(ct.CT_STATE, '') -- GDB-1236
			--GDB-99 004: Added BTIC
			AND		ct.CT_Destination = 'ACT'

			/* NON-NAVX EQUITY PRODUCTS and AMSWP */
			UNION ALL

			SELECT
				Dealer = ct.Dealer, --cth.Dealer, -- GDB-1236
				Trader = ct.TraderId, --cth.Trader, -- GDB-1236
				Trd_Dt = CASE ct.RPGRP WHEN 'NAVX' THEN ct.Reportdate ELSE ct.TRADEDATE END, -- CASE cth.product WHEN 'NAVX' THEN CONVERT(DATE,ct.reportdt) ELSE cth.Trd_Dt END,
				ct.BranchId,
				SettleDate = NULL, --CONVERT(DATE,trd_msg.STLDT), -- Need to check if used anywhere
				Clearing_Destination = ct.CT_Destination, -- cth.dest, -- GDB-1236
				ProductGroup = ct.RPGRP, --cth.product, -- GDB-1236
				Quantity = ABS(ct.QUANTITY), --ABS(ct.qty), -- GDB-1236
				NetMoney = ISNULL(CAST(ct.DEAL_O_NET AS FLOAT),0), -- ISNULL(CAST(trd_msg.NET as float),0), -- GDB-1236
				Cancelled = CT.Cancelled,
				Source = CASE WHEN ct.BrokerId IS NULL THEN 'E' ELSE 'V' END, --CASE WHEN  cth.Broker IS NULL THEN 'E' ELSE 'V' END,
				CT_Source = ct.Source, -- IDBBC-392: USED FOR OTR FICC pass through charge calculation to match  source DW/DWC with InvDetEnrichSource
				Deal_ID = ct.DEAL_NEGOTIATION_ID, -- cth.Deal_ID, -- GDB-1236
				Side = ct.SIDE, --cth.side, -- GDB-1236
				/* DIT-10159 Modified the code below to use Estimated Price for IWMSQ_NAVX security for the month of December 2018. */
				-- GDb-1236 - After EQUITY migration to Falcon, BMPRC is coming up as 0.0 instead of NULL.  IN either case we need to take TRADE_PRICE
				DecPrice = CASE WHEN ct.SECURITY_GRP = 'NAVXSQ' THEN CASE WHEN CONVERT(DATE,ct.Reportdate) BETWEEN '20181201' AND '20181231' AND ct.DEAL_SECURITY_ID = 'IWMSQ_NAVX' THEN CASE WHEN ct.BMPRC IS NULL OR ct.BMPRC = 0.0 THEN ct.TRADE_PRICE ELSE ct.BMPRC END /*ISNULL(ct.BMPRC,ct.TRADE_PRICE)*/ ELSE NULL END 
																ELSE CASE 
																		-- As per Jason there was an issue on 4/4/2018 with rates and operations had to enter corrected rates
																		-- so we need to pull corrected rate
																		-- Operations was not able to update below trade for price. So hard coading this to 263.8282
																		--WHEN CONVERT(DATE,ct.reportdt) = '20180404' AND cth.Deal_ID = 'D20180404SB00000037' THEN 263.8282
																		-- SHIRISH 05/23/2018: As per Jason estimated rate for SPY on 4/4 should be same (263.8282) for all trades
																		WHEN CONVERT(DATE,ct.Reportdate) = '20180404' AND ct.INSTRUMENT = 'SPY' THEN CAST(263.8282 AS float) -- GDB-301
																		WHEN CONVERT(DATE,ct.Reportdate) = '20180404' AND ct.RPGRP = 'NAVX' THEN ct.TRADE_PRICE
																		WHEN CONVERT(DATE,ct.Reportdate) = '20190813' AND ct.INSTRUMENT = 'SPY' THEN CAST(292.3785 AS float) -- GDB-131 -- GDB-301
																		WHEN CONVERT(Varchar(8), ct.TRADEDATE, 112) = '20190813' AND ct.DEAL_NEGOTIATION_ID = 'D20190813SB00000022' THEN CAST(292.3785 AS float) --GDB-131 -- GDB-301
																		ELSE CASE WHEN ct.BMPRC IS NULL OR ct.BMPRC = 0.0 THEN ct.TRADE_PRICE ELSE ct.BMPRC END -- GDb-1236 ISNULL(ct.BMPRC,ct.TRADE_PRICE)
																		END 
																END,
				--GDB-99
				--GDB-1236
				ACT_MEMBER = CASE WHEN CASE WHEN ct.RPGRP  IN ('EFP','BTIC') THEN CONVERT(varchar(8),ct.TRADEDATE,112) WHEN ct.RPGRP = 'NAVX' THEN  CONVERT(DATE,ct.Reportdate) END >= '20150330' THEN ct.ACT_MEMBER ELSE 'N' END,
				IsActiveStream = CAST(NULL as bit),
				Deal_Trd_ID = ct.Deal_Trd_Did, --trd_did,  --GDB-1236
				ClearingID = ct.CLEARING_ID, -- trd_msg.CLEARINGID, -- GDB-1236
				ContraClearingID = ct.CONTRA_CLEARING_ID, -- trd_msg.CONTRA_CLEARINGID, -- GDB-1236
				SwSecType = ct.TradeType2, --ct.Deal_RSGRP, -- trd_msg.RSGRP, -- GDB-1236 --Need to confirm
				ct.DEAL_SECURITY_ID, -- cth.idbinstr_id, -- GDB-1236
				ct.Trd_Deal_Id, -- = Trd_msg.DID, -- IDBBC-41 -- GDB-1236
				ContraDealer = ct.CONTRA_DLR, -- trd_msg.CONTRA_DLR -- IDBBC-108 --GDB-1236
				InvDetEnrichSource = NULL -- IDBBC-216  only used for OTR

			FROM	IDB_Reporting.dbo.IDB_Falcon_CT ct (NOLOCK)
			WHERE	CT.TRADEDATE BETWEEN @date1 AND @date2 --@CurrentStartDate AND @CurrentEndDate
			AND		ct.PGRP IN ('EFP','BTIC','EQSWP','COMBO','AMSWP','CAD')
			AND		(@ProductGroup IS NULL OR ct.PGRP = @ProductGroup)
			AND		'KILL' <> ISNULL(ct.CT_STATE, '') -- GDB-1236
			--GDB-99 004: Added BTIC
			AND		ct.CT_Destination = (CASE WHEN ct.RPGRP IN ('BTIC') THEN 'CME' WHEN ct.RPGRP IN ('EFP','NAVX') THEN 'ACT' WHEN ct.RPGRP IN ('EQSWP','COMBO') THEN 'GUP' ELSE 'GSD' END) --Submission and Netting Fees are charged on ACT clearing trades only for EFP & NAVX and GSD clearing trades only for other products 
			AND		(CASE ct.PGRP WHEN 'EFP' THEN (CASE WHEN ct.SECURITY_GRP = 'EFPETF' THEN 1 ELSE 0 END) ELSE 1 END) = 1


			UNION ALL
			
			-- SHIRISH 06/22/2017 - Changing below query to use Falcon DB instead of SMARTCC 
			SELECT
				Dealer = ct.Dealer, --cth.Dealer, -- GDB-1236
				Trader = ct.TraderId, --cth.Trader, -- GDB-1236
				Trd_Dt = ct.TRADEDATE, -- CASE cth.product WHEN 'NAVX' THEN CONVERT(DATE,ct.reportdt) ELSE cth.Trd_Dt END,
				ct.BranchId,
				SettleDate = NULL, --CONVERT(DATE,trd_msg.STLDT), -- Need to check if used anywhere
				Clearing_Destination = ct.CT_Destination, -- cth.dest, -- GDB-1236
				ProductGroup = ct.RPGRP, --cth.product, -- GDB-1236
				Quantity = ABS(ct.QUANTITY), --ABS(ct.qty), -- GDB-1236
				NetMoney = ISNULL(CAST(ct.DEAL_O_NET AS FLOAT),0), -- ISNULL(CAST(trd_msg.NET as float),0), -- GDB-1236
				Cancelled = CT.Cancelled,
				Source = CASE WHEN ct.BrokerId IS NULL THEN 'E' ELSE 'V' END, --CASE WHEN  cth.Broker IS NULL THEN 'E' ELSE 'V' END,
				CT_Source = ct.Source, -- IDBBC-392: USED FOR OTR FICC pass through charge calculation to match  source DW/DWC with InvDetEnrichSource
				Deal_ID = ct.DEAL_NEGOTIATION_ID, -- cth.Deal_ID, -- GDB-1236
				Side = ct.SIDE, --cth.side, -- GDB-1236
				/* DIT-10159 Modified the code below to use Estimated Price for IWMSQ_NAVX security for the month of December 2018. */
				-- GDb-1236 - After EQUITY migration to Falcon, BMPRC is coming up as 0.0 instead of NULL.  IN either case we need to take TRADE_PRICE
				DecPrice = CASE WHEN ct.BMPRC IS NULL OR ct.BMPRC = 0.0 THEN ct.TRADE_PRICE ELSE ct.BMPRC END,
				--GDB-99
				--GDB-1236
				ACT_MEMBER = 'N',
				IsActiveStream = CAST(NULL as bit),
				Deal_Trd_ID = ct.Deal_Trd_Did, --trd_did,  --GDB-1236
				ClearingID = ct.CLEARING_ID, -- trd_msg.CLEARINGID, -- GDB-1236
				ContraClearingID = ct.CONTRA_CLEARING_ID, -- trd_msg.CONTRA_CLEARINGID, -- GDB-1236
				SwSecType = ct.TradeType2, --ct.Deal_RSGRP, -- trd_msg.RSGRP, -- GDB-1236 --Need to confirm
				ct.DEAL_SECURITY_ID, -- cth.idbinstr_id, -- GDB-1236
				ct.Trd_Deal_Id, -- = Trd_msg.DID, -- IDBBC-41 -- GDB-1236
				ContraDealer = ct.CONTRA_DLR, -- trd_msg.CONTRA_DLR -- IDBBC-108 --GDB-1236
				InvDetEnrichSource = NULL -- IDBBC-216  only used for OTR

			FROM    IDB_Reporting.dbo.IDB_Falcon_CT ct (NOLOCK)

			WHERE	ct.TradeDate BETWEEN @date1 AND @date2 --@CurrentStartDate AND @CurrentEndDate
			AND		ct.pgrp IN ('TRSY','TIPS','USFRN','OTR','BILL','AGCY')
			AND		(@ProductGroup IS NULL OR ct.PGRP = @ProductGroup)
			AND		'KILL' <> ISNULL(ct.CT_STATE,'')
			AND 	ct.CT_Destination = 'GSD' --Submission and Netting Fees are charged on GSD clearing trades only
			AND		ct.Source <> 'AUC' -- IDBBC-265 Need to eliminate auction trades

		)
		INSERT INTO #ClearingTrades
		(
			InvNum,
			InvDbId,
			Dealer,
			Trader,
			BILLING_CODE,
			PeriodId,
			Trd_Dt,
			SettleDate,
			Clearing_Destination,
			ProductGroup,
			Quantity,
			NetMoney,
			Cancelled,
			Source,
			Deal_ID,
			Side,
			DecPrice,
			ACT_MEMBER,			
			IsActiveStream,
			Deal_Trd_ID,	
			SECSuffix, -- New field DIT-10124
			ClearingID,	-- New field DIT-10124
			ContraClearingID, -- New field DIT-10124
			SwSecType,
			DEAL_SECURITY_ID,
			Trd_Deal_Id, -- New filed IDBBC-41
			ContraDealer, -- IDBBC-108
			InvDetEnrichSource -- IDBBC-216
		)	
			SELECT
				InvNum = ABC.InvNum,
				InvDbId = ABC.InvDbId,
				Dealer = ct.Dealer, --cth.Dealer, -- GDB-1236
				Trader = ct.Trader, --cth.Trader, -- GDB-1236
				Billing_Code = ABC.Billing_Code,
				PeriodId = P.PeriodId,
				Trd_Dt = ct.Trd_Dt,
				SettleDate = NULL, --CONVERT(DATE,trd_msg.STLDT), -- Need to check if used anywhere
				Clearing_Destination = ct.Clearing_Destination,
				ProductGroup = ct.ProductGroup,
				Quantity = ct.Quantity,
				NetMoney = ct.NetMoney,
				Cancelled = CT.Cancelled,
				Source = ct.Source,
				Deal_ID = ct.Deal_ID,
				Side = ct.SIDE,
				DecPrice = ct.DecPrice,
				ACT_MEMBER = ct.ACT_MEMBER,
				IsActiveStream = ct.IsActiveStream,
				Deal_Trd_ID = ct.Deal_Trd_ID, --trd_did,  --GDB-1236
				B.SECSuffix,
				ClearingID = ct.ClearingID,
				ContraClearingID = ct.ContraClearingID,
				SwSecType = ct.SwSecType,
				ct.DEAL_SECURITY_ID,
				ct.Trd_Deal_Id,
				ContraDealer = ct.ContraDealer,
				InvDetEnrichSource = ct.InvDetEnrichSource

			FROM	CTE_CT ct
			JOIN	IDB_Billing.dbo.wActiveBranch B (NOLOCK) ON B.BRANCH_ID = ct.BranchId -- SHIRISH 11/4/2014 -- updating query to use permanent table
			JOIN	IDB_Billing.dbo.wActiveBillingCodes ABC (NOLOCK) ON B.PROCESS_ID = ABC.PROCESS_ID AND B.BILLING_CODE = ABC.BILLING_CODE AND ct.ProductGroup = ABC.ProductGroup -- SHIRISH 11/4/2014 -- updating query to use permanent table
			--Service periods are stored in unit of months
			JOIN	IDB_Billing.dbo.wPeriodIDs P (NOLOCK) ON P.PROCESS_ID = B.PROCESS_ID AND MONTH(ct.Trd_Dt) = MONTH(P.PeriodDate) 
															AND YEAR(ct.Trd_Dt) = YEAR(P.PeriodDate) -- SHIRISH 11/4/2014 -- updating query to use permanent table
			WHERE	ct.ProductGroup IN ('EFP','BTIC','EQSWP','COMBO','AMSWP','CAD','NAVX')
			AND		B.PROCESS_ID = @ProcessID

			UNION ALL
			
			-- SHIRISH 06/22/2017 - Changing below query to use Falcon DB instead of SMARTCC 
			SELECT
				InvNum = ABC.InvNum,
				InvDbId = ABC.InvDbId,
				Dealer = ct.Dealer, --cth.Dealer, --SmartCC to Falcon
				Trader = ct.Trader,
				Billing_Code = ABC.Billing_Code,
				PeriodId = P.PeriodId,
				Trd_Dt = ct.Trd_Dt,
				SettleDate = NULL,
				Clearing_Destination = ct.Clearing_Destination,
				ProductGroup = ct.ProductGroup,
				Quantity = ct.Quantity,
				NetMoney = 0,
				Cancelled = ct.Cancelled,
				Source = ct.Source,
				Deal_ID = ct.Deal_ID,
				Side = ct.Side,
				DecPrice = ct.DecPrice,
				ACT_MEMBER = 'N',
				IsActiveStream = CASE WHEN CT.ProductGroup = 'OTR' THEN CASE WHEN ISNULL(sm.tp1_time,0) > 0 THEN 1 ELSE 0 END ELSE CAST(NULL as bit) END,
				Deal_Trd_Id = NULL,
				B.SECSuffix,
				ClearingID = NULL,
				ContraClearingID = NULL,
				SwSecType = ct.SwSecType,
				ct.DEAL_SECURITY_ID,
				ct.Trd_Deal_Id, -- IDBBC-41
				ContraDealer = NULL, -- IDBBC-108
				InvDetEnrichSource = CASE WHEN ct.ProductGroup = 'OTR' THEN ABC.InvDetEnrichSource ELSE NULL END -- IDBBC-216 only used by OTR

			FROM	CTE_CT ct
			JOIN	Instrument.dbo.Security_Master sm WITH (NOLOCK) ON sm.instrid = CT.DEAL_SECURITY_ID
			JOIN	Instrument.dbo.Security_Type AS ST (NOLOCK) ON ST.sec_type_id = sm.sec_type_id AND ST.product_grp = CT.ProductGroup
			JOIN	IDB_Billing.dbo.wActiveBranch B (NOLOCK) ON ct.BRANCHID = B.BRANCH_ID -- SHIRISH 11/4/2014 -- updating query to use permanent table
			-- SHIRISH 12/20/2022 IDBBC-218 
			-- Adding source to this join to make sure we pick up correct clearing trades for a billing code.  This join is useful for OTR dealers where same branch does 
			-- CLOB and DWAS trades even though billing codes are different.  In this case we want me to make sure CLOB billing code does not pick up DWAS trades and vice versa
			JOIN	IDB_Billing.dbo.wActiveBillingCodes ABC (NOLOCK) ON ABC.PROCESS_ID = B.PROCESS_ID
																	AND B.BILLING_CODE = ABC.BILLING_CODE 
																	AND ct.ProductGroup = ABC.ProductGroup 
																	-- IDBBC-265 Invoice Detail Enrich Source is only matched for OTR for CLOB vs DWAS charges.  This should be ignored for other products
																	AND (CASE WHEN ABC.ProductGroup = 'OTR' THEN ABC.InvDetEnrichSource ELSE '' END) = (CASE WHEN ct.ProductGroup = 'OTR' THEN ct.CT_Source ELSE '' END) -- IDBBC-392
			--Service periods are stored in unit of months
			JOIN	IDB_Billing.dbo.wPeriodIDs P (NOLOCK) ON B.PROCESS_ID = P.PROCESS_ID AND MONTH(ct.Trd_Dt) = MONTH(P.PeriodDate) AND YEAR(ct.Trd_Dt) = YEAR(P.PeriodDate) -- SHIRISH 11/4/2014 -- updating query to use permanent table

			WHERE	ct.ProductGroup IN ('TRSY','TIPS','USFRN','OTR','BILL','AGCY')
			AND		B.PROCESS_ID = @ProcessID
			AND		P.PeriodId IS NOT NULL	
			-- IDBBC-233 need to filter out connectivity and market data billing codes so SEC fees are not picked up as these use same branches as transactional billing codes
			AND		RIGHT(ABC.Billing_Code, 2) NOT IN ('_C','_M') 

			--  SHIRISH 07/22/2019 DIT-18425
			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'After Main ClearingTrades block ',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2


			IF (@ProductGroup IS NULL OR @ProductGroup = 'NAVX')
			BEGIN
				INSERT INTO #ClearingTrades
				(
					InvNum,
					InvDbId,
					Dealer,
					Trader,
					BILLING_CODE,
					PeriodId,
					Trd_Dt,
					SettleDate,
					Clearing_Destination,
					ProductGroup,
					Quantity,
					NetMoney,
					Cancelled,
					Source,
					Deal_ID,
					Side,
					DecPrice,
					ACT_MEMBER,				
					IsActiveStream,
					Deal_Trd_ID,	
					SECSuffix, -- New field DIT-10124
					ClearingID,	-- New field DIT-10124
					ContraClearingID, -- New field DIT-10124
					SwSecType
				)	
				SELECT
					InvNum = ABC.InvNum,
					InvDbId = ABC.InvDbId,
					Dealer = ct.Dealer, --cth.Dealer,
					Trader = ct.TraderId, --cth.Trader,
					Billing_Code = ABC.Billing_Code,
					PeriodId = P.PeriodId,
					Trd_Dt = MCT.TradeDate,
					SettleDate = NULL, --CONVERT(DATE,trd_msg.STLDT),
					Clearing_Destination = ct.CT_Destination, -- cth.dest,
					ProductGroup = ct.PGRP, --cth.product,
					Quantity = SUM(ABS(ct.QUANTITY)), --SUM(ABS(ct.qty)),
					NetMoney = SUM(CAST(ct.DEAL_O_NET AS FLOAT)), --SUM(ISNULL(Cast(trd_msg.NET as float),0)),
					Cancelled = 0,
					Source = CASE WHEN ct.BrokerId IS NULL THEN 'E' ELSE 'V' END, --CASE WHEN cth.Broker IS NULL THEN 'E' ELSE 'V' END,
					Deal_ID = ct.DEAL_NEGOTIATION_ID, --cth.Deal_ID,
					Side = ct.SIDE, --cth.side,
					-- Operations was not able to update below trade for price. So hard coading this to 263.8282
					-- GDB-1236 After Equity migration to falcon BMPRC is coming up as 0.0 instead of NULL.  In that case we need to take TRADE_PRICE
					DecPrice = CASE WHEN ct.DEAL_NEGOTIATION_ID = 'D20180404SB00000037' THEN 263.8282 
									WHEN MCT.TradeDate = '20190813' AND ct.INSTRUMENT = 'SPY' THEN 292.3785
									ELSE CASE WHEN ct.BMPRC IS NULL OR ct.BMPRC = 0.0 THEN ct.TRADE_PRICE ELSE ct.BMPRC END -- GDB-1236 ISNULL(ct.BMPRC,ct.TRADE_PRICE) 
							   END,
					ACT_MEMBER = CASE WHEN CONVERT(DATE,ct.Reportdate) >= '20150330' AND ct.ACT_MEMBER = 'true' THEN 'Y' ELSE 'N' END,
					--ACT_MEMBER = CASE WHEN CASE cth.product WHEN 'EFP' THEN CONVERT(varchar(8),cth.trd_dt,112) WHEN 'NAVX' THEN  CONVERT(DATE,ct.reportdt) END >= '20150330' AND trd_msg.ACT_MEMBER = 'true' THEN 'Y' ELSE 'N' END,
					IsActiveStream = CAST(NULL as bit),
					Deal_Trd_ID = ct.Deal_trd_did, --trd_did, -- GDB-1236
					B.SECSuffix,
					ClearingID = ct.CLEARING_ID, --trd_msg.CLEARINGID,
					ContraClearingID = ct.CONTRA_CLEARING_ID, -- trd_msg.CONTRA_CLEARINGID,
					SwSecType = ct.TradeType2 -- ct.Deal_RSGRP -- trd_msg.RSGRP -- GDB-1236 Need to confirm

				FROM	IDB_Reporting.dbo.IDB_Falcon_CT ct (NOLOCK)
				/* SHIRISH 02/04/2019: Adding below join to manually cleared trades where reporting date is null or clearing trade record is in cancelled state. */
				JOIN IDB_Billing.dbo.ManualClearedTrades MCT WITH (NOLOCK) ON ct.TRADEDATE = MCT.TradeDate --cth.Trd_Dt = MCT.TradeDate
																			AND ct.RPGRP = MCT.ProductGroup --cth.product = MCT.ProductGroup
																			AND ct.DEAL_NEGOTIATION_ID = MCT.Deal_ID --cth.Deal_ID = MCT.Deal_ID
																			AND ct.Dealer = MCT.Dealer  --cth.Dealer = MCT.Dealer
																			AND ct.TraderId = MCT.Trader  --cth.Trader = MCT.Trader
																			AND ct.cancelled = MCT.Cancelled
				--JOIN	STRADE.dbo.tw_user AS u (nolock) ON cth.trader = u.usr_id
				--JOIN	IDB_Codebase.dbo.fnProductType() PT ON PT.Product = ct.RPGRP -- SHRISh 06/19/2017 - Added below join to make sure we pick up data from correct source (Falcon/SmartCC)
				JOIN	IDB_Billing.dbo.wActiveBranch B (NOLOCK) ON B.BRANCH_ID = ct.BranchId -- SHIRISH 11/4/2014 -- updating query to use permanent table
																--AND B.SourceDB = PT.SourceDB -- SHIRISH 06/19/2017 - Making sure correct source is picked (Falcon/SmartCC)
				JOIN	IDB_Billing.dbo.wActiveBillingCodes ABC (NOLOCK) ON B.PROCESS_ID = ABC.PROCESS_ID AND B.BILLING_CODE = ABC.BILLING_CODE AND ct.RPGRP = ABC.ProductGroup -- SHIRISH 11/4/2014 -- updating query to use permanent table
				--Service periods are stored in unit of months
				JOIN	IDB_Billing.dbo.wPeriodIDs P (NOLOCK) ON B.PROCESS_ID = P.PROCESS_ID 
																AND MONTH(MCT.TradeDate) = MONTH(P.PeriodDate) 
																AND YEAR(MCT.TradeDate) = YEAR(P.PeriodDate) -- SHIRISH 11/4/2014 -- updating query to use permanent table
		
				WHERE	ct.TRADEDATE BETWEEN @date1 AND @date2
				AND ct.PGRP = 'NAVX'
				AND		'KILL' <> ISNULL(ct.CT_STATE,'') --ISNULL(cts.ecv_status, '')
				AND		ct.CT_Destination = 'ACT' --Submission and Netting Fees are charged on ACT clearing trades only for EFP & NAVX and GSD clearing trades only for other products 
				AND		P.PeriodId IS NOT NULL	
				AND		B.PROCESS_ID = @ProcessID

				 /* SHIRISH 12/11/2015
				  * Adding group by for Quantity and NetMoney to eliminate clearing trades returning multiple records for same deal_nego_id, dealer, source.  
				  * Multiple records cause commission to be double counted.
				  */
				 GROUP BY 
					ABC.InvNum,
					ABC.InvDbId,
					ct.Dealer, --cth.Dealer,
					ct.TraderId, --cth.Trader,
					ABC.Billing_Code,
					P.PeriodId,
					MCT.TradeDate,
					--trd_msg.STLDT,
					ct.CT_Destination, --cth.dest,
					ct.PGRP, --cth.product,
					CT.Cancelled,
					ct.BrokerId, --cth.Broker,
					ct.DEAL_NEGOTIATION_ID, --cth.Deal_ID,
					ct.SIDE, --cth.side,
					CASE WHEN ct.BMPRC IS NULL OR ct.BMPRC = 0.0 THEN ct.TRADE_PRICE ELSE ct.BMPRC END, -- GDB-1236 ISNULL(ct.BMPRC,ct.TRADE_PRICE), --ISNULL(trd_msg.BMPRC,trd_msg.PRICE),
					CASE WHEN CONVERT(DATE,ct.Reportdate) >= '20150330' AND ct.ACT_MEMBER = 'true' THEN 'Y' ELSE 'N' END,
					--CASE WHEN CASE cth.product WHEN 'EFP' THEN CONVERT(varchar(8),cth.trd_dt,112) WHEN 'NAVX' THEN CONVERT(DATE,ct.reportdt) END >= '20150330' AND trd_msg.ACT_MEMBER = 'true' THEN 'Y' ELSE 'N' END,
					ct.Deal_trd_did, --trd_did,
					B.SECSuffix,
					ct.CLEARING_ID, --trd_msg.CLEARINGID,
					ct.CONTRA_CLEARING_ID, --trd_msg.CONTRA_CLEARINGID,
					ct.TradeType2, --ct.Deal_RSGRP, -- trd_msg.RSGRP, --GDB-1236 Need to confirm
					ct.INSTRUMENT -- instr_id

			END
				
		--	SET @RowCounter = @RowCounter + 1    		
		--END -- End cursor loop
		
		--TRUNCATE TABLE #Range_Cursor -- removing records from this table so that it can be re-used for another loop


		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'NAVX Clearing Trades',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2


		/* SHIRISH 05/20/2019: DIT-11041
		 * Jason has confirmed that when calculating commission we need to use trade date.  As we need clearing quantity to calculate commission, 
		 * we also need to use trade date to get ClearingTrades for NAVX.  As original clearing trades use report date and are used to calculate 
		 * submission fees, creating a new temp table to get clearing trades using trade date for just commission calulation
		 */									

		IF (@ProductGroup IS NULL OR @ProductGroup = 'NAVX')
		BEGIN

			INSERT INTO	#CTForNAVXCommission
			SELECT
					Dealer = ct.dealer, --cth.Dealer,
					Trader = ct.TraderId, --cth.Trader,
					Billing_Code = ABC.Billing_Code,
					PeriodId = P.PeriodId,
					Trd_Dt = ct.TRADEDATE, --cth.Trd_Dt,
					ProductGroup = ct.RPGRP, --cth.product,
					Quantity = ABS(ct.Quantity), --ABS(ct.qty),
					NetMoney = ISNULL(Cast(ct.DEAL_O_NET as float),0), --ISNULL(Cast(trd_msg.NET as float),0),
					Source = CASE WHEN ct.BrokerId IS NULL THEN 'E' ELSE 'V' END, --CASE WHEN cth.Broker IS NULL THEN 'E' ELSE 'V' END,
					Deal_ID = ct.DEAL_NEGOTIATION_ID, --cth.Deal_ID,
					/* DIT-10159 Modified the code below to use Estimated Price for IWMSQ_NAVX security for the month of December 2018. */
					-- IDBBC-108 Updating else condition for NAVXSQ to use BMPRC as we need to save this estimated rate on NAVXCOmmissionAdjustment table so it can be displayed on invoice trade details file
					-- To make sure we are not calculating commission adjustment for NAVXSQ clearing quantity will be set to 0 instead of setting  estimated and actual rates to NULL
					-- GDB-1236 After Equity migration to falcon BMPRC is coming up as 0.0 instead of NULL.  In this case we need to use TRADE_PRICE
					DecPrice = CASE WHEN ct.SECURITY_GRP = 'NAVXSQ' 
									THEN CASE WHEN CONVERT(DATE,ct.Reportdate) BETWEEN '20181201' AND '20181231' AND ct.DEAL_SECURITY_ID = 'IWMSQ_NAVX' 
											  THEN CASE WHEN ct.BMPRC IS NULL OR ct.BMPRC = 0.0 
														THEN ct.TRADE_PRICE 
														ELSE ct.BMPRC - ISNULL(ct.TRADE_PRICE,0.0) 
													END /*ISNULL(ct.BMPRC,ct.TRADE_PRICE)*/
											   -- IDBBC-147 After migration to falcon BMPRC includes level and we need to remove level from estimated price when calculating NAVX adjustment
											   ELSE CASE WHEN ct.Tradedate > '20210709' 
														 THEN ct.BMPRC - ISNULL(ct.TRADE_PRICE,0.0) 
														 ELSE ct.BMPRC
													END
										 END 
									ELSE CASE 
											-- As per Jason there was an issue on 4/4/2018 with rates and operations had to enter corrected rates
											-- so we need to pull corrected rate
											-- Operations was not able to update below trade for price. So hard coading this to 263.8282
											--WHEN CONVERT(DATE,ct.reportdt) = '20180404' AND cth.Deal_ID = 'D20180404SB00000037' THEN 263.8282
											-- SHIRISH 05/23/2018: As per Jason estimated rate for SPY on 4/4 should be same (263.8282) for all trades
											WHEN CONVERT(DATE,ct.Reportdate) = '20180404' AND ct.INSTRUMENT = 'SPY' THEN 263.8282
											WHEN CONVERT(DATE,ct.Reportdate) = '20180404' AND ct.RPGRP = 'NAVX' THEN ct.TRADE_PRICE
											WHEN CONVERT(DATE,ct.Reportdate) = '20190813' AND ct.INSTRUMENT = 'SPY' THEN 292.3785 --GDB-131
											WHEN CONVERT(Varchar(8), ct.TRADEDATE, 112) = '20190813' AND ct.DEAL_NEGOTIATION_ID = 'D20190813SB00000022' THEN 292.3785 --GDB-131
											WHEN ct.TRADEDATE = '20230823' AND ct.INSTRUMENT = 'XLP' THEN 72.5684 -- IDBBC-262
											WHEN ct.TRADEDATE = '20230823' AND ct.INSTRUMENT = 'SHE' THEN 87.8868 -- IDBBC-262
											ELSE CASE WHEN ct.BMPRC IS NULL OR ct.BMPRC = 0.0 
													  THEN ct.TRADE_PRICE 
													  -- IDBBC-147 After migration to falcon BMPRC includes level and we need to remove level from estimated price when calculating NAVX adjustment
													  ELSE CASE WHEN ct.Tradedate > '20210709' 
																THEN ct.BMPRC - ISNULL(ct.TRADE_PRICE,0.0) 
																ELSE ct.BMPRC
														   END
												 END -- GDB-1236 ISNULL(ct.BMPRC,ct.TRADE_PRICE) 
											END 
							  END,
					IsActiveStream = CAST(NULL as bit),
					Deal_Trd_Id = ct.Deal_Trd_did, --trd_did, -- GDB-1236
					ct.CONTRA_DLR, --trd_msg.CONTRA_DLR, -- IDBBC-108
					ct.CONTRA_USER_ID --trd_msg.CONTRA_ID -- IDBBC-108
			
			FROM	IDB_Reporting.dbo.IDB_Falcon_CT ct (NOLOCK)
			JOIN	IDB_Billing.dbo.wActiveBranch B (NOLOCK) ON B.BRANCH_ID = ct.BranchId -- SHIRISH 11/4/2014 -- updating query to use permanent table
															--AND B.SourceDB = PT.SourceDB -- SHIRISH 06/19/2017 - Making sure correct source is picked (Falcon/SmartCC)
			JOIN	IDB_Billing.dbo.wActiveBillingCodes ABC (NOLOCK) ON ABC.PROCESS_ID = B.PROCESS_ID 
																		AND B.BILLING_CODE = ABC.BILLING_CODE 
																		AND ct.RPGRP = ABC.ProductGroup -- SHIRISH 11/4/2014 -- updating query to use permanent table
			--Service periods are stored in unit of months
			JOIN	IDB_Billing.dbo.wPeriodIDs P (NOLOCK) ON P.PROCESS_ID = B.PROCESS_ID
															AND MONTH(ct.TRADEDATE) = MONTH(P.PeriodDate) 
															AND YEAR(ct.TRADEDATE) = YEAR(P.PeriodDate) -- SHIRISH 11/4/2014 -- updating query to use permanent table
			LEFT JOIN IDB_Billing.dbo.ManualClearedTrades MCT (NOLOCK) ON MCT.Tradedate = ct.TRADEDATE -- cth.trd_dt --GDB-286
																		AND MCT.Productgroup = ct.RPGRP --cth.product 
																		AND MCT.Deal_Id = ct.DEAL_NEGOTIATION_ID --cth.Deal_ID 
																		AND MCT.Dealer = ct.Dealer --cth.dealer 
																		AND MCT.Trader = ct.TraderId --cth.Trader 
																		AND MCT.Cancelled = ct.cancelled 
			
			WHERE	ct.TRADEDATE BETWEEN @date1 AND @date2  --@CurrentStartDate AND @CurrentEndDate
			AND		ct.PGRP = 'NAVX' --cth.product = 'NAVX' -- SHIRISH 05/28/2019: DIT-11041 no need to use variable as this is only used for NAVX
			AND		'KILL' <> ISNULL(ct.CT_STATE,'') --ISNULL(cts.ecv_status, '') 
			AND		ct.CT_Destination = 'ACT' --cth.dest = 'ACT' --Submission and Netting Fees are charged on ACT clearing trades only for EFP & NAVX and GSD clearing trades only for other products 
			AND		P.PeriodId IS NOT NULL
			AND		B.PROCESS_ID = @ProcessID
			AND		(CASE  --GDB-286
						 WHEN MCT.Deal_Id IS NOT NULL THEN 1 
						 WHEN ISNULL(CT.Cancelled,0) = 0 THEN 1  -- GDB-1236 Adding ISNUL check
						 ELSE 0
					 END
					)  	= 1
		END


		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'Clearing Trades For NAVX',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		 /* BTIC GDB-99 */	
		IF (@ProductGroup IS NULL OR @ProductGroup = 'BTIC')
		BEGIN

			/* BTIC GDB-99 */
			INSERT INTO	#CTForNAVXCommission
			SELECT
					Dealer = ct.Dealer, -- cth.Dealer,
					Trader = ct.TraderId, -- cth.Trader,
					Billing_Code = ABC.Billing_Code,
					PeriodId = P.PeriodId,
					Trd_Dt = ct.TRADEDATE, -- cth.Trd_Dt,
					ProductGroup = ct.RPGRP, --cth.product,
					Quantity = ABS(ct.QUANTITY), --ABS(ct.qty),
					NetMoney = ISNULL(CAST(ct.DEAL_O_NET AS FLOAT),0), --ISNULL(Cast(trd_msg.NET as float),0),
					Source = CASE WHEN ct.BrokerId IS NULL THEN 'E' ELSE 'V' END, --CASE WHEN cth.Broker IS NULL THEN 'E' ELSE 'V' END,
					Deal_ID = ct.DEAL_NEGOTIATION_ID, --cth.Deal_ID,
					/* DIT-10159 Modified the code below to use Estimated Price for IWMSQ_NAVX security for the month of December 2018. */
					DecPrice = ISNULL(ct.BMPRC,ct.TRADE_PRICE),
					IsActiveStream = CAST(NULL as bit),
					Deal_Trd_Id = ct.Deal_trd_did, --trd_did, -- GDB-1236
					ct.CONTRA_DLR,-- trd_msg.CONTRA_DLR, -- IDBBC-108
					ct.CONTRA_USER_ID --trd_msg.CONTRA_ID -- IDBBC-108
			
			FROM	IDB_Reporting.dbo.IDB_Falcon_CT ct (NOLOCK)
			JOIN	IDB_Billing.dbo.wActiveBranch B (NOLOCK) ON B.BRANCH_ID = ct.BranchId -- SHIRISH 11/4/2014 -- updating query to use permanent table
															--AND B.SourceDB = PT.SourceDB -- SHIRISH 06/19/2017 - Making sure correct source is picked (Falcon/SmartCC)
			JOIN	IDB_Billing.dbo.wActiveBillingCodes ABC (NOLOCK) ON ABC.PROCESS_ID = B.PROCESS_ID
																		AND ABC.BILLING_CODE = B.BILLING_CODE 
																		AND ABC.ProductGroup = ct.RPGRP -- SHIRISH 11/4/2014 -- updating query to use permanent table
			--Service periods are stored in unit of months
			JOIN	IDB_Billing.dbo.wPeriodIDs P (NOLOCK) ON P.PROCESS_ID = B.PROCESS_ID
															AND MONTH(P.PeriodDate) = MONTH(ct.TRADEDATE)
															AND YEAR(P.PeriodDate) = YEAR(ct.TRADEDATE) -- SHIRISH 11/4/2014 -- updating query to use permanent table
			WHERE	ct.TRADEDATE BETWEEN @date1 AND @date2 --@CurrentStartDate AND @CurrentEndDate
			AND		ct.PGRP = 'BTIC' -- SHIRISH 05/28/2019: DIT-11041 no need to use variable as this is only used for NAVX
			AND		'KILL' <> ISNULL(ct.CT_STATE,'') --ISNULL(cts.ecv_status, '') 
			AND		ct.CT_Destination = 'CME' --Submission and Netting Fees are charged on ACT clearing trades only for EFP & NAVX and GSD clearing trades only for other products 
			AND		B.PROCESS_ID = @ProcessID
			AND		P.PeriodId IS NOT NULL
			AND		ISNULL(CT.Cancelled,0) = 0  -- GDB-1236 Adding ISNULL check

		END

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'Clearing Trades For BTIC',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/************* END Get Clearing Trades block *************/

		/* SHIRISH DIT-10124
		 * Need to be able to tell deals that are clearing at LEK.  This information is only available in clearing trades (ClearingID = LSCI).
		 * so updating deals from clearing trades.  We only need to do this in invoice mode
		 */

		 IF ((@ReportMode = 0 OR @Debug = 1))
		 BEGIN
			UPDATE	D
			SET		D.ClearingID = CT.ClearingID,
					D.ContraClearingID = CT.ContraClearingID
			FROM	IDB_Billing.dbo.wDeals_AllProd D (NOLOCK)
			JOIN	#ClearingTrades CT ON D.DEAL_NEGOTIATION_ID = CT.DEAL_ID
									   AND (D.Deal_Id = CT.Deal_Trd_ID OR D.DEAL_ORIG_DID = CT.Deal_Trd_ID)
									   AND D.ProductGroup = CT.ProductGroup
									   AND D.Dealer = CT.Dealer
			WHERE	ISNULL(CT.ClearingID,'') <> ''
		 END

		/*
		-- Following code is to identify the applicable tiered billing
		-- plan. This will be applicable only in certain conditions
		-- and will be determined by a flag.
		*/
		--IF (@ReportMode = 2 OR @ReportMode = 0)
		IF (@UseTieredBilling = 1)
		BEGIN

			/* ########### BLOCK TO DETERMINE THE TIERED RATE BASED ON NOTIONAL ############# */
			
			-- The following code is used for tiered billing.
			/* TIERED ON NOTIONAL VALUE */
			INSERT INTO #NOTIONAL_TIER_INFO
			(Dealer, Quantity, ProductGroup, InstrumentType, PeriodId)
			SELECT		D.Dealer,
							SUM(DEAL_QUANTITY) * 1000000.00,
							D.ProductGroup,
							D.SWSecType,
							P.PeriodId
							
			/* Changed view to Table */						
			FROM	IDB_Reporting.dbo.IDB_Deal (nolock) D
			JOIN	IDB_CodeBase.dbo.fnProductType() fpt ON D.ProductGroup = fpt.Product
			--Service periods are stored in unit of months
			JOIN	IDB_Billing.dbo.wPeriodIDs P (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table
					ON MONTH(d.TradeDate) = MONTH(P.PeriodDate) AND YEAR(d.TradeDate) = YEAR(P.PeriodDate) 
			
			WHERE	D.ProductGroup = fpt.Product
			/* 
			-- When using the tiered billing for the Summary Type 'D' and 'PBD'
			-- it is required to determine the notional amount from the beginning of
			-- the month. This is done only once at the end of the day. During the intra-day
			-- run we just let it run using the default rates.
			*/
			AND		D.TradeDate BETWEEN 
										CASE WHEN (@SummaryType IN ('D', 'PBD')) THEN IDB_CodeBase.dbo.fnMonthStartDate(@date1) ELSE @date1 END
										AND @date2
			AND		fpt.ProductInvoiceUsesTieredBilling = 'Y'
			AND		fpt.TieredBillingType = 'NOTIONAL'
			AND		P.PROCESS_ID = @ProcessID

			GROUP BY D.Dealer, D.ProductGroup, D.SWSecType, P.PeriodId
			
			ORDER BY P.PeriodId, D.Dealer, D.ProductGroup, D.SWSecType

			/* Determine the appropriate tier range based on the total notional value */
			UPDATE TI
			SET			TI.Tier_Billing_Plan_Id = TBS.BILLING_PLAN_ID,
							TI.TIER_CHARGE_RATE_PASSIVE = TBS.CHARGE_RATE_PASSIVE,
							TI.TIER_CHARGE_RATE_AGGRESSIVE = TBS.CHARGE_RATE_AGGRESSIVE,
							TI.Tier_EffectiveDate = TBS.Effective_Date,
							TI.Tier_ExpirationDate = TBS.Expiration_Date
							
			FROM		#NOTIONAL_TIER_INFO TI
			JOIN			#TieredBillingSchedule TBS 
																		ON	TI.Dealer = TBS.COMPANY_ACRONYM
																		AND	TI.ProductGroup = TBS.PRODUCT_GROUP
																		AND	TI.InstrumentType = TBS.INSTRUMENT_TYPE
			WHERE	TI.Quantity >= TBS.CHARGE_FLOOR 
			AND			(CASE	WHEN (ISNULL(TBS.CHARGE_CAP,0) = 0 OR TBS.CHARGE_CAP < 0) THEN 1
										WHEN TI.Quantity < ISNULL(TBS.CHARGE_CAP,0) THEN 1 
										ELSE 0
							 END
							) = 1

			/* Update the deals with the appropriate rates */
			UPDATE D
			SET		D.TIER_BILLING_PLAN_ID = TI.TIER_BILLING_PLAN_ID,
					D.TIER_CHARGE_RATE_PASSIVE = TI.TIER_CHARGE_RATE_PASSIVE,
					D.TIER_CHARGE_RATE_AGGRESSIVE = TI.TIER_CHARGE_RATE_AGGRESSIVE
			FROM	IDB_Billing.dbo.wDeals_AllProd D (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table
			JOIN	#NOTIONAL_TIER_INFO TI 
							ON	D.Dealer = TI.Dealer
							AND	D.ProductGroup = TI.ProductGroup
							AND D.SWSecType = TI.InstrumentType
							AND D.PeriodId = TI.PeriodId
																	
			WHERE	D.TradeDate BETWEEN TI.Tier_EffectiveDate AND TI.Tier_ExpirationDate	
			AND		D.PROCESS_ID = @ProcessID
			AND		D.ProductGroup <> 'AGCY' -- SHIRISH  11/10/2014 -- condition added as wDeals_AllProd now holds AGCY records
			
		END

		/* ########### BLOCK TO DETERMINE THE TIERED RATE BASED ON WEIGHTED - NOTIONAL ############# */

		/* TIERED ON Weighted NOTIONAL VALUE */
		INSERT INTO #WGT_NOTIONAL_TIER_INFO
		(Dealer, WgtQuantity, ProductGroup, PeriodId)
		SELECT	D.Dealer,
				SUM(CASE WHEN Deal_Is_Agressive = 0 THEN Deal_Quantity * 1.25 ELSE Deal_Quantity END) * 1000000.00,
				D.ProductGroup,
				P.PeriodId

		/* Changed view to Table */					
		FROM	IDB_Reporting.dbo.IDB_Deal (nolock) D
		JOIN	IDB_CodeBase.dbo.fnProductType() fpt ON D.ProductGroup = fpt.Product
		--Service periods are stored in unit of months
		JOIN	IDB_Billing.dbo.wPeriodIDs P (NOLOCK) ON MONTH(d.TradeDate) = MONTH(P.PeriodDate) AND YEAR(d.TradeDate) = YEAR(P.PeriodDate)
		
		WHERE	D.TradeDate BETWEEN 
									CASE WHEN (@SummaryType IN ('D', 'PBD')) THEN IDB_CodeBase.dbo.fnMonthStartDate(@date1) ELSE @date1 END
									AND @date2
		/* 
		-- When using the tiered billing for the Summary Type 'D' and 'PBD'
		-- it is required to determine the notional amount from the beginning of
		-- the month. This is done only once at the end of the day. During the intra-day
		-- run we just let it run using the default rates.
		*/
		AND		D.ProductGroup = fpt.Product
		AND		fpt.ProductInvoiceUsesTieredBilling = 'Y'
		AND		fpt.TieredBillingType = 'WGT-NOTIONAL'
		AND		P.PROCESS_ID = @ProcessID

		GROUP BY D.Dealer, D.ProductGroup, P.PeriodId
		
		ORDER BY P.PeriodId, D.Dealer, D.ProductGroup

		-- Determine the total Platform volume (single sided) 
		SELECT	PeriodId = P.PeriodId,
				ProductGroup = D.ProductGroup,
				TotalVolume = SUM(DEAL_QUANTITY / 2) * 1000000.00
		
		/* Changed view to Table */
		INTO	#TotalPlatformNotional
		FROM	IDB_Reporting.dbo.IDB_Deal (nolock) D 
		JOIN	IDB_CodeBase.dbo.fnProductType() fpt ON D.ProductGroup = fpt.Product
		--Service periods are stored in unit of months
		JOIN	IDB_Billing.dbo.wPeriodIDs P (NOLOCK) ON MONTH(d.TradeDate) = MONTH(P.PeriodDate) AND YEAR(d.TradeDate) = YEAR(P.PeriodDate) -- SHIRISH 11/5/2014 -- updating query to use permanent table
		
		WHERE	D.ProductGroup = fpt.Product
		/* 
		-- When using the tiered billing for the Summary Type 'D' and 'PBD'
		-- it is required to determine the notional amount from the beginning of
		-- the month. This is done only once at the end of the day. During the intra-day
		-- run we just let it run using the default rates.
		*/
		AND		D.TradeDate BETWEEN 
									CASE WHEN (@SummaryType IN ('D', 'PBD')) THEN IDB_CodeBase.dbo.fnMonthStartDate(@date1) ELSE @date1 END
									AND @date2
		AND		fpt.ProductInvoiceUsesTieredBilling = 'Y'
		AND		fpt.TieredBillingType = 'WGT-NOTIONAL'
		AND		P.PROCESS_ID = @ProcessID
		
		GROUP BY P.PeriodId, D.ProductGroup
		
		-- Calculate the %Total Volume
		UPDATE	WT
		SET			WT.PctTotalVolume = ROUND((CASE WHEN TotalVolume > 0 THEN WgtQuantity / TotalVolume ELSE 0 END)	 * 100,1)
		FROM		#WGT_NOTIONAL_TIER_INFO WT
		JOIN			#TotalPlatformNotional T ON WT.PeriodId = T.PeriodId
																AND WT.ProductGroup = T.ProductGroup

		-- Determine the appropriate tier range based on the total notional value
		UPDATE TI
		SET			TI.Tier_Billing_Plan_Id = TBS.BILLING_PLAN_ID,
						TI.TIER_CHARGE_RATE_PASSIVE = TBS.CHARGE_RATE_PASSIVE,
						TI.TIER_CHARGE_RATE_AGGRESSIVE = TBS.CHARGE_RATE_AGGRESSIVE,
						TI.Tier_EffectiveDate = TBS.Effective_Date,
						TI.Tier_ExpirationDate = TBS.Expiration_Date
						
		FROM		#WGT_NOTIONAL_TIER_INFO TI
		JOIN			#TieredBillingSchedule TBS 
																	ON	TI.Dealer = TBS.COMPANY_ACRONYM
																	AND	TI.ProductGroup = TBS.PRODUCT_GROUP
		WHERE	TI.PctTotalVolume >= TBS.CHARGE_FLOOR 
		AND			(CASE	WHEN (ISNULL(TBS.CHARGE_CAP,0) = 0 OR TBS.CHARGE_CAP < 0) THEN 1
									WHEN TI.PctTotalVolume < ISNULL(TBS.CHARGE_CAP,0) THEN 1 
									ELSE 0
						 END
						) = 1

		UPDATE	D
		SET		D.TIER_BILLING_PLAN_ID = TI.TIER_BILLING_PLAN_ID,
				D.TIER_CHARGE_RATE_PASSIVE = TI.TIER_CHARGE_RATE_PASSIVE,
				D.TIER_CHARGE_RATE_AGGRESSIVE = TI.TIER_CHARGE_RATE_AGGRESSIVE
		FROM	IDB_Billing.dbo.wDeals_AllProd D (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table
		JOIN	#WGT_NOTIONAL_TIER_INFO TI 
						ON	D.Dealer = TI.Dealer
						AND	D.ProductGroup = TI.ProductGroup
						AND D.PeriodId = TI.PeriodId
																
		WHERE	D.TradeDate BETWEEN TI.Tier_EffectiveDate AND TI.Tier_ExpirationDate	
		AND		D.PROCESS_ID = @ProcessID	
		and		D.ProductGroup <> 'AGCY' -- SHIRISH  11/10/2014 -- condition added as #Deals_AllProd now holds AGCY records	
		
		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'Tierd rate based on notional',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		CREATE TABLE #OverrideSchedules
		(
			ScheduleId INT NULL,
			Billing_Code VARCHAR(15) NULL,
			Dealer VARCHAR(15) NULL,
			Product VARCHAR(10) NULL,
			[Source] VARCHAR(5) NULL,
			ScheduleType CHAR(1) NULL,
			IsActiveStream BIT NULL,
			Operator BIT NULL,
			AnonymousLevel VARCHAR(15) NULL,
			BilateralStream BIT NULL,
			IsStdTickInc BIT NULL,
			OFTR BIT NULL,
			ApplyToMonth CHAR(1) NULL,
			AggrChargeRate DECIMAL(12,6) NULL,
			PassChargeRate DECIMAL(12,6) NULL,
			[Floor] DECIMAL(10,2) NULL,
			Cap DECIMAL(10,2) NULL,
			EffectiveStartDate DATE NULL,
			EffectiveEndDate DATE NULL,
			TierId INT NULL,
			--IncrementalTiers BIT NULL,
			TierFunction VARCHAR(50) NULL,
			-- For Time tiers AggrPass flag on OverrideTier is used to update Aggr/Pass rate.  Here we need to take AggrPass flag as NULL
			AggrPass BIT NULL,
			TierType VARCHAR(10) NULL,
			ResultType VARCHAR(10) NULL,

			-- Threshold information
			UsesThreshld INT NULL,
			ThresholdMet INT NULL,
			ThresholdProducts VARCHAR(100) NULL,
			TH_IsActiveStream BIT NULL,
			TH_Operator BIT NULL,
			TH_AggrPass BIT NULL,
			ThresholdMin DECIMAL(10,2) NULL,
			ThresholdMax DECIMAL(10,2) NULL,
			TH_VolumeType VARCHAR(10) NULL,
			TH_EffectiveStartDate DATE NULL,
			TH_EffectiveEndDate DATE NULL,
			
			-- Pool Takers
			PoolTakers VARCHAR(512) NULL,
			PT_EffectiveStartDate DATE NULL,
			PT_EffectiveEndDate DATE NULL,

			-- Pool Participants
			PP_GroupName VARCHAR(20) NULL,
			PoolParticipants VARCHAR(512) NULL,
			IncludePP BIT NULL,
			PP_EffectiveStartDate DATE NULL,
			PP_EffectiveEndDate DATE NULL,

			-- Get comma separated list of all child dealers
			Child_Dealers VARCHAR(512) NULL,
			ParentScheduleId INT NULL,
			TenorStart INT NULL,
			TenorEnd INT NULL,
			TN_EffectiveStartDate DATE NULL,
			TN_EffectiveEndDate DATE NULL,
			Security_Currency VARCHAR(4) NULL,
			ProcessId INT NOT NULL
		)

		/* IDBBC-310 create table to store EUREPO per trade commission calculated using override schedules */
		CREATE TABLE #EUREPOOverrideCommission
		(
			Deal_Trade_Date		DATE
			,Deal_Id			VARCHAR(255)
			,AggrRate			DECIMAL(36,18)
			,PassRate			DECIMAL(36,18)
			,OverrideCommission	FLOAT			
		)

		INSERT INTO #OverrideSchedules
		(
			ScheduleId,
			Billing_Code,
			Dealer,
			Product,
			Source,
			ScheduleType,
			IsActiveStream,
			Operator,
			AnonymousLevel,
			BilateralStream,
			IsStdTickInc,
			OFTR,
			ApplyToMonth,
			AggrChargeRate,
			PassChargeRate,
			Floor,
			Cap,
			EffectiveStartDate,
			EffectiveEndDate,
			TierId,
			--IncrementalTiers,
			TierFunction, -- GDB-294
			AggrPass,
			TierType,
			ResultType,
			UsesThreshld,
			ThresholdMet,
			ThresholdProducts,
			TH_IsActiveStream,
			TH_Operator,
			TH_AggrPass,
			ThresholdMin,
			ThresholdMax,
			TH_VolumeType,
			TH_EffectiveStartDate,
			TH_EffectiveEndDate,
			PoolTakers,
			PT_EffectiveStartDate,
			PT_EffectiveEndDate,
			PP_GroupName,
			PoolParticipants,
			IncludePP,
			PP_EffectiveStartDate,
			PP_EffectiveEndDate,
			Child_Dealers,
			ParentScheduleId,
			TenorStart,
			TenorEnd,
			TN_EffectiveStartDate,
			TN_EffectiveEndDate,
			Security_Currency,
			ProcessId
		)
		SELECT		S.ScheduleId,
					S.Billing_Code,
					S.Dealer,
					S.Product,
					S.Source,
					S.ScheduleType,
					S.IsActiveStream,
					S.Operator,
					S.AnonymousLevel,
					S.BilateralStream,
					S.IsStdTickInc,
					S.OFTR,
					S.ApplyToMonth,
					-- For Time based tiers when AggrPass flag is NULL or 1 then take AggrChargeRate
					--AggrChargeRate = CASE WHEN ST.TierType = 'T' AND (ST.AggrPass IS NULL OR ST.AggrPass = 1) THEN T.Rate ELSE S.AggrChargeRate END,
					--PassChargeRate = CASE WHEN ST.TierType = 'T' AND (ST.AggrPass IS NULL OR ST.AggrPass = 0) THEN T.Rate ELSE S.PassChargeRate END,
					S.AggrChargeRate,
					S.PassChargeRate,
					S.Floor,
					S.Cap,
					S.EffectiveStartDate,
					S.EffectiveEndDate,
					
					-- Tier Information
					--TierId = CASE WHEN ST.TierType = 'T' THEN NULL ELSE ST.TierId END,
					ST.TierId,
					--ST.IncrementalTiers,
					TierFunction = CASE ST.TierFunction WHEN 'R' THEN 'Retroactive'
														WHEN 'I' THEN 'Incremental'
														WHEN 'IR' THEN 'IncrementalRunningTotal'
														ELSE NULL
								   END,
					-- For Time tiers AggrPass flag on OverrideTier is used to update Aggr/Pass rate.  Here we need to take AggrPass flag as NULL
					--AggrPass = CASE WHEN ST.TierType = 'T' THEN NULL ELSE ST.AggrPass END, 
					ST.AggrPass,
					ST.TierType,
					ST.ResultType,

					---- Time based tiers
					--TimeStart = CAST(CAST( CAST((T.TierStart) AS int) / 60 AS varchar) + ':'  + right('0' + CAST(CAST((T.TierStart) AS int) % 60 AS varchar(2)),2) AS TIME),
					--TimeEnd = CAST(CAST( CAST((T.TierEnd) AS int) / 60 AS varchar) + ':'  + right('0' + CAST(CAST((T.TierEnd) AS int) % 60 AS varchar(2)),2) AS TIME),

					-- Threshold information
					UsesThreshld = CASE WHEN TH.ScheduleId IS NOT NULL THEN 1 ELSE 0 END,
					ThresholdMet = CASE WHEN TH.ScheduleId IS NULL THEN 1 ELSE 0 END, -- Set to 1 for schedules which do not use threshold
					
					TH.ThresholdProducts,
					TH_IsActiveStream = TH.IsActiveStream,
					TH_Operator = TH.Operator,
					TH_AggrPass = TH.AggrPass,
					TH.ThresholdMin,
					TH.ThresholdMax,
					TH_VolumeType = TH.VolumeType,
					TH_EffectiveStartDate = TH.EffectiveStartDate,
					TH_EffectiveEndDate = TH.EffectiveEndDate,
					
					-- Pool Takers
					PT.PoolTakers,
					PT_EffectiveStartDate = PT.EffectiveStartDate,
					PT_EffectiveEndDate = PT.EffectiveEndDate,

					-- Pool Participants
					PP_GroupName = PP.GroupName,
					PP.PoolParticipants,
					IncludePP = PP.Include,
					PP_EffectiveStartDate = PP.EffectiveStartDate,
					PP_EffectiveEndDate = PP.EffectiveEndDate,

					-- Get comma separated list of all child dealers
					Child_Dealers = STUFF(
											(SELECT ',' + Dealer
												FROM	IDB_Billing.dbo.OverrideChildBillingCodes
												WHERE	ScheduleId = S.ScheduleId
												AND	(
														@PeriodStartDate BETWEEN EffectiveStartDate AND EffectiveEndDate
														OR 
														@PeriodEndDate BETWEEN EffectiveStartDate AND EffectiveEndDate
													)
											FOR XML PATH ('')),1,1,''
											),
					ParentScheduleId = CBC.ScheduleId,
					TN.TenorStart,
					TN.TenorEnd,
					TN_EffectiveStartDate = TN.EffectiveStartDate,
					TN_EffectiveEndDate = TN.EffectiveEndDate,
					S.Security_Currency,
					@ProcessID

		FROM		IDB_Billing.dbo.OverrideSchedules S (NOLOCK)
		LEFT JOIN	IDB_Billing.dbo.OverrideScheduleTier ST (NOLOCK) ON ST.ScheduleId = S.ScheduleId
																	AND (	
																			@PeriodStartDate BETWEEN ST.EffectiveStartDate AND ST.EffectiveEndDate
																			OR
																			@PeriodEndDate BETWEEN ST.EffectiveStartDate AND ST.EffectiveEndDate
																		)
		LEFT JOIN	IDB_Billing.dbo.OverrideScheduleThreshold TH (NOLOCK) ON TH.ScheduleId = S.ScheduleId
																			AND (
																					@PeriodStartDate BETWEEN TH.EffectiveStartDate AND TH.EffectiveEndDate
																					OR 
																					@PeriodEndDate BETWEEN TH.EffectiveStartDate AND TH.EffectiveEndDate
																				)
		LEFT JOIN	IDB_Billing.dbo.OverridePoolTakers PT (NOLOCK) ON PT.ScheduleId = S.ScheduleId
																	AND (
																			@PeriodStartDate BETWEEN PT.EffectiveStartDate AND PT.EffectiveEndDate
																			OR 
																			@PeriodEndDate BETWEEN PT.EffectiveStartDate AND PT.EffectiveEndDate
																		)
		LEFT JOIN IDB_Billing.dbo.OverridePoolParticipantsGroup PP (NOLOCK) ON PP.ScheduleId = S.ScheduleId
																			AND (
																					@PeriodStartDate BETWEEN PP.EffectiveStartDate AND PP.EffectiveEndDate
																					OR 
																					@PeriodEndDate BETWEEN PP.EffectiveStartDate AND PP.EffectiveEndDate
																				)
		LEFT JOIN IDB_Billing.dbo.OverrideChildBillingCodes CBC (NOLOCK) ON CBC.ChildScheduleID = S.ScheduleId
																		AND (
																				@PeriodStartDate BETWEEN CBC.EffectiveStartDate AND CBC.EffectiveEndDate
																				OR 
																				@PeriodEndDate BETWEEN CBC.EffectiveStartDate AND CBC.EffectiveEndDate
																			)
																		AND CBC.ApplyToChild = 1
		-- GDB-1628 Get Tenor associated with the schedule
		LEFT JOIN IDB_Billing.dbo.OverrideScheduleTenor TN (NOLOCK) ON TN.ScheduleId = S.ScheduleId
																	AND (
																			@PeriodStartDate BETWEEN TN.EffectiveStartDate AND TN.EffectiveEndDate
																			OR 
																			@PeriodEndDate BETWEEN TN.EffectiveStartDate AND TN.EffectiveEndDate
																		)
		WHERE	(
					@PeriodStartDate BETWEEN S.EffectiveStartDate AND S.EffectiveEndDate
					OR		
					@date2 BETWEEN S.EffectiveStartDate AND S.EffectiveEndDate
				)
		AND		(@BillingCode IS NULL OR S.Billing_Code = @BillingCode OR S.Billing_Code IS NULL) -- We need to pick up default billing codes as well
		AND		(@ProductGroup IS NULL OR S.Product = @ProductGroup)


		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'Get Override Schedules',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2


		/* IDBC-218: The below block will run only at EOD final run for "D" */
		IF (
				@SummaryType = 'D'
				AND @ReportMode > 0 
				AND @EODrun = 1
				AND EXISTS (SELECT 1 FROM #OverrideSchedules WHERE TierId IS NOT NULL OR ScheduleType = 'R' AND ProcessId = @ProcessID)  -- IDBBC-140 Run this block if there are schedules to process instead of just for OTR
		   )
		BEGIN

			DECLARE @OSProducts VARCHAR(30)

			SELECT @OSProducts = STRING_AGG(Product, ',') 
			FROM 
			(
				SELECT DISTINCT Product
				FROM #OverrideSchedules
			) X

			-- IDBBC-17: 12/17/2019 Get numbe of business days till today in current peirod to be used for VADV tier type rebate calculation
			DECLARE @BusinessDaysForAvg INT, @BusinessDaysInPeriod INT, @BusinessDaysMTD INT

			-- IDBBC-268 Converting #PoolTakerTradesVsOperators to INSERT INTO	IDB_Billing.dbo.wPoolTakerTradesVsOperators
			;WITH CTE_DEALS
			AS
			(
				SELECT DEAL_TRADE_DATE,
						DEAL_NEGOTIATION_ID,
						Dealer,
						ProductGroup,
						IsActiveStream,
						Operator
				FROM IDB_Reporting.dbo.IDB_FALCON_DEALS (NOLOCK)
				WHERE DEAL_TRADE_DATE BETWEEN @PeriodStartDate AND @PeriodEndDate
				AND ProductGroup IN (@OSProducts)
				AND DEAL_STATUS <> 'CANCELLED'
				AND DEAL_LAST_VERSION = 1
			)
			INSERT INTO	IDB_Billing.dbo.wPoolTakerTradesVsOperators
			(
				PROCESS_ID,
				Deal_Trade_Date,
				DEAL_NEGOTIATION_ID,
				Dealer,
				CounterParty,
				IsActiveStream,
				Operator
			)
			SELECT	DISTINCT
					@ProcessID,
					DLR.DEAL_TRADE_DATE,
					DLR.DEAL_NEGOTIATION_ID,
					DLR.Dealer,
					CounterParty = CP.Dealer,
					DLR.IsActiveStream,
					DLR.Operator
			
			FROM	#OverrideSchedules OS
			JOIN	CTE_DEALS DLR ON	DLR.DEAL_TRADE_DATE BETWEEN OS.EffectiveStartDate AND OS.EffectiveEndDate
												AND DLR.ProductGroup = OS.Product
												AND DLR.Dealer = OS.Dealer
												AND DLR.IsActiveStream = OS.IsActiveStream
												AND DLR.Operator = OS.Operator
			JOIN	CTE_DEALS CP ON CP.DEAL_TRADE_DATE = DLR.DEAL_TRADE_DATE
									AND CP.ProductGroup = DLR.ProductGroup
									AND CP.DEAL_NEGOTIATION_ID = DLR.DEAL_NEGOTIATION_ID
									AND (OS.PoolParticipants IS NOT NULL OR CP.Dealer IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(OS.PoolTakers,',')))
									AND CP.Operator <> DLR.Operator
			WHERE	(OS.PoolTakers IS NOT NULL OR OS.PoolParticipants IS NOT NULL)
			AND OS.ProcessId = @ProcessID

			--  SHIRISH 07/22/2019 DIT-18425
			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'After PoolTakersVsOperators',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2

			SELECT	ScheduleId,
					Billing_Code,
					Dealer,
					Product,
					Source,
					ScheduleType,
					IsActiveStream,
					Operator,
					AnonymousLevel,
					BilateralStream,
					IsStdTickInc,
					OFTR,
					ApplyToMonth,
					AggrChargeRate,
					PassChargeRate,
					Floor,
					Cap,
					EffectiveStartDate,
					EffectiveEndDate,
					
					-- Tier Information
					TierId,
					--IncrementalTiers,
					TierFunction, -- IDBBC-294
					AggrPass,
					TierType,
					-- GDB-1628 For rebate schedules that do not use tiers, we need to use ResultType as Amount as we need to calculate rebate amount and save it in RebateAndBillingTracker
					ResultType = CASE WHEN TierId IS NULL AND ScheduleType = 'R' THEN 'Amount' ELSE ResultType END,  

					-- Threshold information
					UsesThreshld,
					ThresholdProducts,
					TH_IsActiveStream,
					TH_Operator,
					TH_AggrPass,
					ThresholdMin,
					ThresholdMax,
					TH_VolumeType,
					TH_EffectiveStartDate,
					TH_EffectiveEndDate,
					
					---- Pool Takers
					PoolTakers,
					PT_EffectiveStartDate,
					PT_EffectiveEndDate,

					-- Pool Participants
					PP_GroupName,
					PoolParticipants,
					IncludePP,
					PP_EffectiveStartDate,
					PP_EffectiveEndDate,

					--Child_Billing_Codes,
					Child_Dealers,

					TenorStart,
					TenorEnd,
					TN_EffectiveStartDate,
					TN_EffectiveEndDate,
					Security_Currency,
			
					RowNum = ROW_NUMBER() OVER (ORDER BY ScheduleId)

			INTO	#OverrideSchedulesWTiersLoop
			FROM	#OverrideSchedules
			WHERE	TierId IS NOT NULL
			AND		ProcessId = @ProcessID
			OR		ScheduleType = 'R'

			--  SHIRISH 07/22/2019 DIT-18425
			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'After #OverrideSchedulesWTiersLoop',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2

			IF @Debug = 1
				SELECT '#OverrideSchedulesWTiersLoop', * FROM #OverrideSchedulesWTiersLoop

			DECLARE @TotalRows INT, @Row INT = 1, @DealerPlatformPerc FLOAT
			DECLARE @ScheduleId INT, @dlr VARCHAR(15), @BillCode VARCHAR(15), @product VARCHAR(15), @Src VARCHAR(5), @ScheduleType CHAR(1), @IsActiveStream BIT, @Operator BIT, 
					@AnonymousLevel VARCHAR(15), @BilateralStream BIT, @IsStdTickInc BIT, @OFTR BIT, @AggrChargeRate DECIMAL(12,6), @PassChargeRate DECIMAL(12,6),
					@ApplyToMonth CHAR(1), @SchFloor DECIMAL(10,2), @SchCap DECIMAL(10,2), @EffectiveStartDate DATE, @EffectiveEndDate DATE

			DECLARE @TierId INT, /*@IncrementalTiers BIT,*/ @AggrPass SMALLINT, @TierType CHAR(5), @ResultType VARCHAR(10), @TierFunction VARCHAR(50)

			DECLARE @UsesThreshold BIT, @ThresholdProducts VARCHAR(100), @TH_IsActiveStream BIT, @TH_Operator BIT, @TH_AggrPass BIT, 
					@ThresholdMin DECIMAL(10,2), @ThresholdMax DECIMAL(10,2), @TH_VolumeType VARCHAR(10),@TH_EffectiveStartDate DATE, @TH_EffectiveEndDate DATE

			DECLARE @PoolTakers VARCHAR(512), @PT_EffectiveStartDate DATE, @PT_EffectiveEndDate DATE

			DECLARE @PP_Group VARCHAR(20), @PoolParticipants VARCHAR(512), @IncludePP BIT, @PP_EffectiveStartDate DATE, @PP_EffectiveEndDate DATE
			
			DECLARE @Child_Dealers VARCHAR(512)

			DECLARE @TenorStart INT, @TenorEnd INT, @TN_EffectiveStartDate DATE, @TN_EffectiveEndDate DATE

			DECLARE @ThresholdMet BIT, @DlrVolume DECIMAL(20,5), @ValueForThresholdCheck DECIMAL(10,2),@PlatformVolume DECIMAL(20,5)
			DECLARE @AggrRate DECIMAL(12,5), @PassRate DECIMAL(12,5) -- Tier Rate to be applied
			DECLARE @Amount DECIMAL(10,2) -- Total Commission/Rebate
			DECLARE @CurrentAggrRate DECIMAL(10,2), @CurrentPassRate DECIMAL(10,2) -- if rate entry already exists then that can be compared to calculated rate to see if entry needs to be updated
			DECLARE @ValueForTierMatch DECIMAL(20,5) -- Value used to maatch tier.  For P schedules this will be % of platform volume, For VAGG schedules this will be aggregate volume and for VADV schedule this will be average daily volume
			DECLARE @LoopStartDate DATE, @LoopEndDate DATE, @LoopEndDateVADV DATE -- IDBBC-94 These are used for each dealer loop to make sure we are using correct period start and end dates
			DECLARE @PTTVO_Volume INT
			DECLARE @Security_Currency VARCHAR(4)

			SELECT @TotalRows = COUNT(1) FROM #OverrideSchedulesWTiersLoop


			--  SHIRISH 07/22/2019 DIT-18425
			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'Loop to process volume based schedules',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2

			WHILE @TotalRows >= @Row  -- Loop to process volume based billing schedules
			BEGIN

				SELECT	@ScheduleId = ScheduleId, @dlr = Dealer, @BillCode = Billing_Code, @product = Product, @Src = Source, @ScheduleType = ScheduleType, @IsActiveStream = IsActiveStream, @Operator = Operator,
						@AnonymousLevel = AnonymousLevel, @BilateralStream = BilateralStream, @IsStdTickInc = IsStdTickInc, @OFTR = OFTR, @AggrChargeRate = AggrChargeRate, @PassChargeRate = PassChargeRate,
						@ApplyToMonth = ApplyToMonth, @SchFloor = [Floor], @SchCap = Cap,
						@TierId = TierId, /*@IncrementalTiers = IncrementalTiers,*/ @AggrPass = AggrPass, @TierType = TierType, @ResultType = ResultType, @TierFunction = TierFunction,
						@UsesThreshold = UsesThreshld, @ThresholdProducts = ThresholdProducts, @TH_IsActiveStream = TH_IsActiveStream, @TH_Operator = TH_Operator, @TH_AggrPass = TH_AggrPass,
						@ThresholdMin = ThresholdMin, @ThresholdMax = ThresholdMax, @TH_VolumeType = TH_VolumeType, @TH_EffectiveStartDate = TH_EffectiveStartDate, @TH_EffectiveEndDate = TH_EffectiveEndDate,
						@PoolTakers = PoolTakers, @PT_EffectiveStartDate = PT_EffectiveStartDate, @PT_EffectiveEndDate = PT_EffectiveEndDate,
						@PP_Group = PP_GroupName, @PoolParticipants = PoolParticipants, @IncludePP = IncludePP, @PP_EffectiveStartDate = PP_EffectiveStartDate, @PP_EffectiveEndDate = PP_EffectiveEndDate,
						@Child_Dealers = Child_Dealers,
						@TenorStart = TenorStart, @TenorEnd = TenorEnd, @TN_EffectiveStartDate = TN_EffectiveStartDate, @TN_EffectiveEndDate = TN_EffectiveEndDate,
						@Security_Currency = Security_Currency
				
				FROM	#OverrideSchedulesWTiersLoop
				WHERE	RowNum = @Row

				SELECT @Scheduleid = @ScheduleId, @LoopStartDate = CASE WHEN @EffectiveStartDate > @PeriodStartDate THEN @EffectiveStartDate ELSE @PeriodStartDate END -- IDBBC-94 

				IF @TierType = 'VADV'
					SELECT @LoopEndDate = CASE WHEN @EffectiveEndDate < @EndDateForVADV THEN @EffectiveEndDate ELSE @EndDateForVADV END -- IDBBC-94
				ELSE
					SELECT @LoopEndDate = CASE WHEN @EffectiveEndDate < @PeriodEndDate THEN @EffectiveEndDate ELSE @PeriodEndDate END -- IDBBC-94

				-- for VADV schedules use actual number of business days.  For other schedules use value 1
				IF @TierType = 'VADV'
					SET @BusinessDaysForAvg = IDB_Codebase.dbo.fnCalculateBusinessDays(@LoopStartDate,@LoopEndDate,'US',@product) -- IDBBC-94 -- IDBBC-140  Using product variable instead of hardcoding to OTR
				ELSE 
					SET @BusinessDaysForAvg = 1

				SET @BusinessDaysInPeriod = IDB_Codebase.dbo.fnCalculateBusinessDays(@LoopStartDate,@LoopEndDate,'US',@product) -- IDBBC-196
				SET @BusinessDaysMTD = IDB_Codebase.dbo.fnCalculateBusinessDays(@LoopStartDate,@Date2,'US',@product) -- IDBBC-196

				-- IDBBC-49 Set all variables used for rate/rebate calculation to NULL as we want to make sure there are no residue values from previous loop when we start processing each loop
				SELECT @AggrRate = NULL, @PassRate = NULL, @CurrentAggrRate = NULL, @CurrentPassRate = NULL, @ValueForThresholdCheck = NULL, @ThresholdMet = NULL, @DlrVolume = NULL, @PTTVO_Volume = NULL, @Amount = NULL
				--, @TotalRebate = NULL
				
				IF @Debug  = 1
					SELECT ScheduleId = @ScheduleId, LoopStartDate = @LoopStartDate, LoopEndDate = @LoopEndDate, BusinessDaysForAvg = @BusinessDaysForAvg, BusinessDaysInPeriod = @BusinessDaysInPeriod, BusinessDaysMTD = @BusinessDaysMTD, Security_Currency = @Security_Currency

				IF @UsesThreshold = 1
				BEGIN
						SELECT	@DlrVolume = SUM(DEAL_QUANTITY)
						FROM	IDB_Reporting.dbo.IDB_FALCON_DEALS (NOLOCK)
						WHERE	DEAL_TRADE_DATE BETWEEN @LoopStartDate AND @LoopEndDate
						AND		ProductGroup IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(@ThresholdProducts,',')) -- Pass all threshold products
						AND		Source = @Src -- IDBBC-218 We do not need to include DWCHS trades in these calculations
						AND		(
									Dealer = @dlr
									OR
									Dealer IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(@Child_Dealers,',')) -- Include volume for all children
									OR
									Dealer IN (SELECT DW_FIRM_CODE FROM IDB_Reporting.dbo.DWC_DW_FIRM_MAPPING_TABLE (NOLOCK) WHERE DW_ACRONYM = @dlr) -- IDBBC-210 Pick up trades from matched dealer. Updated to IN dealer list as we could have more than one mapping
									OR 
									Dealer IN (SELECT DW_FIRM_CODE FROM IDB_Reporting.dbo.DWC_DW_FIRM_MAPPING_TABLE (NOLOCK) WHERE DW_ACRONYM IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(@Child_Dealers,','))) -- IDBBC-210 Pick up trades from mapped child dealers
								)
						AND		DEAL_LAST_VERSION = 1
						AND		DEAL_STATUS <> 'CANCELLED'					
						AND		(@TH_IsActiveStream IS NULL OR IsActiveStream = @TH_IsActiveStream)
						AND		(@TH_Operator IS NULL OR Operator = @TH_Operator)
						AND		(@TH_AggrPass IS NULL OR DEAL_IS_AGRESSIVE = @TH_AggrPass)
						AND		(@Security_Currency IS NULL OR Security_Currency = @Security_Currency) -- IDBBC-260
						-- IDBBC-347 for EUREPOSPRD trades we need to charge commission and fees on the legs
							-- IDBBC-401 Adding EUREPOSPRDGC to below condition
						AND		(CASE WHEN TradeType2 IN ('EUREPOSPRD','EUREPOSPRDGC') AND DEAL_LEG = 0 THEN 0 ELSE 1 END) = 1
						-- IDBBC-260 For EUREPO GC trades only Take allocation volume
						AND		(SWSecType <> 'EUREPOGC' OR (SWSecType = 'EUREPOGC' AND MessageType = 46))


					-- Get value for threshold check. 
					-- FOR Volume type VADV need to get average daily volume by dividing dealer volume by number of business days.  
					-- For volume type VAGG take dealer volume as is
					IF @TH_VolumeType = 'P'
					BEGIN

						-- Get Platform volume for % Calculation
							SELECT	@PlatformVolume = SUM(DEAL_QUANTITY)/(CASE WHEN @TH_AggrPass IS NULL THEN 1 ELSE 2 END) -- IF schedule uses AggrPass flag then take single sided platform volume
							FROM	IDB_Reporting.dbo.IDB_FALCON_DEALS (NOLOCK)
							-- IDBBC-260 EUREPO uses Deal_Repo_Start_Date to pull trades
							WHERE	DEAL_TRADE_DATE BETWEEN @LoopStartDate AND @LoopEndDate
							AND		ProductGroup IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(@ThresholdProducts,','))
							AND		DEAL_LAST_VERSION = 1
							AND		DEAL_STATUS <> 'CANCELLED'
							AND		Source = @Src -- IDBBC-218 We do not need to include DWCHS trades in these calculations
							AND		(@IsActiveStream IS NULL OR IsActiveStream = @IsActiveStream) -- IDBBC-140
							AND		(@Operator IS NULL OR Operator = @Operator) -- IDBBC-140
							AND		(@Security_Currency IS NULL OR Security_Currency = @Security_Currency) -- IDBBC-260
							-- IDBBC-347 for EUREPOSPRD trades we need to charge commission and fees on the legs
							-- IDBBC-401 Adding EUREPOSPRDGC to below condition
							AND		(CASE WHEN TradeType2 IN ('EUREPOSPRD','EUREPOSPRDGC') AND DEAL_LEG = 0 THEN 0 ELSE 1 END) = 1
							-- IDBBC-260 For EUREPO GC trades only Take allocation volume
							AND		(SWSecType <> 'EUREPOGC' OR (SWSecType = 'EUREPOGC' AND MessageType = 46))


						-- Calculate % that needs to be matched with a particular tier to determin rate to be applied
						-- GDB-1628 Removing Ceiling function call as percentage tiers use up to two decimal points
						SET @ValueForThresholdCheck = (@DlrVolume/@PlatformVolume)*100.00 -- SHIRISH 06/03/2019: IDB-18449 Making sure calculated percentage is whole number
					END
					ELSE -- Threshold Volume Type V (VAGG, VADV)
						SET @ValueForThresholdCheck = @DlrVolume/(CASE WHEN @TH_VolumeType = 'VADV' THEN @BusinessDaysForAvg ELSE 1 END)

					IF @ValueForThresholdCheck BETWEEN @ThresholdMin AND @ThresholdMax
						SET @ThresholdMet = 1
					ELSE
						SET @ThresholdMet = 0

					--IF @Debug = 1
					--	SELECT @ScheduleId, 'ValueForThresholdCheck' = @ValueForThresholdCheck, 'ThresholdMet' = @ThresholdMet, @ThresholdMin, @ThresholdMax

				END
				-- If schedule does not use threshold then set threshold met to 1 so we can proceed
				ELSE 
					SET @ThresholdMet = 1

				-- Update Threshold met column on the overrideschedule table.  So we can figure out which schedules need to be mapped to the trades
				UPDATE	#OverrideSchedules
				SET		ThresholdMet = @ThresholdMet
				WHERE	ScheduleId = @ScheduleId
				AND ProcessId = @ProcessID

				/***** START Threshold Loop *****/
				IF @ThresholdMet = 1 
				BEGIN

					-- If Both pool participants and Pool takers both are set to null and if there are records in #PoolTakerTradesVsOperator table that means there is another schedule for this billing code
					-- that uses either pool taker or pool participants setting.  In that case we need to delete this volume from dealer volume to get correct volume for schedule
					IF @PoolParticipants IS NULL AND @PoolTakers IS NULL AND @TierId IS NOT NULL -- we only need to proess tier based schedule here
					BEGIN

						-- GET PPTVO volume 
						SELECT	@PTTVO_Volume = SUM(IFD.DEAL_QUANTITY)
						FROM	IDB_Billing.dbo.wPoolTakerTradesVsOperators PTTVO (NOLOCK) 
						JOIN	IDB_Reporting.dbo.IDB_FALCON_DEALS IFD (NOLOCK) ON IFD.DEAL_TRADE_DATE = PTTVO.Deal_Trade_Date
																			   AND IFD.DEAL_NEGOTIATION_ID = PTTVO.DEAL_NEGOTIATION_ID
																			   AND IFD.Dealer = PTTVO.CounterParty
						WHERE	PTTVO.PROCESS_ID = @ProcessID
						AND		PTTVO.Dealer = @dlr
						AND		PTTVO.IsActiveStream = @IsActiveStream
						AND		PTTVO.Operator = @Operator
						AND		IFD.DEAL_LAST_VERSION = 1
						AND		IFD.DEAL_STATUS  <> 'CANCELLED'

						-- Get dealer volume
							SELECT	@DlrVolume = SUM(IFD.DEAL_QUANTITY) - ISNULL(@PTTVO_Volume,0) 
							FROM	IDB_Reporting.dbo.IDB_FALCON_DEALS IFD (NOLOCK)
							-- IDBBC-140 Added join below to get correct volume for a dealer when multiple billing codes share same dealer code but branch id is different
							LEFT JOIN	IDB_Billing.dbo.wActiveBranch AB (NOLOCK) ON AB.PROCESS_ID = @ProcessID
																			 AND AB.BRANCH_ID = IFD.BranchId
																			 AND AB.BILLING_CODE = @BillCode
							WHERE	IFD.DEAL_TRADE_DATE BETWEEN @LoopStartDate AND @LoopEndDate
							AND		IFD.ProductGroup = @product
							AND		IFD.DEAL_LAST_VERSION = 1 -- IDBBC-163
							AND		IFD.DEAL_STATUS  <> 'CANCELLED' -- IDBBC-163
							AND		((IFD.ProductGroup = 'BILL' AND IFD.DEAL_SYNTHETIC = 0) OR (IFD.ProductGroup <> 'BILL')) -- IDBBC-163 Only pick up legs
							AND		IFD.Source = @Src -- IDBBC-218 We do not need to include DWCHS trades in these calculations
							AND		(
										IFD.Dealer = @dlr
										OR
										IFD.Dealer IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(@Child_Dealers,',')) -- Include volume for all children
										OR
										IFD.Dealer IN (SELECT DW_FIRM_CODE FROM IDB_Reporting.dbo.DWC_DW_FIRM_MAPPING_TABLE (NOLOCK) WHERE DW_ACRONYM = @dlr) -- IDBBC-210 Pick up trades from matched dealer. Updated to IN dealer list as we could have more than one mapping
										OR 
										IFD.Dealer IN (SELECT DW_FIRM_CODE FROM IDB_Reporting.dbo.DWC_DW_FIRM_MAPPING_TABLE (NOLOCK) WHERE DW_ACRONYM IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(@Child_Dealers,','))) -- IDBBC-210 Pick up trades from matched child dealer
									)
							AND		(@IsActiveStream IS NULL OR IFD.IsActiveStream = @IsActiveStream) -- IDBBC-140
							AND		(@Operator IS NULL OR IFD.Operator = @Operator) -- IDBBC-140
							AND		(@AggrPass IS NULL OR IFD.DEAL_IS_AGRESSIVE = @AggrPass) -- GDB-1628 if schedule uses AggrPass flag then use Deal_is_Aggressive flag to get correct volume for schedule
							AND		(@AnonymousLevel IS NULL OR IFD.AnonymousLevel IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(@AnonymousLevel,',')))
							AND		(@BilateralStream IS NULL OR IFD.BilateralStream = @BilateralStream)
							AND		(@IsStdTickInc IS NULL OR IFD.IsStdTickInc = @IsStdTickInc)
							AND		(@Child_Dealers IS NOT NULL OR AB.BRANCH_ID IS NOT NULL) -- IDBBC-140 Ignore branch id when schedule uses child billing codes
							AND		(@TenorStart IS NULL OR (IFD.issued_as/12) BETWEEN @TenorStart AND @TenorEnd) -- IDBBC-163 Adding missing tenor check condition
							AND		(@Security_Currency IS NULL OR IFD.Security_Currency = @Security_Currency) -- IDBBC-260
							-- IDBBC-347 for EUREPOSPRD trades we need to charge commission and fees on the legs
							-- IDBBC-401 Adding EUREPOSPRDGC to below condition
							AND		(CASE WHEN IFD.TradeType2 IN ('EUREPOSPRD','EUREPOSPRDGC') AND IFD.DEAL_LEG = 0 THEN 0 ELSE 1 END) = 1
							-- IDBBC-260 For EUREPO GC trades only Take allocation volume
							AND		(IFD.SWSecType <> 'EUREPOGC' OR (IFD.SWSecType = 'EUREPOGC' AND IFD.MessageType = 46))


						IF @Debug = 1
							--SELECT 'DlrVolume', @DlrVolume, 'PTTVO_Volume', @PTTVO_Volume
							SELECT 'DlrVolume'= @DlrVolume, 'PTTVOVol'=  @PTTVO_Volume

					END
					ELSE IF @PoolTakers IS NOT NULL
					BEGIN

						SELECT	@DlrVolume = SUM(IFD.DEAL_QUANTITY)
						FROM	IDB_Billing.dbo.wPoolTakerTradesVsOperators PTTVO (NOLOCK) 
						JOIN	IDB_Reporting.dbo.IDB_FALCON_DEALS IFD (NOLOCK) ON IFD.DEAL_TRADE_DATE = PTTVO.Deal_Trade_Date
																			   AND IFD.DEAL_NEGOTIATION_ID = PTTVO.DEAL_NEGOTIATION_ID
																			   AND IFD.Dealer = PTTVO.CounterParty
						WHERE	PTTVO.PROCESS_ID = @ProcessID
						AND		PTTVO.Dealer = @dlr
						AND		PTTVO.IsActiveStream = @IsActiveStream
						AND		PTTVO.Operator = @Operator
						AND		PTTVO.CounterParty IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(@PoolTakers,','))
						AND		IFD.DEAL_LAST_VERSION = 1
						AND		IFD.DEAL_STATUS  <> 'CANCELLED'

					END
					ELSE IF @PoolParticipants IS NOT NULL
					BEGIN

						SELECT	@DlrVolume = SUM(IFD.DEAL_QUANTITY)
						FROM	IDB_Billing.dbo.wPoolTakerTradesVsOperators PTTVO (NOLOCK) 
						JOIN	IDB_Reporting.dbo.IDB_FALCON_DEALS IFD (NOLOCK) ON IFD.DEAL_TRADE_DATE = PTTVO.Deal_Trade_Date
																			   AND IFD.DEAL_NEGOTIATION_ID = PTTVO.DEAL_NEGOTIATION_ID
																			   AND IFD.Dealer = PTTVO.Dealer -- IDBBC-163 Here we need to count dealer volume
						WHERE	PTTVO.PROCESS_ID = @ProcessID
						AND		PTTVO.Dealer = @dlr
						AND		PTTVO.IsActiveStream = @IsActiveStream
						AND		PTTVO.Operator = @Operator
						AND		(
									(@IncludePP = 1 AND PTTVO.CounterParty IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(@PoolParticipants,','))) -- Include Pool Participants from Schedule
									OR 
									(@IncludePP = 0 AND PTTVO.CounterParty NOT IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(@PoolParticipants,','))) -- Exclude Pool Participants from Schedule
								)
						AND		IFD.DEAL_LAST_VERSION = 1
						AND		IFD.DEAL_STATUS  <> 'CANCELLED'

					END

					/**** START TierType Loop ****/
					IF @TierType = 'P'
					BEGIN

						-- Get Platform volume for % Calculation
							SELECT	@PlatformVolume = SUM(DEAL_QUANTITY)/(CASE WHEN @AggrPass IS NULL THEN 1 ELSE 2 END) -- IF schedule uses AggrPass flag then take single sided platform volume
							FROM	IDB_Reporting.dbo.IDB_FALCON_DEALS (NOLOCK)
							-- IDBBC-260 EUREPO uses Deal_Repo_Start_Date instead of Deal_Trade_Date
							WHERE	DEAL_TRADE_DATE BETWEEN @LoopStartDate AND @LoopEndDate
							AND		ProductGroup = @product
							AND		DEAL_LAST_VERSION = 1
							AND		DEAL_STATUS  <> 'CANCELLED'		
							AND		((ProductGroup = 'BILL' AND DEAL_SYNTHETIC = 0) OR (ProductGroup <> 'BILL')) 
							AND		Source = @Src -- IDBBC-218 We do not need to include DWCHS trades in these calculations
							AND		(@IsActiveStream IS NULL OR IsActiveStream = @IsActiveStream) -- IDBBC-140
							AND		(@Operator IS NULL OR Operator = @Operator) -- IDBBC-140
							AND		(@Security_Currency IS NULL OR Security_Currency = @Security_Currency) -- IDBBC-260
							-- IDBBC-347 for EUREPOSPRD trades we need to charge commission and fees on the legs
							-- IDBBC-401 Adding EUREPOSPRDGC to below condition
							AND		(CASE WHEN TradeType2 IN ('EUREPOSPRD','EUREPOSPRDGC') AND DEAL_LEG = 0 THEN 0 ELSE 1 END) = 1
							-- IDBBC-260 For EUREPO GC trades only Take allocation volume
							AND		(SWSecType <> 'EUREPOGC' OR (SWSecType = 'EUREPOGC' AND MessageType = 46))


						-- Calculate % that needs to be matched with a particular tier to determin rate to be applied
						-- GDB-1628 Removing Ceiling function call as percentage tiers use up to two decimal points
						SET @ValueForTierMatch = (@DlrVolume/@PlatformVolume)*100.00 -- SHIRISH 06/03/2019: IDB-18449 Making sure calculated percentage is whole number

						IF @Debug = 1
							SELECT ScheduleId = @ScheduleId, ValueForTierMatch = @ValueForTierMatch, DlrVolume = @DlrVolume, PlatformVolume = @PlatformVolume

					END
					ELSE IF @TierType = 'VAGG'
						SELECT @ValueForTierMatch = @DlrVolume -- Aggregate volume

					ELSE IF @TierType = 'VADV'
						SELECT @ValueForTierMatch = @DlrVolume/@BusinessDaysForAvg -- Average daily volume

					/**** END TierType Loop ****/

					/**** START ResultType Loop ****/

					IF @ResultType = 'Rate'
					BEGIN
						-- Calculate rate to be applied
						SELECT	@AggrRate = AggrRate,
								@PassRate = PassRate
						FROM	IDB_Billing.dbo.OverrideTiers
						WHERE	TierId = @TierId
						AND		@ValueForTierMatch BETWEEN TierStart AND TierEnd

						IF @Debug = 1
							SELECT 'ValueForTierMatch', @ValueForTierMatch, AggrRate = @AggrRate, PassRate = @PassRate

						IF @ReportMode = 1 -- Save rate if required in Commission Summary mode
						BEGIN

							-- Create a list of dealers to apply calculated rate.  This will include main dealer from the schedule and all child dealers
							SELECT	Billing_Code,
									RowNum = ROW_NUMBER() OVER (ORDER BY Billing_Code)
							INTO	#ApplyRateToBillingCodes
							FROM	#OverrideSchedules
							WHERE	ScheduleId = @ScheduleId OR ParentScheduleId = @ScheduleId
							AND ProcessId = @ProcessID

							DECLARE @CurRow INT = 1, @TotalBillCodes INT, @CurrBillCode VARCHAR(15)
							SELECT @TotalBillCodes = COUNT(1) FROM #ApplyRateToBillingCodes

							-- Dealer loop to save calculated rate to OTRVolumeBasedCommissionRate
							WHILE @TotalBillCodes >= @CurRow
							BEGIN

								SELECT @CurrBillCode = BILLING_CODE FROM #ApplyRateToBillingCodes WHERE RowNum = @CurRow

								-- insert new entry if rate/month entry is not in the table 
								IF NOT EXISTS (SELECT 1 FROM IDB_Billing.dbo.VolumeBasedCommissionRate (NOLOCK)
												WHERE BillingCode = @CurrBillCode
												AND	ProductGroup = @Product -- IDBBC-260
												AND (@IsActiveStream IS NULL OR IsActiveStream = @IsActiveStream) -- IDBBC-140
												AND (@Operator IS NULL OR Operator = @Operator)  -- IDBBC-26 Added @IsActiveStream -- IDBBC-140
												AND (@AggrPass IS NULL OR AggrPass = @AggrPass) -- GDB-1628  Adding condition to match AggrPass flag
												AND PeriodStartDate = @LoopStartDate --IDBBC-94
												-- GDB-1628 for Period End Date we need to use Effective end date if that is smaller than Period End Date.  Else use perio end date
												-- we can not use @LoopEndDate as for ADV schedules @LoopEndDate changes everyday
												AND PeriodEndDate = CASE WHEN @EffectiveEndDate < @PeriodEndDate THEN @EffectiveEndDate ELSE @PeriodEndDate END -- IDBBC-94
												AND (@Security_Currency IS NULL OR Security_Currency = @Security_Currency)) -- IDBBC-260
								BEGIN
									INSERT INTO IDB_Billing.dbo.VolumeBasedCommissionRate (BillingCode, ProductGroup, PeriodStartDate, PeriodEndDate, AggrRate, PassRate, LastUpdated, BackfillNeeded, IsActiveStream, Operator, AggrPass, Security_Currency) --, BilateralStream, ExcludeAnonymousTrades) -- IDBBC-94 Adding Bilateral Stream flag which was missing from insert and ExcludeAnonymousTrades
									SELECT	@CurrBillCode, 
											@Product, -- IDBBC-260
											@LoopStartDate, -- IDBBC-94
											CASE WHEN @EffectiveEndDate < @PeriodEndDate THEN @EffectiveEndDate ELSE @PeriodEndDate END, -- IDBBC-94
											@AggrRate, 
											@PassRate,
											GETDATE(), 
											0, 
											@IsActiveStream, 
											@Operator,
											@AggrPass,
											@Security_Currency -- IDBBC-260
								
								END
								ELSE -- if entry exists them update it if rate is different
								BEGIN

									SELECT	@CurrentAggrRate = AggrRate,
											@CurrentPassRate = PassRate
									FROM	IDB_Billing.dbo.VolumeBasedCommissionRate (NOLOCK)
									WHERE	BillingCode = @CurrBillCode -- IDBBC-26 fixed column name
									AND		ProductGroup = @Product -- IDBBC-260
									AND		(@IsActiveStream IS NULL OR IsActiveStream = @IsActiveStream) -- IDBBC-26 Added IsActiveStream condition -- IDBBC-140
									AND		(@Operator IS NULL OR Operator = @Operator) -- IDBBC-26 -- IDBBC-140
									AND		(@AggrPass IS NULL OR AggrPass = @AggrPass) -- GDB-1628 Check if schedule uses AggrPass flag
									AND		PeriodStartDate = @LoopStartDate -- IDBBC-94
									-- GDB-1628 for Period End Date we need to use Effective end date if that is smaller than Period End Date.  Else use perio end date
									-- we can not use @LoopEndDate as for ADV schedules @LoopEndDate changes everyday
									AND		PeriodEndDate = CASE WHEN @EffectiveEndDate < @PeriodEndDate THEN @EffectiveEndDate ELSE @PeriodEndDate END
									AND		(@Security_Currency IS NULL OR @Security_Currency = @Security_Currency) -- IDBBC-260

									--IF @Debug = 1
									--	SELECT 'CurrentAggrRate' = @CurrentAggrRate, 'AggrRate' = @AggrRate, 'CurrentPassRate' = @CurrentPassRate,  'PassRate' = @PassRate

									-- IDBBC-98 Added a check to see if ExcludeAnonymousFlag matches existing record.  Update if different
									IF ISNULL(@CurrentAggrRate,0.0) <> @AggrRate OR ISNULL(@CurrentPassRate,0.0) <> @PassRate -- 2019/12/16: IDBBC-13 Adding ISNULL to make sure comparios works if current rate is set to NULL
										UPDATE	IDB_Billing.dbo.VolumeBasedCommissionRate
										SET		AggrRate = @AggrRate,
												PassRate = @PassRate,
												LastUpdated = GETDATE(),
												BackfillNeeded = 1
										WHERE	BillingCode = @CurrBillCode
										AND		ProductGroup = @Product -- IDBBC-260
										AND		(@IsActiveStream IS NULL OR IsActiveStream = @IsActiveStream) -- IDBBC-26 -- IDBBC-140
										AND		(@Operator IS NULL OR Operator = @Operator) -- IDBBC-26 -- IDBBC-140
										AND		(@AggrPass IS NULL OR AggrPass = @AggrPass) -- GDB-1628 Check if schedule uses AggrPass flag
										AND		PeriodStartDate = @LoopStartDate -- IDBBC-94
										-- GDB-1628 for Period End Date we need to use Effective end date if that is smaller than Period End Date.  Else use perio end date
										-- we can not use @LoopEndDate as for ADV schedules @LoopEndDate changes everyday
										AND		PeriodEndDate = CASE WHEN @EffectiveEndDate < @PeriodEndDate THEN @EffectiveEndDate ELSE @PeriodEndDate END
										AND		(@Security_Currency IS NULL OR Security_Currency = @Security_Currency) -- IDBBC-260
								END

								SET @CurRow = @CurRow + 1

							END

							DROP TABLE #ApplyRateToBillingCodes
						END -- END @ReportMode = 1
						ELSE
							SELECT ScheduleID = @ScheduleId, AggrRate = @AggrRate, PassRate = @PassRate, Volume = @ValueForTierMatch
								
					END
					ELSE IF @ResultType = 'Amount'
					BEGIN

						IF @Debug = 1
							SELECT ScheduleID = @ScheduleId, TierID = @TierId, ValueForTIerMatch = @ValueForTierMatch, DlrVolume = @DlrVolume, TierFunction = @TierFunction, ScheduleType = @ScheduleType, Flr = @SchFloor, Cap = @SchCap, BusinessDays = @BusinessDaysForAvg

						IF @TierType = 'T'
						BEGIN

							SELECT	@Amount = SUM(IFD.DEAL_QUANTITY * OT.AggrRate) -- we will use AggrRate and in this case both AggrRate and PassRate are same
							FROM	IDB_Reporting.dbo.IDB_Falcon_Deals IFD (NOLOCK)
							--JOIN	Instrument.dbo.Security_Master SM (NOLOCK) ON SM.instrid = IFD.DEAL_SECURITY_ID
							--JOIN	Instrument.dbo.Security_Type ST (NOLOCK) ON ST.sec_type_id = SM.sec_type_id AND ST.product_grp = @product
							JOIN	IDB_Billing.dbo.OverrideTiers OT (NOLOCK) ON OT.TierId = @TierId
																			 AND CAST(IFD.DEAL_DATE AS TIME) BETWEEN --S.TimeStart AND S.TimeEnd
																					CAST(CAST( CAST(OT.TierStart AS int) / 60 AS varchar) + ':'  + right('0' + CAST(CAST(OT.TierStart AS int) % 60 AS varchar(2)),2) + ':00' AS TIME) -- Time Start. Convert from numeric to time
																					AND 
																					CAST(CAST( CAST(OT.TierEnd AS int) / 60 AS varchar) + ':'  + right('0' + CAST(CAST(OT.TierEnd AS int) % 60 AS varchar(2)),2) + ':59' AS TIME) -- Time End. Convert from numeric to time
							LEFT JOIN IDB_Billing.dbo.wActiveBranch AB (NOLOCK) ON AB.PROCESS_ID = @ProcessID
																				AND AB.BILLING_CODE = @BillCode
																				AND AB.BRANCH_ID = IFD.BranchId
							WHERE	IFD.DEAL_TRADE_DATE BETWEEN @LoopStartDate AND @LoopEndDate
							AND		IFD.ProductGroup = @product
							AND		IFD.DEAL_LAST_VERSION = 1 -- IDBBC-330 Adding missing condition
							AND		IFD.DEAL_STATUS  <> 'CANCELLED' -- IDBBC-330 Adding missing condition
							AND		IFD.Source = @Src -- IDBBC-218 We do not need to include DWCHS trades in these calculations
							AND		(
										IFD.Dealer = @dlr
										OR
										IFD.Dealer IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(@Child_Dealers,',')) -- Include volume for all children
										OR
										IFD.Dealer IN (SELECT DW_FIRM_CODE FROM IDB_Reporting.dbo.DWC_DW_FIRM_MAPPING_TABLE (NOLOCK) WHERE DW_ACRONYM = @dlr) -- IDBBC-210 Pick up trades from matched dealer. Updated to IN dealer list as we could have more than one mapping
										OR 
										IFD.Dealer IN (SELECT DW_FIRM_CODE FROM IDB_Reporting.dbo.DWC_DW_FIRM_MAPPING_TABLE (NOLOCK) WHERE DW_ACRONYM IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(@Child_Dealers,','))) -- IDBBC-210 Pick up trades from matched child dealer
									)
							AND		(@IsActiveStream IS NULL OR IFD.IsActiveStream = @IsActiveStream) -- IDBBC-140
							AND		(@Operator IS NULL OR IFD.Operator = @Operator) -- IDBBC-140
							AND		(@AggrPass IS NULL OR IFD.DEAL_IS_AGRESSIVE = @AggrPass) -- GDB-1628 if schedule uses AggrPass flag then use Deal_is_Aggressive flag to get correct volume for schedule
							AND		(@AnonymousLevel IS NULL OR IFD.AnonymousLevel IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(@AnonymousLevel,',')))
							AND		(@BilateralStream IS NULL OR IFD.BilateralStream = @BilateralStream)
							AND		(@IsStdTickInc IS NULL OR IFD.IsStdTickInc = @IsStdTickInc)
							--AND		(@TenorStart IS NULL OR (SM.issued_as/12) BETWEEN @TenorStart AND @TenorEnd)
							AND		((@TenorStart IS NULL) OR (IFD.ISSUED_AS BETWEEN @TenorStart AND @TenorEnd))
							AND		(@Child_Dealers IS NOT NULL OR AB.BRANCH_ID IS NOT NULL) -- IDBBC-140 ignore branch id if schedule uses child billing codes

						END
						-- GDB-1628 passing dealer volume to the function to get platform volume for getting correct commission/rebate for % based incremental tiers
						ELSE IF @TierId IS NOT NULL
						BEGIN
							-- IDBBC-263
							-- Creating separate block for EUREPO as EUREPO uses different commission calculation formula.
							-- Getting running total to figure out which trade falls into what tier
							IF (@product = 'EUREPO')
							BEGIN
								SELECT	ifd.DEAL_DATE,
										ifd.DEAL_TRADE_DATE,
										ifd.DEAL_NEGOTIATION_ID,
										ifd.DEAL_ID,
										ifd.DEAL_QUANTITY,
										DEAL_DAYS_TO_MATURITY = DATEDIFF(d,ifd.DEAL_REPO_START_DATE,ifd.DEAL_REPO_END_DATE),
										TermAdjustedVol = ifd.DEAL_QUANTITY * DATEDIFF(d,ifd.DEAL_REPO_START_DATE,ifd.DEAL_REPO_END_DATE),
										ifd.DEAL_IS_AGRESSIVE,
										RunningTotal = SUM(ifd.DEAL_QUANTITY * DATEDIFF(d,ifd.DEAL_REPO_START_DATE,ifd.DEAL_REPO_END_DATE)) OVER (ORDER BY ifd.DEAL_TRADE_DATE, ifd.DEAL_NEGOTIATION_ID, ifd.DEAL_ID), -- IDBBC-401 Adding Deal_ID as we charge commission on multiple legs of EUSWAP trades.
										PrevRunningTotal = ISNULL(SUM(ifd.DEAL_QUANTITY * DATEDIFF(d,ifd.DEAL_REPO_START_DATE,ifd.DEAL_REPO_END_DATE)) OVER (ORDER BY ifd.DEAL_TRADE_DATE, ifd.DEAL_NEGOTIATION_ID, ifd.DEAL_ID ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) -- ISNULL will set PrevRunningTotal for first trade to 0
								INTO	#IndivisulaEurepoTrades
								FROM	IDB_Reporting.dbo.IDB_FALCON_DEALS ifd (NOLOCK)
								LEFT JOIN	IDB_Billing.dbo.wActiveBranch AB (NOLOCK) ON AB.PROCESS_ID = @ProcessID
																				 AND AB.BRANCH_ID = IFD.BranchId
																				 AND AB.BILLING_CODE = @BillCode
								WHERE	ifd.DEAL_TRADE_DATE BETWEEN @LoopStartDate AND @LoopEndDate -- IDBBC-305
								AND		IFD.ProductGroup = @product
								AND		IFD.DEAL_LAST_VERSION = 1 -- IDBBC-163
								AND		IFD.DEAL_STATUS  <> 'CANCELLED' -- IDBBC-330 -- IDBBC-163
								AND		IFD.Source = @Src -- IDBBC-218 We do not need to include DWCHS trades in these calculations
								AND		(
											IFD.Dealer = @dlr
											OR
											IFD.Dealer IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(@Child_Dealers,',')) -- Include volume for all children
											OR
											IFD.Dealer IN (SELECT DW_FIRM_CODE FROM IDB_Reporting.dbo.DWC_DW_FIRM_MAPPING_TABLE (NOLOCK) WHERE DW_ACRONYM = @dlr) -- IDBBC-210 Pick up trades from matched dealer. Updated to IN dealer list as we could have more than one mapping
											OR 
											IFD.Dealer IN (SELECT DW_FIRM_CODE FROM IDB_Reporting.dbo.DWC_DW_FIRM_MAPPING_TABLE (NOLOCK) WHERE DW_ACRONYM IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(@Child_Dealers,','))) -- IDBBC-210 Pick up trades from matched child dealer
										)
								-- IDBBC-347 for EUREPOSPRD trades we need to charge commission and fees on the legs
							-- IDBBC-401 Adding EUREPOSPRDGC to below condition
								AND		(CASE WHEN ifd.TradeType2 IN ('EUREPOSPRD','EUREPOSPRDGC') AND ifd.DEAL_LEG = 0 THEN 0 ELSE 1 END) = 1
								-- IDBBC-260 For EUREPO GC trades only Take allocation volume
								AND		(ifd.SWSecType <> 'EUREPOGC' OR (ifd.SWSecType = 'EUREPOGC' AND ifd.MessageType = 46))
								AND		(@IsActiveStream IS NULL OR IFD.IsActiveStream = @IsActiveStream) -- IDBBC-140
								AND		(@Operator IS NULL OR IFD.Operator = @Operator) -- IDBBC-140
								AND		(@AggrPass IS NULL OR IFD.DEAL_IS_AGRESSIVE = @AggrPass) -- GDB-1628 if schedule uses AggrPass flag then use Deal_is_Aggressive flag to get correct volume for schedule
								AND		(@AnonymousLevel IS NULL OR IFD.AnonymousLevel IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(@AnonymousLevel,',')))
								AND		(@BilateralStream IS NULL OR IFD.BilateralStream = @BilateralStream)
								AND		(@IsStdTickInc IS NULL OR IFD.IsStdTickInc = @IsStdTickInc)
								AND		(@Child_Dealers IS NOT NULL OR AB.BRANCH_ID IS NOT NULL) -- IDBBC-140 Ignore branch id when schedule uses child billing codes
								AND		(@TenorStart IS NULL OR (IFD.issued_as/12) BETWEEN @TenorStart AND @TenorEnd) -- IDBBC-163 Adding missing tenor check condition
								AND		(@Security_Currency IS NULL OR IFD.Security_Currency = @Security_Currency) -- IDBBC-260

								IF @Debug = 1
									SELECT '#IndivisulaEurepoTrades', * FROM #IndivisulaEurepoTrades ORDER BY DEAL_TRADE_DATE, DEAL_NEGOTIATION_ID, DEAL_ID

								SELECT	it.Deal_Date,
										it.DEAL_TRADE_DATE,
										it.DEAL_NEGOTIATION_ID,
										it.DEAL_ID,
										it.DEAL_QUANTITY,
										it.TermAdjustedVol,
										it.DEAL_DAYS_TO_MATURITY,
										it.DEAL_IS_AGRESSIVE,
										it.RunningTotal,
										it.PrevRunningTotal,
										ot.TierStart,
										ot.TierEnd,
										ot.AggrRate,
										ot.PassRate,
										QTY_TO_USE = CASE 
															-- DONDITIONS FOR REVERSED Trades.  Reversed trade will have -ve quantity.  Logic is exact opposite of that of logic for regular trade below

															-- Below condition will only happen when first trade is reversed and volume is greater than first tier volume
															WHEN it.DEAL_QUANTITY < 0 AND it.PrevRunningTotal > ot.TierEnd AND it.RunningTotal = 0 THEN (ot.TierEnd - ot.TierStart) * -1
															-- Capture volume for middle tier if term adjusted volume spans more than 2 tiers
															WHEN it.DEAL_QUANTITY < 0 AND it.PrevRunningTotal > ot.TierEnd AND it.RunningTotal < ot.TierStart THEN (ot.TierEnd - ot.TierStart + 1) * -1
															WHEN it.DEAL_QUANTITY < 0 AND it.PrevRunningTotal > ot.TierEnd THEN (ot.TierEnd - it.RunningTotal) * -1
															WHEN it.DEAL_QUANTITY < 0 AND it.RunningTotal < ot.TierStart THEN (it.PrevRunningTotal - ot.TierStart + 1) * -1

															-- CONDITIONS FOR REGULAR trades.  Regular trades will have +ve quantity

															-- when volume for first trade is greater than first tier then take first tier volume.  Do not add extra 1 in this case as first tier starts at 0
															WHEN it.RunningTotal > ot.TierEnd AND it.PrevRunningTotal = 0 THEN ot.TierEnd - ot.TierStart
															-- IDBBC-336 when a trade volume spans more than 2 tiers, then we need to make sure we capture volume for middle tier. 
															WHEN it.RunningTotal > ot.TierEnd AND it.PrevRunningTotal < ot.TierStart THEN ot.TierEnd - ot.TierStart + 1
															WHEN it.RunningTotal > ot.TierEnd THEN ot.TierEnd - it.PrevRunningTotal
															WHEN it.PrevRunningTotal < ot.TierStart THEN it.RunningTotal - ot.TierStart + 1

															-- if none of the above conditions match then take Term Adjusted Volume as is. This means this trade is not crossing any tier boundries
															ELSE it.TermAdjustedVol
													 END,
										Commission = CAST(NULL AS DECIMAL(10,2))
								INTO	#RepoComm
								FROM	#IndivisulaEurepoTrades it
								JOIN	IDB_Billing.dbo.OverrideTiers ot (NOLOCK) ON it.RunningTotal BETWEEN ot.TierStart AND ot.TierEnd
																				  OR it.PrevRunningTotal BETWEEN ot.TierStart AND ot.TierEnd
																				  OR (it.RunningTotal > ot.TierEnd AND it.PrevRunningTotal < ot.TierStart) -- IDBBC-336
																				  -- below condition is for REVERSED trade.  When reversed trade spans more than 2 tiers then below condition will capture middle tiers
																				  OR (it.DEAL_QUANTITY < 0 AND it.PrevRunningTotal > ot.TierStart AND it.RunningTotal < ot.TierEnd)
								WHERE	ot.TierId = @TierId


								-- Calculate commission
								-- IDBBC-310 QTY_TO_USE is TermAdjustedVolume.  So removing Deal_Days_To_Maturity from formula below
								UPDATE		r
								SET			Commission = ((QTY_TO_USE * (CASE WHEN DEAL_IS_AGRESSIVE = 1 THEN AggrRate ELSE PassRate END) * 10000) / (CASE WHEN @Security_Currency = 'GBP' THEN 365 ELSE 360 END)) * ISNULL(EX.Rate, 1.0)
								FROM		#RepoComm r
								LEFT JOIN	IDB_Reporting.dbo.ExchangeRate EX ON @LoopStartDate BETWEEN EX.FromDate AND EX.ToDate
																			AND EX.FromCurrency = @Security_Currency
																			AND EX.ToCurrency = 'USD' -- IDBBC-310


								SELECT	@Amount = SUM(COMMISSION)	FROM	#RepoComm

								IF @Debug = 1
									SELECT	'#RepoComm', * FROM	#RepoComm ORDER BY DEAL_TRADE_DATE, DEAL_NEGOTIATION_ID, DEAL_ID, AggrRate DESC

								INSERT INTO #EUREPOOverrideCommission
								(
								    Deal_Trade_Date,
								    Deal_Id,
								    AggrRate,
								    PassRate,
								    OverrideCommission
								)
								SELECT	DEAL_TRADE_DATE
										,DEAL_ID
										,AggrRate = AVG(AggrRate)
										,PassRate = AVG(PassRate)
										,Commission = SUM(Commission)
								FROM	#RepoComm 
								WHERE	DEAL_TRADE_DATE = @date1
								GROUP BY
										DEAL_TRADE_DATE
										,DEAL_ID


								DROP TABLE #IndivisulaEurepoTrades, #RepoComm
							END
							ELSE IF @TierFunction = 'IncrementalRunningTotal' -- This option will use running total to apply appropriate rate to each trade
							BEGIN
								-- Get running total
								SELECT	ifd.DEAL_DATE,
										ifd.DEAL_ID,
										ifd.DEAL_QUANTITY,
										ifd.DEAL_IS_AGRESSIVE,
										RunningTotal = SUM(DEAL_QUANTITY) OVER (ORDER BY ifd.DEAL_DATE, ifd.DEAL_ID),
										PrevRunningTotal = ISNULL(SUM(ifd.DEAL_QUANTITY) OVER (ORDER BY ifd.DEAL_DATE, ifd.DEAL_ID ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) -- Previous Running Total for first trade will be 0
								INTO	#IndivisulaTrades
								FROM	IDB_Reporting.dbo.IDB_FALCON_DEALS ifd (NOLOCK)
								LEFT JOIN	IDB_Billing.dbo.wActiveBranch AB (NOLOCK) ON AB.PROCESS_ID = @ProcessID
																				 AND AB.BRANCH_ID = IFD.BranchId
																				 AND AB.BILLING_CODE = @BillCode
								WHERE	ifd.DEAL_TRADE_DATE BETWEEN @LoopStartDate AND @LoopEndDate
								AND		IFD.ProductGroup = @product
								AND		IFD.DEAL_LAST_VERSION = 1 -- IDBBC-163
								AND		IFD.DEAL_STATUS  <> 'CANCELLED' -- IDBBC-163
								AND		IFD.Source = @Src -- IDBBC-218 We do not need to include DWCHS trades in these calculations
								AND		(
											IFD.Dealer = @dlr
											OR
											IFD.Dealer IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(@Child_Dealers,',')) -- Include volume for all children
											OR
											IFD.Dealer IN (SELECT DW_FIRM_CODE FROM IDB_Reporting.dbo.DWC_DW_FIRM_MAPPING_TABLE (NOLOCK) WHERE DW_ACRONYM = @dlr) -- IDBBC-210 Pick up trades from matched dealer. Updated to IN dealer list as we could have more than one mapping
											OR 
											IFD.Dealer IN (SELECT DW_FIRM_CODE FROM IDB_Reporting.dbo.DWC_DW_FIRM_MAPPING_TABLE (NOLOCK) WHERE DW_ACRONYM IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(@Child_Dealers,','))) -- IDBBC-210 Pick up trades from matched child dealer
										)
								AND		(@IsActiveStream IS NULL OR IFD.IsActiveStream = @IsActiveStream) -- IDBBC-140
								AND		(@Operator IS NULL OR IFD.Operator = @Operator) -- IDBBC-140
								AND		(@AggrPass IS NULL OR IFD.DEAL_IS_AGRESSIVE = @AggrPass) -- GDB-1628 if schedule uses AggrPass flag then use Deal_is_Aggressive flag to get correct volume for schedule
								AND		(@AnonymousLevel IS NULL OR IFD.AnonymousLevel IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(@AnonymousLevel,',')))
								AND		(@BilateralStream IS NULL OR IFD.BilateralStream = @BilateralStream)
								AND		(@IsStdTickInc IS NULL OR IFD.IsStdTickInc = @IsStdTickInc)
								AND		(@Child_Dealers IS NOT NULL OR AB.BRANCH_ID IS NOT NULL) -- IDBBC-140 Ignore branch id when schedule uses child billing codes
								AND		(@TenorStart IS NULL OR (IFD.issued_as/12) BETWEEN @TenorStart AND @TenorEnd) -- IDBBC-163 Adding missing tenor check condition
								AND		(@Security_Currency IS NULL OR IFD.Security_Currency = @Security_Currency) -- IDBBC-260
								ORDER BY ifd.DEAL_DATE, ifd.DEAL_ID

								-- if a particular deal quantity falls over two tiers then take appropriate quantity that falls in each tier
								SELECT	it.Deal_Date,
										it.DEAL_ID,
										it.DEAL_QUANTITY,
										it.DEAL_IS_AGRESSIVE,
										it.RunningTotal,
										it.PrevRunningTotal,
										ot.AggrRate,
										ot.PassRate,
										QTY_TO_USE = CASE 
															-- when first trade volume is more than first tier range
															WHEN it.RunningTotal > ot.TierEnd AND it.PrevRunningTotal = 0 THEN ot.TierEnd - ot.TierStart
															-- IDBBC-336 when a trade volume spans more than 2 tiers, then we need to make sure we capture volume for middle tier
															WHEN it.RunningTotal > ot.TierEnd AND it.PrevRunningTotal < ot.TierStart THEN ot.TierEnd - ot.TierStart + 1 -- IDBBC-336
															WHEN it.RunningTotal > ot.TierEnd THEN ot.TierEnd - it.PrevRunningTotal
															WHEN it.PrevRunningTotal < ot.TierStart THEN it.RunningTotal - ot.TierStart + 1
															ELSE it.DEAL_QUANTITY
													 END,
										Commission = CAST(NULL AS DECIMAL(10,2))
								INTO	#Comm
								FROM	#IndivisulaTrades it
								JOIN	IDB_Billing.dbo.OverrideTiers ot (NOLOCK) ON it.RunningTotal BETWEEN ot.TierStart AND ot.TierEnd
																				  OR it.PrevRunningTotal BETWEEN ot.TierStart AND ot.TierEnd
																				  OR (it.RunningTotal > ot.TierEnd AND it.PrevRunningTotal < ot.TierStart) -- IDBBC-336
								WHERE	ot.TierId = @TierId


								-- Calculate commission by applying appropriate rate to each trade
								UPDATE  #Comm
								SET		Commission = QTY_TO_USE * (CASE WHEN DEAL_IS_AGRESSIVE = 1 THEN AggrRate ELSE PassRate END)

								SELECT	@Amount = SUM(COMMISSION) 
								FROM	#Comm

								IF @Debug = 1
									SELECT	'#Comm', * FROM	#Comm ORDER BY DEAL_DATE

								DROP TABLE #IndivisulaTrades, #Comm

							END
							ELSE
								-- IDBBC-196 Pass BusinessDaysMTD to calculate override fixed fees till current date
								SELECT @Amount = IDB_Reporting.dbo.fnCalculateVolumeBasedCommissionORRebate(@TierId,@ValueForTierMatch,@DlrVolume,@TierFunction,@ScheduleType,@SchFloor,@SchCap,@BusinessDaysForAvg, @BusinessDaysInPeriod, @BusinessDaysMTD)
						END
						ELSE
						-- GDB-1628 If schedule does not use tiers then this is a rebate schedule that uses -ve rates.  In this case we need to calculate rebate amount using 
						--			IDB_Falcon_Deals and rates set up on schedule.  If Applicable apply cap/floor to the amount.  
						--			Cap/Floor is always set as +ve amount so we need to take absolute amount and then apply cap/floor.  Then apply correct sign according to type of schedule. R = Rebate (-ve), C = Commission (+ve)
						BEGIN
							
							IF @SchFloor = @SchCap
								SET @Amount = @SchCap * (CASE @ScheduleType WHEN 'R' THEN -1 ELSE 1 END)
							ELSE
							BEGIN
								SELECT	@Amount = SUM(IFD.DEAL_QUANTITY * (CASE WHEN IFD.DEAL_IS_AGRESSIVE = 1 THEN @AggrChargeRate ELSE @PassChargeRate END))
								FROM	IDB_Reporting.dbo.IDB_FALCON_DEALS IFD (NOLOCK)
								-- IDBBC-140 join with active branch to match branch id in case there are multiple billing codes with same dealer code
								LEFT JOIN IDB_Billing.dbo.wActiveBranch AB (NOLOCK) ON AB.PROCESS_ID = @ProcessID
																					AND AB.BILLING_CODE = @BillCode
																					AND AB.BRANCH_ID = IFD.BranchId
								--JOIN	Instrument.dbo.Security_Master SM (NOLOCK) ON SM.instrid = IFD.DEAL_SECURITY_ID
								--JOIN	Instrument.dbo.Security_Type ST (NOLOCK) ON ST.sec_type_id = SM.sec_type_id AND ST.product_grp = @product
								WHERE	IFD.DEAL_TRADE_DATE BETWEEN @LoopStartDate AND @LoopEndDate
								AND		IFD.ProductGroup = @product
								AND		IFD.DEAL_LAST_VERSION = 1 -- IDBBC-330 Adding missing condition
								AND		IFD.DEAL_STATUS  <> 'CANCELLED' -- IDBBC-330 Adding missing condition
								AND		IFD.Source = @Src -- IDBBC-218 We do not need to include DWCHS trades in these calculations
								AND		(
											IFD.Dealer = @dlr
											OR
											IFD.Dealer IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(@Child_Dealers,',')) -- Include volume for all children
											OR
											IFD.Dealer IN (SELECT DW_FIRM_CODE FROM IDB_Reporting.dbo.DWC_DW_FIRM_MAPPING_TABLE (NOLOCK) WHERE DW_ACRONYM = @dlr) -- IDBBC-210 Pick up trades from matched dealer. Updated to IN dealer list as we could have more than one mapping
											OR 
											IFD.Dealer IN (SELECT DW_FIRM_CODE FROM IDB_Reporting.dbo.DWC_DW_FIRM_MAPPING_TABLE (NOLOCK) WHERE DW_ACRONYM IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(@Child_Dealers,','))) -- IDBBC-210 Pick up trades from matched child dealer
										)
								AND		(@IsActiveStream IS NULL OR IFD.IsActiveStream = @IsActiveStream) -- IDBBC-140
								AND		(@Operator IS NULL OR IFD.Operator = @Operator) -- IDBBC-140
								AND		(@AggrPass IS NULL OR IFD.DEAL_IS_AGRESSIVE = @AggrPass) -- GDB-1628 if schedule uses AggrPass flag then use Deal_is_Aggressive flag to get correct volume for schedule
								AND		(@AnonymousLevel IS NULL OR IFD.AnonymousLevel IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(@AnonymousLevel,',')))
								AND		(@BilateralStream IS NULL OR IFD.BilateralStream = @BilateralStream)
								AND		(@IsStdTickInc IS NULL OR IFD.IsStdTickInc = @IsStdTickInc)
								AND		(@TenorStart IS NULL OR (IFD.issued_as/12) BETWEEN @TenorStart AND @TenorEnd)
								AND		(@Child_Dealers IS NOT NULL OR AB.BRANCH_ID IS NOT NULL) -- IDBBC-140 ignore branch id if schedule uses child billing codes


								IF @SchFloor <> 0 AND ABS(@Amount) < @SchFloor -- If amount less than floor then apply floor
									SET @Amount = @SchFloor * (CASE @ScheduleType WHEN 'R' THEN -1 ELSE 1 END)
								ELSE IF @SchCap <> -1 AND ABS(@Amount) > @SchCap -- if amount greater than cap then apply cap
									SET @Amount = @SchCap * (CASE @ScheduleType WHEN 'R' THEN -1 ELSE 1 END)
							END
						END

						IF @Debug = 1
							SELECT ScheduleId = @ScheduleId, Amount = @Amount

						-- Insert/Update rebate amount in rebate tracker table only when @ReportMode is 1
						IF @ReportMode = 1
						BEGIN
							-- Delete previous rebate records for the period
							-- IDBBC-119 Use schedule ID from calculated rebate/commission temp table to delete existing records from RebateAndBillingTracker 
							-- as there could be more than one schedules for a dealer that calculate rebate/commission.  SO we don't want to delete all existing records
							-- GDB-1628 When ApplyToMonth is set to N (Next Month) then use next period start and end dates
							DELETE	R
							FROM	IDB_Billing.dbo.RebateAndBillingTracker R (NOLOCK)
							WHERE	Billing_Code = @BillCode
							AND		ProductGroup = @product -- IDBBC-140 Removing hard coding for OTR
							AND		EffectiveStartDate = CASE WHEN @ApplyToMonth = 'N' THEN @NextPeriodStartDate ELSE @PeriodStartDate END -- Rebate and calculated commission is stored for the month. So we need to use Period Start and End Dates here
							AND		EffectiveEndDate = CASE WHEN @ApplyToMonth = 'N' THEN @NextPeriodEndDate ELSE @PeriodEndDate END
							AND		R.ScheduleId = @ScheduleId
							--AND		(@IsActiveStream IS NULL OR IsActiveStream = @IsActiveStream) -- IDBBC-140
							--AND		(@Operator IS NULL OR Operator = @Operator) -- IDBBC-140
							--AND		ApplyToInvoice = CASE @ScheduleType WHEN 'R' THEN 0 ELSE 1 END
							--AND		(@PP_Group IS NULL OR R.PoolParticipant = @PP_Group)
							--AND		(@AggrPass IS NULL OR R.AggrPass = @AggrPass) -- GDB-1628  Adding condition to match AggrPass flag 
								
							-- Insert current rebate amounts in rebate tracker
							INSERT INTO IDB_Billing.dbo.RebateAndBillingTracker  (Billing_Code, ProductGroup, BalanceLastUpdate, BalanceLastUpdateBy, EffectiveStartDate, EffectiveEndDate, RebateAmount, ScheduleId, ApplyToInvoice, PoolParticipant, IsActiveStream, Operator, AggrPass) -- IDBBC-71
							SELECT	@BillCode, 
									@product, 
									GETDATE(), 
									'BlgUser', 
									EffectiveStartDate = CASE WHEN @ApplyToMonth = 'N' THEN @NextPeriodStartDate ELSE @PeriodStartDate END, -- GDB-1628 Use next period start date when apply to month is set to N
									EffectiveEndDate = CASE WHEN @ApplyToMonth = 'N' THEN @NextPeriodEndDate ELSE @PeriodEndDate END, -- GDB-1628 Use next period end date when apply to month is set to N
									@Amount, 
									@ScheduleId, 
									--IDBBC-7 When amount is a rebate then do not apply directly to invoice.  This will appear as a separate line item on invoice
									-- IDBBC-49 When Amount is -ve then its a rebate. WHen +ve its a commission.  WHen amount is commission we want to apply this amount to invoice
									ApplyToInvoice = CASE @ScheduleType WHEN 'R' THEN 0 ELSE 1 END,  
									@PP_Group, 
									@IsActiveStream,
									@Operator,
									@AggrPass  -- GDB-1628  Adding condition to match AggrPass flag
							WHERE	@Amount IS NOT NULL -- IDBBC-119 do not enter a rebate record there is no rebate/commission calculated

						END -- END @ReportMode = 1
						ELSE
							IF @Debug = 1
								SELECT	@BillCode, @ScheduleId, @ResultType, @Amount, @IsActiveStream, @ScheduleType, @PP_Group, @AggrPass,
										EffectiveStartDate = CASE WHEN @ApplyToMonth = 'N' THEN @NextPeriodStartDate ELSE @PeriodStartDate END,
										EffectiveEndDate = CASE WHEN @ApplyToMonth = 'N' THEN @NextPeriodEndDate ELSE @PeriodEndDate END

					END -- END @ResultType = AMOUNT

					/**** END ResultType Loop ****/

				END
				ELSE -- Threshold not met.  We need to do a clean up if threshold was met previously
				BEGIN
					
					IF @ReportMode = 1
					BEGIN
						IF @Debug = 1
							SELECT 'Threshold not met running clean-up'						
						
						IF @ResultType = 'Rate'
						BEGIN
							
							-- IDBBC-140 if current schedule had previously met threshold but does not meet threshold now then we need to update rate stored
							--			 in OTRVolumeBasedCommissionRate to NULL and set backfill needed to 1 so dafault rates are applied
							UPDATE	IDB_Billing.dbo.VolumeBasedCommissionRate
							SET		AggrRate = NULL,
									PassRate = NULL,
									LastUpdated = GETDATE(),
									BackfillNeeded = 1
							WHERE	BillingCode IN (SELECT Billing_Code FROM #OverrideSchedules WHERE ScheduleId = @ScheduleId OR ParentScheduleId = @ScheduleId AND ProcessId = @ProcessID) -- Need to update  rate for children as well
							AND		ProductGroup = @Product
							AND		(@IsActiveStream IS NULL OR IsActiveStream = @IsActiveStream) -- IDBBC-26 -- IDBBC-140
							AND		(@Operator IS NULL OR Operator = @Operator) -- IDBBC-26 -- IDBBC-140
							AND		(@AggrPass IS NULL OR AggrPass = @AggrPass) -- GDB-1628 Check if schedule uses AggrPass flag
							AND		PeriodStartDate = @LoopStartDate -- IDBBC-94
							-- GDB-1628 for Period End Date we need to use Effective end date if that is smaller than Period End Date.  Else use perio end date
							-- we can not use @LoopEndDate as for ADV schedules @LoopEndDate changes everyday
							AND		PeriodEndDate = CASE WHEN @EffectiveEndDate < @PeriodEndDate THEN @EffectiveEndDate ELSE @PeriodEndDate END
							AND		(@Security_Currency IS NULL OR Security_Currency = @Security_Currency) -- IDBBC-260


						END
						ELSE IF @ResultType = 'Amount'
						BEGIN

							-- IDBBC-140 if current schedule had previously met threshold but does not meet threshold now then we need to clean up any records inserted in RebateAndBillingTracker
							DELETE	R
							FROM	IDB_Billing.dbo.RebateAndBillingTracker R (NOLOCK)
							WHERE	Billing_Code = @BillCode
							AND		ProductGroup = @product -- IDBBC-140 Removing hard coding for OTR
							AND		EffectiveStartDate = CASE WHEN @ApplyToMonth = 'N' THEN @NextPeriodStartDate ELSE @PeriodStartDate END -- Rebate and calculated commission is stored for the month. So we need to use Period Start and End Dates here
							AND		EffectiveEndDate = CASE WHEN @ApplyToMonth = 'N' THEN @NextPeriodEndDate ELSE @PeriodEndDate END
							AND		R.ScheduleId = @ScheduleId
							--AND		(@IsActiveStream IS NULL OR IsActiveStream = @IsActiveStream) -- IDBBC-140
							--AND		(@Operator IS NULL OR Operator = @Operator) -- IDBBC-140
							--AND		ApplyToInvoice = CASE @ScheduleType WHEN 'R' THEN 0 ELSE 1 END
							--AND		(@PP_Group IS NULL OR R.PoolParticipant = @PP_Group)
							--AND		(@AggrPass IS NULL OR R.AggrPass = @AggrPass) -- GDB-1628  Adding condition to match AggrPass flag
						END

					END
				END
				/***** END Threshold Loop *****/

				SET @Row = @Row + 1
			END
		END

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After Processing volume based Loop ',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2


		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'Before Match Override with Deals.',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		-- Override Schedule Deal Billing Schedule
		-- Match Override schedules with Deals and assign weight.  Rank schedules by their weight and take only records with rank 1
		-- IDBBC-268 Converting #OverrideScheduleDealBillingSchedule into IDB_Billing.dbo.wOverrideScheduleDealBillingSchedule
		INSERT INTO IDB_Billing.dbo.wOverrideScheduleDealBillingSchedule
		(
			PROCESS_ID,
			ScheduleId,
			Billing_Code,
			Dealer,
			Product,
			Source,
			Deal_Trade_Date,
			Deal_Negotiation_Id,
			Deal_Id,
			IsActiveStream, -- GDB-1628
			Operator, -- GDB-1628
			DEAL_IS_AGGRESSIVE, -- GDB-1628
			[Floor], -- GDB-1628
			Cap, -- GDB-1628
			Deal_Quantity,
			AggrChargeRate,
			PassChargeRate,
			TierID,
			Tenor, -- GDB-1628
			Security_Currency
		)
		SELECT	@ProcessID,
				ODBS_Ranked.ScheduleID,
				ODBS_Ranked.Billing_Code,
				ODBS_Ranked.Dealer,
				ODBS_Ranked.Product,
				ODBS_Ranked.Source,
				ODBS_Ranked.DEAL_TRADE_DATE,
				ODBS_Ranked.DEAL_NEGOTIATION_ID,
				ODBS_Ranked.DEAL_ID,
				ODBS_Ranked.IsActiveStream, -- GDB-1628
				ODBS_Ranked.Operator, -- GDB-1628
				ODBS_Ranked.DEAL_IS_AGRESSIVE, -- GDB-1628
				ODBS_Ranked.[Floor], -- GDB-1628
				ODBS_Ranked.Cap, -- GDB-1628
				ODBS_Ranked.DEAL_QUANTITY,
				AggrChargeRate = ISNULL(ODBS_Ranked.AggrChargeRate,0.0),
				PassChargeRate = ISNULL(ODBS_Ranked.PassChargeRate,0.0),
				ODBS_Ranked.TierId,
				ODBS_Ranked.Tenor, -- GDB-1628
				ODBS_Ranked.Security_Currency -- IDBBC-260
		FROM	(
					SELECT	ODBS.*,
							SchRank = ROW_NUMBER() OVER (PARTITION BY ODBS.Billing_Code, ODBS.Product, ODBS.Source, ODBS.DEAL_ID ORDER BY ODBS.[Weight] DESC)
					FROM	(
		
								SELECT	S.ScheduleId,
										D.BILLING_CODE, --ABC.BILLING_CODE,
										D.Dealer,
										Product = D.ProductGroup,
										D.Source,
										DEAL_TRADE_DATE = D.TradeDate, -- D.DEAL_TRADE_DATE,
										D.DEAL_NEGOTIATION_ID,
										D.DEAL_ID,
										D.IsActiveStream, -- GDB-1628
										D.Operator, -- GDB-1628
										D.DEAL_IS_AGRESSIVE, -- GDB-1628
										D.AnonymousLevel,
										D.BilateralStream,
										D.IsStdTickInc,
										S.[Floor],
										S.Cap,
										DEAL_QUANTITY = D.Quantity,
										S.AggrChargeRate,
										S.PassChargeRate,
										S.TierId,
										Tenor = D.issued_as/12, -- GDb-1628
										D.Security_Currency, -- IDBBC-260
										[Weight] =	CASE WHEN S.AnonymousLevel IS NULL THEN 0.0 ELSE 1.0 END +
													CASE WHEN S.PoolTakers IS NULL THEN 0.0 ELSE 0.9 END +
													CASE WHEN S.Dealer = 'All' THEN 0.0 ELSE 0.8 END +
													CASE WHEN S.BilateralStream IS NULL THEN 0.0 ELSE 0.7 END +
													CASE WHEN S.IsStdTickInc IS NULL THEN 0.0 ELSE 0.6 END +
													CASE WHEN S.AggrPass IS NULL THEN 0.0 ELSE 0.5 END + -- GDB-1628 Schedule using AggrPass flag
													CASE WHEN S.TenorStart IS NULL THEN 0.0 ELSE 0.4 END + -- GDb-1628 Schedule using tenor
													CASE WHEN S.Security_Currency IS NULL THEN 0.0 ELSE 0.3 END -- IDBBC-260


								FROM	IDB_Billing.dbo.wDeals_AllProd D (NOLOCK)
								JOIN	#OverrideSchedules S (NOLOCK) ON D.TradeDate BETWEEN S.EffectiveStartDate AND S.EffectiveEndDate
																		AND D.PROCESS_ID = S.ProcessId
																		AND S.Product = D.ProductGroup
																		AND (CASE	WHEN S.Product <> 'MATCH' AND S.Source = D.Deal_Source THEN 1 
																					WHEN S.Product = 'MATCH' AND S.Source = D.Source THEN 1
																					ELSE 0
																			 END
																			) = 1
																		AND (
																				CASE WHEN S.Product <> 'MATCH' THEN 1
																					 WHEN S.Product = 'MATCH' AND S.ScheduleType = 'C' AND D.Deal_Source = 'MATCH' THEN 1
																					 WHEN S.Product = 'MATCH' AND S.ScheduleType = 'F' AND D.Deal_Source = 'DIRECT' THEN 1
																					 ELSE 0
																				END
																			) = 1																		
																		AND (
																				S.Dealer = 'All' 
																				OR 
																				S.Dealer = D.Dealer 
																			)
																		AND (S.IsActiveStream IS NULL OR S.IsActiveStream = D.IsActiveStream) -- IDBBC-140
																		AND (S.Operator IS NULL OR S.Operator = D.Operator) -- IDBBC-140
																		AND (S.AggrPass IS NULL OR S.AggrPass = D.DEAL_IS_AGRESSIVE) -- GDB-1628
																		AND (S.AnonymousLevel IS NULL OR D.AnonymousLevel IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(S.AnonymousLevel,',')))
																		AND (S.BilateralStream IS NULL OR S.BilateralStream = D.BilateralStream)
																		AND (S.IsStdTickInc IS NULL OR S.IsStdTickInc = D.IsStdTickInc)
																		AND (S.TenorStart IS NULL OR D.issued_as/12 BETWEEN S.TenorStart AND S.TenorEnd) -- Match Tenor if schedule uses specific tenor
																		AND S.ThresholdMet = 1 -- Only apply schedules where threshold is met.  Schedules that do not use threshold will have threshold met set to 1
																		--AND (S.TimeStart IS NULL OR CAST(D.DEAL_DATE AS TIME) BETWEEN S.TimeStart AND S.TimeEnd) -- GDb-1628 If schedule uses Time tiers then match trade time with time window to get rate
																		AND (S.Security_Currency IS NULL OR D.Security_Currency = S.Security_Currency) -- IDBBC-260

								LEFT JOIN IDB_Billing.dbo.wPoolTakerTradesVsOperators PTTVO ON PTTVO.PROCESS_ID = D.PROCESS_ID
																							AND	PTTVO.DEAL_TRADE_DATE = D.TradeDate --D.DEAL_TRADE_DATE
																							AND PTTVO.DEAL_NEGOTIATION_ID = D.DEAL_NEGOTIATION_ID
																							AND PTTVO.Dealer = D.Dealer
																							AND PTTVO.IsActiveStream = D.IsActiveStream
																							AND PTTVO.Operator = D.Operator
								WHERE	D.Process_Id = @ProcessId
								AND		D.ProductGroup IN (SELECT DISTINCT Product FROM #OverrideSchedules)
								AND		S.ScheduleType <> 'R' -- Do not include rebate schedules in this as they use -ve rates and rebate amounts are not stored on individual trades but stored as single amount for the month
								AND		(S.PoolTakers IS NULL OR PTTVO.CounterParty IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(S.PoolTakers,',')))
								AND		(
											S.PoolParticipants IS NULL -- Ignore PoolParticipants
											OR
											(S.IncludePP = 1 AND PTTVO.CounterParty IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(S.PoolParticipants,','))) -- Include Pool Participants from Schedule
											OR 
											(S.IncludePP = 0 AND PTTVO.CounterParty NOT IN (SELECT * FROM IDB_Codebase.dbo.fnParseString(S.PoolParticipants,','))) -- Exclude Pool Participants from Schedule
										)
							) AS ODBS -- Override Deal Billing Schedule staging
				 
				) AS ODBS_Ranked
			WHERE ODBS_Ranked.SchRank = 1

		--	SET @OvSchRow = @OvSchRow + 1
		--END

		-- Update OverrideScheduleDealBillingSchedule using calculated tier based rate
		UPDATE	OSDBS
		SET		AggrChargeRate = CASE WHEN OSDBS.DEAL_IS_AGGRESSIVE = 1 THEN VBR.AggrRate ELSE 0.0 END,
				PassChargeRate = CASE WHEN OSDBS.DEAL_IS_AGGRESSIVE = 0 THEN VBR.PassRate ELSE 0.0 END
		FROM	IDB_Billing.dbo.wOverrideScheduleDealBillingSchedule OSDBS (NOLOCK)
		JOIN	#OverrideSchedules OS ON OS.ScheduleId = OSDBS.ScheduleId 
									 AND OSDBS.PROCESS_ID = OS.ProcessId
									 AND (OS.TierId IS NOT NULL OR OS.ParentScheduleId IS NOT NULL) -- Only update rates on trades where tier schedule is applied or is a child billing code of another schedule
		JOIN	IDB_Billing.dbo.VolumeBasedCommissionRate VBR (NOLOCK) ON OSDBS.Deal_Trade_Date BETWEEN VBR.PeriodStartDate AND VBR.PeriodEndDate
																		 AND VBR.ProductGroup = OS.Product
																		 AND VBR.BillingCode = OS.Billing_Code
																		 AND ISNULL(VBR.IsActiveStream, 0) = ISNULL(OS.IsActiveStream, 0)
																		 AND ISNULL(VBR.Operator,0) = ISNULL(OS.Operator,0)
																		 AND (VBR.AggrPass IS NULL OR VBR.AggrPass = OSDBS.DEAL_IS_AGGRESSIVE) -- GDB-1628
																		 AND (VBR.AggrRate IS NOT NULL OR VBR.PassRate IS NOT NULL) -- IDBBC-144 only use rate from tabale when its not set to null
																		 AND ISNULL(VBR.Security_Currency,'') = ISNULL(OS.Security_Currency,'')
		WHERE	OSDBS.PROCESS_ID = @ProcessID


		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After Update OVerride Schedule.',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		-- IDBBC-268 Converting #DBS_Staging into IDB_Billing.dbo.wDBS_Staging
		INSERT INTO IDB_Billing.dbo.wDBS_Staging
		(
			PROCESS_ID,
			TradeDate,
			DEAL_DATE,
			Deal_Id,
			ProductGroup,
			BILLING_CODE,
			Leg,
			Source,
			TradeType2,
			SwSecType,
			DEAL_DAYS_TO_MATURITY,
			/* NILESH IOS BILLING */
			DEAL_O_FACTOR,
			/* NILESH OFTR BILLING */
			DEAL_TENOR,
			BILLING_PLAN_ID,
			BILLING_TYPE, 
			INSTRUMENT_TYPE,
			bs_Leg,
			bs_Source,
			MTY_START,
			MTY_END,
			CHARGE_RATE_PASSIVE, 
			CHARGE_RATE_AGGRESSIVE, 
			CHARGE_FLOOR,
			CHARGE_CAP,
			SETTLE_RATE_PASSIVE,
			SETTLE_RATE_AGGRESSIVE,
			EFFECTIVE_DATE, 
			EXPIRATION_DATE,
			OVERWRITE, -- SHIRISH 06/01/2017
			CHARGE_RATE_AGGRESSIVE_OVERRIDE,
			CHARGE_RATE_PASSIVE_OVERRIDE, --SHIRISH 05/21/2019: IDB-18449
			Weight,
			/* NILESH 08/01/2013 -- Tiered Billing */
			TIER_BILLING_PLAN_ID,
			TIER_CHARGE_RATE_AGGRESSIVE,
			TIER_CHARGE_RATE_PASSIVE,
			DEAL_SECURITY_ID,	/* DIT-10159 */
			SRT_Instrument, -- IDBBC-15
			MasterBillingCode, -- IDBBC-26
			MasterDealerCode, -- IDBBC-26
			IsActiveStream, -- IDBBC-26
			Operator, -- IDBBC-26
			AnonymousLevel,	--IDBBC-60
			BilateralStream,  -- IDBBC-71
			IsStdTickInc, -- IDBBC-74
			UseGap, -- IDBBC-132
			DEAL_GAP_DV01 -- IDBBC-132
		)
		SELECT 
			@ProcessID,
			d.TradeDate,
			d.DEAL_DATE,
			d.Deal_Id,
			d.ProductGroup,
			d.BILLING_CODE,
			d.Leg,
			d.Source,
			d.TradeType2,
			d.SwSecType,
			d.DEAL_DAYS_TO_MATURITY,
			/* NILESH IOS BILLING */
			d.DEAL_O_FACTOR,
			/* NILESH OFTR BILLING */
			d.DEAL_TENOR,
			BS1.BILLING_PLAN_ID,
			BS1.BILLING_TYPE, 
			BS1.INSTRUMENT_TYPE,
			bs_Leg = BS1.LEG,
			/* 
			-- IDBBC-302: For trades with HFV source we want to have final result set 
			-- with HFV and not V for distinction between Hybrid (V) and the HFV 
			-- (Govt Desk Trades) 
			*/
			bs_Source = CASE WHEN d.Source <> 'HFV' THEN BS1.SOURCE ELSE d.Source END,
			BS1.MTY_START,
			BS1.MTY_END,
			CHARGE_RATE_PASSIVE = CASE WHEN d.ProductGroup IN ('TRSY','TIPS','USFRN','AMSWP','CAD','EFP','NAVX','BTIC','BOX','REVCON','COMBO') AND d.Leg = 'SEC' AND BS1.LEG<>'SEC' THEN 0.0 ELSE BS1.CHARGE_RATE_PASSIVE END, 
			CHARGE_RATE_AGGRESSIVE = CASE WHEN d.ProductGroup IN ('TRSY','TIPS','USFRN','AMSWP','CAD','EFP','NAVX','BTIC','BOX','REVCON','COMBO') AND d.Leg = 'SEC' AND BS1.LEG<>'SEC' THEN 0.0 ELSE BS1.CHARGE_RATE_AGGRESSIVE END, 
			BS1.CHARGE_FLOOR,
			BS1.CHARGE_CAP,
			SETTLE_RATE_PASSIVE = CASE WHEN d.ProductGroup IN ('TRSY','TIPS','USFRN','AMSWP','CAD','EFP','NAVX','BTIC','BOX','REVCON','COMBO') AND d.Leg = 'SEC' AND BS1.LEG<>'SEC' THEN 0.0 ELSE BS1.SETTLE_RATE_PASSIVE END,
			SETTLE_RATE_AGGRESSIVE = CASE WHEN d.ProductGroup IN ('TRSY','TIPS','USFRN','AMSWP','CAD','EFP','NAVX','BTIC','BOX','REVCON','COMBO') AND d.Leg = 'SEC' AND BS1.LEG<>'SEC' THEN 0.0 ELSE BS1.SETTLE_RATE_AGGRESSIVE END,
			BS1.EFFECTIVE_DATE, 
			BS1.EXPIRATION_DATE,
			BS1.OVERWRITE, -- SHIRISH 06/01/2017
		  
			 -- IDBBC-26 Moved volume based rate to wDealBillingSchedule insert query
			CHARGE_RATE_AGGRESSIVE_OVERRIDE = CASE WHEN d.ProductGroup = 'OTR' THEN BS1.CHARGE_RATE_AGGRESSIVE  WHEN BS1.CHARGE_RATE_AGGRESSIVE > 0 THEN o.Charge_Rate ELSE 0 END, /* NILESH 02/14/2012 - Commission Override Change */
			CHARGE_RATE_PASSIVE_OVERRIDE = CASE WHEN d.ProductGroup = 'OTR' THEN BS1.CHARGE_RATE_PASSIVE  WHEN BS1.CHARGE_RATE_PASSIVE > 0 THEN o.Charge_Rate ELSE 0 END, -- SHIRISH 05/21/2019 IDB-18449
			Weight = (CASE WHEN ISNULL(BS1.SUB_INSTRUMENT_TYPE,'All') = 'All' THEN 0.10 ELSE 1.00 END) -- IDBBC-75 Add new weight level for SUB INSTRUMENT TYPE For ACTIVE_STREAMS schedules
					+ (CASE WHEN ISNULL(BS1.INSTRUMENT,'All') = 'All' THEN 0.20 ELSE 1.00 END)
					+ (CASE WHEN BS1.INSTRUMENT_TYPE = 'All' THEN 0.40 ELSE 1.00 END)
					+ (CASE WHEN BS1.LEG = 'All' THEN 0.60 ELSE 1.00 END)
					+ (CASE WHEN BS1.SOURCE = 'All' THEN 0.80 ELSE 1.00 END),
			
			/* NILESH 08/01/2013 -- Tiered Billing */
			d.TIER_BILLING_PLAN_ID,
			d.TIER_CHARGE_RATE_AGGRESSIVE,
			d.TIER_CHARGE_RATE_PASSIVE,
			d.DEAL_SECURITY_ID,	/* DIT-10159 */
			SRT_Instrument = CASE WHEN ISNULL(SS.acronym,'') IN ('SRT','AIR','ES') THEN 1 ELSE 0 END, -- IDBBC-15 -- IDBBC-125 Adding AIR instruments to the list, -- IDBBC-151 Adding acronym ES
			MasterBillingCode = NULL, -- IDBBC-26 --NOT BEING USED
			MasterDealerCode = NULL, -- IDBBC-26 -- NOT BEING USED
			d.IsActiveStream, -- IDBBC-26
			d.Operator, -- IDBBC-26
			d.AnonymousLevel, --IDBBC-60
			d.BilateralStream, -- IDBBC-71
			d.IsStdTickInc, -- IDBBC-74
			BS1.UseGap, -- IDBBC-132
			d.DEAL_GAP_DV01 -- IDBBC-132

		FROM	IDB_Billing.dbo.wDeals_AllProd d (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table
		/* SHIRISH 04/13/2018: Below function gives us date ranges to use for USREPO based on date logic switch from TradeDate to Repo start date */
		LEFT JOIN IDB_Reporting.dbo.fnREPODateSwitch(@date1,@date2) RDS ON d.ProductGroup = RDS.ProductGroup

		JOIN IDB_Billing.dbo.wBillingSchedule BS1 (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table 
					ON d.BILLING_CODE = BS1.BILLING_CODE AND d.ProductGroup = BS1.PRODUCT_GROUP
					/* SHIRISH 04/13/2018: Updating below condition to use correct date field and date range to match schedule based on REPO date switch from TradeDate to repo start date */
					--AND d.TradeDate BETWEEN CONVERT(Varchar(8), BS1.EFFECTIVE_DATE, 112) AND CONVERT(Varchar(8), BS1.EXPIRATION_DATE, 112)
					AND (CASE 
							---- IDBBC-238 Use Deal_Repo_Start_Date for EUREPO
							--WHEN d.ProductGroup = 'EUREPO'
							--	 AND d.DEAL_REPO_START_DATE BETWEEN CONVERT(Varchar(8), BS1.EFFECTIVE_DATE, 112) AND CONVERT(Varchar(8), BS1.EXPIRATION_DATE, 112)
							--THEN 1
							-- Use Trade Date for all products except USREPO
							WHEN d.ProductGroup <> 'USREPO' 
								 AND d.TradeDate BETWEEN CONVERT(Varchar(8), BS1.EFFECTIVE_DATE, 112) AND CONVERT(Varchar(8), BS1.EXPIRATION_DATE, 112) 
							THEN 1
							-- For USREPO use Trade date for dates before switch date
					        WHEN d.ProductGroup = 'USREPO' AND RDS.FieldName = 'TradeDate' 
								 AND d.TradeDate BETWEEN RDS.StartDate AND RDS.EndDate
								 AND d.TradeDate BETWEEN CONVERT(Varchar(8), BS1.EFFECTIVE_DATE, 112) AND CONVERT(Varchar(8), BS1.EXPIRATION_DATE, 112) 
							THEN 1
							-- For USREPO use start date for dates after switch date
							WHEN d.ProductGroup = 'USREPO' AND RDS.FieldName = 'StartDate' 
								 AND d.DEAL_REPO_START_DATE BETWEEN RDS.StartDate AND RDS.EndDate
								 AND d.DEAL_REPO_START_DATE BETWEEN CONVERT(Varchar(8), BS1.EFFECTIVE_DATE, 112) AND CONVERT(Varchar(8), BS1.EXPIRATION_DATE, 112) 
								 AND d.TradeDate >= RDS.SwitchDate -- SHIRISH 04/13/2018: This condition is added as a fail safe so that trades with trade date of 2/28/2018 are not picked up on 3/1/2018
																   --					  as date swtch happened on 3/1/2018 and these trades were already included on 2/28/2018
							THEN 1
							ELSE 0
						 END) = 1

					/* IOS may not have DEAL_DAYS_TO_MATURITY */
					/* NILESH - 09/14/2011
					-- For OFTR - TRSY the maturity range will be used for tenor ranges instead of the maturity days range
					-- So had to modify the following condition.
					*/
					--AND (ISNULL(d.DEAL_DAYS_TO_MATURITY, 0) BETWEEN ISNULL(BS1.MTY_START, 0) AND ISNULL(BS1.MTY_END, 999999))
					AND (
							(d.ProductGroup NOT IN ('TRSY','USREPO','NAVX','EUREPO','REVCON') AND ISNULL(d.DEAL_DAYS_TO_MATURITY, 0) BETWEEN ISNULL(BS1.MTY_START, 0) AND ISNULL(BS1.MTY_END, 99999999)) 
							OR
							--/* NILESH 03/17/2017 -- Added a condition for NAVX. In case of SQ trades if there is an overlap of period before it opens, the maturity gets a negative value and those trades never will get picked up */
							--(d.ProductGroup IN ('NAVX') AND ABS(ISNULL(d.DEAL_DAYS_TO_MATURITY, 0)) BETWEEN ISNULL(BS1.MTY_START, 0) AND ISNULL(BS1.MTY_END, 99999999)) 
							--OR
							(d.ProductGroup IN ('TRSY') AND ISNULL(d.DEAL_TENOR, 0) BETWEEN ISNULL(BS1.MTY_START, 0) AND ISNULL(BS1.MTY_END, 99999999)) 
							OR
							-- SM 06/20/2016 - Added condition for REPO
							(d.ProductGroup IN ('USREPO','EUREPO') AND d.DEAL_DAYS_TO_MATURITY BETWEEN ISNULL(BS1.MTY_START,0) AND ISNULL(BS1.MTY_END,99999999))
							OR 
							-- GDB-1236 Adding EFP, NAVX and BTIC to below condition as maturity is not used for schedules of these products
							-- 
							(d.ProductGroup IN ('AMSWP','CAD','EFP','NAVX','BTIC','BOX','REVCON'))
						) -- IDENTIFY DEAL_TENOR OR DEAL_DAYS_TO_MATURITY for TIPS
						
					/* if schedule did not match on specific INSTRUMENT_TYPE, matched on ALL */
					AND (
							(ISNULL(d.SWSecType, 'All') = BS1.INSTRUMENT_TYPE) 
							OR 
							(BS1.INSTRUMENT_TYPE = 'All')
							OR 
							-- IDBBC-75 For ACTIVE STREAMS schedules we need to match Sub_Instrument_Type to Underlying Pool Security when available. Else match with security name
							(BS1.INSTRUMENT_TYPE = 'ACTIVE STREAMS' AND ISNULL(d.Deal_UnderlyingPoolSecDesc, d.DEAL_SECURITY_NAME) = BS1.SUB_INSTRUMENT_TYPE)
						)
					/* SM 05/13/2016: Adding INSTRUMENT to join to account for INSTRUMENT level billing for NAVX */
					AND (
							(ISNULL(d.DEAL_SECURITY_ID, 'All') = BS1.INSTRUMENT) 
							/* SM 08/31/2016 Updating below condition for Instrument level billing for EFP.  For EFP as there are multiple legs we need
											 to use DEAL_PARENT_SECURITY_ID to match with INSTRUMENT from schedule */							
							OR 
							(ISNULL(d.DEAL_PARENT_SECURITY_ID, 'All') = BS1.INSTRUMENT) 
							OR 
							(ISNULL(BS1.INSTRUMENT,'All') = 'All')
							OR
							-- IDBBC-138
							-- OTR instrument specific schedules only use INSTRUMENT_TYPE. As INSTRUMENT keeps changing and would need to create a new schedule
							-- everytime it changes so OTR schedules do not use INSTRUMENT.  Before Equity migration in GetBillingSchedule used to have INSTRUMENT set to NULL for Falcon section.
							-- We now have INSTRUMENT field populated for EFP/NAVX instrument specific schedules.  So we need to ignore this condition for OTR schedules. 
							d.ProductGroup = 'OTR'
						)
					/* if schedule did not match on specific LEG, matched on ALL */
					AND ((d.Leg = BS1.LEG) OR (BS1.LEG = 'All'))
					/* if schedule did not match on specific SOURCE, matched on ALL */
					/* SHIRISH 02/17/2017: FOR AMSWP we now have RC, V and E schedules. So no need to translate RC trades to refer to V schedules.
										   For rest of the products we still need to translate RC trades to refert to V schedule */
					--AND ((CASE d.Source WHEN 'RC' THEN 'V' ELSE d.Source END = BS1.SOURCE) OR (ISNULL(BS1.SOURCE,'All') = 'All'))					
					AND ((CASE WHEN d.ProductGroup = 'AMSWP' THEN d.Source 
							/* IDBBC-302: The trades with HFV should be matched to V schedules as there is no HFV schedule setup separately */
							ELSE CASE WHEN d.Source IN ('RC', 'HFV') THEN 'V' ELSE d.Source END 
						  END = BS1.SOURCE) 
						OR (ISNULL(BS1.SOURCE,'All') = 'All'))
					AND	BS1.PROCESS_ID = @ProcessID

					/* SHIRISH 03/20/2019: DIT-10448 For GILTS we just want to count billed volume and as leg is set to 'All' in schedule, when matching deal and schedule take only the primary leg */
					AND (CASE	WHEN d.ProductGroup <> 'GILTS' THEN 1
								WHEN d.ProductGroup = 'GILTS' AND d.Leg = 'PRI' THEN 1
								ELSE 0
							END) = 1
						 		
		/* NILESH 02/14/2012 - Change for Commission Override
		-- The following join is to determine the override commission charge rate for each deal
		*/
		LEFT JOIN	#CommissionScheduleOverride as o ON d.Billing_Code = o.BillingCode AND d.ProductGroup = o.ProductGroup
				AND d.TradeDate BETWEEN o.EffectiveStartDate AND o.EffectiveEndDate
				AND (
					(d.ProductGroup NOT IN ('TRSY') AND ISNULL(d.DEAL_DAYS_TO_MATURITY, 0) BETWEEN ISNULL(o.Range_From, 0) AND ISNULL(o.Range_To, 999999)) OR
					 ((d.ProductGroup IN ('TRSY')) AND ISNULL(d.DEAL_TENOR, 0) BETWEEN ISNULL(o.Range_From, 0) AND ISNULL(o.Range_To, 999999)))
				/* if schedule did not match on specific SOURCE, matched on ALL */
				/* NILESH 03/29/2016: Since the RC trades now get entered along with the other platform trades we need to translate the source accordingly. */
				AND ((d.SOURCE = CASE o.SOURCE WHEN 'O' THEN 'RC' ELSE o.Source END) OR (o.SOURCE = 'All'))
				AND ((d.DEAL_TRDTYPE = o.TradeType) OR (o.TradeType= 'All'))
		

		-- SHIRISH 12/19/2019: IDBBC-15 Join to determine if an instrument is EFP SRT instrument
		LEFT JOIN Instrument.dbo.Security_Master AS SM ON SM.instrid = d.DEAL_SECURITY_ID
													  AND d.ProductGroup = 'EFP'
													  AND d.SWSecType = 'EFPEFPMINI'
		LEFT JOIN Instrument.dbo.Security_Subtype AS SS ON SS.sec_subtype_id = SM.sec_subtype_id
													   AND SS.acronym IN ('SRT','AIR','ES') -- IDBBC-125 Adding acronym AIR to the list, IDBBC-151 Adding acronym ES
				
		WHERE	d.PROCESS_ID = @ProcessID
		AND		d.ProductGroup <> 'AGCY' -- SHIRISH  11/10/2014 -- condition added as #Deals_AllProd now holds AGCY records
		-- SHIRISH 01/24/2019: DIT-10124
		-- In invoice mode match trades clearing at LEK (ClearingID = LSCI) with LEK billing codes. So we can generate invoice for LIBUCKI and charge proper trades to LEK
		AND		(	-- when not in invoice mode ignore clearing id.  In this case no need to break clearing fees for LEK billing codes
					@ReportMode>0
					OR
					(	-- For invoice mode match LEK billing code to trades with clearing id LSCI
						@ReportMode=0
						AND
						(
							(ISNULL(d.SECSuffix,'') = '_LEK' AND (ISNULL(d.ClearingId,'') = 'LSCI' OR ISNULL(d.ContraClearingId,'') = 'LSCI')) -- DIT-10124, when SECSuffix is _LEK then match clearing id LSCI
							OR
							-- SHIRISH 04/02/2019: DIT-10744 Removing SECF25 from below statement as we have supressed it from ActiveBilling
							(d.BILLING_CODE = 'SECF59' AND ISNULL(d.SWSECTYPE,'') = 'EFPMETALS') -- DIT-10123, Charge EFPMETALS trades directly to virtu, Added SECF59 for VIRTU EFPMETALS
							OR
							(ISNULL(d.SECSuffix,'') = '_ABN' AND ISNULL(d.SWSECTYPE,'') <> 'EFPMETALS' AND ISNULL(d.ClearingId,'') <> 'LSCI' AND ISNULL(d.ContraClearingId,'') <> 'LSCI') -- DIT-10123, ABN billing codes do not match with EFPMETALS trades
							OR
							-- SHIRISH 04/02/2019: DIT-10744 Removing SECF25 from below statement as we have supressed it from ActiveBilling
							(ISNULL(d.SECSuffix,'') = '' AND d.BILLING_CODE LIKE 'SECF%' AND d.BILLING_CODE NOT IN ('SECF59') AND ISNULL(d.ClearingId,'') <> 'LSCI' AND ISNULL(d.ContraClearingId,'') <> 'LSCI') --  Default condition for rest of SEC Fee billing codes. All trades that do not fit into above 3 conditions will all into this condition 
							OR
							d.BILLING_CODE NOT LIKE 'SECF%' -- SHIRISH 02/06/2019 - All non-SEC fee billing codes will fall into this condition
						)
					)
				)
		/* IDBBC-292: EXCLUDE R8FIN */
		AND		d.Source <> 'R8FIN'


		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #DBS_Staging',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2


		/* IDBBC-292 : Match R8FIN Daily trades with the billing schedule */
		IF @ExcludeMATCH = 0
		BEGIN
			INSERT INTO IDB_Billing.dbo.wDBS_Staging
			(
				PROCESS_ID,
				TradeDate,
				DEAL_DATE,
				Deal_Id,
				ProductGroup,
				BILLING_CODE,
				Leg,
				Source,
				TradeType2,
				SwSecType,
				DEAL_DAYS_TO_MATURITY,
				/* NILESH IOS BILLING */
				DEAL_O_FACTOR,
				/* NILESH OFTR BILLING */
				DEAL_TENOR,
				BILLING_PLAN_ID,
				BILLING_TYPE, 
				INSTRUMENT_TYPE,
				bs_Leg,
				bs_Source,
				MTY_START,
				MTY_END,
				CHARGE_RATE_PASSIVE, 
				CHARGE_RATE_AGGRESSIVE, 
				CHARGE_FLOOR,
				CHARGE_CAP,
				SETTLE_RATE_PASSIVE,
				SETTLE_RATE_AGGRESSIVE,
				EFFECTIVE_DATE, 
				EXPIRATION_DATE,
				OVERWRITE,
				CHARGE_RATE_AGGRESSIVE_OVERRIDE,
				CHARGE_RATE_PASSIVE_OVERRIDE,
				Weight,
				TIER_BILLING_PLAN_ID,
				TIER_CHARGE_RATE_AGGRESSIVE,
				TIER_CHARGE_RATE_PASSIVE,
				DEAL_SECURITY_ID,
				SRT_Instrument,
				MasterBillingCode, 
				MasterDealerCode,
				IsActiveStream,
				Operator,
				AnonymousLevel,
				BilateralStream,
				IsStdTickInc,
				UseGap,
				DEAL_GAP_DV01
			)
			SELECT 
				@ProcessID,
				d.TradeDate,
				d.DEAL_DATE,
				d.Deal_Id,
				d.ProductGroup,
				d.BILLING_CODE,
				d.Leg,
				d.Source,
				d.TradeType2,
				d.SwSecType,
				d.DEAL_DAYS_TO_MATURITY,
				d.DEAL_O_FACTOR,
				d.DEAL_TENOR,
				BS1.ScheduleId,
				BS1.ScheduleType, 
				INSTRUMENT_TYPE = 'OTRNOTE',
				bs_Leg = 'PRI',
				bs_Source = BS1.SOURCE,
				MTY_START = NULL,
				MTY_END = NULL,
				CHARGE_RATE_PASSIVE = BS1.PassChargeRate, 
				CHARGE_RATE_AGGRESSIVE = BS1.AggrChargeRate, 
				BS1.Floor,
				BS1.Cap,
				SETTLE_RATE_PASSIVE = BS1.PassChargeRate,
				SETTLE_RATE_AGGRESSIVE = BS1.AggrChargeRate,
				BS1.EffectiveStartDate, 
				BS1.EffectiveEndDate,
				OVERWRITE = NULL,
				CHARGE_RATE_AGGRESSIVE_OVERRIDE = NULL, 
				CHARGE_RATE_PASSIVE_OVERRIDE = NULL,
				/* Assign weight based on match and precedence. 
					If the schedule matched on INSTRUMENT_TYPE assign weight 1; if it did not match on specific INSTRUMENT_TYPE 
					and matched on ALL, assign reduced weight
				*/
				Weight = 1.00, --(CASE WHEN BS1.INSTRUMENT_TYPE = 'All' THEN 0.40 ELSE 1.00 END),

				d.TIER_BILLING_PLAN_ID,
				d.TIER_CHARGE_RATE_AGGRESSIVE,
				d.TIER_CHARGE_RATE_PASSIVE,
				d.DEAL_SECURITY_ID,	/* DIT-10159 */
				SRT_Instrument = 0,
				MasterBillingCode = NULL,
				MasterDealerCode = NULL,
				d.IsActiveStream,
				d.Operator,
				d.AnonymousLevel,
				d.BilateralStream,
				d.IsStdTickInc,
				UseGap = NULL,
				d.DEAL_GAP_DV01

			FROM	IDB_Billing.dbo.wDeals_AllProd d (NOLOCK)
			JOIN	#OverrideSchedules BS1 (NOLOCK)
						ON d.BILLING_CODE = BS1.Billing_Code AND d.ProductGroup = BS1.Product
						AND BS1.ScheduleType = CASE WHEN d.Deal_Source = 'MATCH' THEN 'C' WHEN d.Deal_Source = 'DIRECT' THEN 'F' END
						AND d.TradeDate BETWEEN BS1.EffectiveStartDate AND BS1.EffectiveEndDate
						AND d.PROCESS_ID = BS1.ProcessId
			--			/* if schedule did not match on specific SOURCE, matched on ALL */
						AND ((d.Source = BS1.SOURCE) 
							OR (ISNULL(BS1.SOURCE,'All') = 'All'))
						AND	BS1.ProcessId = @ProcessID

			WHERE	d.PROCESS_ID = @ProcessID
			AND		d.ProductGroup IN ('MATCH')
			AND		d.Source = 'R8FIN'

			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'After #DBS_Staging R8FIN',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2

		END 


		;WITH DBS_CTE(
			TradeDate, 
			DEAL_DATE,
			Deal_Id, 
			ProductGroup, 
			BILLING_CODE, 
			Leg,
			Source,
			TradeType2,
			SwSecType,
			DEAL_DAYS_TO_MATURITY,
			/* NILESH IOS BILLING */
			DEAL_O_FACTOR,
			/* NILESH OFTR BILLING */
			DEAL_TENOR,
			BILLING_PLAN_ID,
			BILLING_TYPE, 
			INSTRUMENT_TYPE, 
			bs_Leg, 
			bs_Source,
			MTY_START,
			MTY_END,
			CHARGE_RATE_PASSIVE, 
			CHARGE_RATE_AGGRESSIVE, 
			CHARGE_FLOOR, 
			CHARGE_CAP, 
			SETTLE_RATE_PASSIVE,
			SETTLE_RATE_AGGRESSIVE,
			EFFECTIVE_DATE, 
			EXPIRATION_DATE,
			OVERWRITE, -- SHIRISH 06/01/2017
			CHARGE_RATE_AGGRESSIVE_OVERRIDE,
			CHARGE_RATE_PASSIVE_OVERRIDE, -- SHIRISH 05/21/2019 IDB_18449
			Weight, 
			wRank,
			/* NILESH 08/01/2013 -- Tiered Billing */
			TIER_BILLING_PLAN_ID,
			TIER_CHARGE_RATE_AGGRESSIVE,
			TIER_CHARGE_RATE_PASSIVE,
			DEAL_SECURITY_ID, /* DIT 10159 */
			MasterBillingCode, -- IDBBC-26
			MasterDealerCode, -- IDBBC-26
			UseGap -- IDBBC-132
		) 
		AS
		(
			SELECT	
				TradeDate,
				DEAL_DATE,
				Deal_Id,
				ProductGroup,
				BILLING_CODE,
				Leg,
				Source,
				TradeType2,
				SwSecType,
				DEAL_DAYS_TO_MATURITY,
				/* NILESH IOS BILLING */
				DEAL_O_FACTOR,
				/* NILESH OFTR BILLING */
				DEAL_TENOR,
				BILLING_PLAN_ID,
				BILLING_TYPE, 
				INSTRUMENT_TYPE,
				bs_Leg,
				bs_Source,
				MTY_START,
				MTY_END,
				CHARGE_RATE_PASSIVE, 
				CHARGE_RATE_AGGRESSIVE, 
				CHARGE_FLOOR,
				CHARGE_CAP,
				SETTLE_RATE_PASSIVE,
				SETTLE_RATE_AGGRESSIVE,
				EFFECTIVE_DATE, 
				EXPIRATION_DATE,
				OVERWRITE, -- SHIRISH 06/01/2017
				CHARGE_RATE_AGGRESSIVE_OVERRIDE,
				CHARGE_RATE_PASSIVE_OVERRIDE, -- SHIRISH 05/21/2019 IDB-18449
				Weight,
				wRank = RANK() OVER(PARTITION BY BILLING_CODE, CASE WHEN ProductGroup = 'NAVX' THEN 'X' WHEN ProductGroup = 'EFP' AND INSTRUMENT_TYPE = 'EFPEFPMINI' AND SRT_Instrument = 1 THEN 'X' ELSE BILLING_TYPE END, Deal_Id ORDER BY Weight DESC),
				/* NILESH 08/01/2013 -- Tiered Billing */
				TIER_BILLING_PLAN_ID,
				TIER_CHARGE_RATE_AGGRESSIVE,
				TIER_CHARGE_RATE_PASSIVE,
				DEAL_SECURITY_ID, /* DIT 10159 */
				MasterBillingCode, -- IDBBC-26
				MasterDealerCode, -- IDBBC-26
				UseGap -- IDBBC-312

			FROM	IDB_Billing.dbo.wDBS_Staging
			WHERE	PROCESS_ID = @ProcessID
		)
		SELECT
			TradeDate,
			DEAL_DATE,
			Deal_Id,
			ProductGroup,
			BILLING_CODE,
			Leg,
			Source,
			TradeType2,
			SwSecType,
			DEAL_DAYS_TO_MATURITY,
			/* NILESH IOS BILLING */
			DEAL_O_FACTOR,
			/* NILESH OFTR BILLING */
			DEAL_TENOR,
			BILLING_PLAN_ID,
			BILLING_TYPE, 
			INSTRUMENT_TYPE,
			bs_Leg,
			bs_Source,
			MTY_START,
			MTY_END,
			CHARGE_RATE_PASSIVE, 
			CHARGE_RATE_AGGRESSIVE, 
			CHARGE_FLOOR,
			CHARGE_CAP,
			SETTLE_RATE_PASSIVE,
			SETTLE_RATE_AGGRESSIVE,
			EFFECTIVE_DATE, 
			EXPIRATION_DATE,
			OVERWRITE, -- SHIRISH 06/01/2017
			CHARGE_RATE_AGGRESSIVE_OVERRIDE,
			CHARGE_RATE_PASSIVE_OVERRIDE, -- SHIRISH 05/21/2019 IDB-18449
			wRank,
			/* NILESH 08/01/2013 -- Tiered Billing */
			TIER_BILLING_PLAN_ID,
			TIER_CHARGE_RATE_AGGRESSIVE,
			TIER_CHARGE_RATE_PASSIVE,
			DEAL_SECURITY_ID, /* DIT 10159 */
			MasterBillingCode, -- IDBBC-26
			MasterDealerCode, -- IDBBC-26
			UseGap -- IDBBC-132

		INTO	#DBS_Rank_Staging

		FROM	DBS_CTE
		WHERE	wRank = 1 --Get only the top match

		--IF @Debug = 1
		--BEGIN
		--	SELECT 'After #DBS_Rank_Staging'
		--	SELECT 'After #DBS_Rank_Staging', * FROM #DBS_Rank_Staging
		--END

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #DBS_Rank_Staging',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		INSERT INTO IDB_Billing.dbo.wDealBillingSchedule -- SHIRISH 11/6/2014 -- updating query to use permanent table
		(
			PROCESS_ID,
			InvNum,
			InvDbId,
			BILLING_CODE,
			PeriodId,
			Source,
			Dealer,
			DEAL_ID,
			DEAL_NEGOTIATION_ID,
			Leg,
			DEAL_USER_ID,
			BILLING_PLAN_ID,
			BILLING_TYPE,
			INSTRUMENT_TYPE,
			BS_LEG,
			BS_SOURCE,
			CHARGE_RATE_PASSIVE,
			CHARGE_RATE_AGGRESSIVE,
			CHARGE_RATE_AGGRESSIVE_OVERRIDE,	/* NILESH 02/14/2012 - Commission Override Change */
			CHARGE_RATE_PASSIVE_OVERRIDE, -- SHIRISH 05/21/2019 IDB-18449
			CHARGE_FLOOR,
			CHARGE_CAP,
			SETTLE_RATE_PASSIVE, --Not used
			SETTLE_RATE_AGGRESSIVE, --Not used
			EFFECTIVE_DATE,
			EXPIRATION_DATE,
			OVERWRITE, -- SHIRISH 06/01/2017
			DEAL_WAY,
			DEAL_SECURITY_NAME,
			DEAL_IS_AGRESSIVE,
			Quantity,
			DEAL_DAYS_TO_MATURITY,
			/* NILESH IOS BILLING */
			DEAL_O_FACTOR,
			/* NILESH OFTR BILLING */
			DEAL_TENOR,
			AggressiveDeals,
			PassiveDeals,
			AggressiveVol,
			PassiveVol,
			TradeDate,
			DEAL_DATE,
			ProductGroup,
			TradeType2,
			SWSecType,
			DealCommission,
			DealFinalCommission,
			Instrument,
			DEAL_EXTRA_COMMISSION,
			DEAL_CHARGED_QTY,	/* ECDS */
			DEAL_RISK,			/* AMSWP */
			ExchangeRateMultiplier,
			CURRENCY_CODE, --Not used 
			/* NILESH 08/01/2013 -- Tiered Billing */
			TIER_BILLING_PLAN_ID,
			TIER_CHARGE_RATE_AGGRESSIVE,
			TIER_CHARGE_RATE_PASSIVE,
			DEAL_TIM_QTY,	/* ECDS */
			IsActiveStream,
			DEAL_REPO_START_DATE, --Not used
			DEAL_REPO_END_DATE, --Not used
			DEAL_PRICE,
			DEAL_FIRST_AGGRESSOR,
			Operator, -- DWAS 2.5
			DEAL_ORIG_DID,
			DEAL_SECURITY_ID, /* DIT-10159 */
			Deal_Principal, -- IDBBC-35
			Deal_AccInt, -- IDBBC-35
			Deal_Proceeds, -- IDBBC-35
			AnonymousLevel, --IDBBC-60
			BilateralStream, --IDBBC-71
			IsStdTickInc, -- IDBBC-74
			UseGap, -- IDBBC-132
			DEAL_GAP_DV01, -- IDBBC-132
			RepoRecordType, -- IDBBC-234
			Security_Currency, -- IDBBC-238
			Instrument_ETF -- IDBBC-324

			-- IDBBC-178  Adding fields that are used later in the code
			,Deal_Source
			,Deal_O_FININT
			,Broker
			,Deal_Discount_rate
			,DEAL_SEF_TRADE
			,DEAL_FIRST_AGGRESSED
			,DEAL_STLMT_DATE
			,IsHedgedTrade
		)
		SELECT 
			PROCESS_ID = @ProcessID,
			InvNum = d.InvNum,
			InvDbId = d.InvDbId,
			d.BILLING_CODE,
			d.PeriodId,
			d.Source, 
			d.Dealer,
			d.DEAL_ID,
			d.DEAL_NEGOTIATION_ID, 
			d.Leg,
			d.DEAL_USER_ID, 
			BILLING_PLAN_ID = dbs_rs.BILLING_PLAN_ID,
			BILLING_TYPE = dbs_rs.BILLING_TYPE,
			INSTRUMENT_TYPE = dbs_rs.INSTRUMENT_TYPE,
			BS_LEG = dbs_rs.BS_LEG,
			BS_SOURCE = dbs_rs.BS_SOURCE,
			-- SHIRISH 2018/05/07: After DWAS 2.5 go live, use active stream rates only when dealer is operator.
			-- IDBBC-75 Use available rate in order of ACTIVE STREAMS, Override Schedule and regular clob schedule
			--CHARGE_RATE_PASSIVE = CASE WHEN ISNULL(d.IsActiveStream,0) = 1 THEN @ActiveStreamPassiveChargeRate ELSE dbs_rs.CHARGE_RATE_PASSIVE END,
			CHARGE_RATE_PASSIVE = CASE WHEN dbs_rs.INSTRUMENT_TYPE = 'ACTIVE STREAMS' THEN dbs_rs.CHARGE_RATE_PASSIVE ELSE ISNULL(OverrideSchedule.PassChargeRate,dbs_rs.CHARGE_RATE_PASSIVE) END, -- IDBBC-7 -- IDBBC-75
			--CHARGE_RATE_AGGRESSIVE = CASE WHEN ISNULL(d.IsActiveStream,0) = 1 THEN @ActiveStreamAggressiveChargeRate  ELSE dbs_rs.CHARGE_RATE_AGGRESSIVE END,
			CHARGE_RATE_AGGRESSIVE = CASE WHEN dbs_rs.INSTRUMENT_TYPE = 'ACTIVE STREAMS' THEN dbs_rs.CHARGE_RATE_AGGRESSIVE ELSE ISNULL(OverrideSchedule.AggrChargeRate,dbs_rs.CHARGE_RATE_AGGRESSIVE) END, -- IDBBC-7 -- IDBBC-75
			-- IDBBC-26 When this is excluded deal then rate will come from Override schedules.  Otherwise use volume based rate, Override shcedule rate, Charge_Rate_Aggressive_override in this order
			-- IDBBC-75 When this is not a pool taker schedule and matches ACTIVE STREAM schedule then use that rate
			-- User ACTIVE STREAMS schedule when available else use volume based rate, Override shcedule rate, Charge_Rate_Aggressive_override in this order
			-- IDBBC-144 Use ACTIVE STREAMS schedule rate, Override Schedule rate, Charge_Rate_Aggressive rate in order when available
			CHARGE_RATE_AGGRESSIVE_OVERRIDE = CASE WHEN dbs_rs.INSTRUMENT_TYPE = 'ACTIVE STREAMS' THEN dbs_rs.CHARGE_RATE_AGGRESSIVE_OVERRIDE  -- IDBBC-75
												   ELSE ISNULL(OverrideSchedule.AggrChargeRate,dbs_rs.CHARGE_RATE_AGGRESSIVE_OVERRIDE)
											  END,
			CHARGE_RATE_PASSIVE_OVERRIDE = CASE WHEN dbs_rs.INSTRUMENT_TYPE = 'ACTIVE STREAMS' THEN dbs_rs.CHARGE_RATE_PASSIVE_OVERRIDE  -- IDBBC-75
												ELSE ISNULL(OverrideSchedule.PassChargeRate,dbs_rs.CHARGE_RATE_PASSIVE_OVERRIDE)
											  END,
			--CHARGE_FLOOR = dbs_rs.CHARGE_FLOOR,
			--CHARGE_CAP = dbs_rs.CHARGE_CAP,
			-- GDB-1628 when using override schedule also take cap/floor values
			CHARGE_FLOOR = CASE WHEN dbs_rs.INSTRUMENT_TYPE = 'ACTIVE STREAMS' THEN dbs_rs.CHARGE_FLOOR ELSE ISNULL(OverrideSchedule.[Floor],dbs_rs.CHARGE_FLOOR) END,
			CHARGE_CAP = CASE WHEN dbs_rs.INSTRUMENT_TYPE = 'ACTIVE STREAMS' THEN dbs_rs.CHARGE_CAP ELSE ISNULL(OverrideSchedule.Cap,dbs_rs.CHARGE_CAP) END,
			SETTLE_RATE_PASSIVE = dbs_rs.SETTLE_RATE_PASSIVE, --Not used
			SETTLE_RATE_AGGRESSIVE = dbs_rs.SETTLE_RATE_AGGRESSIVE, --Not used
			EFFECTIVE_DATE = dbs_rs.EFFECTIVE_DATE,
			EXPIRATION_DATE = dbs_rs.EXPIRATION_DATE,
			OVERWRITE = 0, -- SHIRISH 06/05/2017
			d.DEAL_WAY, 
			d.DEAL_SECURITY_NAME, 
			d.DEAL_IS_AGRESSIVE,
			d.Quantity,
			d.DEAL_DAYS_TO_MATURITY,
			/* NILESH IOS BILLING */
			d.DEAL_O_FACTOR,
			/* NILESH OFTR BILLING */
			d.DEAL_TENOR,
			AggressiveDeals = CASE d.DEAL_IS_AGRESSIVE WHEN '1' THEN 1 ELSE 0 END,
			PassiveDeals = CASE d.DEAL_IS_AGRESSIVE WHEN '0' THEN 1 ELSE 0 END,
			AggressiveVol = CASE d.DEAL_IS_AGRESSIVE WHEN '1' THEN d.Quantity ELSE 0 END,
			PassiveVol = CASE d.DEAL_IS_AGRESSIVE WHEN '0' THEN d.Quantity ELSE 0 END,
			d.TradeDate,
			d.DEAL_DATE,
			d.ProductGroup,
			d.TradeType2,
			d.SWSecType,
			d.DealCommission,
			d.DealFinalCommission,
			d.Instrument,
			d.DEAL_EXTRA_COMMISSION,
			/* 
			-- For ECDS commission for the Passive trades will be calculated based on the value 
			-- in the Deal_Charged_Qty and not the actual deal quantity.
			*/
			DEAL_CHARGED_QTY = CASE d.DEAL_IS_AGRESSIVE WHEN '0' THEN d.Deal_Charged_Qty ELSE d.Quantity END,		/* ECDS */
			d.DEAL_RISK,	/* AMSWP */
			ExchangeRateMultiplier = ISNULL(d.ExchangeRateMultiplier,1),
			d.CURRENCY_CODE, --Not used
			/* NILESH 08/01/2013 -- Tiered Billing */
			d.TIER_BILLING_PLAN_ID,
			d.TIER_CHARGE_RATE_AGGRESSIVE,
			d.TIER_CHARGE_RATE_PASSIVE,
			DEAL_TIM_QTY = ISNULL(DEAL_TIM_QTY, 0), /* ECDS */
			/* NILESH 05/08/2014 - AMSWP Manual Deal Commission Override */
			d.IsActiveStream,
			d.DEAL_REPO_START_DATE, --Not used
			d.DEAL_REPO_END_DATE, --Not used
			d.DEAL_PRICE,
			d.DEAL_FIRST_AGGRESSOR,
			d.Operator, -- DWAS 2.5
			d.DEAL_ORIG_DID,
			d.DEAL_SECURITY_ID,	/* DIT-10159 */
			d.DEAL_PRINCIPAL, -- IDBBC-35
			d.DEAL_ACCINT, -- IDBBC-35
			d.DEAL_PROCEEDS, -- IDBBC-35
			d.AnonymousLevel, --IDBBC-60
			d.BIlateralStream, -- IDBBC-71
			d.IsStdTickInc, -- IDBBC-74
			dbs_rs.UseGap, -- IDBBC-132
			d.DEAL_GAP_DV01, --IDBBC-132
			d.RepoRecordType, -- IDBBC-234
			d.Security_Currency, -- IDBBC-238
			d.Instrument_ETF -- IDBBC-324

			-- IDBBC-178 Adding fields that are used later in the code
			,d.Deal_Source
			,d.DEAL_O_FININT
			,d.Broker
			,d.Deal_Discount_rate
			,d.DEAL_SEF_TRADE
			,d.DEAL_FIRST_AGGRESSED
			,d.DEAL_STLMT_DATE
			,d.IsHedgedTrade

		FROM	#DBS_Rank_Staging dbs_rs
		JOIN	IDB_Billing.dbo.wDeals_AllProd d (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table
					ON --CONVERT(varchar(8), dbs_rs.TradeDate, 112) = CONVERT(varchar(8), d.TradeDate, 112)
						dbs_rs.TradeDate = d.TradeDate
					AND dbs_rs.ProductGroup = d.ProductGroup
					AND dbs_rs.BILLING_CODE = d.BILLING_CODE
					/* 
					-- NILESH 06/06/2013 
					-- Modified the following condition to support an additional condition
					-- when DEAL_ID is NULL in both #Deals_AllProd and  #DBS_Rank_Staging
					-- tables. This can occur only in a situation when we have added a dummy 
					-- deal when creating an invoice. The dummy deal is added to allow the invoice
					-- creation process to identify this data and generate an invoice in situation when there
					-- was no trades done by the dealer but had to be charged the floor amount. 
					*/
					--AND dbs_rs.DEAL_ID = d.DEAL_ID
					AND ((dbs_rs.DEAL_ID = d.DEAL_ID) OR (dbs_rs.DEAL_ID IS NULL AND d.DEAL_ID IS NULL AND ISNULL(dbs_rs.CHARGE_FLOOR,0) > 0))

		-- Use override schedule rate when available
		-- IDBBC-238 Updating Outer apply to left join
		LEFT JOIN	IDB_Billing.dbo.wOverrideScheduleDealBillingSchedule OverrideSchedule ON OverrideSchedule.PROCESS_ID = @ProcessID
																						 AND OverrideSchedule.Deal_Trade_Date = dbs_rs.TradeDate
																						 AND OverrideSchedule.Billing_Code = dbs_rs.BILLING_CODE
																						 AND OverrideSchedule.Product = dbs_rs.ProductGroup
																						 AND OverrideSchedule.Deal_Id = dbs_rs.Deal_Id
																						 AND OverrideSchedule.ScheduleId IS NOT NULL

		/* NILESH 09/24/2011
		-- This is the condition to exclude the line item for a product when there is no floor and no amount. The reason this
		-- situation would occur is because the query to get the list of active billing codes was enhanced to include the dealers
		-- for which we do not have deals. Reason, they may have a floor amount which needs to be charged even in the absence of 
		-- the deals. Which prior to the change was ignoring it and had to be manually adjusted.
		*/				
		WHERE	(CASE WHEN d.Dealer IS NULL AND ISNULL(dbs_rs.CHARGE_FLOOR,0) = 0 THEN 0 ELSE 1 END) = 1
		AND		d.PROCESS_ID = @ProcessID
		AND		d.ProductGroup <> 'AGCY' -- SHIRISH  11/10/2014 -- condition added as #Deals_AllProd now holds AGCY records

		--IF @Debug = 1
		--BEGIN
		--	SELECT 'After wDealBillingSchedule load'
		--	SELECT 'After wDealBillingSchedule load', * FROM IDB_Billing.dbo.wDealBillingSchedule
		--END

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #DBSBillingSchedule',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/*
		-- NILESH 09/04/2013
		-- Tiered Billing
		-- Need the Staging for updating the billing schedule with 
		-- using tiered rates
		*/
		SELECT	Billing_Code, 
				ProductGroup, 
				Instrument_Type,
				INSTRUMENT, -- SM 05/13/2016: Instrument level billing for NAVX
				Billing_Type, 
				TIER_BILLING_PLAN_ID,
				TIER_CHARGE_RATE_AGGRESSIVE = MAX(TIER_CHARGE_RATE_AGGRESSIVE), 
				TIER_CHARGE_RATE_PASSIVE = MAX(TIER_CHARGE_RATE_PASSIVE) 
		
		INTO	#DealBillingScheduleStaging
		
		FROM	IDB_Billing.dbo.wDealBillingSchedule
		
		WHERE	PROCESS_ID = @ProcessID 
		
		GROUP BY	Billing_Code, 
					ProductGroup, 
					Instrument_Type, 
					INSTRUMENT, -- SM 05/13/2016: Instrument level billing for NAVX
					Billing_Type, 
					TIER_BILLING_PLAN_ID
		
		UPDATE B
		SET
				B.BILLING_PLAN_ID = DBS.TIER_BILLING_PLAN_ID,
				B.CHARGE_RATE_AGGRESSIVE = DBS.TIER_CHARGE_RATE_AGGRESSIVE,
				B.CHARGE_RATE_PASSIVE = DBS.TIER_CHARGE_RATE_PASSIVE
				
		FROM IDB_Billing.dbo.wBillingSchedule B (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table
		JOIN IDB_CodeBase.dbo.fnProductType() fp ON B.PRODUCT_GROUP = fp.Product
		JOIN #DealBillingScheduleStaging DBS ON B.BILLING_CODE = DBS.BILLING_CODE
																	AND	B.PRODUCT_GROUP = DBS.ProductGroup
																	AND B.INSTRUMENT_TYPE = DBS.INSTRUMENT_TYPE
																	AND B.BILLING_TYPE = DBS.BILLING_TYPE
																	AND B.INSTRUMENT = DBS.INSTRUMENT -- SM 05/13/2016: Instrument level billing for NAVX
																	
		WHERE	fp.ProductInvoiceUsesTieredBilling = 'Y'
		AND		(DBS.TIER_CHARGE_RATE_AGGRESSIVE IS NOT NULL OR DBS.TIER_CHARGE_RATE_PASSIVE IS NOT NULL)
		AND		B.PROCESS_ID = @ProcessID


		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #DealBillingScheduleStaging',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		INTO	#CommissionCollected

		FROM	IDB_Billing.dbo.wBillingDeals d (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table
		--It is ok to use hard coded value Commissions in the join below because this insert is specifically for Commission charge
		JOIN	#ChargeType CT ON CT.ChargeType = 'Commissions'
		
		WHERE	d.PROCESS_ID = @ProcessID

		GROUP BY d.InvNum, 
			d.InvDbId, 
			d.Billing_Code, 
			d.ProductGroup, 
			d.PeriodId, 
			d.Source,
			CT.ChargeId

		-- ALL OTHER PRODUCTS i.e. BILL, IOS, and TRSY 

		INSERT INTO #CommissionCollected
		(
			InvNum,
			InvDbId,
			Billing_Code,
			ProductGroup,
			PeriodId,
			Source,
			ChargeId,
			CommissionCollected
		)
	*/	

		--GET COMMISSION COLLECTED FROM ALL DEALS 
		-- SHIRISH 11/10/2014 -- this query also includes AGCY records as AGCY code was updated to use wDeals_AllProd table.
		-- DO NOT ADD condition to remove AGCY records from this query.
		SELECT	
			d.InvNum,
			d.InvDbId,
			d.Billing_Code,
			d.ProductGroup,
			d.PeriodId,
			d.Source,
			CT.ChargeId,
			-- SM - 2015/05/11 - Updated Commission Collected for AMSWP V trades to 0
			CommissionCollected = SUM(CASE WHEN d.productgroup IN ('CAD','AMSWP') AND d.Source <> 'E' THEN 0 
										   -- SHIRISH 2017/02/10: For CAD DealCommission is in Canadian Dollars.  Need to convert that to USD
										   --WHEN d.ProductGroup = 'CAD' THEN ISNULL(d.DealCommission, 0) * ExchangeRateMultiplier
										   ELSE ISNULL(d.DealCommission, 0) 
									   END * (CASE WHEN d.ProductGroup = 'GILTS' THEN d.ExchangeRateMultiplier ELSE 1 END)  -- SHIRISH 09/12/2019: GDB-186 Applying exchange rate for GILTS so commission is coverted into USD
									  ), 
			d.IsActiveStream,
			d.Security_Currency

		INTO	#CommissionCollected
		
		FROM	IDB_Billing.dbo.wDeals_AllProd d (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table
		--It is ok to use hard coded value Commissions in the join below because this insert is specifically for Commission charge
		JOIN	#ChargeType CT ON CT.ChargeType = 'Commissions'
		
		WHERE d.PROCESS_ID = @ProcessID

		GROUP BY d.InvNum, 
			d.InvDbId, 
			d.Billing_Code, 
			d.ProductGroup, 
			d.PeriodId, 
			d.Source,
			CT.ChargeId,
			d.IsActiveStream,
			d.Security_Currency

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #CommissionCollected',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		SELECT	
			d.InvNum,
			d.InvDbId,
			d.Billing_Code,
			d.ProductGroup,
			d.PeriodId,
			d.TradeDate,
			d.Source,
			CT.ChargeId,
			-- SM - 2015/05/11 - Updated Commission Collected for AMSWP V trades to 0
			CommissionCollected = SUM(CASE WHEN d.ProductGroup IN ('CAD','AMSWP') AND d.Source <> 'E' THEN 0 
										   -- SHIRISH 2017/02/10: For CAD DealCommission is in Canadian Dollars.  Need to convert that to USD
										   --WHEN d.ProductGroup = 'CAD' THEN ISNULL(d.DealCommission, 0) * ExchangeRateMultiplier
										   ELSE ISNULL(d.DealCommission, 0) 
									   END)

		INTO #CommissionCollected_Tier
		
		FROM	IDB_Billing.dbo.wDeals_AllProd d (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table
		JOIN IDB_CodeBase.dbo.fnProductType() fp ON d.ProductGroup = fp.Product
		--It is ok to use hard coded value Commissions in the join below because this insert is specifically for Commission charge
		JOIN	#ChargeType CT ON CT.ChargeType = 'Commissions'

		WHERE	ProductInvoiceUsesTieredBilling = 'Y'
		AND		d.PROCESS_ID = @ProcessID
		AND		d.ProductGroup <> 'AGCY' -- SHIRISH  11/10/2014 -- condition added as #Deals_AllProd now holds AGCY records
		
		GROUP BY d.InvNum, 
			d.InvDbId, 
			d.Billing_Code, 
			d.ProductGroup, 
			d.PeriodId, 
			d.TradeDate,
			d.Source,
			CT.ChargeId

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #CommissionCollectedTier',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		--CREATE STAGING DATA FOR INVOICE INVENTORY
		---------------- AGCY -----------------
		INSERT INTO #InvoiceInventory_Staging
		(
			InvNum ,
			InvDbId,
			BILLING_CODE,
			PeriodId,
			Logon_Id,
			ProductGroup,
			Source,
			BILLING_PLAN_ID,
			INSTRUMENT_TYPE,
			BILLING_TYPE,
			AggressiveVolume,
			PassiveVolume,
			TotalVolume,
			AggressiveTrades,
			PassiveTrades,
			TotalTrades,
			CHARGE_RATE_AGGRESSIVE,
			CHARGE_RATE_PASSIVE,
			CHARGE_FLOOR,
			CHARGE_CAP,
			CommissionOwed_PreCap,
			CommissionOwed_PreCap_Override,	/* NILESH 02/14/2012 - Commission Override Change */
			CommissionOwed,
			IsActiveStream
		)
		SELECT
			DBS.InvNum ,
			DBS.InvDbId,
			DBS.BILLING_CODE,
			DBS.PeriodId,
			Logon_Id = @User,
			DBS.ProductGroup,
			DBS.Source,
			DBS.BILLING_PLAN_ID,
			DBS.INSTRUMENT_TYPE,
			DBS.BILLING_TYPE,
			AggressiveVolume = SUM(ISNULL(DBS.AggressiveVol, 0)),
			PassiveVolume = SUM(ISNULL(DBS.PassiveVol, 0)),
			TotalVolume = SUM(ISNULL(DBS.AggressiveVol, 0) + ISNULL(DBS.PassiveVol, 0)),
			AggressiveTrades = SUM(ISNULL(DBS.AggressiveDeals, 0)),
			PassiveTrades = SUM(ISNULL(DBS.PassiveDeals, 0)),
			TotalTrades = SUM(ISNULL(DBS.AggressiveDeals, 0) + ISNULL(DBS.PassiveDeals, 0)),
			DBS.CHARGE_RATE_AGGRESSIVE,
			DBS.CHARGE_RATE_PASSIVE,
			DBS.CHARGE_FLOOR,
			DBS.CHARGE_CAP,
			/* NILESH 08/10/2012 - Add the DEAL_EXTRA_COMMISSION here to include it as commission owed. */
			CommissionOwed_PreCap = CASE DBS.BILLING_TYPE 
						WHEN 'Per_MM' THEN
							SUM(CASE WHEN DealFinalCommission = 1 THEN DealCommission ELSE ((ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE) + (ISNULL(DBS.PassiveVol, 0) * DBS.CHARGE_RATE_PASSIVE)) + ISNULL(DBS.DEAL_EXTRA_COMMISSION,0) END )
						WHEN 'Per_Trade' THEN
							SUM(CASE WHEN DealFinalCommission = 1 THEN DealCommission ELSE ((ISNULL(DBS.AggressiveDeals, 0) * DBS.CHARGE_RATE_AGGRESSIVE) + (ISNULL(DBS.PassiveDeals, 0) * DBS.CHARGE_RATE_PASSIVE)) + ISNULL(DBS.DEAL_EXTRA_COMMISSION,0) END )
						END,
			/* NILESH 02/14/2012 - Commission Override Change */
			CommissionOwed_PreCap_Override = CASE DBS.BILLING_TYPE 
						WHEN 'Per_MM' THEN
							SUM(CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ISNULL(DBS.AggressiveVol, 0) * ISNULL(DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE,0) ELSE 0 END)
						WHEN 'Per_Trade' THEN
							SUM(CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ISNULL(DBS.AggressiveDeals, 0) * ISNULL(DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE,0) ELSE 0 END)
						END,
						
			CommissionOwed = NULL,
			DBS.IsActiveStream
					
		FROM	IDB_Billing.dbo.wDealBillingSchedule DBS

		WHERE	DBS.ProductGroup = 'AGCY'
		AND		PROCESS_ID = @ProcessID

		GROUP BY DBS.InvNum, 
			DBS.InvDbId, 
			DBS.BILLING_CODE, 
			DBS.PeriodId,
			DBS.ProductGroup, 
			DBS.Source,
			DBS.BILLING_PLAN_ID,
			DBS.INSTRUMENT_TYPE, 
			DBS.BILLING_TYPE, 
			DBS.CHARGE_RATE_AGGRESSIVE,
			DBS.CHARGE_RATE_PASSIVE,
			DBS.CHARGE_FLOOR,
			DBS.CHARGE_CAP,
			CHARGE_RATE_AGGRESSIVE_OVERRIDE,
			DBS.IsActiveStream

		UPDATE	#InvoiceInventory_Staging SET
			CommissionOwed = CASE WHEN CommissionOwed_PreCap < CHARGE_FLOOR THEN CHARGE_FLOOR
						WHEN ((CHARGE_CAP > -1) AND (CommissionOwed_PreCap > CHARGE_CAP)) THEN CHARGE_CAP
						ELSE CommissionOwed_PreCap
					END
		WHERE	ProductGroup = 'AGCY'


		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After Invoice Inventory Staging',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/* IDBBC-268 Converting #DBS_AllProd into IDB_Billing.dbo.wDBS_AllProd */
		INSERT INTO IDB_Billing.dbo.wDBS_AllProd
		(
			PROCESS_ID,
			RowNum,
			InvNum,
			InvDbId,
			BILLING_CODE,
			PeriodId,
			ProductGroup,
			Source,
			BILLING_PLAN_ID,
			INSTRUMENT_TYPE,
			BILLING_TYPE,
			AggressiveVolume,
			PassiveVolume,
			TotalVolume,
			AggressiveTrades,
			PassiveTrades,
			TotalTrades,
			CHARGE_RATE_AGGRESSIVE,
			CHARGE_RATE_AGGRESSIVE_OVERRIDE,
			CHARGE_RATE_PASSIVE_OVERRIDE,
			CHARGE_RATE_PASSIVE,
			CHARGE_FLOOR,
			CHARGE_CAP,
			CommissionOwed_PreCap,
			CommissionOwed_PreCap_AfterDailyCap,
			CommissionOwed_PreCap_Override,
			CommissionOwed,
			ExchangeRateMultiplier,
			TradeDate,
			TIER_BILLING_PLAN_ID,
			TIER_CHARGE_RATE_AGGRESSIVE,
			TIER_CHARGE_RATE_PASSIVE,
			Instrument,
			Deal_Way,
			ClearingQty,
			EstimatedRate,
			DEAL_NEGOTIATION_ID,
			IsActiveStream,
			Is_OFTRDiscountRate,
			Discount_Amount,
			Trader,
			DEAL_REPO_START_DATE,
			DEAL_ID,
			TradeType2,
			DEAL_DATE,
			DEAL_SECURITY_ID,
			DEAL_IS_AGRESSIVE,
			Leg,
			FF_CommissionOwed_PreCap,
			FF_CommissionOwed_PreCap_Override,
			AnonymousLevel,
			BilateralStream,
			IsStdTickInc,
			Dealer,
			ContraDealer,
			ContraUserId,
			UseGap,
			DEAL_GAP_DV01,
			NetMoney,
			Quantity,
			RepoRecordType,
			Security_Currency,
			RepoTicketFees,
			Instrument_ETF -- IDBBC-324
		)
		SELECT
			PROCESS_ID = @ProcessID,
			RowNum = ROW_NUMBER() OVER (ORDER BY @ProcessID), --IDENTITY(Int, 1, 1),
			DBS.InvNum ,
			DBS.InvDbId,
			DBS.BILLING_CODE,
			DBS.PeriodId,
			DBS.ProductGroup,
			DBS.Source,
			DBS.BILLING_PLAN_ID,
			DBS.INSTRUMENT_TYPE,
			DBS.BILLING_TYPE,
			AggressiveVolume = CASE WHEN DBS.ProductGroup = 'NAVX' AND ISNULL(DBS.AggressiveVol, 0) > 0 THEN ISNULL(CT.Quantity / 1000, DBS.AggressiveVol) ELSE ISNULL(DBS.AggressiveVol, 0) END,
			PassiveVolume = CASE WHEN DBS.ProductGroup = 'NAVX' AND ISNULL(DBS.PassiveVol, 0) > 0 THEN ISNULL(CT.Quantity / 1000,DBS.PassiveVol) ELSE ISNULL(DBS.PassiveVol, 0) END,
			TotalVolume = CASE WHEN DBS.ProductGroup = 'NAVX' AND ISNULL(DBS.AggressiveVol, 0) > 0 THEN ISNULL(CT.Quantity / 1000,DBS.AggressiveVol) ELSE ISNULL(DBS.AggressiveVol, 0) END + CASE WHEN DBS.ProductGroup = 'NAVX' AND ISNULL(DBS.PassiveVol, 0) > 0 THEN ISNULL(CT.Quantity / 1000,DBS.PassiveVol) ELSE ISNULL(DBS.PassiveVol, 0) END,
			AggressiveTrades = ISNULL(DBS.AggressiveDeals, 0),
			PassiveTrades = ISNULL(DBS.PassiveDeals, 0),
			TotalTrades = ISNULL(DBS.AggressiveDeals, 0) + ISNULL(DBS.PassiveDeals, 0),
			CHARGE_RATE_AGGRESSIVE = DBS.CHARGE_RATE_AGGRESSIVE, --CASE WHEN ISNULL(DBS.IsActiveStream,0) = 1 THEN @ActiveStreamAggressiveChargeRate ELSE DBS.CHARGE_RATE_AGGRESSIVE END,
			DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE,
			DBS.CHARGE_RATE_PASSIVE_OVERRIDE, -- SHIRISH 05/21/2019 IDB_18449
			CHARGE_RATE_PASSIVE = DBS.CHARGE_RATE_PASSIVE, --CASE WHEN ISNULL(DBS.IsActiveStream,0) = 1 THEN @ActiveStreamPassiveChargeRate ELSE DBS.CHARGE_RATE_PASSIVE END,
			DBS.CHARGE_FLOOR,
			DBS.CHARGE_CAP,
			/* Commission collected (TW_DEAL.COMMISSION_COLLECTED) is rounded to two decimal places at the 
				deal (leg) level and if commission owed is not, it will result in negative net commission 
				(e.g. -0.00333333333333741) for deals where net commission should be zero i.e. we collected 
				exactly what they owe.

				Round commission owed at deal (leg) level.
			*/
			/* NILESH : 02/04/2011 - Seperated the cases by products as the IOS
				commission is factored instead of just calculated based on the
				volume
			*/
			/* NILESH 08/10/2012 - Add the DEAL_EXTRA_COMMISSION here to include it as commission owed. */
			CommissionOwed_PreCap = ROUND(CASE 
						/* SHIRISH 2016/12/12: Do not calculate commission for SEC Fee billing codes */
						/* SHIRISH 2024/10/29: Adding CAT fee billing codes to below condition */
						WHEN DBS.BILLING_CODE LIKE 'SECF%' OR DBS.BILLING_CODE LIKE 'CATF%' THEN CAST(NULL AS FLOAT)
						/* BILL */
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'BILL' THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE ((ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE) + (ISNULL(DBS.PassiveVol, 0) * DBS.CHARGE_RATE_PASSIVE)) + ISNULL(DBS.DEAL_EXTRA_COMMISSION,0) END )
						WHEN DBS.BILLING_TYPE = 'Per_Trade' AND DBS.ProductGroup = 'BILL' THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE ((ISNULL(DBS.AggressiveDeals, 0) * DBS.CHARGE_RATE_AGGRESSIVE) + (ISNULL(DBS.PassiveDeals, 0) * DBS.CHARGE_RATE_PASSIVE)) + ISNULL(DBS.DEAL_EXTRA_COMMISSION,0) END )
						WHEN DBS.BILLING_TYPE = 'DRate' AND DBS.ProductGroup = 'BILL' THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE ((ISNULL(DBS.AggressiveVol, 0) * @VolUnitMultiplier) * ((DBS.CHARGE_RATE_AGGRESSIVE / 100.00) / 100.00) * DBS.DEAL_DAYS_TO_MATURITY / 360.00) + ((ISNULL(DBS.PassiveVol, 0) * @VolUnitMultiplier) * ((DBS.CHARGE_RATE_PASSIVE / 100.00) / 100.00) * DBS.DEAL_DAYS_TO_MATURITY / 360.00) + ISNULL(DBS.DEAL_EXTRA_COMMISSION,0) END )
						/* IOS */
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'IOS'  THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE ((ISNULL(DBS.AggressiveVol, 0) * DBS.DEAL_O_FACTOR * DBS.CHARGE_RATE_AGGRESSIVE) + (ISNULL(DBS.PassiveVol, 0) * DBS.DEAL_O_FACTOR * DBS.CHARGE_RATE_PASSIVE)) + ISNULL(DBS.DEAL_EXTRA_COMMISSION,0) END )
						WHEN DBS.BILLING_TYPE = 'Per_Trade' AND DBS.ProductGroup = 'IOS' THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE ((ISNULL(DBS.AggressiveDeals, 0) * DBS.DEAL_O_FACTOR * DBS.CHARGE_RATE_AGGRESSIVE) + (ISNULL(DBS.PassiveDeals, 0) * DBS.DEAL_O_FACTOR * DBS.CHARGE_RATE_PASSIVE)) + ISNULL(DBS.DEAL_EXTRA_COMMISSION,0) END )

						/* TRSY */
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'TRSY'  THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE ((ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE) + (ISNULL(DBS.PassiveVol, 0) * DBS.CHARGE_RATE_PASSIVE)) + ISNULL(DBS.DEAL_EXTRA_COMMISSION,0) END )
						WHEN DBS.BILLING_TYPE = 'Per_Trade' AND DBS.ProductGroup = 'TRSY' THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE ((ISNULL(DBS.AggressiveDeals, 0) * DBS.CHARGE_RATE_AGGRESSIVE) + (ISNULL(DBS.PassiveDeals, 0) * DBS.CHARGE_RATE_PASSIVE)) + ISNULL(DBS.DEAL_EXTRA_COMMISSION,0) END )

						/* GILTS */ -- DIT-10448
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'GILTS' AND DBS.leg = 'PRI' THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE ((ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE) + (ISNULL(DBS.PassiveVol, 0) * DBS.CHARGE_RATE_PASSIVE)) + ISNULL(DBS.DEAL_EXTRA_COMMISSION,0) END )

						/* TIPS */
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'TIPS'  THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE ((ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE) + (ISNULL(DBS.PassiveVol, 0) * DBS.CHARGE_RATE_PASSIVE)) + ISNULL(DBS.DEAL_EXTRA_COMMISSION,0) END )
						WHEN DBS.BILLING_TYPE = 'Per_Trade' AND DBS.ProductGroup = 'TIPS' THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE ((ISNULL(DBS.AggressiveDeals, 0) * DBS.CHARGE_RATE_AGGRESSIVE) + (ISNULL(DBS.PassiveDeals, 0) * DBS.CHARGE_RATE_PASSIVE)) + ISNULL(DBS.DEAL_EXTRA_COMMISSION,0) END )				
					
						/* EFP */
						WHEN DBS.BILLING_TYPE = 'Per_Contract' AND DBS.ProductGroup = 'EFP'  THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE ((ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE) + (ISNULL(DBS.PassiveVol, 0) * DBS.CHARGE_RATE_PASSIVE)) + ISNULL(DBS.DEAL_EXTRA_COMMISSION,0) END )
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'EFP' AND DBS.SWSecType = 'EFPVOL' THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE (ISNULL(CT.NetMoney, 0) * (CASE WHEN DBS.AggressiveVol > 0 THEN DBS.CHARGE_RATE_AGGRESSIVE ELSE DBS.CHARGE_RATE_PASSIVE END)) END)
						/* IDBBC-11: Added the support for Instrument based Schedule */
						/* IDBBC-35: As requested by Jason starting 2/21/2020 we are using Deal_Proceeds from deal as notional to calculate commission for SRT trades as that matches blotter.  Original code used Net Money from Clearing Trades as notional */
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'EFP' AND DBS.SWSecType = 'EFPEFPMINI' THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE (ISNULL((CASE WHEN DBS.TradeDate >= '20200221' THEN DBS.Deal_Proceeds ELSE CT.NetMoney END), 0) * (CASE WHEN DBS.AggressiveVol > 0 THEN DBS.CHARGE_RATE_AGGRESSIVE ELSE DBS.CHARGE_RATE_PASSIVE END)) END)

						/* EQSWP */
						/* NILESH 07/17/2015 -- Seperated the calculation for the Index Swaps & ETF Swaps */
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'EQSWP' AND DBS.SWSecType = 'EQSWPETF' THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE (ISNULL(CT.NetMoney, 0) * ISNULL(DBS.DEAL_TENOR,0) * (CASE WHEN DBS.AggressiveVol > 0 THEN DBS.CHARGE_RATE_AGGRESSIVE ELSE DBS.CHARGE_RATE_PASSIVE END)) END)
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'EQSWP' AND DBS.SWSecType <> 'EQSWPETF' THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE (ISNULL(DBS.Quantity, 0) * @VolUnitMultiplier * ISNULL(DBS.DEAL_TENOR,0) * (CASE WHEN DBS.AggressiveVol > 0 THEN DBS.CHARGE_RATE_AGGRESSIVE ELSE DBS.CHARGE_RATE_PASSIVE END)) END)
						WHEN DBS.BILLING_TYPE = 'Per_Trade' AND DBS.ProductGroup = 'EQSWP' THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE ((ISNULL(DBS.AggressiveDeals, 0) * DBS.CHARGE_RATE_AGGRESSIVE) + (ISNULL(DBS.PassiveDeals, 0) * DBS.CHARGE_RATE_PASSIVE)) + ISNULL(DBS.DEAL_EXTRA_COMMISSION,0) END )
						WHEN DBS.BILLING_TYPE = 'Per_Contract' AND DBS.ProductGroup = 'EQSWP'  THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE ((ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE) + (ISNULL(DBS.PassiveVol, 0) * DBS.CHARGE_RATE_PASSIVE)) + ISNULL(DBS.DEAL_EXTRA_COMMISSION,0) END )

						/* ECDS */
						/* 
						-- NILESH 05/29/2014 
						-- Added condition for UMIB since they need to be charged no matter on what side of the trade they are on. This
						-- was done since the Trade Engine does not charge the passive trade unless it complies to some of the work-up
						-- & TIM rules. This is done effective 1st May 2014.
						*/
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'ECDS'  THEN
							(CASE	WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission 
										ELSE ((CASE 
													WHEN DEAL_IS_AGRESSIVE = 1 THEN ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE 
													/* UMIB  - PASSIVE SIDE OF THE TRADE  Effective 1st May 2014 */
													WHEN DEAL_IS_AGRESSIVE = 0 AND DBS.BILLING_CODE = 'UMIB1' AND DBS.TradeDate > '20140430' THEN ISNULL(DBS.PassiveVol, 0) * ISNULL(DBS.CHARGE_RATE_PASSIVE,0) 
													/* ALL OTHER CASES (includes UMIB prior to 1st May 2014 as well - PASSIVE SIDE OF THE TRADE */
													ELSE ISNULL(DBS.DEAL_CHARGED_QTY, 0) * DBS.CHARGE_RATE_PASSIVE 
												END) 
													+ ISNULL(DBS.DEAL_EXTRA_COMMISSION,0)) -- * DBS.ExchangeRateMultiplier
							END)

						/* AMSWP */
							/* 
							-- NILESH 05/08/2014 
							-- Modified the code to override the charge rates for 
							-- AMSWP for the manually entered deals from SmartCC
							-- This was done to support Voice Trades for AMSWP
							-- prior to the Release 30 during which time market support
							-- entered the deals manually from SmartCC.
							--
							-- SHIRISH 08/10/2015
							-- Manually entered trades (RC, V) use gap commission and commission is calculated and saved on DealCommission field on deal.  As we don't know gap commission we are just 
							-- reading DealCommission field and do not calculate commission here.  We are only calculating commission for E trades.
							-- SHIRISH 06/08/2021
							-- IDBBC-132
							-- Updating AMSWP commission calculation to use DEAL_GAP_DV01 when UseGap is set to 1 else use DEAL_RISK
							*/					
							WHEN DBS.BILLING_TYPE = 'Delta' AND DBS.ProductGroup = 'AMSWP'  THEN
							(CASE	WHEN (DBS.DealFinalCommission = 1 OR DBS.Source <> 'E') THEN DBS.DealCommission
										ELSE ((CASE WHEN DEAL_IS_AGRESSIVE = 1 THEN 
													--ISNULL(DBS.AggressiveVol, 0) * CAST(ISNULL(DBS.DEAL_RISK, 0) AS FLOAT) * DBS.CHARGE_RATE_AGGRESSIVE
													ISNULL(DBS.AggressiveVol, 0) * (CASE WHEN DBS.UseGap = 1 THEN DBS.DEAL_GAP_DV01 ELSE CAST(ISNULL(DBS.DEAL_RISK, 0) AS FLOAT) END) * CASE ISNULL(DBS.Source,'E') WHEN 'V' THEN @AMSWP_ME_Fixed_CommRate ELSE DBS.CHARGE_RATE_AGGRESSIVE END
												ELSE 
													--ISNULL(DBS.PassiveVol, 0) * CAST(ISNULL(DBS.DEAL_RISK, 0) AS FLOAT) * DBS.CHARGE_RATE_PASSIVE
													ISNULL(DBS.PassiveVol, 0) * (CASE WHEN DBS.UseGap = 1 THEN DBS.DEAL_GAP_DV01 ELSE CAST(ISNULL(DBS.DEAL_RISK, 0) AS FLOAT) END) * CASE ISNULL(DBS.Source,'E') WHEN 'V' THEN @AMSWP_ME_Fixed_CommRate ELSE DBS.CHARGE_RATE_PASSIVE END
												END) 
											+ ISNULL(DBS.DEAL_EXTRA_COMMISSION,0)) 
										END)

						/* CAD */
							/* 
							-- SHIRISH 01/23/2015 - Copying above logic for AMSWP
							*/					
							WHEN DBS.BILLING_TYPE = 'Delta' AND DBS.ProductGroup = 'CAD'  THEN
							(CASE	WHEN (DBS.DealFinalCommission = 1 OR DBS.Source <> 'E') THEN DBS.DealCommission 
										ELSE ((CASE WHEN DEAL_IS_AGRESSIVE = 1 THEN 
													--ISNULL(DBS.AggressiveVol, 0) * CAST(ISNULL(DBS.DEAL_RISK, 0) AS FLOAT) * DBS.CHARGE_RATE_AGGRESSIVE
													ISNULL(DBS.AggressiveVol, 0) * CAST(ISNULL(DBS.DEAL_RISK, 0) AS FLOAT) * CASE ISNULL(DBS.Source,'E') WHEN 'V' THEN @AMSWP_ME_Fixed_CommRate ELSE DBS.CHARGE_RATE_AGGRESSIVE END
												ELSE 
													--ISNULL(DBS.PassiveVol, 0) * CAST(ISNULL(DBS.DEAL_RISK, 0) AS FLOAT) * DBS.CHARGE_RATE_PASSIVE
													ISNULL(DBS.PassiveVol, 0) * CAST(ISNULL(DBS.DEAL_RISK, 0) AS FLOAT) * CASE ISNULL(DBS.Source,'E') WHEN 'V' THEN @AMSWP_ME_Fixed_CommRate ELSE DBS.CHARGE_RATE_PASSIVE END
												END) 
											+ ISNULL(DBS.DEAL_EXTRA_COMMISSION,0)) 
										END) * DBS.ExchangeRateMultiplier -- SHIRISH 2017/02/10: Calculated commission is in CAD. Need to convert it to USD using Exchange Rate

						/* UCDS */
						/* ################## THIS WILL USE EITHER THE TIERED RATE OR REGULAR RATE ###################### */
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'UCDS'  THEN
							(CASE	WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission 
										ELSE ((CASE WHEN DEAL_IS_AGRESSIVE = 1 THEN ISNULL(DBS.AggressiveVol, 0) * ISNULL(DBS.TIER_CHARGE_RATE_AGGRESSIVE,DBS.CHARGE_RATE_AGGRESSIVE) 
															ELSE (ISNULL(DBS.DEAL_CHARGED_QTY, 0) + ISNULL(DBS.DEAL_TIM_QTY,0)) * ISNULL(DBS.TIER_CHARGE_RATE_PASSIVE,DBS.CHARGE_RATE_PASSIVE) END) 
													+ ISNULL(DBS.DEAL_EXTRA_COMMISSION,0))
							END)

						/* CDXEM */
						/* ################## THIS WILL USE EITHER THE TIERED RATE OR REGULAR RATE ###################### */
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'CDXEM'  THEN
							(CASE	WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission 
										ELSE ((CASE WHEN DEAL_IS_AGRESSIVE = 1 THEN ISNULL(DBS.AggressiveVol, 0) * ISNULL(DBS.TIER_CHARGE_RATE_AGGRESSIVE,DBS.CHARGE_RATE_AGGRESSIVE) 
															ELSE (ISNULL(DBS.DEAL_CHARGED_QTY, 0) + ISNULL(DBS.DEAL_TIM_QTY,0)) * ISNULL(DBS.TIER_CHARGE_RATE_PASSIVE,DBS.CHARGE_RATE_PASSIVE) END) 
													+ ISNULL(DBS.DEAL_EXTRA_COMMISSION,0))
							END)

						/* USFRN */
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'USFRN'  THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE ((ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE) + (ISNULL(DBS.PassiveVol, 0) * DBS.CHARGE_RATE_PASSIVE)) + ISNULL(DBS.DEAL_EXTRA_COMMISSION,0) END )
						WHEN DBS.BILLING_TYPE = 'Per_Trade' AND DBS.ProductGroup = 'USFRN' THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE ((ISNULL(DBS.AggressiveDeals, 0) * DBS.CHARGE_RATE_AGGRESSIVE) + (ISNULL(DBS.PassiveDeals, 0) * DBS.CHARGE_RATE_PASSIVE)) + ISNULL(DBS.DEAL_EXTRA_COMMISSION,0) END )

						/* NAVX */
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'NAVX' AND DBS.SWSecType = 'NAVXMINOR' THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE ISNULL(CT.NetMoney, 0) * (CASE WHEN DBS.AggressiveVol > 0 THEN DBS.CHARGE_RATE_AGGRESSIVE ELSE DBS.CHARGE_RATE_PASSIVE END) END )								
						WHEN DBS.BILLING_TYPE = 'Per_Contract' AND DBS.ProductGroup = 'NAVX' AND DBS.SWSecType <> 'NAVXMINOR' THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE ((CASE WHEN ISNULL(DBS.AggressiveVol, 0) > 0 THEN ISNULL(CT.Quantity/1000,DBS.AggressiveVol) ELSE ISNULL(DBS.AggressiveVol, 0) END * DBS.CHARGE_RATE_AGGRESSIVE) + (CASE WHEN ISNULL(DBS.PassiveVol, 0) > 0 THEN ISNULL(CT.Quantity/1000,DBS.PassiveVol) ELSE ISNULL(DBS.PassiveVol, 0) END * DBS.CHARGE_RATE_PASSIVE)) + ISNULL(DBS.DEAL_EXTRA_COMMISSION,0) END )
							
						/* BTIC GDB-99 009*/ 
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'BTIC' THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE ISNULL(CT.NetMoney, 0) * (CASE WHEN DBS.AggressiveVol > 0 THEN DBS.CHARGE_RATE_AGGRESSIVE ELSE DBS.CHARGE_RATE_PASSIVE END) END )

						/* OTR */
						/* SHIRISH 04/11/2016 - Updated OTR section for DWAS commission calculation
												using aggressive and passive charge rate constants for active streaming records 
						 * SHIRISH 05/04/2018 - After DWAS 2.5 go live, we will be charging commission for active stream records when a dealer is operator.  
												We will not be charging commission when a dealer is user
						*/
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'OTR'  THEN
							(CASE 
								  WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission 
								  ELSE	 (ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE) + (ISNULL(DBS.PassiveVol, 0) * DBS.CHARGE_RATE_PASSIVE) + ISNULL(DBS.DEAL_EXTRA_COMMISSION,0)
							 END)
						WHEN DBS.BILLING_TYPE = 'Per_Trade' AND DBS.ProductGroup = 'OTR' THEN
							(CASE 
								  WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission 
								  ELSE	 (ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE) + (ISNULL(DBS.PassiveVol, 0) * DBS.CHARGE_RATE_PASSIVE) + ISNULL(DBS.DEAL_EXTRA_COMMISSION,0)
							 END )

						/* EQSWP */
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'EQSWP' AND DBS.SWSecType = 'EQSWPETF' THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE (ISNULL(CT.NetMoney, 0) * ISNULL(DBS.DEAL_TENOR,0) * (CASE WHEN DBS.AggressiveVol > 0 THEN DBS.CHARGE_RATE_AGGRESSIVE ELSE DBS.CHARGE_RATE_PASSIVE END)) END)
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'EQSWP' AND DBS.SWSecType <> 'EQSWPETF' THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE (ISNULL(DBS.Quantity, 0) * @VolUnitMultiplier * ISNULL(DBS.DEAL_TENOR,0)  * (CASE WHEN DBS.AggressiveVol > 0 THEN DBS.CHARGE_RATE_AGGRESSIVE ELSE DBS.CHARGE_RATE_PASSIVE END)) END)

						/* OTC COMBO */
						WHEN DBS.BILLING_TYPE = 'Per_Contract' AND DBS.ProductGroup = 'COMBO' THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE DBS.Deal_Proceeds * DBS.CHARGE_RATE_AGGRESSIVE END )

						/* USREPO */
						-- SHIRISH 2016/08/29: Adding DEAL_PRICE to commission calculation
						-- SHIRISH 02/01/2017: adding ISNULL check for DEAL_PRICE as if price is NULL then CommissionOwed will be 0 instead of NULL
						-- SHIRISH 04/13/2023: IDBBC-234 For USREPOGC we need to calculate commission on allocation and not on shell
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'USREPO' AND (DBS.SWSecType <> 'USREPOGC' OR DBS.RepoRecordType = 'A') THEN
							((((ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE) + (ISNULL(DBS.PassiveVol, 0) * DBS.CHARGE_RATE_PASSIVE))) * DBS.DEAL_DAYS_TO_MATURITY * (ISNULL(DBS.DEAL_PRICE,100)/100) * 10000)/360							

						/* BOX */
						WHEN DBS.BILLING_TYPE = 'Per_Contract' AND DBS.ProductGroup = 'BOX'  THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE ((ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE) + (ISNULL(DBS.PassiveVol, 0) * DBS.CHARGE_RATE_PASSIVE)) + ISNULL(DBS.DEAL_EXTRA_COMMISSION,0) END )

						/* REVCON */
						WHEN DBS.BILLING_TYPE = 'Per_Contract' AND DBS.ProductGroup = 'REVCON'  THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE ((ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE) + (ISNULL(DBS.PassiveVol, 0) * DBS.CHARGE_RATE_PASSIVE)) + ISNULL(DBS.DEAL_EXTRA_COMMISSION,0) END )

						/* EUREPO */
						-- For EUREPOGC we need to calculate commission on allocation and not on shell
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'EUREPO' AND (DBS.SWSecType <> 'EUREPOGC' OR DBS.RepoRecordType = 'A') THEN
							(
								(
									(
										((ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE) + (ISNULL(DBS.PassiveVol, 0) * DBS.CHARGE_RATE_PASSIVE)) 
										* DBS.DEAL_DAYS_TO_MATURITY 
										--* (ISNULL(DBS.DEAL_PRICE,100)/100) 
										* 10000
									) / (CASE WHEN DBS.Security_Currency = 'GBP' THEN 365 ELSE 360 END)  -- £ uses a 365 day count convention for brokerage calculation
										-- Do not apply €2 per € ticket OR £2 per Sterling ticket when incremental tier rate is applied.  In that case charge rates will be set to 0.0
								) --+ (2 * (CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0.0 OR DBS.CHARGE_RATE_PASSIVE > 0.0 THEN 1 ELSE 0 END)) /* €2 per € ticket OR £2 per Sterling ticket on top of brokerage */
							) * DBS.ExchangeRateMultiplier -- SHIRISH 20230616: Calculated commission will be in EUR or GBP.  GBP commission needs to be converted to EUR							

						/* IDBBC-292: R8FIN-MATCH */
						/* MATCH */
						WHEN DBS.ProductGroup = 'MATCH' THEN (ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE)

						END, 2),
						


			/* NILESH 09/09/2013 -- Daily Caps */
			CommissionOwed_PreCap_AfterDailyCap = CAST(NULL AS FLOAT),
			
			CommissionOwed_PreCap_Override = ROUND(CASE 
						/* SHIRISH 2016/12/12: Do not calculate commission for SEC Fee billing codes */
						WHEN DBS.BILLING_CODE LIKE 'SECF%' OR DBS.BILLING_CODE LIKE 'CATF%' THEN CAST(NULL AS FLOAT)
						/* BILL */
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'BILL' THEN
							CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE ELSE 0 END
						WHEN DBS.BILLING_TYPE = 'Per_Trade' AND DBS.ProductGroup = 'BILL' THEN
							CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ISNULL(DBS.AggressiveDeals, 0) * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE ELSE 0 END
						/* IOS */
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'IOS'  THEN
							CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ISNULL(DBS.AggressiveVol, 0) * DBS.DEAL_O_FACTOR * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE ELSE 0 END
						WHEN DBS.BILLING_TYPE = 'Per_Trade' AND DBS.ProductGroup = 'IOS' THEN
							CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ISNULL(DBS.AggressiveDeals, 0) * DBS.DEAL_O_FACTOR * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE ELSE 0 END

						/* TRSY */
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'TRSY'  THEN
							--CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE ELSE 0 END
						--	(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE ((ISNULL(DBS.AggressiveVol, 0) * (DBS.CHARGE_RATE_AGGRESSIVE + CASE WHEN ISNULL(DBS.CHARGE_RATE_AGGRESSIVE,0)<> 0 THEN ISNULL(D.Discount_Amount,0) ELSE 0 END)) + (ISNULL(DBS.PassiveVol, 0) * DBS.CHARGE_RATE_PASSIVE)) + ISNULL(DBS.DEAL_EXTRA_COMMISSION,0) END )
						--WHEN DBS.BILLING_TYPE = 'Per_Trade' AND DBS.ProductGroup = 'TRSY' THEN
						--	CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ISNULL(DBS.AggressiveDeals, 0) * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE ELSE 0 END

							CASE 
								 WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission
								 -- 
								 --WHEN DBS.TradeType2 = 'OUTRIGHT' THEN 0 -- SHIRISH 06/02/2017: Need to revisit. Outright trades do not get discount rates
								 -- SHIRISH 06/05/2017: When Commission override is NULL then it will match Commission PreCap
								 WHEN DBS.TradeType2 = 'OUTRIGHT' OR (DBS.CHARGE_RATE_AGGRESSIVE > 0 AND ISNULL(D.Discount_Amount,0) = 0) THEN ISNULL(DBS.AggressiveVol, 0) * ISNULL(DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE,DBS.CHARGE_RATE_AGGRESSIVE) 
								 WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 AND ISNULL(D.Discount_Amount,0) <> 0 THEN ISNULL(DBS.AggressiveVol, 0) * (DBS.CHARGE_RATE_AGGRESSIVE + D.Discount_Amount)
								 ELSE 0 
							END

						/* GILTS */ -- DIT-10448
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'GILTS' AND DBS.Leg = 'PRI'  THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE ((ISNULL(DBS.AggressiveVol, 0) * (DBS.CHARGE_RATE_AGGRESSIVE + CASE WHEN ISNULL(DBS.CHARGE_RATE_AGGRESSIVE,0)<> 0 THEN ISNULL(D.Discount_Amount,0) ELSE 0 END)) + (ISNULL(DBS.PassiveVol, 0) * DBS.CHARGE_RATE_PASSIVE)) + ISNULL(DBS.DEAL_EXTRA_COMMISSION,0) END)

						/* TIPS */
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'TIPS'  THEN
							CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE ELSE 0 END
						WHEN DBS.BILLING_TYPE = 'Per_Trade' AND DBS.ProductGroup = 'TIPS' THEN
							CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ISNULL(DBS.AggressiveDeals, 0) * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE ELSE 0 END

						/* EFP */
						WHEN DBS.BILLING_TYPE = 'Per_Contract' AND DBS.ProductGroup = 'EFP'  THEN
							CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE ELSE 0 END
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'EFP' AND DBS.SWSecType = 'EFPVOL' THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE (ISNULL(CT.NetMoney, 0) * (CASE WHEN DBS.AggressiveVol > 0 THEN DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE ELSE 0 END)) END)
						/* IDBBC-11: Added the support for Instrument based Schedule */
						/* IDBBC-35: As requested by Jason starting 2/21/2020 we are using Deal_Proceeds from deal as notional to calculate commission for SRT trades as that matches blotter.  Original code used Net Money from Clearing Trades as notional */
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'EFP' AND DBS.SWSecType = 'EFPEFPMINI' THEN
							(CASE WHEN DBS.DealFinalCommission = 1 THEN DBS.DealCommission ELSE (ISNULL((CASE WHEN DBS.TradeDate >= '20200221' THEN DBS.Deal_Proceeds ELSE CT.NetMoney END), 0) * (CASE WHEN DBS.AggressiveVol > 0 THEN DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE ELSE DBS.CHARGE_RATE_PASSIVE_OVERRIDE END)) END) -- IDBBC-140

						/* EQSWP */
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'EQSWP' THEN
							CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE * @VolUnitMultiplier * ISNULL(DBS.DEAL_TENOR, 0) ELSE 0 END
						WHEN DBS.BILLING_TYPE = 'Per_Trade' AND DBS.ProductGroup = 'EQSWP' THEN
							CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ISNULL(DBS.AggressiveDeals, 0) * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE ELSE 0 END
						WHEN DBS.BILLING_TYPE = 'Per_Contract' AND DBS.ProductGroup = 'EQSWP'  THEN
							CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE ELSE 0 END

						/* ECDS */
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'ECDS'  THEN
							CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE /* * DBS.ExchangeRateMultiplier*/ ELSE 0 END	
						
						/* AMSWP */
						-- IDBBC-132 if UseGap is set then use Deal_GAP_DV01 else use Deal_Risk
						WHEN DBS.BILLING_TYPE = 'Delta' AND DBS.ProductGroup = 'AMSWP'  THEN
							CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN 
									ISNULL(DBS.AggressiveVol, 0) * (CASE WHEN DBS.UseGap = 1 THEN DBS.DEAL_GAP_DV01 ELSE CAST(ISNULL(DBS.DEAL_RISK, 0) AS FLOAT) END) * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE
								 ELSE 0 
							END

						/* CAD */
						WHEN DBS.BILLING_TYPE = 'Delta' AND DBS.ProductGroup = 'CAD'  THEN
							CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN 
									ISNULL(DBS.AggressiveVol, 0) * CAST(ISNULL(DBS.DEAL_RISK, 0) AS FLOAT) * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE
								 ELSE 0 
							END * DBS.ExchangeRateMultiplier

						/* UCDS */
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'UCDS'  THEN
							CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE ELSE 0 END	

						/* CDXEM */
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'CDXEM'  THEN
							CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE ELSE 0 END	

						/* USFRN */
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'USFRN'  THEN
							CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE ELSE 0 END
						WHEN DBS.BILLING_TYPE = 'Per_Trade' AND DBS.ProductGroup = 'USFRN' THEN
							CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ISNULL(DBS.AggressiveDeals, 0) * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE ELSE 0 END

						/* NAVX */
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'NAVX' AND DBS.SWSecType = 'NAVXMINOR' THEN
							CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ISNULL(CT.NetMoney, 0) * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE ELSE 0 END
						WHEN DBS.BILLING_TYPE = 'Per_Contract' AND DBS.ProductGroup = 'NAVX' AND DBS.SWSecType <> 'NAVXMINOR' THEN
							CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ISNULL(CT.Quantity/1000,DBS.AggressiveVol) * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE ELSE 0 END

						/* BTIC GDB-99 */
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'BTIC' THEN
							CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ISNULL(CT.NetMoney, 0) * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE ELSE 0 END

						/* OTR */
						/* SHIRISH 05/21/2019: IDB-18449 Updating below commission calculation to use both aggressive and passive volume and rates */
						/* SHIRISH 07/02/2020: IDBBC-76 When calculating override commission we need to check if override rate is > 0*/
						/* SHIRISH 10/27/2021: GDB-1628 Also need to check if Charge_Rate_Passive is > 0.  Else for passive trades where we only have charge rate passive below code will not calculate commission */
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'OTR' THEN
							CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE > 0 OR DBS.CHARGE_RATE_PASSIVE_OVERRIDE > 0 THEN (ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE) + (ISNULL(DBS.PassiveVol, 0) * DBS.CHARGE_RATE_PASSIVE_OVERRIDE) + ISNULL(DBS.DEAL_EXTRA_COMMISSION,0) ELSE 0 END
						WHEN DBS.BILLING_TYPE = 'Per_Trade' AND DBS.ProductGroup = 'OTR' THEN
							CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE > 0 OR DBS.CHARGE_RATE_PASSIVE_OVERRIDE > 0 THEN (ISNULL(DBS.AggressiveDeals, 0) * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE) + (ISNULL(DBS.PassiveVol, 0) * DBS.CHARGE_RATE_PASSIVE_OVERRIDE) + ISNULL(DBS.DEAL_EXTRA_COMMISSION,0) ELSE 0 END

						/* OTC COMBO */
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'COMBO'  THEN 
							CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE ELSE 0 END
							
						/* USREPO */
						/* IDBBC-234 for USREPOGC commissions are calculated on allocations */
						WHEN DBS.BILLING_TYPE = 'Per_MM' AND DBS.ProductGroup = 'USREPO' AND (DBS.SWSecType <> 'USREPOGC' OR DBS.RepoRecordType = 'A') THEN
							CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE ELSE 0 END

						/* BOX */
						WHEN DBS.BILLING_TYPE = 'Per_Contract' AND DBS.ProductGroup = 'BOX'  THEN
							CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE ELSE 0 END

						/* REVCON */
						WHEN DBS.BILLING_TYPE = 'Per_Contract' AND DBS.ProductGroup = 'REVCON'  THEN
							CASE WHEN DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ISNULL(DBS.AggressiveVol, 0) * DBS.CHARGE_RATE_AGGRESSIVE_OVERRIDE ELSE 0 END

						END, 2),
											
			CommissionOwed = NULL,
			ExchangeRateMultiplier = DBS.ExchangeRateMultiplier,
			
			/* NILESH 08/01/2013 -- Tiered Billing */
			TradeDate,
			TIER_BILLING_PLAN_ID,
			TIER_CHARGE_RATE_AGGRESSIVE,
			TIER_CHARGE_RATE_PASSIVE,
			Instrument = DBS.Instrument,
			Deal_Way = DBS.DEAL_WAY,
			ClearingQty = CT.Quantity,
			EstimatedRate = CT.DecPrice, -- only applicable to NAVX
			DBS.DEAL_NEGOTIATION_ID,
			/* SHIRISH - 04/21/2016 - 
			 * Adding DWAS OTR active stream column.  Only Mark Y for liquidity providers (Passive Side).  Dealers on agressive side are takers and are not charged 
			 * This column will add a record into CommissionSummary with Active_Stream = 'Y' for Liquidity providers.
			 * In case liquidity providers also do regular OTR trades then CommissionSummary will have two records.  One with Active_Stream = Y and other with N
			 */
			DBS.IsActiveStream, --= CASE WHEN DBS.ProductGroup = 'OTR' AND ISNULL(DBS.Active_Stream,'N') = 'Y' THEN 'Y' ELSE 'N' END,
-------------------------------------------
			Is_OFTRDiscountRate = CASE WHEN D.Dealer = DBS.Dealer -- Vipin: 20170404
											AND DBS.DEAL_USER_ID = DBS.DEAL_FIRST_AGGRESSOR
											AND D.Billing_Code = DBS.BILLING_CODE 
											AND D.ProductGroup = DBS.ProductGroup
											AND (D.SWSecType IS NULL OR (D.SWSecType) = DBS.SWSecType)
											AND (DBS.DEAL_DAYS_TO_MATURITY BETWEEN MaturityFrom and MaturityTo)
											AND D.Source = CASE WHEN DBS.Source = 'RC' THEN 'V' ELSE DBS.Source END
											AND DBS.TradeDate Between Effective_Startdate and Effective_Enddate
											AND DBS.DEAL_IS_AGRESSIVE = 1
									   THEN 1
									   ELSE 0
								  END,
			Discount_Amount			= ISNULL(D.Discount_Amount,0),	/* Currently only valid for TRSY as of 03/01/2017 */
			Trader = DBS.DEAL_USER_ID,
			DEAL_REPO_START_DATE,
			DBS.DEAL_ID,
			DBS.TradeType2,
			DEAL_DATE,
			DBS.DEAL_SECURITY_ID,	/* DIT - 10159 */
			DBS.DEAL_IS_AGRESSIVE,
			DBS.Leg,	/* DIT-11311 */
			/* DIT-11179 - Calculation of commission for the fixed fee clients */
			FF_CommissionOwed_PreCap = CASE WHEN IFFCR.FixedFeeCommRate IS NOT NULL AND (ISNULL(DBS.AggressiveVol, 0)) > 0 AND DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ROUND(IFFCR.FixedFeeCommRate * DBS.AggressiveVol,2) ELSE CAST(NULL AS FLOAT) END,
			FF_CommissionOwed_PreCap_Override = CASE WHEN IFFCR.FixedFeeCommRate IS NOT NULL AND (ISNULL(DBS.AggressiveVol, 0)) > 0 AND DBS.CHARGE_RATE_AGGRESSIVE > 0 THEN ROUND(IFFCR.FixedFeeCommRate * DBS.AggressiveVol,2) ELSE CAST(NULL AS FLOAT) END,
			AnonymousLevel,	/* IDBBC-60 */
			BilateralStream, -- IDBBC-71
			IsStdTickInc, -- IDBBC-74
			DBS.Dealer, -- IDBBC-108 Adding dealer as we need to save dealer in NAVXCommissionAdjustments table
			CT.ContraDealer, -- IDBBC-108 Need to save this inforamation in NAVXCommissionAdjustments
			CT.ContraUserId, -- IDBBC-108 Need to save this inforamation in NAVXCommissionAdjustments
			DBS.UseGap,
			DBS.DEAL_GAP_DV01,
			CT.NetMoney,
			CT.Quantity,
			DBS.RepoRecordType, -- IDBBC-234
			dbs.Security_Currency, -- IDBBC-310
			/* IDBBC-330 When Quantity is -ve then this is Reversed trade.  Apply -ve RepoTicketFees so it cancels out with fees for original trade */
			RepoTicketFees = (CASE WHEN DBS.ProductGroup = 'EUREPO' THEN 2 * DBS.ExchangeRateMultiplier END) * (CASE WHEN DBS.Quantity < 0 THEN -1 ELSE 1 END), -- IDBBC-310
			DBS.Instrument_ETF -- IDBBC-324


		FROM	IDB_Billing.dbo.wDealBillingSchedule DBS (NOLOCK) -- SHIRISH 11/6/2014 -- updating query to use permanent table
		JOIN IDB_CodeBase.dbo.fnProductType() fp ON DBS.ProductGroup = fp.Product
		/* 2015-02-18 - SHIRISH - Below join is used to get NetMoney for commission calculation for EQSWP */
		/* 2015/03/04 - SHIRISH - Using GROUP BY/SUM to eliminate this join returning more than one record for any deal
								  If this join returns more than one record for a deal it causes calculated volume to be incorrect */
		/* 2018-08-22 - Nilesh - Modified the below query to remove the sum and match the individual CTs, This was to support NAVX Floor commission requirement for trades with multiple counter parties */
		OUTER APPLY
		(
			/* SHIRISH 05/20/2019: DIT-11041
			 * Adding a UNION join for NAVX trades to use #CTForNAVXCommission instead of #ClearingTrades as we need to use trade date instead of report date for commission calculation
			 */
			SELECT	DecPrice, Quantity, NetMoney, ContraDealer, ContraUserId -- IDBBC-108 
			FROM	#CTForNAVXCommission #CTN
			WHERE	#CTN.Deal_ID = DBS.Deal_Negotiation_ID
			AND		(#CTN.Deal_Trd_ID = DBS.Deal_Id OR #CTN.Deal_Trd_ID = DBS.DEAL_ORIG_DID)	/* NILESH 08/22/2018 */
			AND		#CTN.BILLING_CODE = DBS.BILLING_CODE
			AND		DBS.ProductGroup IN ('NAVX','BTIC')	/* GDB-99 */
			AND		#CTN.Source = (CASE WHEN DBS.Source = 'RC' THEN 'V' ELSE DBS.Source END)
			/* NILESH OTR FICC Changes */
			AND		ISNULL(#CTN.IsActiveStream,0) = ISNULL(DBS.IsActiveStream,0)
			AND		ISNULL(#CTN.Trader,'') = ISNULL(DBS.DEAL_USER_ID,'')

			UNION ALL

			SELECT	DecPrice, Quantity, NetMoney, ContraDealer = NULL, ContraUserId = NULL -- IDBBC-108
			FROM	#ClearingTrades CT
			WHERE	Cancelled = 0
			AND		ProductGroup ='EQSWP' -- SHIRISH 05/20/2019: DIT-11041 Removing NAVX from this condtion as we now have separate NAVX block
			AND		CT.Deal_ID = DBS.Deal_Negotiation_ID
			AND		(CT.Deal_Trd_ID = DBS.Deal_Id OR CT.Deal_Trd_ID = DBS.DEAL_ORIG_DID)	/* NILESH 08/22/2018 */
			AND		CT.BILLING_CODE = DBS.BILLING_CODE
			AND		CT.ProductGroup = DBS.ProductGroup
			AND		CT.Source = (CASE WHEN DBS.Source = 'RC' THEN 'V' ELSE DBS.Source END)
			/* NILESH OTR FICC Changes */
			AND		ISNULL(CT.IsActiveStream,0) = ISNULL(DBS.IsActiveStream,0)
			AND		ISNULL(CT.Trader,'') = ISNULL(DBS.DEAL_USER_ID,'')

			-- SHIRISH 03/07/2019: IDB-18432: We found a defect that UNION will only return unique records and is eliminating some of the clearing trades. Updating to UNION ALL to eliminate this issue
			UNION ALL /*IDB-18432: Adding UNION to summerize data for EFP. In cases when there are multiple counter parties the quantity gets doubled.
			      The NAVX query does not need to be grouped as we need to apply minimum commission on each counterparty leg.*/

			SELECT	DecPrice, Quantity = SUM(Quantity), NetMoney = SUM(NetMoney), ContraDealer = NULL, ContraUserId = NULL -- IDBBC-108 Using NULL for contra dealer as we don't want to create multiple records
			FROM	#ClearingTrades CT
			WHERE	Cancelled = 0
			AND		( ProductGroup ='EFP' AND SWSecType IN ('EFPVOL','EFPEFPMINI')) -- IDBBC-11 SHIRISH 12/04/2019 Need net money for SRT instruments for commission calculation
			AND		CT.Deal_ID = DBS.Deal_Negotiation_ID
			AND		(CT.Deal_Trd_ID = DBS.Deal_Id OR CT.Deal_Trd_ID = DBS.DEAL_ORIG_DID)	/* NILESH 08/22/2018 */
			AND		CT.BILLING_CODE = DBS.BILLING_CODE
			AND		CT.ProductGroup = DBS.ProductGroup
			AND		CT.Source = (CASE WHEN DBS.Source = 'RC' THEN 'V' ELSE DBS.Source END)
			/* NILESH OTR FICC Changes */
			AND		ISNULL(CT.IsActiveStream,0) = ISNULL(DBS.IsActiveStream,0)
			AND		ISNULL(CT.Trader,'') = ISNULL(DBS.DEAL_USER_ID,'')
			GROUP BY DecPrice
		) CT
		-- SHIRISH 06/06/2017: This needs to be reviewed and discount rates need to be ranked to get it to work properly
	   LEFT JOIN #IDB_OFTRDiscountRate D on D.Dealer = DBS.Dealer -- Vipin: 20170404
										AND DBS.DEAL_USER_ID = DBS.DEAL_FIRST_AGGRESSOR
										AND D.Billing_Code = DBS.BILLING_CODE 
										AND D.ProductGroup = DBS.ProductGroup
										AND ((D.SWSecType IS NULL AND DBS.SWSecType <> 'TRSYBASIS') OR (D.SWSecType = DBS.SWSecType))
										AND DBS.DEAL_DAYS_TO_MATURITY BETWEEN MaturityFrom and MaturityTo
										AND D.Source = CASE WHEN DBS.Source = 'RC' THEN 'V' ELSE DBS.Source END
										AND DBS.TradeDate Between Effective_Startdate and Effective_Enddate
										AND DBS.DEAL_IS_AGRESSIVE = 1
		--LEFT JOIN (	SELECT	Deal_ID,Billing_Code,ProductGroup,Source,DecPrice,Quantity, SUM(NetMoney) NetMoney
		--			FROM	#ClearingTrades 
		--			WHERE	Cancelled = 0
		--			AND		ProductGroup IN ('EQSWP','NAVX')
		--			GROUP BY Deal_ID,Billing_Code,ProductGroup,Source,DecPrice,Quantity) CT ON DBS.Deal_Negotiation_ID = CT.Deal_ID
		--																AND DBS.BILLING_CODE = CT.BILLING_CODE
		--																AND CT.ProductGroup = DBS.ProductGroup
		--																AND (CASE WHEN DBS.Source = 'RC' THEN 'V' ELSE DBS.Source END) = CT.Source
		/* DIT-11179 - Fixed fee table calculated EOD nightly based on aggressive volume */
		LEFT JOIN IDB_Reporting.dbo.IDB_FixedFee_CommRate AS IFFCR ON DBS.Dealer = IFFCR.Dealer 
																	AND DBS.BILLING_CODE = IFFCR.Billing_Code 
																	AND CASE WHEN DBS.Dealer = 'GS' AND DBS.ProductGroup = 'USFRN' THEN 'TRSY' ELSE DBS.ProductGroup END = IFFCR.ProductGroup -- IDBBC-92 For GS USRN is part of TRSY fixed fees.  We need to apply TRSY fixed fee rate to USFRN
																	AND (DBS.TradeDate BETWEEN IFFCR.PeriodStartDate AND IFFCR.PeriodEndDate)

		/* NOT INCLUDING UCDS SINCE IT USES THEH DAILY CAP AND HAS TO BE PROCESSED DIFFERENTLY */
		--WHERE	DBS.ProductGroup IN ('BILL', 'IOS', 'TRSY', 'TIPS', 'EFP', 'ECDS', 'AMSWP', 'UCDS', 'CDXEM') --Select specific products
		WHERE	fp.Billable = 2
		--AND fp.UsesDailyCapForBilling = 'N'
		AND		fp.Product <> 'AGCY'
		AND		DBS.PROCESS_ID = @ProcessID
		
		/*This order by is to get the correct deals that need to be considered for commission cap*/
		ORDER BY InvNum ,
			InvDbId,
			BILLING_CODE,
			PeriodId,
			ProductGroup,
			DEAL_DATE,
			DEAL_ID


		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After DBS_AllProd',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/* This was done as the amount were rounded of to 2 decimal so when reporting in EURO the conversion did not happen correctly. */
		UPDATE IDB_Billing.dbo.wDBS_AllProd
		SET	CommissionOwed_PreCap = CommissionOwed_PreCap * ExchangeRateMultiplier,
			CommissionOwed_PreCap_Override = CommissionOwed_PreCap_Override * ExchangeRateMultiplier
		WHERE	PROCESS_ID = @ProcessID
		AND		ProductGroup IN ('ECDS','GILTS') -- SHIRISH 09/12/2019: GDB-186 Need to convert to USD when we store in GDB

		/* SHIRISH 06/12/2018: As per EFP team going forward we are going to charge min $50 commission on each trade.
		 *					   Updating commission to $50 if commission is less than $50
		 * NILESH 08/10/2018: This was commented as the live date was decided. However uncommenting since it is now live
		   as of 8/9/2018.
		 * SHIRISH 08/15/2018: As per Jason Spire (SPX1) needs to be excluded from this block
		 */
		 /*NILESH 09/17/2018: Added WFS2 and WFSD1 as per Jason to be excluded from this rule. */
		-- Shirish 04/19/2022: IDBBC-176 Added TRC1 to exclude rule as requested by Jason
		-- Shirish 08/08/2024: IDBBC-324 Moving EFP/NAVX floor charges to a table instead of hard coding

		UPDATE	DAP
		SET		CommissionOwed_PreCap = ECF.CommissionFloor
		FROM	IDB_Billing.dbo.wDBS_AllProd DAP (NOLOCK)
		JOIN	IDB_Billing.dbo.IDBEquityCommissionFloors ECF (NOLOCK) ON DAP.TradeDate BETWEEN ECF.EffectiveStartDate AND ECF.EffectiveEndDate
																	  AND ECF.Productgroup = DAP.ProductGroup
																	  AND (ECF.Instrument = ISNULL(DAP.Instrument_ETF,DAP.Instrument) OR ECF.Instrument IS NULL)
																	  AND (ECF.BillingCode = DAP.BILLING_CODE OR ECF.BillingCode IS NULL)
																	  AND ECF.CommissionFloor > DAP.CommissionOwed_PreCap
		LEFT JOIN IDB_Billing.dbo.IDBEquityCommissionFloorExceptions EXC (NOLOCK) ON DAP.TradeDate BETWEEN EXC.EffectiveStartDate AND EXC.EffectiveEndDate
																				 AND EXC.ProductGroup = DAP.ProductGroup
																				 AND (EXC.Instrument = ISNULL(DAP.Instrument_ETF,DAP.Instrument) OR EXC.Instrument IS NULL)
																				 AND EXC.BillingCode = DAP.BILLING_CODE
		WHERE	DAP.PROCESS_ID = @ProcessID
		AND		EXC.BillingCode IS NULL


		IF @Owner = 'UK'
		BEGIN
			UPDATE	dap
			SET		dap.CommissionOwed_PreCap = eoc.OverrideCommission,
					dap.CHARGE_RATE_AGGRESSIVE = eoc.AggrRate,
					dap.CHARGE_RATE_PASSIVE = eoc.PassRate
			FROM	IDB_Billing.dbo.wDBS_AllProd dap
			JOIN	#EUREPOOverrideCommission eoc ON eoc.Deal_Trade_Date = dap.TradeDate
												 AND dap.ProductGroup = 'EUREPO' -- moved from where clause
												 AND eoc.Deal_Id = dap.DEAL_ID
			WHERE	PROCESS_ID = @ProcessID

			IF @ReportMode = 0
				INSERT INTO #RepoTicketFeesByInvoice
				(
				    InvNum,
				    InvDbId,
				    Billing_Code,
				    PeriodId,
				    ProductGroup,
					Source,
					ChargeId,
					Volume,
				    TicketFees
				)
				SELECT	DAP.InvNum,
						DAP.InvDbId,
						DAP.BILLING_CODE,
						DAP.PeriodId,
						DAP.ProductGroup,
						DAP.Source,
						CT.ChargeId,
						Volume = COUNT(*),
						TicketFees = SUM(DAP.RepoTicketFees)
				FROM	IDB_Billing.dbo.wDBS_AllProd DAP (NOLOCK)
				JOIN	#ChargeType CT ON CT.ChargeType = 'EUREPO Fees'
				WHERE	DAP.PROCESS_ID = @ProcessID
				AND		DAP.ProductGroup = 'EUREPO'
				GROUP BY
						DAP.InvNum,
						DAP.InvDbId,
						DAP.BILLING_CODE,
						DAP.PeriodId,
						DAP.ProductGroup,
						DAP.Source,
						CT.ChargeId

		END

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After DBS_AllProd Update',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/* SHIRISH 10/29/2019: IDBBC-7 Volume based commission calculation */
		-- IDBBC-103 Adding full column list for insert
		IF (@ReportMode = 0 OR @Debug = 1) -- Rebate needs to be calculated only in invoice mode
			INSERT INTO #VolumeBasedCommission
			(
				InvNum,
				InvDbId,
				BillingCode,
				ProductGroup,
				PlatformTotal,
				PlatFormVolumeDescription,
				AggrVol,
				PassVol,
				PassiveComm,
				TotalComm,
				DWASVol,
				OperatorVol,
				BilateralVol, -- IDBBC-103
				InsertUpdate
			)
			EXEC IDB_Billing.dbo.GetVolumeBasedCommission @BillingCode,@InvDate

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After GetVolumeBasedCommission',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/* END Volume based commission Calculation */

		/* SHIRISH 05/11/2018: TMG is providing liquidity (passive) and they get a rebate.  As TMG is passive on these trades 
							   We need to take this rebate amount out from Aggressive commission on the trade.  If there are multiple 
							   dealers on aggressive side then we need to take out percentage of rebate from their commission based on 
							   their percentage of aggressive quantity */
		/* DIT 9920 Modified the below query to implement the new rebate scheme for TMG */

		-- SHIRISH 03/12/2019: DIT-9920 Getting quarter start date, then notional traded volume for TMG and AT for the current quarter and weather quarterly volume discount condition is met.
		--					   IF TMG and AT combined volume for the quarter gets to 40B then they get additional discount
		-- SHIRISH 05/02/2019: DIT-9920 We found issues with the original implementation of TMG rebate code.  So re-writing code to fix errors in rebate calculation
		-- IDBBC-159 Effective 1/1/2022, TMG/AT rebates for OFTR TRSY Trades are being turned off
		IF (
				@date2 <= '20211231' 
				AND (@ProductGroup IS NULL OR @ProductGroup = 'TRSY')
				AND (@BillingCode IS NULL OR @BillingCode IN ('TMG1','AT1'))
		   )
		BEGIN

			DECLARE @qtrStartDate DATETIME, @trsyNotionalVolume INT, @metQtrVolumeDiscount int
		
			SET @qtrStartDate = DATEADD(qq, DATEDIFF(qq, 0, @date2), 0)
		
			SELECT	@trsyNotionalVolume = SUM(Quantity)
			FROM	IDB_Reporting.dbo.IDB_DEAL (NOLOCK) 
			WHERE	TRADEDATE BETWEEN @qtrStartDate AND @date2
			AND		PRODUCTGROUP = 'TRSY'
			AND		DEALER IN ('TMG','AT')

			SET @metQtrVolumeDiscount = CASE WHEN (@trsyNotionalVolume/1000)  >= 40 THEN 1 ELSE 0 END

			--IF @Debug=1
			--	SELECT 'QtrStartDate', @qtrStartDate, 'TRSYNotionalVolume', @trsyNotionalVolume, '@metQtrVolumeDiscount', @metQtrVolumeDiscount

			-- TMG Rebate section
			-- SHIRISH 07/22/2019 DIT-18425
			-- Creating a temp table with data from IDB_Falcon_Deals so we don't have to keep refering to permanent table which causes proc to run very slow
			SELECT	IFD.DEAL_NEGOTIATION_ID,
					IFD.DEAL_ID,
					IFD.DEAL_QUANTITY,
					IFD.DEAL_IS_AGRESSIVE,
					IFD.DEAL_SECURITY_ID,
					IFD.ProductGroup,
					IFD.Dealer,
					IFD.LinkIDVrt,
					IFD.DEAL_LEG_NUM

			INTO	#IDB_FALCON_DEALS

			FROM	IDB_Reporting.dbo.IDB_FALCON_DEALS AS IFD (NOLOCK)
			WHERE	IFD.DEAL_TRADE_DATE BETWEEN @date1 AND @date2
			AND		IFD.ProductGroup = 'TRSY'
			AND		IFD.DEAL_STATUS <> 'CANCELLED'
			AND		IFD.DEAL_SYNTHETIC = 0
			AND		IFD.DEAL_LAST_VERSION = 1

			-- SHIRISH 05/02/2019: DIT-9920 Getting total TMG rebate on each trade.  If trade is part of cross deal then use cross deal id so we get 1 record for each deal
			SELECT	DEAL_NEGOTIATION_ID = ISNULL(CAST(XD.CrossDealId AS VARCHAR(255)), DA.DEAL_NEGOTIATION_ID),
					DA.TradeDate,
					DA.ProductGroup,
					Rebate= SUM(CASE 
									WHEN DA.TradeDate BETWEEN '20180601' AND '20181130' THEN D.DEAL_QUANTITY * 5 
									WHEN DA.TradeDate > '20181130' THEN 
																		CASE ISNULL(SI.SWAPBOX,SM2.issued_as/12)   --Determine the SWAP BOX of the On-THE-RUN security on the Hedge Leg
																		
																			WHEN 2 THEN /* 2-Year SWAP BOX */
																				CASE	 WHEN ISNULL(SI.OnTheRun,SM.on_the_run) = -1 OR ISNULL(SI.OnTheRun,SM.on_the_run) = 2 THEN D.DEAL_QUANTITY * (CASE @metQtrVolumeDiscount WHEN 1 THEN 2.5 ELSE 2 END) --When Issued and Old 2 Years
																						WHEN ISNULL(SI.OnTheRun,SM.on_the_run) = 0 OR ISNULL(SI.OnTheRun,SM.on_the_run) > 2 THEN D.DEAL_QUANTITY * (CASE @metQtrVolumeDiscount WHEN 1 THEN 3.5 ELSE 3 END) --Beginning with the second issue out from the active issue
																				END
																		
																			ELSE /* 3-30 Year Swap Box */
																				CASE	 WHEN ISNULL(SI.OnTheRun,SM.on_the_run)= -1 OR ISNULL(SI.OnTheRun,SM.on_the_run) = 2 THEN D.DEAL_QUANTITY * (CASE @metQtrVolumeDiscount WHEN 1 THEN 3.5 ELSE 3 END) --When Issued and Old 2 Years
																						WHEN ISNULL(SI.OnTheRun,SM.on_the_run) = 0 OR ISNULL(SI.OnTheRun,SM.on_the_run) > 2 THEN D.DEAL_QUANTITY * (CASE @metQtrVolumeDiscount WHEN 1 THEN 5.5 ELSE 5 END) --Beginning with the second issue out from the active issue
																				END
																		END
								END),
					TotalAggressiveQuantity = CAST(0 AS FLOAT),
					Issued_as = ISNULL(SI.Issued_As,SM2.issued_as), --SM2.issued_as,
					on_the_run = ISNULL(SI.OnTheRun,SM.on_the_run) --SM.on_the_run

			INTO	#TMGRebate
			FROM	IDB_Billing.dbo.wDBS_AllProd DA (NOLOCK)
			JOIN	#IDB_FALCON_DEALS AS D (NOLOCK) ON DA.DEAL_ID = D.DEAL_ID
			JOIN	Instrument.dbo.Security_Master AS SM WITH (NOLOCK) ON D.DEAL_SECURITY_ID = SM.instrid
			JOIN	Instrument.dbo.Security_Type AS ST WITH (NOLOCK) ON SM.sec_type_id = ST.sec_type_id AND ST.product_grp = D.ProductGroup
			/* DIT 9920 - Below two joins are to get the SWAP box from the Hedge Leg */
			JOIN	#IDB_FALCON_DEALS AS DH (NOLOCK) ON D.DEAL_NEGOTIATION_ID = DH.DEAL_NEGOTIATION_ID And D.LinkIDVrt = DH.LinkIDVrt AND D.DEALER = DH.DEALER AND DH.DEAL_LEG_NUM = 2
			JOIN	Instrument.dbo.Security_Master AS SM2 (NOLOCK) ON DH.DEAL_SECURITY_ID = SM2.instrid
			/* SHIRISH 05/08/2019 - DIT-9920 - We have created a table to store Issued_as and SwapBox information at the time of the trade as securities can roll over and values can change.  
											   Using join to this table to make sure we use correct values to calculate rebate */
			LEFT JOIN IDB_Reporting.dbo.IDB_OFTR_CREDITS_SECINFO SI (NOLOCK) ON SI.DEAL_TRADE_DATE = DA.TradeDate AND SI.DEAL_ID = DA.DEAL_ID
			/* 
			-- DIT 9920
			-- The below join with CrossDealsForBrokerCommission will help us determine IF ANY TMG trade was matched 
			-- with a different trade and if so we will use the CtrossDealId to get aggtressive side trades.
			*/
			LEFT JOIN IDB_Reporting.dbo.CrossDealsForBrokerCommission XD WITH (NOLOCK) ON DA.TradeDate = XD.TradeDate AND DA.ProductGroup = XD.ProductGroup AND DA.DEAL_NEGOTIATION_ID = XD.Deal_Negotiation_Id 

			WHERE	DA.PROCESS_ID = @ProcessID
			AND		DA.ProductGroup = 'TRSY'
			AND		D.Dealer IN ('TMG')
			AND		D.DEAL_IS_AGRESSIVE = 0	/* Rebate only applicable on the passive trades by TMG */

			--Exclude hedge legs for rebate calculations
			AND		CAST(DA.Charge_Rate_Aggressive as FLOAT) > 0

			--DIT 9920 Added Date switch for change in rebate schemes
			AND		(
						(DA.TradeDate BETWEEN '20180601' AND '20181130' AND (SM.on_the_run >= 5 OR SM.on_the_run = 0) AND	DA.TradeType2 NOT IN ('TRSYROLL'))
						OR
						(DA.TradeDate > '20181130')
					)
			GROUP BY 
					ISNULL(CAST(XD.CrossDealId AS VARCHAR(255)), DA.DEAL_NEGOTIATION_ID),
					DA.TradeDate,
					DA.ProductGroup,
					SM2.issued_as,
					SM.on_the_run,
					SI.Issued_As,
					SI.SwapBox,
					SI.OnTheRun

			--IF @Debug=1
			--	SELECT '#TMGRebate', * FROM #TMGRebate WHERE DEAL_NEGOTIATION_ID = '464E02640000022F'

			-- SHIRISH 05/02/2019: DIT-9920 Need to update #TMGRebate table with total aggressive quantity on the deal so rebate and be distributed in all aggressive dealers according to their quantity.
			UPDATE	r
			SET		r.TotalAggressiveQuantity = COALESCE(CD.Quantity,SD.Quantity,1) 
			FROM	#TMGRebate r
			-- Getting agressive quantity of cross deal
			OUTER APPLY 
			(
				SELECT	Quantity = SUM(D.DEAL_QUANTITY)
				FROM	IDB_Reporting.dbo.CrossDealsForBrokerCommission XD (NOLOCK)
				JOIN	IDB_Billing.dbo.wDBS_AllProd DA (NOLOCK) ON DA.PROCESS_ID = @ProcessID
														AND XD.TradeDate = DA.TradeDate
														AND XD.ProductGroup = DA.ProductGroup
														AND XD.Deal_Negotiation_Id = DA.DEAL_NEGOTIATION_ID 
				JOIN	#IDB_FALCON_DEALS AS D (NOLOCK) ON DA.DEAL_ID = D.DEAL_ID -- IDB-18425
				WHERE	XD.TradeDate = r.TradeDate
				AND		XD.ProductGroup = r.ProductGroup
				AND		XD.CrossDealId = r.DEAL_NEGOTIATION_ID
				AND		CAST(DA.CHARGE_RATE_AGGRESSIVE AS FLOAT) > 0
				AND		D.DEAL_IS_AGRESSIVE = 1
				AND		D.Dealer <> 'TMG'
			) CD 
			-- Getting agressive quantity of a regular deal
			OUTER APPLY
			(
				SELECT	Quantity = SUM(D.DEAL_QUANTITY)
				FROM	IDB_Billing.dbo.wDBS_AllProd DA (NOLOCK)
				JOIN	#IDB_FALCON_DEALS AS D (NOLOCK) ON DA.DEAL_ID = D.DEAL_ID  -- IDB_18425
				WHERE	DA.PROCESS_ID = @ProcessID
				AND		DA.TradeDate = r.TradeDate
				AND		DA.ProductGroup = r.ProductGroup
				AND		DA.DEAL_NEGOTIATION_ID = r.DEAL_NEGOTIATION_ID
				AND		CAST(DA.CHARGE_RATE_AGGRESSIVE AS FLOAT) > 0
				AND		D.DEAL_IS_AGRESSIVE = 1
				AND		D.Dealer <> 'TMG'
			) SD

			--IF @Debug=1
			--	SELECT '#TMGRebate after AggrQtyUpdate', * FROM #TMGRebate WHERE DEAL_NEGOTIATION_ID = '464E02640000022F'

			-- SHIRISH 05/02/2019: Apply rebate to each dealer on aggressive side.
			-- For cross deal we need to get deal_nego_id of all the dealers on aggressive side		
			;WITH TMG_REBATE
			AS
			(
				SELECT DEAL_NEGOTIATION_ID = ISNULL(CD.Deal_Negotiation_Id,r.DEAL_NEGOTIATION_ID),
					   r.TradeDate,
					   r.ProductGroup,
					   r.Rebate,
					   r.TotalAggressiveQuantity
				FROM	#TMGRebate r
				OUTER APPLY 
				(
					SELECT	XD.Deal_Negotiation_Id
					FROM	IDB_Reporting.dbo.CrossDealsForBrokerCommission XD (NOLOCK)
					JOIN	IDB_Billing.dbo.wDBS_AllProd DA ON DA.PROCESS_ID = @ProcessID
															AND XD.TradeDate = DA.TradeDate
															AND XD.ProductGroup = DA.ProductGroup
															AND XD.Deal_Negotiation_Id = DA.DEAL_NEGOTIATION_ID 
					JOIN	#IDB_FALCON_DEALS AS D (NOLOCK) ON DA.DEAL_ID = D.DEAL_ID -- IDB-18425
					WHERE	XD.TradeDate = r.TradeDate
					AND		XD.ProductGroup = r.ProductGroup
					AND		XD.CrossDealId = r.DEAL_NEGOTIATION_ID
					AND		CAST(DA.CHARGE_RATE_AGGRESSIVE AS FLOAT) > 0
					AND		D.DEAL_IS_AGRESSIVE = 1
					AND		D.Dealer <> 'TMG'
				) CD 
			)
			UPDATE	DA
			SET		DA.CommissionOwed_PreCap_Override = (CASE WHEN ISNULL(DA.CommissionOwed_PreCap_Override,0) > 0 THEN DA.CommissionOwed_PreCap_Override ELSE DA.CommissionOwed_PreCap END) - (TR.Rebate * (DA.AggressiveVolume/TR.TotalAggressiveQuantity)),
					/* DIT-11179 : Commission for Fixed fee clients */
					DA.FF_CommissionOwed_PreCap_Override = (CASE WHEN ISNULL(DA.FF_CommissionOwed_PreCap_Override,0) > 0 THEN DA.FF_CommissionOwed_PreCap_Override ELSE DA.FF_CommissionOwed_PreCap END) - (TR.Rebate * (DA.AggressiveVolume/TR.TotalAggressiveQuantity))
			FROM	IDB_Billing.dbo.wDBS_AllProd DA (NOLOCK)
			JOIN	TMG_REBATE TR ON TR.TradeDate = DA.TradeDate
								  AND TR.ProductGroup = DA.ProductGroup
								  AND TR.DEAL_NEGOTIATION_ID = DA.DEAL_NEGOTIATION_ID
								  AND DA.AggressiveTrades = 1
								  AND CAST(DA.Charge_Rate_Aggressive as float) > 0
			WHERE	DA.PROCESS_ID = @ProcessID

			TRUNCATE TABLE #TMGRebate

			-- End TMG Rebate section

			--  SHIRISH 07/22/2019 DIT-18425
			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'After TMG-Rebate',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2

			-- AT Rebate Section

			INSERT INTO #TMGRebate
			SELECT	DEAL_NEGOTIATION_ID = ISNULL(CAST(XD.CrossDealId AS VARCHAR(255)), DA.DEAL_NEGOTIATION_ID),
					DA.TradeDate,
					DA.ProductGroup,
					Rebate= SUM(CASE 
									WHEN DA.TradeDate BETWEEN '20180601' AND '20181130' THEN D.DEAL_QUANTITY * 5 
									WHEN DA.TradeDate > '20181130' THEN 
																		CASE ISNULL(SI.SWAPBOX,SM2.issued_as/12) --Determine the SWAP BOX of the On-THE-RUN security on the Hedge Leg
																		
																			WHEN 2 THEN /* 2-Year SWAP BOX */
																				CASE	 WHEN ISNULL(SI.OnTheRun,SM.on_the_run) = -1 OR ISNULL(SI.OnTheRun,SM.on_the_run) = 2 THEN D.DEAL_QUANTITY * (CASE @metQtrVolumeDiscount WHEN 1 THEN 2.5 ELSE 2 END) --When Issued and Old 2 Years
																						WHEN ISNULL(SI.OnTheRun,SM.on_the_run) = 0 OR ISNULL(SI.OnTheRun,SM.on_the_run) > 2 THEN D.DEAL_QUANTITY * (CASE @metQtrVolumeDiscount WHEN 1 THEN 3.5 ELSE 3 END) --Beginning with the second issue out from the active issue
																				END
																		
																			ELSE /* 3-30 Year Swap Box */
																				CASE	 WHEN ISNULL(SI.OnTheRun,SM.on_the_run) = -1 OR ISNULL(SI.OnTheRun,SM.on_the_run) = 2 THEN D.DEAL_QUANTITY * (CASE @metQtrVolumeDiscount WHEN 1 THEN 3.5 ELSE 3 END) --When Issued and Old 2 Years
																						WHEN ISNULL(SI.OnTheRun,SM.on_the_run) = 0 OR ISNULL(SI.OnTheRun,SM.on_the_run) > 2 THEN D.DEAL_QUANTITY * (CASE @metQtrVolumeDiscount WHEN 1 THEN 5.5 ELSE 5 END) --Beginning with the second issue out from the active issue
																				END
																		END
								END),
					TotalAggressiveQuantity = CAST(0 AS FLOAT),
					Issues_As = ISNULL(SI.Issued_As,SM2.issued_as), --SM2.issued_as,
					on_the_run = ISNULL(SI.OnTheRun,SM.on_the_run) --SM.on_the_run

			FROM	IDB_Billing.dbo.wDBS_AllProd DA (NOLOCK)
			JOIN	#IDB_FALCON_DEALS AS D (NOLOCK) ON DA.DEAL_ID = D.DEAL_ID
			JOIN	Instrument.dbo.Security_Master AS SM WITH (NOLOCK) ON D.DEAL_SECURITY_ID = SM.instrid
			JOIN	Instrument.dbo.Security_Type AS ST WITH (NOLOCK) ON SM.sec_type_id = ST.sec_type_id AND ST.product_grp = D.ProductGroup
			/* DIT 9920 - Below two joins are to get the SWAP box from the Hedge Leg */
			JOIN	#IDB_FALCON_DEALS AS DH (NOLOCK) ON D.DEAL_NEGOTIATION_ID = DH.DEAL_NEGOTIATION_ID And D.LinkIDVrt = DH.LinkIDVrt AND D.DEALER = DH.DEALER AND DH.DEAL_LEG_NUM = 2
			JOIN	Instrument.dbo.Security_Master AS SM2 (NOLOCK) ON DH.DEAL_SECURITY_ID = SM2.instrid
			/* SHIRISH 05/08/2019 - DIT-9920 - We have created a table to store Issued_as and SwapBox information at the time of the trade as securities can roll over and values can change.  
											   Using join to this table to make sure we use correct values to calculate rebate */
			LEFT JOIN IDB_Reporting.dbo.IDB_OFTR_CREDITS_SECINFO SI (NOLOCK) ON SI.DEAL_TRADE_DATE = DA.TradeDate AND SI.DEAL_ID = DA.DEAL_ID
			/* 
			-- DIT 9920
			-- The below join with CrossDealsForBrokerCommission will help us determine IF ANY TMG trade was matched 
			-- with a different trade and if so we will use the CtrossDealId to get aggtressive side trades.
			*/
			LEFT JOIN IDB_Reporting.dbo.CrossDealsForBrokerCommission XD WITH (NOLOCK) ON DA.TradeDate = XD.TradeDate AND DA.ProductGroup = XD.ProductGroup AND DA.DEAL_NEGOTIATION_ID = XD.Deal_Negotiation_Id 

			WHERE	DA.PROCESS_ID = @ProcessID
			AND		DA.ProductGroup = 'TRSY'
			AND		D.Dealer IN ('AT')
			AND		D.DEAL_IS_AGRESSIVE = 0	/* Rebate only applicable on the passive trades by TMG */

			--Exclude hedge legs for rebate calculations
			AND		CAST(DA.Charge_Rate_Aggressive as FLOAT) > 0

			--DIT 9920 Added Date switch for change in rebate schemes
			AND		(
						(DA.TradeDate BETWEEN '20180601' AND '20181130' AND (SM.on_the_run >= 5 OR SM.on_the_run = 0) AND	DA.TradeType2 NOT IN ('TRSYROLL'))
						OR
						(DA.TradeDate > '20181130')
					)
			GROUP BY 
					ISNULL(CAST(XD.CrossDealId AS VARCHAR(255)), DA.DEAL_NEGOTIATION_ID),
					DA.TradeDate,
					DA.ProductGroup,
					SM2.issued_as,
					SM.on_the_run,
					SI.Issued_As,
					SI.SwapBox,
					SI.OnTheRun

			-- SHIRISH 05/02/2019: DIT-9920 Need to update #TMGRebate table with total aggressive quantity on the deal so rebate and be distributed in all aggressive dealers according to their quantity.
			UPDATE	r
			SET		r.TotalAggressiveQuantity = COALESCE(CD.Quantity,SD.Quantity,1) 
			FROM	#TMGRebate r
			-- Getting agressive quantity of cross deal
			OUTER APPLY 
			(
				SELECT	Quantity = SUM(D.DEAL_QUANTITY)
				FROM	IDB_Reporting.dbo.CrossDealsForBrokerCommission XD (NOLOCK)
				JOIN	IDB_Billing.dbo.wDBS_AllProd DA ON DA.PROCESS_ID = @ProcessID
														AND XD.TradeDate = DA.TradeDate
														AND XD.ProductGroup = DA.ProductGroup
														AND XD.Deal_Negotiation_Id = DA.DEAL_NEGOTIATION_ID 
				JOIN	#IDB_FALCON_DEALS AS D (NOLOCK) ON DA.DEAL_ID = D.DEAL_ID
				WHERE	XD.TradeDate = r.TradeDate
				AND		XD.ProductGroup = r.ProductGroup
				AND		XD.CrossDealId = r.DEAL_NEGOTIATION_ID
				AND		CAST(DA.CHARGE_RATE_AGGRESSIVE AS FLOAT) > 0
				AND		D.DEAL_IS_AGRESSIVE = 1
				AND		D.Dealer <> 'AT'
			) CD 
			-- Getting agressive quantity of a regular deal
			OUTER APPLY
			(
				SELECT	Quantity = SUM(D.DEAL_QUANTITY)
				FROM	IDB_Billing.dbo.wDBS_AllProd DA
				JOIN	#IDB_FALCON_DEALS AS D (NOLOCK) ON DA.DEAL_ID = D.DEAL_ID
				WHERE	DA.PROCESS_ID = @ProcessID
				AND		DA.TradeDate = r.TradeDate
				AND		DA.ProductGroup = r.ProductGroup
				AND		DA.DEAL_NEGOTIATION_ID = r.DEAL_NEGOTIATION_ID
				AND		CAST(DA.CHARGE_RATE_AGGRESSIVE AS FLOAT) > 0
				AND		D.DEAL_IS_AGRESSIVE = 1
				AND		D.Dealer <> 'AT'
			) SD

			--IF @Debug=1
			--	SELECT '#TMGRebate after AggrQtyUpdate', * FROM #TMGRebate WHERE DEAL_NEGOTIATION_ID = 'DEAL_4820'

			-- SHIRISH 05/02/2019: Apply rebate to each dealer on aggressive side.
			-- For cross deal we need to get deal_nego_id of all the dealers on aggressive side		
			;WITH TMG_REBATE
			AS
			(
				SELECT DEAL_NEGOTIATION_ID = ISNULL(CD.Deal_Negotiation_Id,r.DEAL_NEGOTIATION_ID),
					   r.TradeDate,
					   r.ProductGroup,
					   r.Rebate,
					   r.TotalAggressiveQuantity
				FROM	#TMGRebate r
				OUTER APPLY 
				(
					SELECT	XD.Deal_Negotiation_Id
					FROM	IDB_Reporting.dbo.CrossDealsForBrokerCommission XD (NOLOCK)
					JOIN	IDB_Billing.dbo.wDBS_AllProd DA (NOLOCK) ON DA.PROCESS_ID = @ProcessID
																	AND XD.Deal_Negotiation_Id = DA.DEAL_NEGOTIATION_ID 
																	AND XD.TradeDate = DA.TradeDate
																	AND XD.ProductGroup = DA.ProductGroup
					JOIN	#IDB_FALCON_DEALS AS D (NOLOCK) ON DA.DEAL_ID = D.DEAL_ID
					WHERE	XD.TradeDate = r.TradeDate
					AND		XD.ProductGroup = r.ProductGroup
					AND		XD.CrossDealId = r.DEAL_NEGOTIATION_ID
					AND		CAST(DA.CHARGE_RATE_AGGRESSIVE AS FLOAT) > 0
					AND		D.DEAL_IS_AGRESSIVE = 1
					AND		D.Dealer <> 'AT'
				) CD 
			)
			UPDATE	DA
			SET		DA.CommissionOwed_PreCap_Override = (CASE WHEN ISNULL(DA.CommissionOwed_PreCap_Override,0) > 0 THEN DA.CommissionOwed_PreCap_Override ELSE DA.CommissionOwed_PreCap END) - (TR.Rebate * (DA.AggressiveVolume/TR.TotalAggressiveQuantity)),
					/* DIT-11179 - Commission calculation for Fixed Fee Client */
					DA.FF_CommissionOwed_PreCap_Override = (CASE WHEN ISNULL(DA.FF_CommissionOwed_PreCap_Override,0) > 0 THEN DA.FF_CommissionOwed_PreCap_Override ELSE DA.FF_CommissionOwed_PreCap END) - (TR.Rebate * (DA.AggressiveVolume/TR.TotalAggressiveQuantity))
			FROM	IDB_Billing.dbo.wDBS_AllProd DA (NOLOCK)
			JOIN	TMG_REBATE TR ON TR.TradeDate = DA.TradeDate
								  AND TR.ProductGroup = DA.ProductGroup
								  AND TR.DEAL_NEGOTIATION_ID = DA.DEAL_NEGOTIATION_ID
								  AND DA.AggressiveTrades = 1
								  AND CAST(DA.Charge_Rate_Aggressive as float) > 0
			WHERE	DA.PROCESS_ID = @ProcessID

			--TRUNCATE TABLE #TMGRebate
			-- End AT rebate section
	
			-- Clean-up temp tables from TMG Rebate section
			IF OBJECT_ID('tempdb.dbo.#TMGRebate') IS NOT NULL DROP TABLE #TMGRebate
			IF OBJECT_ID('tempdb.dbo.#IDB_FALCON_DEALS') IS NOT NULL DROP TABLE #IDB_FALCON_DEALS

			--  SHIRISH 07/22/2019 DIT-18425
			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'After AT-Rebate',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2

		END

		/* NAVX Commission adjustments -- Done only when generating invoices - ReportMode = 0*/
		/* SHIRISH: 11/05/2015 - This block calculates commission adjustments due to NAVX rate adjustment */
		/* IDBBC-108 Update code below to calculate and store NAVX adjustments so there is no need to re-calculate adjustment which generating invoice trade details file and NAVX adjustment report for Rich */
		IF ((@ReportMode = 0 OR @Debug = 1) AND @Owner = 'US') -- IDBBC-16 No need to run NAVX commission adjustment block when owner is UK
		BEGIN

			SELECT	InvDate = @InvDate,
					D.BILLING_CODE,
					D.Dealer,
					D.DEAL_NEGOTIATION_ID,
					D.DEAL_ID,
					D.TradeDate,
					D.Instrument,
					D.EstimatedRate,
					/* DIT-11260 : For SQ instrument trades use the Estimated rate for Actual Rate as it is same. Currently GDB does not support separate entry for SQ trades */
					ActualRate = CASE WHEN D.TradeType2 = 'NAVXSQ' THEN D.EstimatedRate ELSE R.ACTUAL_NAVX END,
					RateDiff = CASE WHEN D.TradeType2 = 'NAVXSQ' THEN 0.0 ELSE ISNULL(D.EstimatedRate,0)-ISNULL(R.ACTUAL_NAVX,0) END,
					D.ClearingQty,
					Adjustment = CAST((ISNULL(D.EstimatedRate,0)-ISNULL(R.ACTUAL_NAVX,0)) * 
												(CASE WHEN D.TradeType2 = 'NAVXSQ' THEN 0 ELSE D.ClearingQty END) *  --IDBBC-108 There is no adjustment for NAVXSQ so setting clearing quantity to 0
												(CASE	
														WHEN D.EstimatedRate < R.ACTUAL_NAVX AND D.Deal_Way = 'B' THEN -1  -- if difference is -ve then seller owes buyer
														WHEN D.EstimatedRate > R.ACTUAL_NAVX AND D.Deal_Way = 'B' THEN -1  -- if difference is +ve then buyer owes seller
														ELSE 1
												  END) AS DECIMAL(18,4)),
					OrigCommission = ISNULL(CommissionOwed_PreCap,0),
					D.ContraDealer,
					D.ContraUserId,
					D.ProductGroup
			INTO	#NAVXCommAdjustment
			FROM	IDB_Billing.dbo.wDBS_AllProd D (NOLOCK)
			/* DIT-10159 Modified the join with the cross apply for the actual rates for the specific actual rates requirements of IWMSQ_NAVX for month of December 2018. */
			OUTER APPLY 
			(
				SELECT ACTUAL_NAVX = CASE WHEN X.TradeDate BETWEEN '20181201' AND '20181231' AND D.DEAL_SECURITY_ID = 'IWMSQ_NAVX' THEN 131.6913 
										  ELSE X.ACTUAL_NAVX 
									 END
				FROM IDB_Billing.dbo.IDB_DAILY_NAVX X WITH (NOLOCK)	
				WHERE D.TradeDate = X.TradeDate
				AND D.Instrument = X.Instrument
			) R
			-- SHIRISH 02/06/2018: Below join will get trades that were reprinted
			LEFT JOIN IDB_Billing.dbo.NAVX_Reprint NR WITH (NOLOCK) ON D.TradeDate = NR.TradeDate
																	AND D.DEAL_NEGOTIATION_ID = NR.Deal_negotiation_Id
			WHERE	D.PROCESS_ID = @ProcessID
			AND		D.ProductGroup = 'NAVX'
			AND		D.BILLING_CODE NOT LIKE 'SECF%'
			AND		D.BILLING_CODE NOT LIKE 'CATF%' -- IDBBC-332
			--AND		D.EstimatedRate IS NOT NULL -- IDBBC-108 Need to comment this condition.  Instead we will set all NAVXSQ trades with clearing quantity = 0 when calculating adjustment
			-- SM 06/07/2016: There are no rates in system for GDXJ and EEM.  They should be excluded from this calculation
			-- SM 10/03/2017: According to Jason international NAVX do not get adjusted.  Adding instrument VWO to code below to remove adjustments
			-- NS 01/08/2018: Added new International securities 'IEFA','EFA'
			AND		D.Instrument NOT IN ('GDXJ','EEM','VWO','IEFA','EFA')
			AND		NOT (D.Instrument in ( 'TUSA','TSUA') and D.TradeDate Between '20180701' and '20180731')
			AND		NR.Deal_Negotiation_Id IS NULL -- do not calculate commission adjustment for reprinted trades

			-- Update #DBS_AllProd using temp table #NAVXCommAdjustments
			UPDATE	D
			SET		D.CommissionOwed_PreCap = NA.OrigCommission + NA.Adjustment
			FROM	IDB_Billing.dbo.wDBS_AllProd D (NOLOCK)
			JOIN	#NAVXCommAdjustment NA ON NA.TradeDate = D.TradeDate
										  AND NA.ProductGroup = D.ProductGroup -- New
										  AND NA.DEAL_NEGOTIATION_ID = D.DEAL_NEGOTIATION_ID -- New
										  AND NA.BILLING_CODE = D.BILLING_CODE
										  AND NA.DEAL_ID = D.DEAL_ID
										  -- IDBBC-112 Need below two join conditions for ContraDealer and ContraUserId as there could be multiple counter parties on a trade.
										  -- As for NAVX we calculate commission separately for all counter parties we need to make sure we are applying appropriate 
										  -- adjustment to each record
										  AND NA.ContraDealer = D.ContraDealer 
										  AND NA.ContraUserId = D.ContraUserId
										  --AND D.ProductGroup = 'NAVX'
			WHERE	D.PROCESS_ID = @ProcessID

			-- Update NAVX adjustment table only in invoice mode
			IF @ReportMode = 0
			BEGIN
				-- Delete if Adjustment records already exist in the table
				DELETE	Adj 
				FROM	IDB_Billing.dbo.NAVXCommissionAdjustments Adj (NOLOCK)
				JOIN	(
							SELECT	DISTINCT 
									InvDate,
									BILLING_CODE
							FROM	#NAVXCommAdjustment
						) tmp ON tmp.InvDate = Adj.InvDate
							  AND tmp.BILLING_CODE = Adj.Billing_Code

				-- Insert Adjustment records into table
				INSERT INTO IDB_Billing.dbo.NAVXCommissionAdjustments (InvDate, Billing_Code, Dealer, Deal_Negotiation_Id, Deal_Id, TradeDate, Instrument, EstimatedRate, ActualRate, 
																		RateDiff, ClearingQuantity, Adjustment, OrigCommission, FinalCommission, ContraDealer, ContraUserId, InsertedOn)
				SELECT	InvDate, BILLING_CODE, Dealer, DEAL_NEGOTIATION_ID, DEAL_ID, TradeDate, Instrument, EstimatedRate, ActualRate, 
						RateDiff, ClearingQty, Adjustment, OrigCommission, OrigCommission + Adjustment, ContraDealer, ContraUserId, GETDATE()
				FROM	#NAVXCommAdjustment

			END

		END

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After NAVX Commission Adjustment',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2
		
		/* ###############################  DAILY CAPS ################################# */
		
		SELECT
			RowNum = MAX(RowNum),
			InvNum  = MAX(InvNum),
			InvDbId = MAX(InvDbId),
			DailyRowNum = IDENTITY(Int, 1, 1),
			D.BILLING_CODE,
			D.PeriodId,
			D.TradeDate,
			D.ProductGroup,
			D.Source,
			D.INSTRUMENT_TYPE,
			D.BILLING_TYPE,
			D.TIER_BILLING_PLAN_ID,
			CommissionOwed_PreCap = SUM(D.CommissionOwed_PreCap),
			CommissionOwed_AfterDailyCap = CAST(0 AS FLOAT)
			
		INTO	#DBS_AllProd_Daily
		
		FROM	IDB_Billing.dbo.wDBS_AllProd D (NOLOCK)

		WHERE	D.PROCESS_ID = @ProcessID

		GROUP BY	D.BILLING_CODE,
							D.PeriodId,
							D.TradeDate,
							D.ProductGroup,
							D.Source,
							D.INSTRUMENT_TYPE,
							D.BILLING_TYPE,
							D.TIER_BILLING_PLAN_ID	


		/* Determine if the Daily Charge/Floor is applicable or not */		
		UPDATE D 
		SET  D.CommissionOwed_AfterDailyCap =	CASE 
													WHEN TBS.DAILY_CHARGE_FLOOR > 0 AND CommissionOwed_PreCap < TBS.DAILY_CHARGE_FLOOR THEN TBS.DAILY_CHARGE_FLOOR 
													WHEN TBS.DAILY_CHARGE_CAP > 0 AND CommissionOwed_PreCap > TBS.DAILY_CHARGE_CAP THEN TBS.DAILY_CHARGE_CAP 
												END
		FROM	#DBS_AllProd_Daily D (NOLOCK)
		JOIN #TieredBillingSchedule TBS ON
								D.BILLING_CODE = TBS.BILLING_CODE
								AND D.ProductGroup = TBS.PRODUCT_GROUP
								AND D.INSTRUMENT_TYPE = TBS.INSTRUMENT_TYPE
								AND D.BILLING_TYPE = TBS.BILLING_TYPE
								AND D.TIER_BILLING_PLAN_ID = TBS.BILLING_PLAN_ID

		
		
		--SELECT '#DBS_AllProd_Daily----->', * FROM #DBS_AllProd_Daily
		
		UPDATE D
		SET	D.CommissionOwed_PreCap_AfterDailyCap = CASE WHEN D_RT.RowNum = D.RowNum THEN D_RT.CommissionOwed_AfterDailyCap ELSE CAST(0 AS FLOAT) END
				
		FROM		IDB_Billing.dbo.wDBS_AllProd D (NOLOCK)
		JOIN			#DBS_AllProd_Daily D_RT
						ON
								D.TradeDate = D_RT.TradeDate
								AND D.ProductGroup = D_RT.ProductGroup
								AND D.BILLING_CODE = D_RT.BILLING_CODE
								AND D.INSTRUMENT_TYPE = D_RT.INSTRUMENT_TYPE
								AND D.BILLING_TYPE = D_RT.BILLING_TYPE
		
		/* Only where Daily Cap was applied */						
		WHERE	D.PROCESS_ID = @ProcessID
		AND		ISNULL(D_RT.CommissionOwed_AfterDailyCap,0) > 0	
		
		
		--SELECT 'After Daily Cap Changes #DBS_AllProd----->', * FROM IDB_Billing.dbo.wDBS_AllProd WHERE PROCESS_ID = @processId

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After Daily Caps',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/* ########################### END DAILY CAPS ################################# */
		
		/* NILESH 02/24/2011 */
		/* We cannot have row num in the original #DealBillingSchedule as this is used to roll up 
		 all the deals for a invoice as a single row so we will create a staging area with rownum.
		*/
		Select	RowNum = IDENTITY(Int, 1, 1),
				DEAL_NEGOTIATION_ID,
				DEAL_ID,
				Dealer,
				DEAL_USER_ID,
				DEAL_WAY,
				Source,
				ProductGroup,
				DEAL_O_FACTOR,
				CHARGE_RATE_AGGRESSIVE,
				CHARGE_RATE_PASSIVE,		
				DealCommission,
				/* Tiered Billing */
				TradeDate,
				TIER_CHARGE_RATE_AGGRESSIVE,
				TIER_CHARGE_RATE_PASSIVE,
				OVERWRITE, -- SHIRISH 06/01/2017
				Leg, -- SHIRISH 10/17/2018
				UseGap, -- IDBBC-132
				ProcessID = PROCESS_ID
				-- IDBBC-178
				,Deal_source
				,Deal_Principal
				,Deal_Accint
				,Deal_Proceeds
				,Deal_O_FININT
				,DEAL_EXTRA_COMMISSION
				,Broker
				,DEAL_SECURITY_NAME
				,Quantity
				,TradeType2
				,DEAL_PRICE
				,DEAL_IS_AGRESSIVE
				,DealFinalCommission
				,DEAL_TENOR
				,DEAL_DAYS_TO_MATURITY
				,DEAL_CHARGED_QTY
				,DEAL_RISK
				,ExchangeRateMultiplier
				,Deal_Discount_rate
				,Deal_SEF_Trade
				,DEAL_TIM_QTY
				,DEAL_FIRST_AGGRESSED
				,DEAL_FIRST_AGGRESSOR
				,IsActiveStream
				,Operator
				,DEAL_DATE
				,DEAL_STLMT_DATE
				,DEAL_REPO_START_DATE
				,DEAL_REPO_END_DATE
				,DEAL_GAP_DV01
				,RepoRecordType
				,IsHedgedTrade
				,Security_Currency

		
		INTO	#DealBillingSchedule_Staging
		
		FROM	IDB_Billing.dbo.wDealBillingSchedule (NOLOCK) -- SHIRISH 11/6/2014 -- updating query to use permanent table
		
		/* DIT-10448 : GILTS 
		VK:08/28/2019 - GDB-99: BTIC-GDB Billing
		SM:05/03/2023 - Adding BOX
		*/
		WHERE	ProductGroup IN ('BILL','IOS','TRSY','TIPS','EFP','ECDS','AMSWP','UCDS','CDXEM','USFRN','NAVX','OTR','CAD','EQSWP','COMBO','USREPO','GILTS', 'BTIC','BOX','EUREPO','MATCH','REVCON') --Select specific products
		AND		PROCESS_ID = @ProcessID
		
		/*This order by is to get the correct deals that need to be considered for commission cap*/
		ORDER BY 
				InvNum ,
				InvDbId,
				BILLING_CODE,
				PeriodId,
				ProductGroup,
				DEAL_DATE,
				DEAL_ID

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #DealBillingSchedule_Staging',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/* SHIRISH 2018/09/01: 
		 * As NAVX commission is now based on Clearing  Trades and not on deals, there could be more than one record
		 * in #DBS_AllProd.  This causes rowNum to be different from #DealBillingSchedule_Staging table.  We need to do a sum on 
		 * #DBS_AllProd to make sure two tables have same number of records
		 */
		
		SELECT	RowNum = IDENTITY(INT, 1, 1),
				InvNum,
				InvDbId,
				BILLING_CODE,
				PeriodId,
				ProductGroup,
				DEAL_DATE,
				DEAL_ID,
				CommissionOwed_PreCap = SUM(CommissionOwed_PreCap),
				CommissionOwed_PreCap_Override = SUM(CommissionOwed_PreCap_Override),
				/* DIT-11179 : Commission for Fixed fee clients */
				FF_CommissionOwed_PreCap = SUM(FF_CommissionOwed_PreCap),
				FF_CommissionOwed_PreCap_Override = SUM(FF_CommissionOwed_PreCap_Override),
				Security_Currency, -- IDBBC-310
				RepoTicketFees = SUM(ISNULL(RepoTicketFees,0))
		INTO	#DBS_AllProd_Staging
		FROM	IDB_Billing.dbo.wDBS_AllProd
		WHERE	PROCESS_ID = @ProcessID
		GROUP BY
				InvNum,
				InvDbId,
				BILLING_CODE,
				PeriodId,
				ProductGroup,
				DEAL_DATE,
				DEAL_ID,
				Security_Currency
		ORDER BY 
				InvNum ,
				InvDbId,
				BILLING_CODE,
				PeriodId,
				ProductGroup,
				DEAL_DATE,
				DEAL_ID


		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #DBS_AllProd_Staging',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/* deals_commission table code block */
		
		/* NILESH: 02/24/2011
		-- Collect the commission related information for the IOS legs. This 
		-- information is used during the generation of the Firm Activity Report
		-- for IOS distributed to the Dealers.
		-- The data will be used to populate the Deals_Commission table.
		*/
		/* 
		-- NILESH : 07/15/2014 : 
		-- We need distinct because for AMSWP there were dummy billing code created
		-- to accomodate the manually created deals. In this case when there are duplicate
		-- deals one for the original billing code and another one for the dummy billing code
		*/
		-- IDBBC-268 Converting 
		-- IDBBC-178 Only populate Deal_Commission in D mode.
		IF @SummaryType = 'D'
		BEGIN
			INSERT INTO IDB_Billing.dbo.wDeal_Commission
		(
			PROCESS_ID,
			RowNum,
			Deal_Negotiation_Id,
			Dealer,
			Deal_User_Id,
			Deal_Way,
			Source,
			ProductGroup,
			DEAL_PRINCIPAL,
			DEAL_ACCINT,
			DEAL_PROCEEDS,
			Deal_O_Factor,
			Deal_O_FinInt,
			CHARGE_RATE_AGGRESSIVE,
			CHARGE_RATE_PASSIVE,
			DealCommission,
			DealCommission_Override,
			Deal_TradeDate,
			BrokerId,
			SecurityName,
			Quantity,
			TradeType,
			Deal_Price,
			Deal_Aggressive_Passive,
			Deal_Final_Commission,
			Deal_Days_To_Maturity,
			Deal_Id,
			DEAL_EXTRA_COMMISSION,
			DEAL_CHARGED_QTY,
			DEAL_RISK,
			ExchangeRateMultiplier,
			CURRENCY_CODE,
			DEAL_DISCOUNT_RATE,
			TIER_CHARGE_RATE_AGGRESSIVE,
			TIER_CHARGE_RATE_PASSIVE,
			DEAL_SEF_TRADE,
			DEAL_TIM_QTY,
			DEAL_FIRST_AGGRESSED,
			IsActiveStream,
			DEAL_FIRST_AGGRESSOR,
			OVERWRITE,
			DEAL_DATE,
			DEAL_STLMT_DATE,
			DEAL_REPO_START_DATE,
			DEAL_REPO_END_DATE,
			Billing_Code,
			Operator,
			ZeroBrokerCommission,
			FF_DealCommission,
			FF_DealCommission_Override,
			UseGap,
			DEAL_GAP_DV01,
			RepoRecordType,
			IsHedgedTrade, -- GDB-2898
			Security_Currency, --IDBBC-310
			RepoTicketFees -- IDBBC-310
		)
		SELECT	--DISTINCT
				PROCESS_ID = @ProcessID,
				dbs.RowNum,
				Deal_Negotiation_Id = dbs.Deal_Negotiation_Id,
				Dealer = dbs.Dealer,
				Deal_User_Id = dbs.Deal_User_Id,
				Deal_Way = dbs.Deal_Way,
				Source = CASE WHEN dbs.ProductGroup = 'MATCH' THEN dbs.Deal_Source ELSE dbs.Source END,
				ProductGroup = dbs.ProductGroup,
				DEAL_PRINCIPAL = dbs.DEAL_PRINCIPAL,
				DEAL_ACCINT = dbs.DEAL_ACCINT,
				DEAL_PROCEEDS = dbs.DEAL_PROCEEDS,
				Deal_O_Factor = dbs.Deal_O_Factor,
				Deal_O_FinInt = dbs.Deal_O_Finint,
				CHARGE_RATE_AGGRESSIVE = ISNULL(EOC.AggrRate,dbs.CHARGE_RATE_AGGRESSIVE), -- For EUREPO update tier rate used to calculate commission
				CHARGE_RATE_PASSIVE = ISNULL(EOC.PassRate,dbs.CHARGE_RATE_PASSIVE), -- For EUREPO update tier rate used to calculate commission
				/* NILESH 08/10/2012: 
				-- We need to remove the extra commission as the CommissionOwed_Precap already has this
				-- factored in for the commission summary and invoices. So when generating the deals
				-- for the Deals_Commission we need should to reduce this value to avoid overage in the 
				-- total commission owed by the dealer.
				*/
				/* IDBBC-310
				*  User Override commission amount when available.
				*  Currently only applicable to EUREPO
				*/
				DealCommission = CAST(dap.CommissionOwed_PreCap AS FLOAT) - CAST(ISNULL(dbs.DEAL_EXTRA_COMMISSION,0) AS FLOAT),
				DealCommission_Override = dap.CommissionOwed_PreCap_Override,
				Deal_TradeDate = dbs.TradeDate,	/* NS:10/12/2011-Added date for future data mining requirements . */	
				/* NS:10/27/2011- Added following new columns to the table for the broker commission summary reports */
				BrokerId = dbs.Broker,
				SecurityName = dbs.Deal_Security_Name,
				Quantity = dbs.Quantity,
				/*NILESH 10/19/2012 - Changed to TradeType2 from TradeType for specific SWAP Security Type */
				TradeType = dbs.TradeType2,
				Deal_Price = dbs.Deal_Price,
				Deal_Aggressive_Passive = CASE dbs.DEAL_IS_AGRESSIVE WHEN 1 THEN 'A' ELSE 'P' END,
				Deal_Final_Commission = CASE dbs.DealFinalCommission WHEN 1 THEN 'Y' ELSE 'N' END,
				-- To be identified whether to use DEAL_TENOR or DEAL_DAYS_TO_MATURITY for TIPS
				Deal_Days_To_Maturity =  CASE WHEN dbs.ProductGroup = 'TRSY' THEN DEAL_TENOR ELSE DEAL_DAYS_TO_MATURITY END,
				Deal_Id = dbs.DEAL_ID,
				DEAL_EXTRA_COMMISSION = CAST(ISNULL(dbs.DEAL_EXTRA_COMMISSION,0) AS FLOAT),
				DEAL_CHARGED_QTY =  CASE WHEN dbs.ProductGroup IN ('ECDS','UCDS','CDXEM') THEN CASE dbs.DEAL_IS_AGRESSIVE WHEN 1 THEN dbs.Quantity ELSE dbs.DEAL_CHARGED_QTY END ELSE NULL END,		/* CHANGE FOR ECDS,UCDS */
				DEAL_RISK = dbs.DEAL_RISK,
				ExchangeRateMultiplier = dbs.ExchangeRateMultiplier,
				/* ############################################################# */
				/* CHANGE THIS ONCE READY TO ACTIVATE THE CURRENCY DATA IN PROD */
				--CURRENCY_CODE
				/* ############################################################ */
				CURRENCY_CODE = CASE WHEN dbs.ProductGroup = 'GILTS' THEN 'GBP' ELSE 'USD' END,  -- DIT-10448 updating currency code for GILTS to GBP
				DEAL_DISCOUNT_RATE = dbs.DEAL_DISCOUNT_RATE,
				/* NILESH 08/01/2013 -- Tiered Billing */
				dbs.TIER_CHARGE_RATE_AGGRESSIVE,
				dbs.TIER_CHARGE_RATE_PASSIVE,
				DEAL_SEF_TRADE = dbs.DEAL_SEF_TRADE,
				DEAL_TIM_QTY =  CASE WHEN dbs.ProductGroup IN ('ECDS','UCDS','CDXEM') THEN dbs.DEAL_TIM_QTY ELSE NULL END,		/* CHANGE FOR ECDS,UCDS */
				dbs.DEAL_FIRST_AGGRESSED,
				dbs.IsActiveStream, -- Added SHIRISH 04/20/2016, SHIRISH 08/29/2017: Updating Active_Stream to IsActiveStream
				dbs.DEAL_FIRST_AGGRESSOR, --Vipin 03/31/2017
				dbs.OVERWRITE, -- SHIRISH 06/01/2017
				dbs.DEAL_DATE, -- SHIRISH 08/22/2017
				dbs.DEAL_STLMT_DATE, -- SHIRISH 08/22/2017
				dbs.DEAL_REPO_START_DATE, -- SHIRISH 08/22/2017
				dbs.DEAL_REPO_END_DATE, -- SHIRISH 08/22/2017
				Billing_Code = dap.BILLING_CODE,
				dbs.Operator,
				/*
				-- DIT-11179 - NILESH
				-- Added the below flag to mark a trade for which the broker will not receive the commission.
				-- This was to implement the requirement where GS is aggressive and would have paid commission
				-- if they would not have been paying the Fixed Fee. This could be achieved by making the charge 
				-- rate as 0. However, business wanted the ability to have the commission information outside the 
				-- fixed fee.
				*/
				ZeroBrokerCommission = CASE WHEN dbs.ProductGroup IN ('TRSY','USFRN') AND dbs.TradeDate > '20190531' AND dbs.Dealer = 'GS' AND dbs.DEAL_IS_AGRESSIVE = 1 THEN 1 ELSE 0 END, -- IDBBC-92 For GS USRN is part of TRSY fixed fees.
				/* DIT-11179 : Commission for the fixed fee clients */
				FF_DealCommission = CAST(dap.FF_CommissionOwed_PreCap AS FLOAT) - CAST(ISNULL(dbs.DEAL_EXTRA_COMMISSION,0) AS FLOAT),
				FF_DealCommission_Override = dap.FF_CommissionOwed_PreCap_Override,
				dbs.UseGap, -- IDBBC-132
				dbs.DEAL_GAP_DV01, -- IDBBC-132
				dbs.RepoRecordType, -- IDBBC-234
				dbs.IsHedgedTrade, -- GDB-2898
				dbs.Security_Currency, -- IDBBC-310
				dap.RepoTicketFees -- IDBBC-310
				
		FROM	#DealBillingSchedule_Staging AS dbs
		
		INNER JOIN #DBS_AllProd_Staging AS dap ON  dbs.RowNum = dap.RowNum
		
		/* NS:10/12/2011-Generalized for additional products. */	
		INNER JOIN IDB_CodeBase.dbo.fnProductType() fp ON dbs.ProductGroup = fp.Product
		/* NS:10/12/2011-Generalized for additional products. */
		/* IDBBC-310
		*  Update EUREPO aggr/pass rates with tier rate used to calculate commission
		*/
		LEFT JOIN #EUREPOOverrideCommission EOC ON EOC.Deal_Trade_Date = dbs.TradeDate
												AND dbs.ProductGroup = 'EUREPO'
												AND EOC.Deal_Id = dbs.DEAL_ID

		WHERE	fp.NeedCommissionAtDealLevel = 'Y' --dbs.ProductGroup = 'IOS' /* For now this is only for the IOS product */
		
		/* 
		-- NILESH : 03/04/2015
		-- The following condition is added so that in case of EFP & NAVX products we do not include the
		-- records with the billing codes like SECF%. These records are due to the billing schedules that 
		-- were created to invoice the SEC Fees on a seperate invoice instead of doing it on the same 
		-- invoice. The following condition will avoid these records from being included in the 
		-- Deal_Commission table as they are strictly meant for the Invoicing purposes.
		*/
		AND dap.BILLING_CODE NOT LIKE 'SECF%'
		AND dap.BILLING_CODE NOT LIKE 'CATF%' -- IDBBC-332
		/* SHIRISH 10/18/2018: For AMSWP and CAD as falcon schedule will be set for Leg = All, we will match both PRI as well as SEC leg to the schedule
							   But when inserting data into DEAL_COMMISSION we only need to insert PRI leg.
		* SHIRISH 03/20/2019: DIT-10448 Need to add GILTS to below condition as it also uses All for leg in schedule
		* SHIRISH 05/21/2021: GDB-1236 Adding EFP, NAVX and BTIC for Equity migration from SmartCC to Falcon
		* SHIRISH 05/25/2023: IDBBC-240 Adding new product BOX
		*/
		AND (CASE 
				  WHEN dbs.ProductGroup IN ('AMSWP','CAD','GILTS','EFP','NAVX','BTIC','BOX','REVCON','COMBO') AND dbs.leg = 'SEC' THEN 0
				  ELSE 1
			 END) = 1

		--ORDER BY dbs.RowNum ASC  not need to order for insert
		
			--  SHIRISH 07/22/2019 DIT-18425
			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'After #DealsCommission',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2
		END
		/* Deal Commission table will hold data in the product's default currency . */
		/* ############################################################# */
		/* UNCOMMENT THIS ONCE READY TO ACTIVATE THE CURRENCY DATA IN PROD */
		--UPDATE #DealsCommission
		--SET	DealCommission = CAST(DealCommission as float) / ExchangeRateMultiplier,
		--		DealCommission_Override = CAST(DealCommission_Override as float) / ExchangeRateMultiplier
		--WHERE ProductGroup = 'ECDS'


		/* End of the deals_commission table code block */

		---------------- BILL/IOS/TRSY/TIPS/EFP/ECDS/AMSWP/CAD/EQSWP BLOCK ENDS -----------------
		
		/* GET MAX CAP AND FLOOR
			Although cap and floor are at each schedule level in billing schedule, they should be applied
			at the billing code and product group level. 
			Since BILL DRate schedule is created by maturity; each billing code could have multiple schedules 
			for each maturity bucket, and each schedule will have cap and floor. It was decided that we should
			get MAX(cap) and MAX(floor) i.e get one set of cap and floor at the billing coder and product group level

			This temp table will be used when calculating commission owed
			#DBS_AllProd is a good source to get MAX(cap) and MAX(floor) because at this point
			billing codes are associated to the appropriate schedule and have invoice numbers

		*/
		SELECT	InvNum ,
				InvDbId,
				BILLING_CODE,
				PeriodId,
				ProductGroup, 
				/* NILESH 01/30/2017 -- Added source for AMSWP as we have three different schedules for E, V, and RC */
				/* IDBBC-292 - Added condition for R8FIN */
				Source = CASE WHEN ProductGroup = 'AMSWP' THEN Source WHEN ProductGroup = 'MATCH' THEN Source ELSE NULL END, 
				CHARGE_FLOOR = MAX(CHARGE_FLOOR),
				CHARGE_CAP = MAX(CHARGE_CAP)

		INTO	#ABS_Cap_Floor_AllProd

		FROM	IDB_Billing.dbo.wDBS_AllProd (NOLOCK)

		WHERE	PROCESS_ID = @ProcessID
		AND		CASE WHEN (INSTRUMENT_TYPE = 'TRSYNOTE' AND ProductGroup = 'TRSY' AND ISNULL(CHARGE_CAP,0) > 0 AND ISNULL(CHARGE_FLOOR,0) > 0) THEN 0 ELSE 1 END = 1

		GROUP BY 
				InvNum ,
				InvDbId,
				BILLING_CODE,
				PeriodId,
				ProductGroup
				/* NILESH 01/30/2017 -- Added source for AMSWP as we have three different schedules for E, V, and RC */
				/* IDBBC-292 - Condition for R8FIN */
				,CASE WHEN ProductGroup = 'AMSWP' THEN Source WHEN ProductGroup = 'MATCH' THEN Source ELSE NULL END 

		--IF @Debug = 1
		--BEGIN
		--	SELECT 'After #ABS_Cap_Floor_AllProd load'
		--	SELECT 'After #ABS_Cap_Floor_AllProd load', * FROM #ABS_Cap_Floor_AllProd
		--END

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #ABS_Cap_Floor_AllProd',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/* SHIRISH 2016/10/25: Get List of DEALERS who are on fixed fees for TRSY SHORT COUPONS */
		SELECT	DISTINCT 
				COMPANY_ACRONYM, 
				BILLING_CODE, 
				PRODUCT_GROUP,
				CHARGE_CAP
		INTO	#DealersWFixedFeesForShrtCpn
		FROM	IDB_Billing.dbo.wBillingSchedule (NOLOCK)
		WHERE	PRODUCT_GROUP = 'TRSY'
		AND		INSTRUMENT_TYPE = 'TRSYNOTE'
		AND		ISNULL(CHARGE_CAP,0) > 0
		AND		ISNULL(CHARGE_FLOOR,0) > 0
		AND		PROCESS_ID = @ProcessID

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #DealersWFixedFeesForShrtCpn',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2
			
		/* GENERATE RUNNING TOTALS 
			Cap and floor should be applied at billing code and product group level. If there are multiple 
			billing schedules e.g. different schedules by billing type or different DRate schedules by maturity
			they will all have cap and floor; however, cap and floor should be applied at billing coder 
			and product group level
			
			If the schedule is for "ALL" sources, the cap applies to ALL (E & V combined) sources as well 
			So when checking for commission cap, both, E & V commission amounts should be treated as one total
			However, the data on commission summary report or invoice inventory will be broken down by each source
			
			The correlated sub queries below create running totals for CommissionOwed_PreCap for each BILLING_CODE 
			by ProductGroup, and PeriodId 
			
			Deals should be processed in order of DEAL_DATE when when applying cap , because.dbo..
			When cap is applied E & V commission owed should reflect the commission accrued in order of 
			DEAL_DATE  
			#DBS_AllProd orders deals by BILLING_CODE,	PeriodId, ProductGroup, and DEAL_DATE in order to create RowNum
			
		*/

		/* SHIRISH 04/14/2016 - Updating code to calculate running totals to use cursor loop instead of a inner query to improve performance
		   02/21/2020 - IDBBC-34 - Re-writing below code using sum over partition of invoice number, priod id, productgroup and source to calculate running total
								   This will eliminate need to do a self join and running a loop to calculate running totals
		 */
		SELECT	DAP.RowNum,
				DAP.InvNum ,
				DAP.InvDbId,
				DAP.BILLING_CODE,
				DAP.PeriodId,
				DAP.ProductGroup,
				DAP.Source,
				DAP.BILLING_PLAN_ID,
				DAP.INSTRUMENT_TYPE,
				DAP.BILLING_TYPE,
				/* IDBBC-234 Do not count USREPOGC allocation quantity or record count */
				/* IDBBC-394: For USREPO we pull both Shell and Allocation records.  So removing allocation volume to avoid double counting volume
							  For EUREPO we only pull allocation record.  So in this case we need to count allocation record to capture correct volume
				*/
				AggressiveVolume = CASE WHEN DAP.ProductGroup = 'USREPO' AND ISNULL(DAP.RepoRecordType,'') = 'A' THEN 0 ELSE DAP.AggressiveVolume END, 
				PassiveVolume = CASE WHEN DAP.ProductGroup = 'USREPO' AND ISNULL(DAP.RepoRecordType,'') = 'A' THEN 0 ELSE DAP.PassiveVolume END,
				TotalVolume = CASE WHEN DAP.ProductGroup = 'USREPO' AND ISNULL(DAP.RepoRecordType,'') = 'A' THEN 0 ELSE DAP.TotalVolume END,
				AggressiveTrades = CASE WHEN DAP.ProductGroup = 'USREPO' AND ISNULL(DAP.RepoRecordType,'') = 'A' THEN 0 ELSE DAP.AggressiveTrades END,
				PassiveTrades = CASE WHEN DAP.ProductGroup = 'USREPO' AND ISNULL(DAP.RepoRecordType,'') = 'A' THEN 0 ELSE DAP.PassiveTrades END,
				TotalTrades = CASE WHEN DAP.ProductGroup = 'USREPO' AND ISNULL(DAP.RepoRecordType,'') = 'A' THEN 0 ELSE DAP.TotalTrades END,
				DAP.CHARGE_RATE_AGGRESSIVE,
				DAP.CHARGE_RATE_PASSIVE,
				DAP.CHARGE_FLOOR,
				DAP.CHARGE_CAP,
				DAP.CommissionOwed_PreCap,
				
				/* Daily Cap */
				/* 
				-- This column will be updated later with the total daily cap only if
				-- 1. Daily Cap is applicable to this product
				-- 2. The total CommissionOwed_PreCap for the day goes beyond the applicable daily cap.
				
				In everyother case it will be left as NULL value.
				
				*/
				DAP.CommissionOwed_PreCap_AfterDailyCap,			
				
				/* Running total */
				CommissionOwed_PreCap_RT = SUM(ISNULL(DAP.CommissionOwed_PreCap,0)) OVER (PARTITION BY DAP.InvNum,DAP.PeriodId, DAP.ProductGroup,CASE WHEN DAP.ProductGroup = 'AMSWP' THEN DAP.Source ELSE 'X' END ORDER BY DAP.InvNum,DAP.PeriodId, DAP.ProductGroup,CASE WHEN DAP.ProductGroup = 'AMSWP' THEN DAP.Source ELSE 'X' END,DAP.RowNum), -- IDBBC-34
				/* Previous running total - running total of the previous deal within the BILLING_CODE, 
					ProductGroup, PeriodId and Billing Schedule grouping */
				CommissionOwed_PreCap_Prev_RT = SUM(ISNULL(DAP.CommissionOwed_PreCap,0)) OVER (PARTITION BY DAP.InvNum,DAP.PeriodId, DAP.ProductGroup,CASE WHEN DAP.ProductGroup = 'AMSWP' THEN DAP.Source ELSE 'X' END ORDER BY DAP.InvNum,DAP.PeriodId, DAP.ProductGroup,CASE WHEN DAP.ProductGroup = 'AMSWP' THEN DAP.Source ELSE 'X' END,DAP.RowNum ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), -- IDBBC-34

				/* NILESH 02/14/2012 - Commission Override Change */
				DAP.CommissionOwed_PreCap_Override,
				/* Running total */
				CommissionOwed_PreCap_Override_RT = SUM(ISNULL(DAP.CommissionOwed_PreCap_Override,0)) OVER (PARTITION BY DAP.InvNum,DAP.PeriodId, DAP.ProductGroup,CASE WHEN DAP.ProductGroup = 'AMSWP' THEN DAP.Source ELSE 'X' END ORDER BY DAP.InvNum,DAP.PeriodId, DAP.ProductGroup,CASE WHEN DAP.ProductGroup = 'AMSWP' THEN DAP.Source ELSE 'X' END,DAP.RowNum), -- IDBBC-34
				/* Previous running total - running total of the previous deal within the BILLING_CODE, 
					ProductGroup, PeriodId and Billing Schedule grouping */
				CommissionOwed_PreCap_Override_Prev_RT = SUM(ISNULL(DAP.CommissionOwed_PreCap_Override,0)) OVER (PARTITION BY DAP.InvNum,DAP.PeriodId, DAP.ProductGroup,CASE WHEN DAP.ProductGroup = 'AMSWP' THEN DAP.Source ELSE 'X' END ORDER BY DAP.InvNum,DAP.PeriodId, DAP.ProductGroup,CASE WHEN DAP.ProductGroup = 'AMSWP' THEN DAP.Source ELSE 'X' END,DAP.RowNum ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), -- IDBBC-34

				DAP.CommissionOwed,
				
				/* NILESH 08/01/2013 -- Tiered Billing */
				DAP.TradeDate,
				DAP.TIER_BILLING_PLAN_ID,
				DAP.TIER_CHARGE_RATE_AGGRESSIVE,
				DAP.TIER_CHARGE_RATE_PASSIVE,
				--Prev_Row = CAST(NULL as INT), --DAP2.RowNum, 
				DAP.IsActiveStream,
				DAP.Leg,	/* DIT-11311 */
				DAP.Security_Currency,
				DAP.RepoTicketFees,
				DAP.DEAL_NEGOTIATION_ID,
				RnForBoxVol = ROW_NUMBER() OVER (PARTITION BY DAP.BILLING_CODE, DAP.ProductGroup, DAP.DEAL_NEGOTIATION_ID ORDER BY DAP.RowNum)


		INTO	#DBS_AllProd_PreCap_RT

		FROM	IDB_Billing.dbo.wDBS_AllProd DAP
		WHERE	DAP.PROCESS_ID = @ProcessID


		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After Running Totals',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/* NILESH - Tiered Billing */
		-- This is an additional step added to get the data
		-- at date level for regenerating commission data for "D"
		-- type. It is just a staging step and the original temp table 
		-- is generated by summing up the data from this step
		SELECT
			InvNum,
			InvDbId,
			BILLING_CODE,
			PeriodId,
			ProductGroup,
			Source,
			BILLING_PLAN_ID,
			INSTRUMENT_TYPE,
			BILLING_TYPE,
			AggressiveVolume = SUM(AggressiveVolume),
			PassiveVolume = SUM(PassiveVolume),
			TotalVolume = SUM(TotalVolume),
			AggressiveTrades = SUM(AggressiveTrades),
			PassiveTrades = SUM(PassiveTrades),
			TotalTrades = SUM(TotalTrades),
			CHARGE_RATE_AGGRESSIVE,
			CHARGE_RATE_PASSIVE,
			CHARGE_FLOOR,
			CHARGE_CAP,
			CommissionOwed_PreCap = SUM(CommissionOwed_PreCap),
			CommissionOwed_PreCap_AfterDailyCap = SUM(CommissionOwed_PreCap_AfterDailyCap),	/* NILESH 09/09/2013 -- Daily Caps */
			CommissionOwed_PreCap_Override = SUM(CommissionOwed_PreCap_Override),	/* NILESH 02/14/2012 - Commission Override Change */
			CommissionOwed = CAST(NULL AS Float),
			SourceWeight = CAST(NULL AS Float), --See comments below
			/* NILESH 08/01/2013 -- Tiered Billing */
			TradeDate,
			TIER_BILLING_PLAN_ID,
			TIER_CHARGE_RATE_AGGRESSIVE,
			TIER_CHARGE_RATE_PASSIVE,
			IsActiveStream,
			Leg,		/* DIT-11311 */
			Security_Currency,
			RepoTicketFees = SUM(RepoTicketFees)

		INTO	#InvoiceInventory_Staging_AllProd_Tier

		FROM	#DBS_AllProd_PreCap_RT 

		GROUP BY InvNum, 
			InvDbId, 
			BILLING_CODE, 
			PeriodId,
			ProductGroup, 
			Source,
			BILLING_PLAN_ID,
			INSTRUMENT_TYPE, 
			BILLING_TYPE, 
			CHARGE_RATE_AGGRESSIVE,
			CHARGE_RATE_PASSIVE,
			CHARGE_FLOOR,
			CHARGE_CAP,
			/* NILESH -- Tiered Billing */
			TradeDate,
			TIER_BILLING_PLAN_ID,
			TIER_CHARGE_RATE_AGGRESSIVE,
			TIER_CHARGE_RATE_PASSIVE,
			IsActiveStream,
			Leg,	/* DIT-11311*/
			Security_Currency

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #InvoiceInventory_Staging_AllProd_Tier',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2
		
		SELECT
				IIS_AllProd.InvNum,
				IIS_AllProd.InvDbId,
				IIS_AllProd.BILLING_CODE,
				IIS_AllProd.PeriodId,
				Logon_Id = @User,
				IIS_AllProd.ProductGroup,
				IIS_AllProd.Source,
				IIS_AllProd.BILLING_PLAN_ID,
				IIS_AllProd.INSTRUMENT_TYPE,
				IIS_AllProd.BILLING_TYPE,
				AggressiveVolume = SUM(IIS_AllProd.AggressiveVolume),
				PassiveVolume = SUM(IIS_AllProd.PassiveVolume),
				TotalVolume = SUM(IIS_AllProd.TotalVolume),
				AggressiveTrades = SUM(IIS_AllProd.AggressiveTrades),
				PassiveTrades = SUM(IIS_AllProd.PassiveTrades),
				TotalTrades = SUM(IIS_AllProd.TotalTrades),
				IIS_AllProd.CHARGE_RATE_AGGRESSIVE,
				IIS_AllProd.CHARGE_RATE_PASSIVE,
				CF.CHARGE_FLOOR,
				CF.CHARGE_CAP,
				CommissionOwed_PreCap = SUM(IIS_AllProd.CommissionOwed_PreCap),
				CommissionOwed_PreCap_AfterDailyCap = SUM(IIS_AllProd.CommissionOwed_PreCap_AfterDailyCap),		/*NILESH 09/09/2013 -- Daily Cap */
				CommissionOwed_PreCap_Override = SUM(IIS_AllProd.CommissionOwed_PreCap_Override),	/* NILESH 02/14/2012 - Commission Override Change */
				CommissionOwed = CAST(NULL AS Float),
				SourceWeight = CAST(NULL AS Float), --See comments below
				/* NILESH 08/01/2013 -- Tiered Billing */
				TIER_DAILY_CHARGE_CAP = CAST(-1 AS FLOAT),
				TIER_DAILY_CHARGE_FLOOR = CAST(-1 AS FLOAT),
				IIS_AllProd.TIER_BILLING_PLAN_ID,
				IIS_AllProd.TIER_CHARGE_RATE_AGGRESSIVE,
				IIS_AllProd.TIER_CHARGE_RATE_PASSIVE,
				IIS_AllProd.IsActiveStream,
				IIS_AllProd.Security_Currency,
				RepoTicketFees = SUM(IIS_AllProd.RepoTicketFees)

			INTO	#InvoiceInventory_Staging_AllProd

			FROM	#InvoiceInventory_Staging_AllProd_Tier IIS_AllProd
			JOIN	#ABS_Cap_Floor_AllProd CF ON IIS_AllProd.InvNum = CF.InvNum 
								AND IIS_AllProd.InvDbId = CF.InvDbId 
								AND IIS_AllProd.BILLING_CODE = CF.BILLING_CODE
								AND IIS_AllProd.PeriodId = CF.PeriodId 
								AND IIS_AllProd.ProductGroup = CF.ProductGroup 
								AND (CASE WHEN IIS_AllProd.ProductGroup = 'AMSWP' AND IIS_AllProd.Source <> CF.Source THEN 0 ELSE 1 END) = 1 /* NILESH 01/30/2017 -- Added source for AMSWP as we have three different schedules for E, V, and RC */

			--AND (CASE WHEN IIS_AllProd.ProductGroup <> 'TRSY' THEN 1 WHEN IIS_AllProd.ProductGroup = 'TRSY' AND IIS_AllProd.Leg = 'PRI' THEN 1 ELSE 0 END) = 1	/* DIT-11311 */			
			GROUP BY IIS_AllProd.InvNum, 
				IIS_AllProd.InvDbId, 
				IIS_AllProd.BILLING_CODE, 
				IIS_AllProd.PeriodId,
				IIS_AllProd.ProductGroup, 
				IIS_AllProd.Source,
				IIS_AllProd.BILLING_PLAN_ID,
				IIS_AllProd.INSTRUMENT_TYPE, 
				IIS_AllProd.BILLING_TYPE, 
				IIS_AllProd.CHARGE_RATE_AGGRESSIVE,
				IIS_AllProd.CHARGE_RATE_PASSIVE,
				CF.CHARGE_FLOOR,
				CF.CHARGE_CAP,
				IIS_AllProd.TIER_BILLING_PLAN_ID,
				IIS_AllProd.TIER_CHARGE_RATE_AGGRESSIVE,
				IIS_AllProd.TIER_CHARGE_RATE_PASSIVE,
				IIS_AllProd.IsActiveStream,
				IIS_AllProd.Security_Currency

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #InvoiceInventory_Staging_AllProd',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/* NILESH - Tiered Billing */
		/* Update the daily charge floor and cap */
		UPDATE IIS
		SET			IIS.TIER_DAILY_CHARGE_FLOOR = DAILY_CHARGE_FLOOR,
						IIS.TIER_DAILY_CHARGE_CAP = DAILY_CHARGE_CAP
		FROM		#InvoiceInventory_Staging_AllProd IIS
		JOIN			#TieredBillingSchedule TBS ON
					IIS.TIER_BILLING_PLAN_ID = TBS.BILLING_PLAN_ID
					AND IIS.BILLING_CODE = TBS.BILLING_CODE
					AND IIS.ProductGroup = TBS.PRODUCT_GROUP
					AND IIS.INSTRUMENT_TYPE = TBS.INSTRUMENT_TYPE

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After Update #InvoiceInventory_Staging_AllProd',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2
		
		SELECT
			InvNum = DBSB1.InvNum,
			InvDbId = DBSB1.InvDbId,
			BILLING_CODE = DBSB1.BILLING_CODE,
			PeriodId = DBSB1.PeriodId,
			ProductGroup = DBSB1.ProductGroup,
			Source = DBSB1.Source,
			/*DIT-11311*/
			/* IDBBC-378
			* Box commission is calculated on each leg.  In this case volume gets multipled by number of legs.
			* To avoid this we are ranking legs and taking volume from 1st leg only
			*/
			Volume = ISNULL(SUM(CASE WHEN (DBSB1.ProductGroup = 'BOX' AND DBSB1.RnForBoxVol = 1) OR (DBSB1.ProductGroup <> 'BOX') THEN DBSB1.AggressiveVolume + DBSB1.PassiveVolume ELSE 0 END), 0),
			--Volume = ISNULL(SUM(CASE WHEN DBSB1.ProductGroup <> 'TRSY' THEN DBSB1.AggressiveVolume + DBSB1.PassiveVolume WHEN DBSB1.ProductGroup = 'TRSY' AND DBSB1.Leg = 'PRI' THEN (DBSB1.AggressiveVolume + DBSB1.PassiveVolume) ELSE 0 END), 0),
			CHARGE_FLOOR = MAX(CF.CHARGE_FLOOR),
			CHARGE_CAP = MAX(CF.CHARGE_CAP),
			CommissionOwed_PreCap = SUM(DBSB1.CommissionOwed_PreCap),
			CommissionOwed_PreCap_Override = SUM(DBSB1.CommissionOwed_PreCap_Override),	/* NILESH 02/14/2012 - Commission Override Change */
			CommissionOwed = SUM(CASE WHEN CF.CHARGE_CAP = -1 THEN ISNULL(CommissionOwed_PreCap_AfterDailyCap,DBSB1.CommissionOwed_PreCap)	/* NILESH 09/09/2013 -- Daily Caps */
						WHEN (CF.CHARGE_CAP = 0) THEN 0
						WHEN ((CF.CHARGE_CAP > 0) AND (DBSB1.CommissionOwed_PreCap_RT <= CF.CHARGE_CAP)) THEN ISNULL(CommissionOwed_PreCap_AfterDailyCap,DBSB1.CommissionOwed_PreCap)		/* NILESH 09/09/2013 -- Daily Caps */ 
						/* If previous deal's running total value is just short of cap and the current deal's running total 
						is greater than cap; get the difference between previous running total and cap
						This gives the same result as to getting from CommissionOwed_PreCap_RT, only the 
						amount that is required to reach the cap, which basically will include the deal toward the cap; 

						Not doing this will exclude the deal from being accounted toward the cap and 
						when CommissionOwed amount from both sources is summed, it will be less than the cap
						*/
						WHEN ((CF.CHARGE_CAP > 0) AND (ISNULL(DBSB1.CommissionOwed_PreCap_RT,0) > CF.CHARGE_CAP) AND (ISNULL(DBSB1.CommissionOwed_PreCap_Prev_RT,0) < CF.CHARGE_CAP)) THEN (CF.CHARGE_CAP - ISNULL(DBSB1.CommissionOwed_PreCap_Prev_RT,0))
						END
					),
			/* SOURCE WEIGHT
				Check if the billing code has both E & V deals within a product group, period, instrument type etc
				#DealBillingSchedule associates the billing schedule to each deal; if the schedule is for 
				"All" sources, the same values (cap, floor etc) will be associated to both E and V deals
				If the schedule is for "All" Sources, COUNT(DISTINCT DBS_AllProd_RT.Source) will return 2 if there are
				both E & V deals within this grouping. It will return 1 if there are either just E or just V deals
				within this grouping

				If the schedule is Source specific, COUNT(DISTINCT DBS_AllProd_RT.Source) will return 1

				This value will be used when checking Floor against commission owed
			*/
			SourceWeight = (SELECT COUNT(DISTINCT DBSB2.Source)
						FROM	#DBS_AllProd_PreCap_RT DBSB2
						WHERE	DBSB1.InvNum = DBSB2.InvNum 
						AND	DBSB1.InvDbId = DBSB2.InvDbId 
						AND	DBSB1.BILLING_CODE = DBSB2.BILLING_CODE
						AND	DBSB1.ProductGroup = DBSB2.ProductGroup
						AND	DBSB1.PeriodId = DBSB2.PeriodId
						AND (CASE WHEN DBSB1.ProductGroup = 'AMSWP' AND DBSB1.Source <> DBSB2.Source THEN 0 ELSE 1 END) = 1 /* NILESH 01/30/2017 -- Added source for AMSWP as we have three different schedules for E, V, and RC */
					),
			/* SHIRISH - 02/23/2016
			   Adding a column for rebate calculations.  When CommissionOwed is -ve then CommissionOwed gets zeroed out
			   Adding this column to be used for rabete calculation and -ve CommissionOwed can be preserved
			*/		
			CommissionOwedForRebate = SUM(CASE WHEN CF.CHARGE_CAP = -1 THEN ISNULL(CommissionOwed_PreCap_AfterDailyCap,DBSB1.CommissionOwed_PreCap)	/* NILESH 09/09/2013 -- Daily Caps */
						WHEN (CF.CHARGE_CAP = 0) THEN 0
						WHEN ((CF.CHARGE_CAP > 0) AND (DBSB1.CommissionOwed_PreCap_RT <= CF.CHARGE_CAP)) THEN ISNULL(CommissionOwed_PreCap_AfterDailyCap,DBSB1.CommissionOwed_PreCap)		/* NILESH 09/09/2013 -- Daily Caps */ 
						/* If previous deal's running total value is just short of cap and the current deal's running total 
						is greater than cap; get the difference between previous running total and cap
						This gives the same result as to getting from CommissionOwed_PreCap_RT, only the 
						amount that is required to reach the cap, which basically will include the deal toward the cap; 

						Not doing this will exclude the deal from being accounted toward the cap and 
						when CommissionOwed amount from both sources is summed, it will be less than the cap
						*/
						WHEN ((CF.CHARGE_CAP > 0) AND (DBSB1.CommissionOwed_PreCap_RT > CF.CHARGE_CAP) AND (DBSB1.CommissionOwed_PreCap_Prev_RT < CF.CHARGE_CAP)) THEN (CF.CHARGE_CAP - DBSB1.CommissionOwed_PreCap_Prev_RT)
						END
					),
			DBSB1.IsActiveStream, -- SHIRISH 04/20/2016
			DBSB1.Security_Currency,
			RepoTicketFees = SUM(DBSB1.RepoTicketFees)

		INTO	#Commissions_AllProd

		FROM	#DBS_AllProd_PreCap_RT DBSB1
		JOIN	#ABS_Cap_Floor_AllProd CF ON DBSB1.InvNum = CF.InvNum 
						AND DBSB1.InvDbId = CF.InvDbId 
						AND DBSB1.BILLING_CODE = CF.BILLING_CODE
						AND DBSB1.PeriodId = CF.PeriodId 
						AND DBSB1.ProductGroup = CF.ProductGroup 
						AND (CASE WHEN DBSB1.ProductGroup = 'AMSWP' AND DBSB1.Source <> CF.Source THEN 0 ELSE 1 END) = 1 /* NILESH 01/30/2017 -- Added source for AMSWP as we have three different schedules for E, V, and RC */

		GROUP BY DBSB1.InvNum, 
			DBSB1.InvDbId, 
			DBSB1.BILLING_CODE, 
			DBSB1.PeriodId,
			DBSB1.ProductGroup, 
			DBSB1.Source,
			DBSB1.IsActiveStream,
			DBSB1.Security_Currency

		--IF @Debug = 1
		--BEGIN
		--	SELECT 'After #Commissions_AllProd Load'
		--	SELECT 'After #Commissions_AllProd load', * FROM #Commissions_AllProd
		--END

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #CommissionsAllProd',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/* NILESH -- Tiered Billing */
		-- Following temp table to be used exclusively for
		-- regenerating the commissionsummary data
		SELECT
			InvNum = DBSB1.InvNum,
			InvDbId = DBSB1.InvDbId,
			BILLING_CODE = DBSB1.BILLING_CODE,
			PeriodId = DBSB1.PeriodId,
			TradeDate = DBSB1.TradeDate,		
			ProductGroup = DBSB1.ProductGroup,
			Source = DBSB1.Source,
			Volume = ISNULL(SUM(DBSB1.AggressiveVolume + DBSB1.PassiveVolume), 0),
			CHARGE_FLOOR = MAX(CF.CHARGE_FLOOR),
			CHARGE_CAP = MAX(CF.CHARGE_CAP),
			CommissionOwed_PreCap = SUM(DBSB1.CommissionOwed_PreCap),
			CommissionOwed_PreCap_Override = SUM(DBSB1.CommissionOwed_PreCap_Override),	/* NILESH 02/14/2012 - Commission Override Change */
			CommissionOwed = SUM(CASE WHEN CF.CHARGE_CAP = -1 THEN DBSB1.CommissionOwed_PreCap 
						WHEN (CF.CHARGE_CAP = 0) THEN 0
						WHEN ((CF.CHARGE_CAP > 0) AND (DBSB1.CommissionOwed_PreCap_RT <= CF.CHARGE_CAP)) THEN DBSB1.CommissionOwed_PreCap 
						/* If previous deal's running total value is just short of cap and the current deal's running total 
						is greater than cap; get the difference between previous running total and cap
						This gives the same result as to getting from CommissionOwed_PreCap_RT, only the 
						amount that is required to reach the cap, which basically will include the deal toward the cap; 

						Not doing this will exclude the deal from being accounted toward the cap and 
						when CommissionOwed amount from both sources is summed, it will be less than the cap
						*/
						WHEN ((CF.CHARGE_CAP > 0) AND (DBSB1.CommissionOwed_PreCap_RT > CF.CHARGE_CAP) AND (DBSB1.CommissionOwed_PreCap_Prev_RT < CF.CHARGE_CAP)) THEN (CF.CHARGE_CAP - DBSB1.CommissionOwed_PreCap_Prev_RT)
						END
					),
			/* SOURCE WEIGHT
				Check if the billing code has both E & V deals within a product group, period, instrument type etc
				#DealBillingSchedule associates the billing schedule to each deal; if the schedule is for 
				"All" sources, the same values (cap, floor etc) will be associated to both E and V deals
				If the schedule is for "All" Sources, COUNT(DISTINCT DBS_AllProd_RT.Source) will return 2 if there are
				both E & V deals within this grouping. It will return 1 if there are either just E or just V deals
				within this grouping

				If the schedule is Source specific, COUNT(DISTINCT DBS_AllProd_RT.Source) will return 1

				This value will be used when checking Floor against commission owed
			*/
			SourceWeight = (SELECT COUNT(DISTINCT DBSB2.Source)
						FROM	#DBS_AllProd_PreCap_RT DBSB2
						WHERE	DBSB1.InvNum = DBSB2.InvNum 
						AND	DBSB1.InvDbId = DBSB2.InvDbId 
						AND	DBSB1.BILLING_CODE = DBSB2.BILLING_CODE
						AND	DBSB1.ProductGroup = DBSB2.ProductGroup
						AND	DBSB1.PeriodId = DBSB2.PeriodId
						AND	DBSB1.TradeDate = DBSB2.TradeDate
						AND (CASE WHEN DBSB1.ProductGroup = 'AMSWP' AND DBSB1.Source <> DBSB2.Source THEN 0 ELSE 1 END) = 1 /* NILESH 01/30/2017 -- Added source for AMSWP as we have three different schedules for E, V, and RC */
					)

		INTO	#Commissions_AllProd_Tier

		FROM	#DBS_AllProd_PreCap_RT DBSB1
		--  IDBBC-178 This is only used by products using tierd billing
		JOIN	IDB_CodeBase.dbo.fnProductType() PT ON PT.ProductInvoiceUsesTieredBilling = 'Y'
												   AND PT.Product = DBSB1.ProductGroup
		JOIN	#ABS_Cap_Floor_AllProd CF ON DBSB1.InvNum = CF.InvNum 
						AND DBSB1.InvDbId = CF.InvDbId 
						AND DBSB1.BILLING_CODE = CF.BILLING_CODE
						AND DBSB1.PeriodId = CF.PeriodId 
						AND DBSB1.ProductGroup = CF.ProductGroup 
						AND (CASE WHEN DBSB1.ProductGroup = 'AMSWP' AND DBSB1.Source <> CF.Source THEN 0 ELSE 1 END) = 1 /* NILESH 01/30/2017 -- Added source for AMSWP as we have three different schedules for E, V, and RC */

		GROUP BY DBSB1.InvNum, 
			DBSB1.InvDbId, 
			DBSB1.BILLING_CODE, 
			DBSB1.PeriodId,
			DBSB1.TradeDate,
			DBSB1.ProductGroup, 
			DBSB1.Source

		--IF @Debug = 1
		--BEGIN
		--	SELECT 'After #Commissions_AllProd_Tier load'
		--	SELECT 'AFter #Commissions_AllProd_Tier load', * FROM #Commissions_AllProd_Tier
		--END

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #CommissionsAllProd_Tier',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/* APPLY FLOOR TO GET COMMISSION OWED
			Cap and floor should be applied at billing code and product group level. If there are multiple 
			billing schedules e.g. different schedules by billing type or different DRate schedules by maturity
			they will all have cap and floor; however, cap and floor should be applied at billing code 
			and product group level
			
			It was decided that we should get MAX(cap) and MAX(floor) i.e get one set of cap and floor at the 
			billing coder and product group level

			If the schedule is for "ALL" sources, the floor applies to ALL (E & V combined) sources as well 
			So when checking for commission floor, both, E & V commission amounts should be treated as one total
			However, the data on commission summary report or invoice inventory will be broken down by each source

			If the sum of CommissionOwed_PreCap for all available sources (just E or just V or E + V) within 
			billing_code, ProductGroup, PeriodId and Billing Schedule grouping is less than the floor, split the
			difference between floor and sum of CommissionOwed_PreCap and add it to CommissionOwed_PreCap of the source

			Example: 
				Floor = 5000
				CommissionOwed_PreCap for E = 3000
				CommissionOwed_PreCap for V = 1000
				Sum of CommissionOwed_PreCap that's short of floor = 1000
				Divide the difference by available sources (in this case E & V) - 1000 / 2
				Add the split value to E and V
				CommissionOwed_PreCap for E after floor is applied = 3500
				CommissionOwed_PreCap for V after floor is applied = 1500

				Total commission = 5000 (Same as floor)
			
			The correlated sub query in the where clause is to ensure that CommissionOwed is updated only for the
			billing codes (within billing_code, ProductGroup, PeriodId and Billing Schedule grouping) where the 
			sum of CommissionOwed_PreCap for all available sources (just E or just V or E + V)is less than the floor

			The correlated sub query in update returns the sum of CommissionOwed_PreCap for all available 
			sources (just E or just V or E + V) within billing_code, ProductGroup, PeriodId and Billing Schedule grouping
		*/
		UPDATE	CB1 
		SET		CB1.CommissionOwed = CB1.CommissionOwed_PreCap 
							+ ((CB1.CHARGE_FLOOR - (SELECT SUM(CB3.CommissionOwed_PreCap) 
										FROM #Commissions_AllProd CB3
										WHERE CB1.BILLING_CODE = CB3.BILLING_CODE
										AND	CB1.ProductGroup = CB3.ProductGroup
										AND	CB1.PeriodId = CB3.PeriodId
										AND (CASE WHEN CB1.ProductGroup = 'AMSWP' AND CB1.Source <> CB3.Source THEN 0 ELSE 1 END) = 1 /* NILESH 01/30/2017 -- Added source for AMSWP as we have three different schedules for E, V, and RC */
										--Source should not be part of this grouping
										GROUP BY CB3.BILLING_CODE, 
											CB3.ProductGroup, 
											CB3.PeriodId )
							) / CB1.SourceWeight)

		FROM	#Commissions_AllProd CB1
		WHERE EXISTS (SELECT 1 FROM #Commissions_AllProd CB2
				WHERE CB1.BILLING_CODE = CB2.BILLING_CODE
				AND	CB1.ProductGroup = CB2.ProductGroup
				AND	CB1.PeriodId = CB2.PeriodId
				AND (CASE WHEN CB1.ProductGroup = 'AMSWP' AND CB1.Source <> CB2.Source THEN 0 ELSE 1 END) = 1 /* NILESH 01/30/2017 -- Added source for AMSWP as we have three different schedules for E, V, and RC */
				--Source should not be part of this grouping
				GROUP BY CB2.BILLING_CODE, CB2.ProductGroup, CB2.PeriodId
				/* Divide CB2.CHARGE_FLOOR by CB2.SourceWeight otherwise CHARGE_FLOOR will double up
					if there deals from multiple sources in this grouping
				*/
				HAVING SUM(CB2.CommissionOwed_PreCap) < SUM(CB2.CHARGE_FLOOR / CB2.SourceWeight)
			)

		/* SHIRISH 2016/10/25: Add Fixed Fees for Short Coupons */
		UPDATE	C
		SET		CommissionOwed = CommissionOwed + (D.CHARGE_CAP/C.SourceWeight)
		FROM	#Commissions_AllProd C
		JOIN	#DealersWFixedFeesForShrtCpn D ON C.BILLING_CODE = D.BILLING_CODE
											   AND C.ProductGroup = D.PRODUCT_GROUP

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After Update #Commissions_AllProd',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		-- IDBBC-189 Add EFP/NAVx fixed fee commission adjustments IF there is no Cap or Floor
		IF @Owner = 'US'
		BEGIN

			-- Get total adjustment for the month by billing code and product
			SELECT	FFS.BillingCode, 
					FFS.productgroup,
					Adjustment = SUM(FFS.amount)
			INTO	#EFPNAVXAdjustments
			FROM	IDB_Billing.dbo.wActiveBilling AS WAB (NOLOCK)
			JOIN	IDB_Billing.dbo.wActiveBillingCodes AS WABC (NOLOCK) ON WABC.Billing_Code = WAB.BILLING_CODE
			JOIN	IDB_Reporting.dbo.FixedFeeSchedule AS FFS (NOLOCK) ON FFS.BillingCode = WABC.Billing_Code
																		AND FFS.productgroup = WABC.ProductGroup
			JOIN	#ABS_Cap_Floor_AllProd ABSCF ON ABSCF.BILLING_CODE = FFS.BillingCode
												  AND ABSCF.ProductGroup = FFS.ProductGroup
			WHERE	WAB.PROCESS_ID = @ProcessID
			AND		WABC.PROCESS_ID = @ProcessID
			AND		(FFS.period_start_date BETWEEN @date1 AND @date2 
					OR 
					-- IDBBC-68 Get monthly fixed fees.  These can be identified by different period start and End dates and month end date will be between period start and end
					(FFS.period_start_date <> FFS.period_end_date AND @date2 BETWEEN FFS.period_start_date AND FFS.period_end_date))
			AND		FFS.productgroup IN ('EFP','NAVX')
			AND		FFS.schedule_type = 'REVENUE'
			AND		FFS.active_ind = 1
			AND		ABSCF.CHARGE_FLOOR = 0
			AND		ABSCF.CHARGE_CAP = -1
			GROUP BY	
					FFS.BillingCode, 
					FFS.productgroup

			-- Apply fixed fee adjustments to the commission where there is no cap or floor
			UPDATE	C
			SET		C.CommissionOwed_PreCap = C.CommissionOwed_PreCap + ENADJ.Adjustment,
					C.CommissionOwed = C.CommissionOwed + ENADJ.Adjustment,
					C.CommissionOwedForRebate = C.CommissionOwedForRebate + ENADJ.Adjustment
			FROM	#Commissions_AllProd C
			JOIN	#EFPNAVXAdjustments ENADJ ON ENADJ.BillingCode = C.BILLING_CODE
											 AND ENADJ.productgroup = C.ProductGroup

		END

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #EFPNAVXAdjustments',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		INSERT INTO #InvoiceInventory_Staging
		(
			InvNum ,
			InvDbId,
			BILLING_CODE,
			PeriodId,
			Logon_Id,
			ProductGroup,
			Source,
			BILLING_PLAN_ID,
			INSTRUMENT_TYPE,
			BILLING_TYPE,
			AggressiveVolume,
			PassiveVolume,
			TotalVolume,
			AggressiveTrades,
			PassiveTrades,
			TotalTrades,
			CHARGE_RATE_AGGRESSIVE,
			CHARGE_RATE_PASSIVE,
			CHARGE_FLOOR,
			CHARGE_CAP,
			CommissionOwed_PreCap,
			CommissionOwed_PreCap_Override,
			CommissionOwed,
			/* NILESH 08/01/2013 -- Tiered Billing */
			TIER_DAILY_CHARGE_CAP,
			TIER_DAILY_CHARGE_FLOOR,
			TIER_BILLING_PLAN_ID,
			TIER_CHARGE_RATE_AGGRESSIVE,
			TIER_CHARGE_RATE_PASSIVE,
			IsActiveStream,
			Security_Currency,
			RepoTicketFees
		)
		SELECT
			InvNum ,
			InvDbId,
			BILLING_CODE,
			PeriodId,
			Logon_Id = @User,
			ProductGroup,
			Source,
			/* NILESH -- Tiered Billing */
			BILLING_PLAN_ID = ISNULL(TIER_BILLING_PLAN_ID, BILLING_PLAN_ID),
			INSTRUMENT_TYPE,
			BILLING_TYPE,
			AggressiveVolume,
			PassiveVolume,
			TotalVolume,
			AggressiveTrades,
			PassiveTrades,
			TotalTrades,
			/* NILESH -- Tiered Billing */
			CHARGE_RATE_AGGRESSIVE = ISNULL(TIER_CHARGE_RATE_AGGRESSIVE,CHARGE_RATE_AGGRESSIVE),
			/* NILESH -- Tiered Billing */
			CHARGE_RATE_PASSIVE = ISNULL(TIER_CHARGE_RATE_PASSIVE,CHARGE_RATE_PASSIVE),
			CHARGE_FLOOR,
			CHARGE_CAP,
			CommissionOwed_PreCap,
			CommissionOwed_PreCap_Override,
			CommissionOwed,
			/* NILESH 08/01/2013 -- Tiered Billing */
			TIER_DAILY_CHARGE_CAP,
			TIER_DAILY_CHARGE_FLOOR,
			TIER_BILLING_PLAN_ID,
			TIER_CHARGE_RATE_AGGRESSIVE,
			TIER_CHARGE_RATE_PASSIVE,
			IsActiveStream,
			Security_Currency,
			RepoTicketFees

		FROM	#InvoiceInventory_Staging_AllProd

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'Insert Invoice Inventory Staging',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		--SUM UP TO GET TOTAL COMMISSION OWED
		INSERT INTO #CommissionOwed
		(
			InvNum,
			InvDbId,
			BILLING_CODE,
			ProductGroup, 
			PeriodId,
			Source, 
			ChargeId,
			Volume,
			CommissionOwed,
			CommissionOwedForRebate,
			IsActiveStream,
			Security_Currency,
			RepoTicketFees
		)
		SELECT
			CB.InvNum ,
			CB.InvDbId,
			CB.Billing_Code,
			CB.ProductGroup,
			CB.PeriodId,
			CB.Source,
			CT.ChargeId,
			Volume = SUM(CB.Volume),
			CommissionOwed = ISNULL(SUM(CB.CommissionOwed), 0),
			CommissionOwedForRebate = ISNULL(SUM(CB.CommissionOwedForRebate),0),
			CB.IsActiveStream,
			CB.Security_Currency,
			RepoTicketFees = SUM(CB.RepoTicketFees)

		FROM	#Commissions_AllProd CB
		--It is ok to use hard coded value Commissions in the join below because this insert is specifically for Commission charge
		JOIN	#ChargeType CT ON CT.ChargeType = 'Commissions'
		/* DIT-10448 : GILTS */
		/* SM 20230503 - Adding BOX */
		/* IDBBC-292 -- Added MATCH product */
		WHERE	CB.ProductGroup IN ('BILL','IOS','TRSY','TIPS','EFP','ECDS','AMSWP','UCDS','CDXEM','USFRN','NAVX','OTR','CAD','EQSWP','COMBO','USREPO','GILTS','BTIC','BOX','EUREPO','MATCH','REVCON')  -- SHIRISH 10/03/2019 GDB-99

		GROUP BY CB.InvNum, 
			CB.InvDbId, 
			CB.Billing_Code, 
			CB.ProductGroup, 
			CB.PeriodId, 
			CB.Source,
			CT.ChargeId,
			CB.IsActiveStream,
			CB.Security_Currency

		
		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #Commissions_AllProd',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/* NILESH -- Tiered Billing */
		-- The table is used for regenerating the commission summary data
		INSERT INTO #CommissionOwed_Tier
		(
			InvNum,
			InvDbId,
			BILLING_CODE,
			ProductGroup, 
			PeriodId,
			TradeDate,
			Source, 
			ChargeId,
			Volume,
			CommissionOwed
		)
		SELECT
			CB.InvNum ,
			CB.InvDbId,
			CB.Billing_Code,
			CB.ProductGroup,
			CB.PeriodId,
			CB.TradeDate,
			CB.Source,
			CT.ChargeId,
			Volume = SUM(CB.Volume),
			CommissionOwed = ISNULL(SUM(CB.CommissionOwed), 0)

		FROM	#Commissions_AllProd_Tier CB
		JOIN IDB_CodeBase.dbo.fnProductType() fp ON CB.ProductGroup = fp.Product
		--It is ok to use hard coded value Commissions in the join below because this insert is specifically for Commission charge
		JOIN	#ChargeType CT ON CT.ChargeType = 'Commissions'
		WHERE ProductInvoiceUsesTieredBilling = 'Y'
		--CB.ProductGroup IN ('BILL', 'IOS', 'TRSY', 'TIPS', 'EFP', 'ECDS', 'AMSWP','UCDS','CDXEM')

		GROUP BY CB.InvNum, 
			CB.InvDbId, 
			CB.Billing_Code, 
			CB.ProductGroup, 
			CB.PeriodId, 
			CB.TradeDate,
			CB.Source,
			CT.ChargeId

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #Commission_AllProd_Tier',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		---------------- BILL,IOS,TRSY,TIPS,EFP,ECDS,AMSWP,CAD,EQSWP,COMBO BLOCK ENDS-----------------

		--SUM UP TO GET TOTAL COMMISSION OWED
		INSERT INTO #CommissionOwed
		(
			InvNum,
			InvDbId,
			BILLING_CODE,
			ProductGroup, 
			PeriodId,
			Source, 
			ChargeId,
			Volume,
			CommissionOwed,
			IsActiveStream
		)
		SELECT
			IIS.InvNum ,
			IIS.InvDbId,
			IIS.Billing_Code,
			IIS.ProductGroup,
			IIS.PeriodId,
			IIS.Source,
			CT.ChargeId,
			Volume = ISNULL(SUM(IIS.AggressiveVolume + IIS.PassiveVolume), 0),
			CommissionOwed = ISNULL(SUM(IIS.CommissionOwed), 0),
			IIS.IsActiveStream

		FROM	#InvoiceInventory_Staging IIS
		--It is ok to use hard coded value Commissions in the join below because this insert is specifically for Commission charge
		JOIN	#ChargeType CT ON CT.ChargeType = 'Commissions'
		WHERE	IIS.ProductGroup = 'AGCY'

		GROUP BY IIS.InvNum, 
			IIS.InvDbId, 
			IIS.Billing_Code, 
			IIS.ProductGroup, 
			IIS.PeriodId, 
			IIS.Source,
			CT.ChargeId,
			IIS.IsActiveStream

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #CommissionOwed',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/* RD: 01/06/2010 - SUM UP CommissionOwed_PreCap TO GET TOTAL PRE CAP COMMISSION. 
		THIS IS FOR THE COMMISSION SUMMARY REPORT. IT SHOULD NOT BE USED FOR INVOICES */

		SELECT
			IIS.InvNum ,
			IIS.InvDbId,
			IIS.Billing_Code,
			IIS.ProductGroup,
			IIS.PeriodId,
			IIS.Source,
			CT.ChargeId,
			CommissionWithoutCap = ISNULL(SUM(IIS.CommissionOwed_PreCap), 0),
			CommissionWithoutCapOverride = ISNULL(SUM(IIS.CommissionOwed_PreCap_Override), 0),	/* NILESH 02/14/2012 - Commission Override Change */
			IIS.IsActiveStream, -- SHIRISH 04/20/2016
			IIS.Security_Currency,
			RepoTicketFees = SUM(IIS.RepoTicketFees)

		INTO #CommissionWithoutCap

		FROM	#InvoiceInventory_Staging IIS
		--It is ok to use hard coded value Commissions in the join below because this insert is specifically for Commission charge
		JOIN	#ChargeType CT ON CT.ChargeType = 'Commissions'

		GROUP BY IIS.InvNum, 
			IIS.InvDbId, 
			IIS.Billing_Code, 
			IIS.ProductGroup, 
			IIS.PeriodId, 
			IIS.Source,
			CT.ChargeId,
			IIS.IsActiveStream,
			IIS.Security_Currency

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #CommissionWithoutCap',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/* NILESH Tiered Billing */
		-- USED TO REGENERATE THE COMMISSIONSUMMARY
		-- DATA WHEN USING TIER RATES. SHOULD NOT BE USED
		-- FOR ANY OTHER PURPOSES.
		SELECT
			IIS.InvNum ,
			IIS.InvDbId,
			IIS.Billing_Code,
			IIS.ProductGroup,
			IIS.PeriodId,
			IIS.TradeDate,
			IIS.Source,
			CT.ChargeId,
			CommissionWithoutCap = ISNULL(SUM(IIS.CommissionOwed_PreCap), 0),
			CommissionWithoutCapOverride = ISNULL(SUM(IIS.CommissionOwed_PreCap_Override), 0)	/* NILESH 02/14/2012 - Commission Override Change */

		INTO #CommissionWithoutCap_Tier

		FROM	#InvoiceInventory_Staging_AllProd_Tier IIS
		JOIN IDB_CodeBase.dbo.fnProductType() fp ON IIS.ProductGroup = fp.Product
		
		--It is ok to use hard coded value Commissions in the join below because this insert is specifically for Commission charge
		JOIN	#ChargeType CT ON CT.ChargeType = 'Commissions'

		WHERE ProductInvoiceUsesTieredBilling = 'Y'
		
		GROUP BY IIS.InvNum, 
			IIS.InvDbId, 
			IIS.Billing_Code, 
			IIS.ProductGroup, 
			IIS.PeriodId, 
			IIS.TradeDate,
			IIS.Source,
			CT.ChargeId

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #CommissionWithoutCap_Tier',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		--GENERATE INVOICE INVENTORY FROM THE STAGING DATA
		--Do not include Commission Owed and Commission Collected in this query because the break down is on Instrument_Type and Billing_Type. 
		--Commission Owed and Commission Collected are invoice level values
		INSERT INTO #InvoiceInventory
		(
			InvNum,
			InvDbId,
			DetailNum,
			logon_id,
			ProductGroup,
			Source,
			Billing_Plan_Id,
			instrument_type,
			ItemAmount,
			InvInvTypeId,
			who,
			created,
			periodid,
			ChargeId
		) 
		SELECT	
			InvNum = UnPvt.InvNum ,
			InvDbId = UnPvt.InvDbId,
			DetailNum = NULL, 
			Logon_Id = UnPvt.Logon_Id ,
			ProductGroup = UnPvt.ProductGroup,
			Source = UnPvt.Source,
			Billing_Plan_Id = UnPvt.Billing_Plan_Id,
			Instrument_Type = UnPvt.Instrument_Type,
			ItemAmount = UnPvt.ItemAmount,
			InvInvTypeId = IIT.InvInvTypeId,
			Who = UnPvt.Logon_Id ,
			Created = GETDATE(),
			PeriodId = PeriodId,
			ChargeId = UnPvt.ChargeId

		FROM
		(
			SELECT
				InvNum ,
				InvDbId,
				Billing_Code,
				PeriodId,
				Logon_Id ,
				ProductGroup,
				Source,
				Billing_Plan_Id,	--= ISNULL(TIER_BILLING_PLAN_ID, Billing_Plan_Id),
				Instrument_Type,
				Billing_Type,
				AggressiveVolume = CONVERT(float, AggressiveVolume),
				PassiveVolume = CONVERT(float, PassiveVolume),
				TotalVolume = CONVERT(float, TotalVolume),
				AggressiveTrades = CONVERT(float, AggressiveTrades),
				PassiveTrades = CONVERT(float, PassiveTrades),
				TotalTrades = CONVERT(float, TotalTrades),
				CHARGE_RATE_AGGRESSIVE = CONVERT(float, CHARGE_RATE_AGGRESSIVE),	--CONVERT(float, ISNULL(TIER_CHARGE_RATE_AGGRESSIVE, CHARGE_RATE_AGGRESSIVE)),
				CHARGE_RATE_PASSIVE = CONVERT(float, CHARGE_RATE_PASSIVE),	--CONVERT(float, ISNULL(TIER_CHARGE_RATE_PASSIVE,CHARGE_RATE_PASSIVE)),
				CHARGE_FLOOR = CONVERT(float, CHARGE_FLOOR),
				CHARGE_CAP = CONVERT(float, CHARGE_CAP),
				ChargeId = CT.ChargeId --, --ChargeId will be used later to join InvoiceDetail to get corresponding DetailNum
				--CommissionOwed = CONVERT(float, CommissionOwed),
				/* NILESH 08/01/2013 -- Tiered Billing */
				--TIER_BILLING_PLAN_ID = TIER_BILLING_PLAN_ID,
				--TIER_CHARGE_RATE_AGGRESSIVE = TIER_CHARGE_RATE_AGGRESSIVE,
				--TIER_CHARGE_RATE_PASSIVE = TIER_CHARGE_RATE_PASSIVE

			FROM	#InvoiceInventory_Staging

			--It is ok to use hard coded value Commissions in the join below because this data is specifically for Commissions
			JOIN	#ChargeType CT ON CT.ChargeType = 'Commissions'
		) IIS
		UNPIVOT (ItemAmount FOR InventoryCode IN (AggressiveVolume, PassiveVolume, TotalVolume, AggressiveTrades, PassiveTrades, TotalTrades, Charge_Rate_Aggressive, Charge_Rate_Passive, Charge_Floor, Charge_Cap)) AS UnPvt	--, TIER_CHARGE_RATE_AGGRESSIVE, TIER_CHARGE_RATE_PASSIVE)) AS UnPvt
		JOIN IDB_Billing.dbo.InvoiceInventoryType IIT ON UnPvt.InventoryCode = IIT.Code 
					AND IIT.Billing_Type = UnPvt.Billing_Type
					/*Replacing this AND caluse with the one above for now. This should be used if CommissionOwed and/or CommissionCollected need to be grouped by instrument_type
					AND (ISNULL(IIT.Billing_Type, '') = (CASE WHEN UnPvt.InventoryCode IN ('CommissionOwed', 'CommissionCollected') THEN '' ELSE UnPvt.Billing_Type END)) */
					
		/* NILESH 09/12/2011 : 
		-- Added following condition to eliminate any line items with a value of 0. This could occur when we try to include a line item for a 
		-- dealer with a floor amount but has 0 (zero) deals in the billing period. 
		*/			
		WHERE	UnPvt.ItemAmount <> 0	

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #InvoiceInventory Insert1',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		--INSERT COMMISSION OWED AND COMMISSION COLLECTED INTO #InvoiceInventory
		INSERT INTO #InvoiceInventory
		(
			InvNum,
			InvDbId,
			DetailNum,
			logon_id,
			ProductGroup,
			Source,
			Billing_Plan_Id,
			instrument_type,
			ItemAmount,
			ItemAmountForRebate,
			InvInvTypeId,
			who,
			created,
			periodid,
			ChargeId
		) 
		SELECT
			InvNum = CO.InvNum,
			InvDbId = CO.InvDbId,
			DetailNum = NULL,
			Logon_Id = @User,
			ProductGroup = CO.ProductGroup,
			Source = CO.Source,
			Billing_Plan_Id = NULL, --Commission owed is not broken by billing plan id in invoice inventory. This is the total commission owed by product group
			Instrument_Type = NULL, --Commission owed is not broken by instrument type in invoice inventory. This is the total commission owed by product group
			ItemAmount = CO.CommissionOwed,
			/* IDBBC-75
			 * For NAVX we want to preserve ItemAmountForRebate as this gets applied to the current period's commission owed amount and for remaining rebate calculation
			 * For rest of the products we are setting it to 0 as we are adding a separate line item to invoice as Rebate using a union query below
			 */
			ItemAmountForRebate = CASE WHEN CO.ProductGroup = 'NAVX' THEN CO.CommissionOwedForRebate ELSE 0 END, 
			InvInvTypeId = IIT.InvInvTypeId,
			Who = @User,
			Created = GETDATE(),
			PeriodId = PeriodId,
			ChargeId = CO.ChargeId

		FROM	#CommissionOwed CO
		--It is ok to use hard coded value CommissionOwed in the join below because this join is specifically for Commission Owed
		JOIN	IDB_Billing.dbo.InvoiceInventoryType IIT ON IIT.Code = 'CommissionOwed' AND IIT.Billing_Type IS NULL 

		UNION ALL

		/* IDBBC-75
		 * For OTR when rebate is available then add a separate line item. 0 in CommissionOwed and -ve CommissionOwedForRebate indicates its a rebate
		 */
		SELECT
			InvNum = CO.InvNum,
			InvDbId = CO.InvDbId,
			DetailNum = NULL,
			Logon_Id = @User,
			ProductGroup = CO.ProductGroup,
			Source = CO.Source,
			Billing_Plan_Id = NULL, --Commission owed is not broken by billing plan id in invoice inventory. This is the total commission owed by product group
			Instrument_Type = NULL, --Commission owed is not broken by instrument type in invoice inventory. This is the total commission owed by product group
			ItemAmount = CO.CommissionOwedForRebate,
			ItemAmountForRebate = CO.CommissionOwedForRebate, 
			InvInvTypeId = IIT.InvInvTypeId,
			Who = @User,
			Created = GETDATE(),
			PeriodId = PeriodId,
			ChargeId = CO.ChargeId

		FROM	#CommissionOwed CO
		JOIN	IDB_Billing.dbo.InvoiceInventoryType IIT ON IIT.Code = 'OTRREBATE' AND IIT.Billing_Type IS NULL 
		WHERE	CO.ProductGroup = 'OTR'
		AND		CO.CommissionOwed = 0
		AND		CO.CommissionOwedForRebate < 0

		UNION ALL

		/* IDBBC-310
		 * Add EUREPO ticket fees on a separate line item
		 */
		SELECT
			InvNum = RTF.InvNum,
			InvDbId = RTF.InvDbId,
			DetailNum = NULL,
			Logon_Id = @User,
			ProductGroup = RTF.ProductGroup,
			Source = RTF.Source,
			Billing_Plan_Id = NULL, --Commission owed is not broken by billing plan id in invoice inventory. This is the total commission owed by product group
			Instrument_Type = NULL, --Commission owed is not broken by instrument type in invoice inventory. This is the total commission owed by product group
			ItemAmount = RTF.TicketFees,
			ItemAmountForRebate = 0, 
			InvInvTypeId = IIT.InvInvTypeId,
			Who = @User,
			Created = GETDATE(),
			PeriodId = RTF.PeriodId,
			ChargeId = RTF.ChargeId

		FROM	#RepoTicketFeesByInvoice RTF
		JOIN	IDB_Billing.dbo.InvoiceInventoryType IIT ON IIT.Code = 'EUREPOFEES' AND IIT.Billing_Type IS NULL 
		WHERE	RTF.ProductGroup = 'EUREPO'

		UNION ALL

		SELECT
			InvNum = CC.InvNum,
			InvDbId = CC.InvDbId,
			DetailNum = NULL,
			Logon_Id = @User, 
			ProductGroup = CC.ProductGroup,
			Source = CC.Source,
			Billing_Plan_Id = NULL, --Commission owed is not broken by billing plan id in invoice inventory. This is the total commission owed by product group
			Instrument_Type = NULL,--Commission collected is not broken by instrument type in invoice inventory. This is the total commission collected by product group
			--Multiply with -1 to negate the value. This gives the effect of crediting Commission Collected so when all the 
			--charges (commission owed, commission collected, fees etc) in the inventory are summed up commission collected is deducted from the total
			ItemAmount = (CC.CommissionCollected * -1),
			ItemAmountForRebate = NULL,
			InvInvTypeId = IIT.InvInvTypeId,
			Who = @User ,
			Created = GETDATE(),
			PeriodId = PeriodId,
			ChargeId = CC.ChargeId

		FROM	#CommissionCollected CC
		--It is ok to use hard coded value CommissionCollected in the join below because this join is specifically for Commission Collected
		JOIN	IDB_Billing.dbo.InvoiceInventoryType IIT ON IIT.Code = 'CommissionCollected' AND IIT.Billing_Type IS NULL 

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #InvoiceInventory Insert2',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		--CALCULATE NET COMMISSION
		SELECT
			CO.InvNum ,
			CO.InvDbId,
			CO.Billing_Code,
			CO.ProductGroup,
			CO.PeriodId,
			CO.Source,
			CO.ChargeId,
			CO.Volume,
			CommissionOwed = CO.CommissionOwed,
			CommissionCollected = CC.CommissionCollected,
			NetCommission = CO.CommissionOwed - CC.CommissionCollected, --DW perspective
			CO.IsActiveStream,
			CO.Security_Currency,
			CO.RepoTicketFees

		INTO #NetCommission

		FROM	#CommissionOwed CO
		JOIN	#CommissionCollected CC ON 
				CO.InvNum = CC.InvNum 
				AND CO.InvDbId = CC.InvDbId 
				AND CO.Billing_Code = CC.Billing_Code 
				AND CO.ProductGroup = CC.ProductGroup
				AND CO.PeriodId = CC.PeriodId 
				AND CO.Source = CC.Source
				AND CO.ChargeId = CC.ChargeId
				AND ISNULL(CO.IsActiveStream,0) = ISNULL(CC.IsActiveStream,0)
				AND ISNULL(CO.Security_Currency,'') = ISNULL(CC.Security_Currency,'')

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #NetCommission',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/* NILESH -- Tiered Billing */
		-- Table used to regenerate the commissionsummary
		-- data using tiered rates.
		SELECT
			CO.InvNum ,
			CO.InvDbId,
			CO.Billing_Code,
			CO.ProductGroup,
			CO.PeriodId,
			CO.TradeDate,
			CO.Source,
			CO.ChargeId,
			CO.Volume,
			CommissionOwed = CO.CommissionOwed,
			CommissionCollected = CC.CommissionCollected,
			NetCommission = CO.CommissionOwed - CC.CommissionCollected --DW perspective

		INTO #NetCommission_Tier

		FROM	#CommissionOwed_Tier CO
		JOIN	#CommissionCollected_Tier CC ON 
				CO.InvNum = CC.InvNum 
				AND CO.InvDbId = CC.InvDbId 
				AND CO.Billing_Code = CC.Billing_Code 
				AND CO.ProductGroup = CC.ProductGroup
				AND CO.PeriodId = CC.PeriodId 
				AND CO.TradeDate = CC.TradeDate
				AND CO.Source = CC.Source
				AND CO.ChargeId = CC.ChargeId

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #NetCommission_Tier',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/****************** TRACE CHARGES ***************************/
		/* RD: 03/03/2010 - GET TRACE DEALS. JOIN ACTIVE BRANCE, ACTIVE BILLING ECT TO GET INVNUM, BILLING_CODE AND PERIOD */
		/* RD: 05/12/2010 - See modification comments made on 05/12/2010
					TRACE charges do not apply to BILL
		*/
		/* RD:07/12/2010 - IDB_Monitor database will split into 3 databases in rel 18. As a result, we now have a view
					for TRACE. Changed the TRACE query to replace base table with the view
		*/

		/* RD:10/18/2010 - NOTE FOR IOS
			TRACE for IOS will not go live until Feb 2011 (need to confirm the date)
			Need to change the ProductGroup filter in where clause when TRACE starts for IOS
		*/
		/* NS: 03/02/2011 - TRACE charges do not apply to TRSY (OFTR) */

		/* NS: 05/10/2011 
		-- Generate list of Trace records using the trace setup data.
		-- Here we want to include all the trace records for all the 
		-- billing code & product combination available in the trace
		-- setup list.
		*/
		
		/* NILESH 
		-- Historical Data Related changes. We want to here extract individual
		-- Trace eligible product get the corresponding dates and use to get the
		-- trace data.
		*/
		
		/* SHIRISH - 11/4/2014 - Updating code to remove cursor and use while loop */
		DECLARE @TraceProduct varchar(8)
		/* SHIRISH - 03/09/2017 
		-- Moving ETraceBillable and VTraceBillable to cursor and eliminating need to join with TraceEligibleProducts
		-- again in query to get #TraceDeals
		*/
		DECLARE @ETraceBillable char(1), @VTraceBillable char(1), @TraceEligibleStartDate datetime
		
		INSERT INTO #Range_Cursor 
		(
			CurrentStartDate,
			CurrentEndDate,
			ProductGroup,
			ETraceBillable,
			VTraceBillable,
			TraceEligibleStartDate,
			RowNum
		)
		SELECT	CurrentStartDate = @date1,
				CurrentEndDate = @date2,
				Product,
				tep.ETraceBillable,
				tep.VTraceBillable,
				TraceEligibleStartDate,
				ROW_NUMBER() OVER (ORDER BY Product) as RowNum
		FROM	#TraceEligibleProducts tep
		ORDER BY Product 

		SET @RowCounter = 1
		SET @MaxRows = (SELECT COUNT(*) FROM #Range_Cursor)

		WHILE (@RowCounter <= @MaxRows) -- this loop replaces cursor
		BEGIN	-- Begin cursor loop 
		
			
			SELECT	@CurrentStartDate = CurrentStartDate,
					@CurrentEndDate = CurrentEndDate,
					@TraceProduct = ProductGroup,
					@ETraceBillable = ETraceBillable,
					@VTraceBillable = VTraceBillable,
					@TraceEligibleStartDate = TraceEligibleStartDate
			FROM	#Range_Cursor
			WHERE	RowNum = @RowCounter	
			

			/* SHIRISH 03/09/2017:
			 * Below query had two left joins with TW_User and fnGetTraceExceptionUsers(), somehow when both left joins are enabled this query is taking very long time to complete.  
			 * When running in YTD mode it takes more than 2 hours to run.  To reduce this time we are separating these two left joins into two UNION queries with inner join. 
			 *
			 * SHIRISH 01/19/2022:
			 * IDBBC-158 Removing stored procedure call to load Trace trades.  Trace trades are already loaded into IDB_TRACE_DETAILS table and can be referred directly from table
			 */

			INSERT INTO #TraceDeals
			(
				Billing_Code,
				PeriodId,
				TradeDate,
				Deal_Negotiation_Id,
				Dealer,
				Trader_Id,
				ProductGroup,
				Side,
				Trace_Status,
				TraceSubmissionFees,
				TracePassThruFees,
				EffectiveStartDate,
				EffectiveEndDate,
				TracePassThruFlag,
				TraceSource
			)
			SELECT	Billing_Code = ABT.Billing_Code,
					PeriodId = P.PeriodId,
					TradeDate = Trace.trd_date,
					Deal_Negotiation_Id = Trace.Deal_Id,
					Dealer = Trace.Dealer,
					Trader_Id = Trace.Trader_Id,
					ProductGroup = Trace.ProductGroup,
					Side = Case Trace.side WHEN 1 THEN 'B' ELSE 'O' END,
					Trace_Status = Trace.Trace_Status,
					TraceSubmissionFees = ts.SubmissionFee,	/* These are the TRACE charges incurred during the submission */
					TracePassThruFees = CASE ABT.TracePassThruFlag WHEN 'Y' THEN ts.SubmissionFee ELSE CAST(0 AS FLOAT) END,			/* These are the TRACE charges that will be passed thru to the dealer */
					ABT.EffectiveStartDate,
					ABT.EffectiveEndDate,
					ABT.TracePassThruFlag,
					TraceSource = Trace.Source
			
			FROM	IDB_Reporting.dbo.IDB_TRACE_DETAILS AS Trace -- IDBBC-16
			JOIN	#TraceSchedule ts ON Trace.ProductGroup = ts.ProductGroup
			JOIN	IDB_Billing.dbo.wPeriodIDs P (NOLOCK) ON MONTH(Trace.trd_date) = MONTH(P.PeriodDate) AND YEAR(Trace.trd_date) = YEAR(P.PeriodDate) AND P.PROCESS_ID = @ProcessID -- SHIRISH 11/4/2014 -- updating query to use permanent table
			JOIN	IDB_Billing.dbo.wActiveBranch B (NOLOCK) ON P.PROCESS_ID = B.PROCESS_ID AND Trace.Dealer = B.COMPANY_ACRONYM COLLATE SQL_Latin1_General_Cp437_CI_AS
			JOIN	#TracePassThruSetup ABT ON B.Billing_Code = ABT.Billing_Code AND Trace.ProductGroup = ABT.ProductGroup
			JOIN	Falcon.dbo.[User] as UF WITH (NOLOCK) ON UF.UserLogin COLLATE SQL_Latin1_General_Cp437_CI_AS = Trace.Trader_Id AND UF.CompanyID = B.BRANCH_ID
			
			/* Shirish 02/19/2019: DIT-18425 Trd_Date is same as Trace_Submission_Date which is a calculated field so switching to Trd_Date improves performance of the query */
			WHERE	Trace.Trd_Date Between @CurrentStartDate AND @CurrentEndDate
					AND (CASE WHEN Trace.Source = 'E' and @ETraceBillable = 'Y' THEN 1
					          WHEN Trace.Source = 'V' and @VTraceBillable = 'Y' THEN 1
							  ELSE 0
						 END) = 1
				/*
				-- Following condition is to ensure that we do not get include the Trace records prior to the actual 
				-- live date of Trace for a particular trace eligible product
				-- 1st March 2010 for AGCY since that is the day it went live
				-- 16th May 2011 for MBS since that is the day it went live	
				*/
				/* Shirish 02/19/2019: DIT-18425 Trd_Date is same as Trace_Submission_Date which is a calculated field so switching to Trd_Date improves performance of the query */
				--AND	(Trace.Trace_Submission_Date >= @TraceEligibleStartDate)
				AND	(Trace.Trd_Date >= @TraceEligibleStartDate)
				/*
				-- Following condition is to ensure that each trace record pairs with the correct trace setup record 
				-- do determine the correct state for TracePassThruFlag value on that Trace Submission Date
				*/		
				AND (ABT.EffectiveStartDate IS NULL OR	(ABT.EffectiveStartDate IS NOT NULL AND Trace.Trace_Submission_Date BETWEEN ABT.EffectiveStartDate AND ABT.EffectiveEndDate))
				
				/* NILESH: 10/04/2011
				-- Following condition is to ensure that we pick up the correct Trace Submission fees when there 
				-- are multiple records with different effective date ranges
				*/
				AND	(Trace.Trace_Submission_Date BETWEEN ts.EffectiveStartDate AND ts.EffectiveEndDate)		
				
				AND	(Trace.ProductGroup = @TraceProduct)

			UNION ALL

			SELECT	Billing_Code = ABT.Billing_Code,
					PeriodId = P.PeriodId,
					TradeDate = Trace.trd_date,
					Deal_Negotiation_Id = Trace.Deal_Id,
					Dealer = Trace.Dealer,
					Trader_Id = Trace.Trader_Id,
					ProductGroup = Trace.ProductGroup,
					Side = Case Trace.side WHEN 1 THEN 'B' ELSE 'O' END,
					Trace_Status = Trace.Trace_Status,
					TraceSubmissionFees = ts.SubmissionFee,	/* These are the TRACE charges incurred during the submission */
					TracePassThruFees = CASE ABT.TracePassThruFlag WHEN 'Y' THEN ts.SubmissionFee ELSE CAST(0 AS FLOAT) END,			/* These are the TRACE charges that will be passed thru to the dealer */
					ABT.EffectiveStartDate,
					ABT.EffectiveEndDate,
					ABT.TracePassThruFlag,
					TraceSource = Trace.Source
			
			FROM	IDB_Reporting.dbo.IDB_TRACE_DETAILS AS Trace -- IDBBC-16
			JOIN	#TraceSchedule ts ON Trace.ProductGroup = ts.ProductGroup
			JOIN	IDB_Billing.dbo.wPeriodIDs P (NOLOCK) ON MONTH(Trace.trd_date) = MONTH(P.PeriodDate) AND YEAR(Trace.trd_date) = YEAR(P.PeriodDate) AND P.PROCESS_ID = @ProcessID -- SHIRISH 11/4/2014 -- updating query to use permanent table
			JOIN	IDB_Billing.dbo.wActiveBranch B (NOLOCK) ON P.PROCESS_ID = B.PROCESS_ID AND Trace.Dealer = B.COMPANY_ACRONYM COLLATE SQL_Latin1_General_Cp437_CI_AS -- SHIRISH 11/4/2014 -- updating query to use permanent table
			JOIN	#TracePassThruSetup ABT ON B.Billing_Code = ABT.Billing_Code AND Trace.ProductGroup = ABT.ProductGroup
			/* NILESH 10/30/2013 : Added join to the list of exception users which are created to shadow the original user from getting displayed in the blotter (Business requirement) */
			/* SHIRISH 03/09/2017 - Updating below join to inner join */
			JOIN IDB_Reporting.dbo.fnGetTraceExceptionUsers() ExpUser ON Trace.Trader_Id = ExpUser.Exp_User_ID COLLATE SQL_Latin1_General_Cp437_CI_AS AND Trace.Dealer = ExpUser.Exp_Dealer	COLLATE SQL_Latin1_General_Cp437_CI_AS
			
			/* Shirish 02/19/2019: DIT-18425 Trd_Date is same as Trace_Submission_Date which is a calculated field so switching to Trd_Date improves performance of the query */
			WHERE	Trace.Trd_Date Between @CurrentStartDate AND @CurrentEndDate
					AND (CASE WHEN Trace.Source = 'E' and @ETraceBillable = 'Y' THEN 1
					          WHEN Trace.Source = 'V' and @VTraceBillable = 'Y' THEN 1
							  ELSE 0
						 END) = 1
				/*
				-- Following condition is to ensure that we do not get include the Trace records prior to the actual 
				-- live date of Trace for a particular trace eligible product
				-- 1st March 2010 for AGCY since that is the day it went live
				-- 16th May 2011 for MBS since that is the day it went live	
				*/
				/* Shirish 02/19/2019: DIT-18425 Trd_Date is same as Trace_Submission_Date which is a calculated field so switching to Trd_Date improves performance of the query */
				--AND	(Trace.Trace_Submission_Date >= @TraceEligibleStartDate)
				AND	(Trace.Trd_Date >= @TraceEligibleStartDate)

				/*
				-- Following condition is to ensure that each trace record pairs with the correct trace setup record 
				-- do determine the correct state for TracePassThruFlag value on that Trace Submission Date
				*/		
				AND (ABT.EffectiveStartDate IS NULL OR	(ABT.EffectiveStartDate IS NOT NULL AND Trace.Trace_Submission_Date BETWEEN ABT.EffectiveStartDate AND ABT.EffectiveEndDate))
				
				/* NILESH: 10/04/2011
				-- Following condition is to ensure that we pick up the correct Trace Submission fees when there 
				-- are multiple records with different effective date ranges
				*/
				AND	(Trace.Trace_Submission_Date BETWEEN ts.EffectiveStartDate AND ts.EffectiveEndDate)		
				
				AND	(Trace.ProductGroup = @TraceProduct)

			--  SHIRISH 07/22/2019 DIT-18425
			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'Trace for ' + @TraceProduct,DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2
						
			SET @RowCounter = @RowCounter + 1
					
		END -- End cursor loop

		TRUNCATE TABLE #Range_Cursor
		
		/* Get all the trace deals whose trace charges will be invoiced */
		SELECT	
			InvNum = ABC.InvNum,
			InvDbId = ABC.InvDbId,
			Billing_Code = Trace.Billing_Code,
			PeriodId = Trace.PeriodId,
			TradeDate = Trace.TradeDate,
			Deal_Negotiation_Id = Trace.Deal_Negotiation_Id,
			Dealer = Trace.Dealer,
			Trader_Id = Trace.Trader_Id,
			ProductGroup = Trace.ProductGroup,
			Side = Trace.Side,
			Trace_Status = Trace.Trace_Status,
			TraceSubmissionFees = Trace.TraceSubmissionFees,
			TracePassThruFees = Trace.TracePassThruFees,
			TraceSource = trace.TraceSource
			
		INTO	#TraceDealsForInvoice

		FROM    #TraceDeals trace
		JOIN	IDB_Billing.dbo.wActiveBillingCodes ABC (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table
				ON trace.BILLING_CODE = ABC.BILLING_CODE AND Trace.ProductGroup = ABC.ProductGroup AND ABC.PROCESS_ID = @ProcessID
		/* Fail Safe condition */
		WHERE	trace.TracePassThruFlag = 'Y'

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'Trace for Invoice ',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2
					
		/* Get all the trace deals whose trace charges will not be invoiced but need to be reported on the TRACE charges report */	
		SELECT	Billing_Code = Trace.Billing_Code,
			PeriodId = Trace.PeriodId,
			TradeDate = Trace.TradeDate,
			Deal_Negotiation_Id = Trace.Deal_Negotiation_Id,
			Dealer = Trace.Dealer,
			Trader_Id = Trace.Trader_Id,
			ProductGroup = Trace.ProductGroup,
			Side = Trace.Side,
			Trace_Status = Trace.Trace_Status,
			TraceSubmissionFees = Trace.TraceSubmissionFees,
			TracePassThruFees = Trace.TracePassThruFees,
			TraceSource = trace.TraceSource
			
		INTO	#TraceDealsForCommReportOnly

		FROM    #TraceDeals trace
		
		WHERE	trace.TracePassThruFlag = 'N'

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'Trace for #TraceDealsForCommReportOnly',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2
		
		/* Determine the total trace pass thru charges */
		SELECT	InvNum,
			InvDbId,
			Billing_Code,
			PeriodId,
			ProductGroup = td.ProductGroup,
			/* TRACE charges do not apply to BILL
				Although Source does not apply to AGCY, need the value because Source is used in joins below
				DW AGCY contains only E data, so it's ok to hard-code E for Source here.

			*/
			/*NILESH: Now will be using the Source value returned from the trace view */
			Source = td.TraceSource,	--CONVERT(varchar(8), 'E'), 
			ChargeType.ChargeId,
			TradeCt = COUNT(*), /* It's ok to do count(*) because each row in #TraceDeals represents 1 trace eligible trade */
			TraceSubmissionFees = SUM(TraceSubmissionFees),
			TracePassThruFees = SUM(TracePassThruFees)
			
		INTO	#TF
		
		FROM	#TraceDealsForInvoice as td	-- #TraceFees as td
		--It is ok to use hard coded value TRACE in the join below because this insert is specifically for TRACE fees
		JOIN	#ChargeType ChargeType ON ChargeType.ChargeType = 'TRACE'

		/* This condition is just a fail safe. Although at this point #TraceDeals will only contain AGCY data
			adding the filter just in case
		*/
		JOIN	#TraceEligibleProducts tp ON td.ProductGroup = tp.Product
		--WHERE	ProductGroup = 'AGCY' 

		GROUP BY td.InvNum, td.InvDbId, td.Billing_Code, td.PeriodId, td.ProductGroup, ChargeType.ChargeId,td.TraceSource

		/* Determine the trace charges which need to be reported but not pass through */
		SELECT	Billing_Code,
			PeriodId,
			ProductGroup = td.ProductGroup,
			/* TRACE charges do not apply to BILL
				Although Source does not apply to AGCY, need the value because Source is used in joins below
				DW AGCY contains only E data, so it's ok to hard-code E for Source here.

			*/
			Source = td.TraceSource,	--CONVERT(varchar(8), 'E'), 
			ChargeType.ChargeId,
			TradeCt = COUNT(*), /* It's ok to do count(*) because each row in #TraceDeals represents 1 trace eligible trade */
			TraceSubmissionFees = SUM(TraceSubmissionFees),
			TracePassThruFees = SUM(TracePassThruFees)
			
		INTO	#TF_COMMREPORT
		
		FROM	#TraceDealsForCommReportOnly as td	
		--It is ok to use hard coded value TRACE in the join below because this insert is specifically for TRACE fees
		JOIN	#ChargeType ChargeType ON ChargeType.ChargeType = 'TRACE'

		/* This condition is just a fail safe.*/
		JOIN	#TraceEligibleProducts tp ON td.ProductGroup = tp.Product

		GROUP BY td.Billing_Code, td.PeriodId, td.ProductGroup, ChargeType.ChargeId,td.TraceSource

		
		/* NILESH -- Tiered Billing */
		-- The temp table is used to regenerate 
		-- CommissionSummary data using Tier Rates.
		SELECT	InvNum,
			InvDbId,
			Billing_Code,
			PeriodId,
			TradeDate,
			ProductGroup = td.ProductGroup,
			/* TRACE charges do not apply to BILL
				Although Source does not apply to AGCY, need the value because Source is used in joins below
				DW AGCY contains only E data, so it's ok to hard-code E for Source here.

			*/
			/*NILESH: Now will be using the Source value returned from the trace view */
			Source = td.TraceSource,	--CONVERT(varchar(8), 'E'), 
			ChargeType.ChargeId,
			TradeCt = COUNT(*), /* It's ok to do count(*) because each row in #TraceDeals represents 1 trace eligible trade */
			TraceSubmissionFees = SUM(TraceSubmissionFees),
			TracePassThruFees = SUM(TracePassThruFees)
			
		INTO	#TF_Tier
		
		FROM	#TraceDealsForInvoice as td	-- #TraceFees as td
		JOIN IDB_CodeBase.dbo.fnProductType() fp ON td.ProductGroup = fp.Product
		--It is ok to use hard coded value TRACE in the join below because this insert is specifically for TRACE fees
		JOIN	#ChargeType ChargeType ON ChargeType.ChargeType = 'TRACE'

		/* This condition is just a fail safe. Although at this point #TraceDeals will only contain AGCY data
			adding the filter just in case
		*/
		JOIN	#TraceEligibleProducts tp ON td.ProductGroup = tp.Product
		
		WHERE	fp.ProductInvoiceUsesTieredBilling = 'Y'

		GROUP BY td.InvNum, td.InvDbId, td.Billing_Code, td.PeriodId, td.TradeDate, td.ProductGroup, ChargeType.ChargeId,td.TraceSource

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After Trace Fees',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		---------------End Trace related queries-------------------------------------

		-- ICAP XRT Fees
		-- IDBBC-77 Updating ICAP XRT fee report query below to match query used in code to calculate fees
		IF @Debug = 1 AND @BillingCode = 'ICAP1'
		BEGIN

			;WITH CTE_FP
			AS
			(
				SELECT	DISTINCT 
						trade_date = IFD.DEAL_TRADE_DATE,
						deal_id = IFD.DEAL_NEGOTIATION_ID,
						liquidity_provider = IFD.PoolParticipant
				--FROM Actives_Report.dbo.FirmLiquidityProviders 
				--WHERE trade_date BETWEEN @date1 AND @date2
				--AND firm_id = 'ICAP'

				FROM	IDB_Reporting.dbo.IDB_FALCON_DEALS AS IFD 
				WHERE	IFD.DEAL_TRADE_DATE BETWEEN @date1 AND @date2
				AND		Dealer = 'ICAP'
				AND		IFD.IsActiveStream = 1
				AND		IFD.DEAL_LAST_VERSION = 1
				AND		IFD.DEAL_STATUS <> 'CANCELLED'
			)
			SELECT 'ICAP FEES WITH PARTICIPANT',
				CT.Dealer,
				CT.Trader,
				CT.BILLING_CODE,
				CT.Trd_Dt,
				CT.ProductGroup,
				CT.Quantity,
				CT.Cancelled,
				CT.Source,
				CT.DEAL_ID,
				CT.Side,
				CT.IsActiveStream,
				CT.SwSecType,
				CT.DEAL_SECURITY_ID,
				Submission_Netting_Fees = (CASE WHEN Cancelled = 1 THEN CS.SubmissionFee
												   ELSE CS.SubmissionFee + CS.FixedNettingFee 
											END) + 
										  ((CS.VariableNettingFee) * CASE WHEN Cancelled = 1 THEN 0 ELSE Quantity END),
				X.liquidity_provider

			FROM	#ClearingTrades CT
			--It is ok to use hard coded value Clearing in the join below because this insert is specifically for Submission & Netting Fees
			JOIN	#ChargeType ChargeType ON ChargeType.ChargeType = 'Clearing'
			JOIN	#ClearingSchedule CS ON CT.ProductGroup = CS.ProductGroup AND CS.TradeType = 'DW' 
			LEFT JOIN CTE_FP X ON X.trade_date = CT.Trd_Dt AND X.deal_id = CT.DEAL_ID
			/* NILESH: 10/04/2011
			-- Following condition is to ensure that we pick up the correct Clearing fees when there are 
			-- multiple records with different effective date ranges
			-- SHIRISH: 07/21/2015
			-- We do not want to show FICC charges for OTR at the moment.  So excluding OTR product
			-- NILESH: 01/16/2018
			-- Modified the condition to include the OTR submission fees for reporting purposes only as per business.
			-- SHIRISH: 01/30/2018
			-- Need to add FICC charges for OTR, commenting condition below that only 
			*/
			WHERE	CT.ProductGroup NOT IN ('EFP','NAVX')
			--AND		(CASE WHEN @ReportMode = 0 AND CT.ProductGroup = 'OTR' THEN 0 ELSE 1 END) = 1 
			AND		CT.Trd_Dt BETWEEN CS.EffectiveStartDate AND CS.EffectiveEndDate
			-- SHIRISH 01/15/2019: For non EFP/NAVX products there should not be any record with SECSuffix but adding this condition to ignore nulls
			AND		ISNULL(CT.SECSuffix,'') = '' 
			ORDER BY CT.Trd_Dt, CT.DEAL_ID
		END



		/********************** FICC GSD Submission and Netting Fees ************************/
		
		/* 02/10/2015 - SHIRISH - Separating EFP & NAVX block as Submission & Netting Fee calculations are different from other products 
								  User NetMoney and VolumeMultiplier for EFP and NAVX
		*/
		IF @Debug = 1 -- IDBBC-265 Debug added to display trade by trade fees
		BEGIN
			SELECT
				CT.Billing_Code,
				CT.ProductGroup,
				CT.Trd_Dt,
				CT.DEAL_ID,
				--CT.PeriodId,
				CT.Source,
				ChargeType.ChargeId,
				CT.DEAL_SECURITY_ID,
				CT.Cancelled,
				--TotalCTs = COUNT(*),
				Total_Quantity = Quantity,
				cs.SubmissionFee,
				CS.FixedNettingFee,
				CS.VariableNettingFee,
				--Do not charge netting fees on cancelled clearing trades
				/*Using MAX on CS.VariableNettingFee because the query contains GROUP BY; 
				as a result CS.VariableNettingFee should be contained in either an aggregate function or the GROUP BY clause
				There should only be one clearing schedule per product group, so using MAX on VariableNettingFee is ok; 
				*/
				Submission_Netting_Fees = (CASE WHEN Cancelled = 1 THEN CS.SubmissionFee
												   ELSE CS.SubmissionFee + CS.FixedNettingFee END) + 
										  (CS.VariableNettingFee * (CASE WHEN Cancelled = 1 THEN 0 ELSE Quantity END))

			FROM	#ClearingTrades CT
			--It is ok to use hard coded value Clearing in the join below because this insert is specifically for Submission & Netting Fees
			JOIN	#ChargeType ChargeType ON ChargeType.ChargeType = 'Clearing'
			JOIN	#ClearingSchedule CS ON CT.ProductGroup = CS.ProductGroup
										 AND (CASE WHEN CS.ProductGroup <> 'OTR' THEN 1
												   WHEN CS.ProductGroup = 'OTR' AND CS.TradeType = CT.InvDetEnrichSource THEN 1
												   ELSE 0
											 END) = 1

			/* NILESH: 10/04/2011
			-- Following condition is to ensure that we pick up the correct Clearing fees when there are 
			-- multiple records with different effective date ranges
			-- SHIRISH: 07/21/2015
			-- We do not want to show FICC charges for OTR at the moment.  So excluding OTR product
			-- NILESH: 01/16/2018
			-- Modified the condition to include the OTR submission fees for reporting purposes only as per business.
			-- SHIRISH: 01/30/2018
			-- Need to add FICC charges for OTR, commenting condition below that only 
			*/
			WHERE	CT.ProductGroup NOT IN ('EFP','NAVX','EUREPO')
			AND		CT.Trd_Dt BETWEEN CS.EffectiveStartDate AND CS.EffectiveEndDate
			-- SHIRISH 01/15/2019: For non EFP/NAVX products there should not be any record with SECSuffix but adding this condition to ignore nulls
			AND		ISNULL(CT.SECSuffix,'') = '' 
			AND		CT.BILLING_CODE <> 'GX299'
			ORDER BY	CT.BILLING_CODE, CT.ProductGroup, CT.Trd_Dt, CT.DEAL_ID

		END

	

		SELECT
			CT.InvNum,
			CT.InvDbId,
			CT.Billing_Code,
			CT.ProductGroup,
			CT.PeriodId,
			CT.Source,
			ChargeType.ChargeId,
			TotalCTs = COUNT(*),
			Total_Quantity = SUM(Quantity),
			--Do not charge netting fees on cancelled clearing trades
			/*Using MAX on CS.VariableNettingFee because the query contains GROUP BY; 
			as a result CS.VariableNettingFee should be contained in either an aggregate function or the GROUP BY clause
			There should only be one clearing schedule per product group, so using MAX on VariableNettingFee is ok; 
			*/
			Submission_Netting_Fees = SUM(CASE WHEN Cancelled = 1 THEN CS.SubmissionFee
											   ELSE CS.SubmissionFee + CS.FixedNettingFee END) + 
									  (MAX(CS.VariableNettingFee) * SUM(CASE WHEN Cancelled = 1 THEN 0 ELSE Quantity END)),
			IsActiveStream = CT.IsActiveStream

		INTO #SNF

		FROM	#ClearingTrades CT
		--It is ok to use hard coded value Clearing in the join below because this insert is specifically for Submission & Netting Fees
		JOIN	#ChargeType ChargeType ON ChargeType.ChargeType = 'Clearing'
		JOIN	#ClearingSchedule CS ON CT.ProductGroup = CS.ProductGroup
									 AND (CASE WHEN CS.ProductGroup <> 'OTR' THEN 1
											   WHEN CS.ProductGroup = 'OTR' AND CS.TradeType = CT.InvDetEnrichSource THEN 1
											   ELSE 0
										 END) = 1

		/* NILESH: 10/04/2011
		-- Following condition is to ensure that we pick up the correct Clearing fees when there are 
		-- multiple records with different effective date ranges
		-- SHIRISH: 07/21/2015
		-- We do not want to show FICC charges for OTR at the moment.  So excluding OTR product
		-- NILESH: 01/16/2018
		-- Modified the condition to include the OTR submission fees for reporting purposes only as per business.
		-- SHIRISH: 01/30/2018
		-- Need to add FICC charges for OTR, commenting condition below that only 
		*/
		WHERE	CT.ProductGroup NOT IN ('EFP','NAVX','EUREPO')
		--AND		(CASE WHEN @ReportMode = 0 AND CT.ProductGroup = 'OTR' THEN 0 ELSE 1 END) = 1 
		AND		CT.Trd_Dt BETWEEN CS.EffectiveStartDate AND CS.EffectiveEndDate
		-- SHIRISH 01/15/2019: For non EFP/NAVX products there should not be any record with SECSuffix but adding this condition to ignore nulls
		AND		ISNULL(CT.SECSuffix,'') = '' 
		-- SHIRISH 01/28/2022: IDBBC-166 - For OTR only match DW trades to clearing schedule trade type DW.  
		--AND		(CASE WHEN CT.ProductGroup = 'OTR' AND CT.BILLING_CODE NOT LIKE 'DWC_%' AND CS.TradeType = 'DWC' THEN 0 ELSE 1 END) = 1
		AND		CT.BILLING_CODE <> 'GX299'
		GROUP BY CT.InvNum, CT.InvDbId, CT.Billing_Code, CT.ProductGroup, CT.PeriodId, CT.Source, ChargeType.ChargeId,CT.IsActiveStream

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #SNF <> EFP/NAVX',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/*  IDBBC-166
		 *	01/28/2022 - SHIRISH
		 *	Calculate Pass Through Fees for DW Clob dealers
		 *  IDBBC-196
		 *  Code change to improve performance of the section
		 */

		 SELECT IFD.DEAL_TRADE_DATE,
				IFD.ProductGroup,
				IFD.UniqueSequenceNumber,
				IFD.Dealer,
				IFD.BranchId,
				IFD.DEAL_QUANTITY,
				IFD.DEAL_NEGOTIATION_ID, -- IDBBC-218
				AB.BILLING_CODE -- IDBBC-218
		 INTO	#IFDForSNFDWC
		 FROM	IDB_Reporting.dbo.IDB_FALCON_DEALS IFD (NOLOCK)
		 JOIN	IDB_Billing.dbo.wActiveBranch AB (NOLOCK) ON AB.BRANCH_ID = IFD.BranchId AND AB.PROCESS_ID = @ProcessID -- IDBBC-216 --IDBBC-218  Adding missing processid condition
		 WHERE	DEAL_TRADE_DATE BETWEEN @date1 AND @date2
		 AND	DEAL_TRADE_DATE <= '20221104' -- IDBBC-216 Clearing trades are now available after migration
		 AND	ProductGroup = 'OTR'
		 AND	Source = 'DWC'
		 AND	DEAL_LAST_VERSION = 1
		 AND	DEAL_STATUS <> 'CANCELLED'

		 --IF @Debug = 1
			--SELECT '#IFDForSNFDWC', * FROM #IFDForSNFDWC

		 INSERT INTO #SNF
		 SELECT 
			ABC.InvNum,
			ABC.InvDbId,
			Billing_Code = ISNULL(MPN.NEW_BILLING_CODE, ABC.Billing_Code), -- IDBBC-216 
			ABC.ProductGroup,
			P.PeriodId,
			Source = 'E',
			ChargeType.ChargeId,
			TotalCTs = COUNT(*),
			Total_Quantity = SUM(IFD.DEAL_QUANTITY),
			--Do not charge netting fees on cancelled clearing trades
			/*Using MAX on CS.VariableNettingFee because the query contains GROUP BY; 
			as a result CS.VariableNettingFee should be contained in either an aggregate function or the GROUP BY clause
			There should only be one clearing schedule per product group, so using MAX on VariableNettingFee is ok; 
			*/
			Submission_Netting_Fees = SUM(IFD.DEAL_QUANTITY * COALESCE(OV.VariableNettingFee, CS.VariableNettingFee, 0)),
			IsActiveStream = 0
		 FROM	IDB_Billing.dbo.wActiveBillingCodes ABC (NOLOCK)
		 JOIN	IDB_Billing.dbo.wActiveBranch AB (NOLOCK) ON AB.PROCESS_ID = ABC.PROCESS_ID AND AB.BILLING_CODE = ABC.Billing_Code
		 JOIN	IDB_Billing.dbo.wPeriodIDs P (NOLOCK) ON P.PROCESS_ID = ABC.PROCESS_ID
		 JOIN	#IFDForSNFDWC IFD (NOLOCK) ON IFD.ProductGroup = ABC.ProductGroup
										  --AND IFD.Dealer = AB.COMPANY_ACRONYM
										  AND IFD.BranchId = AB.BRANCH_ID
										  AND IFD.BILLING_CODE = AB.BILLING_CODE -- IDBBC-218
										  
		 JOIN	#ChargeType ChargeType ON ChargeType.ChargeType = 'Clearing'
		 JOIN	#ClearingSchedule CS ON ABC.ProductGroup = CS.ProductGroup AND CS.TradeType = ABC.InvDetEnrichSource -- DWC
		 LEFT JOIN IDB_Reporting.dbo.[DWC_DW_FIRM_MAPPING_TABLE] MPO (NOLOCK) ON MPO.DW_BILLING_CODE = ABC.Billing_Code -- IDBBC-216 map to OLD CLOB BIlling Codes
		 LEFT JOIN IDB_Reporting.dbo.[DWC_DW_FIRM_MAPPING_TABLE] MPN (NOLOCK) ON MPN.NEW_BILLING_CODE = ABC.Billing_Code -- IDBBC-216 map to NEW CLOB billing codes
		 -- IDBBC-218 Moving this join here as we also need to match original CLOB billing code for migrated billing codes
		 LEFT JOIN IDB_Billing.dbo.DWC_PassThroughFees_Override OV ON (OV.Dealer = AB.COMPANY_ACRONYM OR OV.Dealer = MPN.DW_FIRM_CODE)
																  AND IFD.DEAL_TRADE_DATE BETWEEN OV.EffectiveStartDate AND OV.EffectiveEndDate
		 WHERE	ABC.PROCESS_ID = @ProcessID
		 AND	ABC.InvDetEnrichSource = 'DWC'
		 AND	RIGHT(ABC.Billing_Code,2) NOT IN ('_C', '_M') 
		 AND	MONTH(IFD.DEAL_TRADE_DATE) = MONTH(P.PeriodDate)
		 AND	YEAR(IFD.DEAL_TRADE_DATE) = YEAR(P.PeriodDate)
		-- IDBBC-210 After billing code/Branch migration from DWC to DW, we can have same branch doing both CLOB and Streams trades but billing codes will be different
		--		     Below condition is used to assign trades to correct billing codes
		 AND	(CASE WHEN IFD.DEAL_TRADE_DATE >= '20221101' AND MPN.NEW_BILLING_CODE IS NOT NULL THEN 1 -- CLOB trades will be assigned to new billing codes after migration date
					  WHEN IFD.DEAL_TRADE_DATE < '20221101' AND MPO.DW_BILLING_CODE IS NOT NULL THEN 1 -- CLOB trades will be assigned to old DWC billing codes
					  ELSE 0
				 END) = 1

		 GROUP BY 
				ABC.InvNum,
				ABC.InvDbId,
				ISNULL(MPN.NEW_BILLING_CODE, ABC.Billing_Code),
				ABC.ProductGroup,
				P.PeriodId,
				ChargeType.ChargeId

		DROP TABLE #IFDForSNFDWC -- Drop temp table its not going to be used anymore

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, '#SNF DWC',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		-- EFP, NAVX block
		INSERT INTO #SNF
		SELECT
			CT.InvNum,
			CT.InvDbId,
			CT.Billing_Code,
			CT.ProductGroup,
			CT.PeriodId,
			CT.Source,
			ChargeType.ChargeId,
			TotalCTs = COUNT(*),
			Total_Quantity = SUM(Quantity),
			--Do not charge netting fees on cancelled clearing trades
			/*Using MAX on CS.VariableNettingFee because the query contains GROUP BY; 
			as a result CS.VariableNettingFee should be contained in either an aggregate function or the GROUP BY clause
			There should only be one clearing schedule per product group, so using MAX on VariableNettingFee is ok; 
			*/
			Submission_Netting_Fees = SUM((CT.NetMoney / @VolUnitMultiplier) * CS.SubmissionFee),
			CT.IsActiveStream

		FROM	#ClearingTrades CT
		--It is ok to use hard coded value Clearing in the join below because this insert is specifically for Submission & Netting Fees
		JOIN	#ChargeType ChargeType ON ChargeType.ChargeType = 'Clearing'
		JOIN	#ClearingSchedule CS ON CT.ProductGroup = CS.ProductGroup

		/* NILESH: 10/04/2011
		-- Following condition is to ensure that we pick up the correct Clearing fees when there are 
		-- multiple records with different effective date ranges
		*/
		WHERE	CT.ProductGroup IN ('EFP','NAVX')
		AND		CT.Trd_Dt BETWEEN CS.EffectiveStartDate AND CS.EffectiveEndDate -- IDBBC-45 Submission fee rate is applied based on Trade date and not settle date. Changing date to trade date.
		--  SHIRISH 05/01/2017: Belowclearing trades for CTCMETAL were cancelled by mistake.  These are valid and we need to use them to calculate SEC fees
		-- GDB-1236 Adding ISNULL check for Cancelled flag
		AND		((ISNULL(CT.Cancelled,0) = 0) OR (CT.Deal_Id IN ('D20170412SB00000015','D20170412SB00000023') AND CT.Dealer = 'CTCMETAL'))-- Do not calculate SEC Fees for cancelled trades
		AND		CT.Side = 2 -- Use NetMoney for only SELL side (SIDE = 2)
		/*	FOR EFP & NAVX we need a separate invoice for SEC Submission Fees.
			there are specific billing codes set up in system with with prefix SECF
			we need to calculate SEC submission fees for these billing codes ONLY
		*/  
		AND		CT.BILLING_CODE LIKE 'SECF%'
		-- SHIRISH: 11/08/2015 - We only want to calculate fees where dealer is NOT an ACT_MEMBEr
		AND		CT.ACT_MEMBER = 'N'
		-- SHIRISH 01/15/2019: DIT-10124 - For invoice mode trades cleared at LEK should be charged to LEK billing code.  Remaining trades should be charged to original SEC billing code.
		--								   When not in invoice mode all trades are charged to original SEC billing code
		-- SHIRISH 01/30/2019: DIT-10123 - For VIRTU, EFPMETALS trades should be charged directly to VIRTU and rest of the trades should be charged to ABN AMRO
		AND		(
					@ReportMode <> 0
					OR
					(
						(@ReportMode = 0 OR @Debug = 1)
						AND
						(
							(ISNULL(CT.SECSuffix,'') = '_LEK' AND (ISNULL(CT.ClearingId,'') = 'LSCI' OR ISNULL(CT.ContraClearingID,'') = 'LSCI')) -- DIT-10124, when SECSuffix is _LEK then match clearingid/contra clearind id is LSCI
							OR
							-- SHIRISH 04/02/2019: DIT-10744 Removing SECF25 from below statement as we have supressed it from ActiveBilling
							(CT.BILLING_CODE = 'SECF59' AND ISNULL(CT.SWSECTYPE,'') = 'EFPMETALS') -- DIT-10123, Charge EFPMETALS trades directly to virtu, added SECF59 which is set up for VIRTU EFPMETALS
							OR
							(ISNULL(CT.SECSuffix,'') = '_ABN' AND ISNULL(CT.SWSECTYPE,'') <> 'EFPMETALS' AND ISNULL(CT.ClearingId,'') <> 'LSCI' AND ISNULL(CT.ContraClearingID,'') <> 'LSCI') -- DIT-10123, ABN billing codes do not match with EFPMETALS trades and clearingid/contra clearind id is not LSCI
							OR
							-- SHIRISH 04/02/2019: DIT-10744 Removing SECF25 from below statement as we have supressed it from ActiveBilling
							(ISNULL(CT.SECSuffix,'') = '' AND CT.BILLING_CODE NOT IN ('SECF59') AND ISNULL(CT.ClearingId,'') <> 'LSCI' AND ISNULL(CT.ContraClearingID,'') <> 'LSCI') -- All trades that do not fit into above 3 conditions will all into this condition 
						)
					)
				)

		
		GROUP BY CT.InvNum, CT.InvDbId, CT.Billing_Code, CT.ProductGroup, CT.PeriodId, CT.Source, ChargeType.ChargeId,CT.IsActiveStream

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #SNF-EFP/NAVX',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		-- USREPO Block
		IF (@ProductGroup IS NULL OR @ProductGroup = 'USREPO')
		BEGIN
			INSERT INTO #SNF
			SELECT	InvNum = ABC.InvNum,
					InvDbId = ABC.InvDbId,
					Billing_Code = ABC.Billing_Code,
					ProductGroup = ABC.ProductGroup,
					PeriodId = P.PeriodId,
					Source = 'E',-- S.Source,
					CT.ChargeId,
					TotalCTs = COUNT(*),
					Total_Quantity = SUM(S.Quantity),
					Submission_Netting_Fees = SUM(Total_Submission_Fees),
					IsActiveStream = CAST(NULL as BIT)

			FROM	IDB_Reporting.dbo.fnGetRepoGSDSubmissions(@date1,@date2,@Dealer,NULL,NULL,1) S
			-- SHIRISH 06/14/2017: Using below join to make sure we pick up data from correct source (FALCON/SMARTCC)
			JOIN	IDB_Billing.dbo.wActiveBranch AB ON S.TraderBranch = AB.BRANCH_ID -- IDBBC-195 Need to use Branch ID instead of Falcon Branch Id as it causes duplicates for STATE1 billing code
			JOIN	IDB_Billing.dbo.wActiveBillingCodes ABC ON AB.BILLING_CODE = ABC.BILLING_CODE AND AB.PROCESS_ID = ABC.PROCESS_ID
			JOIN	IDB_Billing.dbo.wPeriodIDs P ON MONTH(S.REPO_START_DATE) = MONTH(P.PeriodDate) AND YEAR(S.REPO_START_DATE) = YEAR(P.PeriodDate)  AND AB.PROCESS_ID = P.PROCESS_ID
			JOIN	#ChargeType CT ON CT.ChargeType = 'Clearing'
			WHERE	ABC.ProductGroup = 'USREPO'
			AND		AB.PROCESS_ID = @ProcessID
			GROUP BY ABC.InvNum, ABC.InvDbId, ABC.Billing_Code,ABC.ProductGroup,P.PeriodId,CT.ChargeId--,S.Source
		END

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #SNF-USREPO',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2


		/* NILESH -- Tiered Billing */
		-- Temp table used to regenerate CommissionSummary
		-- data using Tier Rates
		/* 02/10/2015 - SHIRISH - Separating EFP & NAVX block as calculations are different from other products */
		SELECT
			CT.InvNum,
			CT.InvDbId,
			CT.Billing_Code,
			CT.ProductGroup,
			CT.PeriodId,
			CT.Trd_Dt,
			CT.Source,
			ChargeType.ChargeId,
			TotalCTs = COUNT(*),
			Total_Quantity = SUM(Quantity),
			--Do not charge netting fees on cancelled clearing trades
			/*Using MAX on CS.VariableNettingFee because the query contains GROUP BY; 
			as a result CS.VariableNettingFee should be contained in either an aggregate function or the GROUP BY clause
			There should only be one clearing schedule per product group, so using MAX on VariableNettingFee is ok; 
			*/
			--Submission_Netting_Fees = SUM(CASE WHEN Cancelled = 1 THEN CS.SubmissionFee ELSE CS.SubmissionFee + CS.FixedNettingFee END) + (MAX(CS.VariableNettingFee) * SUM(CASE WHEN Cancelled = 1 THEN 0 ELSE Quantity END))
			Submission_Netting_Fees = SUM(CASE WHEN Cancelled = 1 THEN CS.SubmissionFee ELSE CS.SubmissionFee + CS.FixedNettingFee END) + 
									  (MAX(CS.VariableNettingFee) * SUM(CASE WHEN Cancelled = 1 THEN 0 ELSE Quantity END)),
			CT.IsActiveStream

		INTO #SNF_Tier

		FROM	#ClearingTrades CT
		JOIN	IDB_CodeBase.dbo.fnProductType() fp ON CT.ProductGroup = fp.Product
		--It is ok to use hard coded value Clearing in the join below because this insert is specifically for Submission & Netting Fees
		JOIN	#ChargeType ChargeType ON ChargeType.ChargeType = 'Clearing'
		JOIN	#ClearingSchedule CS ON CT.ProductGroup = CS.ProductGroup

		/* NILESH: 10/04/2011
		-- Following condition is to ensure that we pick up the correct Clearing fees when there are 
		-- multiple records with different effective date ranges
		-- SHIRISH: 07/21/2015
		-- We do not want to show FICC charges for OTR at the moment.  So excluding OTR product
		-- SHIRISH: 01/30/2018
		-- Need to add FICC charges for OTR, commenting condition below that only 
		*/
		WHERE	CT.ProductGroup NOT IN ('EFP','NAVX')
		--AND		(CASE WHEN @ReportMode = 0 AND CT.ProductGroup = 'OTR' THEN 0 ELSE 1 END) = 1
		AND		CT.Trd_Dt BETWEEN CS.EffectiveStartDate AND CS.EffectiveEndDate
		AND		fp.ProductInvoiceUsesTieredBilling = 'Y'
			
		GROUP BY CT.InvNum, CT.InvDbId, CT.Billing_Code, CT.ProductGroup, CT.PeriodId, CT.Source, ChargeType.ChargeId, CT.Trd_Dt,CT.IsActiveStream

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After SNFTier<>EFP/NAVX',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		-- EFP, NAVX block
		INSERT INTO #SNF_Tier
		SELECT
			CT.InvNum,
			CT.InvDbId,
			CT.Billing_Code,
			CT.ProductGroup,
			CT.PeriodId,
			CT.Trd_Dt,
			CT.Source,
			ChargeType.ChargeId,
			TotalCTs = COUNT(*),
			Total_Quantity = SUM(Quantity),
			--Do not charge netting fees on cancelled clearing trades
			/*Using MAX on CS.VariableNettingFee because the query contains GROUP BY; 
			as a result CS.VariableNettingFee should be contained in either an aggregate function or the GROUP BY clause
			There should only be one clearing schedule per product group, so using MAX on VariableNettingFee is ok; 
			*/
			Submission_Netting_Fees = SUM((CT.NetMoney / @VolUnitMultiplier) * CS.SubmissionFee),
			CT.IsActiveStream

		FROM	#ClearingTrades CT
		JOIN	IDB_CodeBase.dbo.fnProductType() fp ON CT.ProductGroup = fp.Product
		--It is ok to use hard coded value Clearing in the join below because this insert is specifically for Submission & Netting Fees
		JOIN	#ChargeType ChargeType ON ChargeType.ChargeType = 'Clearing'
		JOIN	#ClearingSchedule CS ON CT.ProductGroup = CS.ProductGroup

		/* NILESH: 10/04/2011
		-- Following condition is to ensure that we pick up the correct Clearing fees when there are 
		-- multiple records with different effective date ranges
		*/
		WHERE	CT.ProductGroup IN ('EFP','NAVX')
		AND		CT.SettleDate BETWEEN CS.EffectiveStartDate AND CS.EffectiveEndDate
		AND		fp.ProductInvoiceUsesTieredBilling = 'Y'
		AND		ISNULL(CT.Cancelled,0) = 0 -- Do not calculate SEC Fees for cancelled trades -- GDB-1236 Adding ISNULL check
		AND		CT.Side = 2 -- Use NetMoney for only SELL side (SIDE = 2)
		/*	FOR EFP & NAVX we need a separate invoice for SEC Submission Fees.
			there are specific billing codes set up in system with with prefix SECF
			we need to calculate SEC submission fees for these billing codes ONLY
		*/  
		AND		CT.BILLING_CODE LIKE 'SECF%'
		AND		CT.ACT_MEMBER = 'N'
			
		GROUP BY CT.InvNum, CT.InvDbId, CT.Billing_Code, CT.ProductGroup, CT.PeriodId, CT.Source, ChargeType.ChargeId, CT.Trd_Dt,CT.IsActiveStream

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After SNF-EFP/NAVX',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/********************** END FICC GSD Submission and Netting Fees ************************/

		/********************  CAT Fees ********************/

		SELECT
			CT.InvNum,
			CT.InvDbId,
			CT.Billing_Code,
			CT.ProductGroup,
			CT.PeriodId,
			CT.Source,
			ChargeType.ChargeId,
			TotalCTs = COUNT(*),
			Total_Quantity = SUM(CASE WHEN CT.ACT_MEMBER = 'Y' AND CT.Side = 2 THEN 0 ELSE CT.Quantity END),
			--Do not charge netting fees on cancelled clearing trades
			/*Using MAX on CS.VariableNettingFee because the query contains GROUP BY; 
			as a result CS.VariableNettingFee should be contained in either an aggregate function or the GROUP BY clause
			There should only be one clearing schedule per product group, so using MAX on VariableNettingFee is ok; 
			*/
			CATFees = SUM((CASE WHEN CT.ACT_MEMBER = 'Y' AND CT.Side = 2 THEN 0 ELSE CT.Quantity END) * CFS.CATFee )

		INTO	#CATFees
		FROM	#ClearingTrades CT
		--It is ok to use hard coded value Clearing in the join below because this insert is specifically for Submission & Netting Fees
		JOIN	#ChargeType ChargeType ON ChargeType.ChargeCat = 'CAT'
		JOIN	#CATFeeSchedule CFS ON CFS.ProductGroup = CT.ProductGroup AND CFS.ClearingScheduleDesc = ChargeType.ChargeDescription
		/* NILESH: 10/04/2011
		-- Following condition is to ensure that we pick up the correct Clearing fees when there are 
		-- multiple records with different effective date ranges
		*/
		WHERE	CT.ProductGroup IN ('EFP','NAVX')
		AND		CT.Trd_Dt BETWEEN CFS.EffectiveStartDate AND CFS.EffectiveEndDate -- IDBBC-45 Submission fee rate is applied based on Trade date and not settle date. Changing date to trade date.
		-- GDB-1236 Adding ISNULL check for Cancelled flag
		AND		ISNULL(CT.Cancelled,0) = 0
		/*	FOR EFP & NAVX we need a separate invoice for CAT Fees.
			there are specific billing codes set up in system with with prefix CATF
			we need to calculate SEC submission fees for these billing codes ONLY
		*/  
		AND		CT.BILLING_CODE LIKE 'CATF%'
		
		GROUP BY CT.InvNum, CT.InvDbId, CT.Billing_Code, CT.ProductGroup, CT.PeriodId, CT.Source, ChargeType.ChargeId

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After CatFees',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2


		/******************** END CAT Fees ********************/

/*########################### FAIL CHARGES BLOCK ######################### */

		IF (@productgroup IS NULL OR @productgroup = 'AGCY')
			EXEC IDB_Reporting.dbo.GetFailCharges_AGCY @ProcessID,@date1,@date2 -- IDBBC-144

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After GetFailCharges_AGCY',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2
			
		IF (@productgroup IS NULL OR @productgroup = 'BILL')
			EXEC IDB_Reporting.dbo.GetFailCharges_BILL @ProcessID,@date1,@date2 -- IDBBC-144
			
		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After GetFailCharges_BILL',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		IF (@productgroup IS NULL OR @productgroup = 'TRSY')
			EXEC IDB_Reporting.dbo.GetFailCharges_TRSY @ProcessID,@date1,@date2 -- IDBBC-144
			
		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After GetFailCharges_TRSY',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		IF (@productgroup IS NULL OR @productgroup = 'TIPS')
			EXEC IDB_Reporting.dbo.GetFailCharges_TIPS @ProcessID,@date1,@date2 -- IDBBC-144
			
		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After GetFailCharges_TIPS',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		IF (@productgroup IS NULL OR @productgroup = 'USFRN')
			EXEC IDB_Reporting.dbo.GetFailCharges_USFRN @ProcessID,@date1,@date2 -- IDBBC-144
			
		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After GetFailCharges_USFRN',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		IF (@productgroup IS NULL OR @productgroup = 'OTR')
			EXEC IDB_Reporting.dbo.GetFailCharges_OTR @ProcessID,@date1,@date2 -- IDBBC-144
	    
		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After GetFailCharges_OTR',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/* NILESH 11/09/2010:
		-- We will now calculate the Fail charges on individual trades based on the 
		-- days we have used to calculate the Fail charges.
		*/
		UPDATE IDB_Billing.dbo.wFailCharges_Staging -- SHIRISH 11/5/2014 -- updating query to use permanent table
		SET FailCharge = ROUND(((((1 / CAST(360 AS Float)) /* Daily Charge */ * 0.01 * (CASE WHEN (3 - TMPGReferenceRate) > 0 THEN (3 - TMPGReferenceRate) ELSE 0 END) * DEAL_PROCEEDS) * FailDays_Used_To_Calculate) * (CASE WHEN Side_Dealer_Perspective = 'Sell' Then 1 ELSE -1 END)),2)
		WHERE PROCESS_ID = @ProcessID

		/* Insert the Failed TRSY CTs */
		INSERT INTO #Failed_TRSY_CTs
		(
			InvNum,
			InvDbId,
			Billing_Code,
			Dealer,
			PeriodId,
			Source,
			Deal_Negotiation_Id,
			Trader_ID,
			Side_Dealer_Perspective,
			Instrument,
			Deal_Security_Name,
			Price_MC,
			DecPrice,
			Principal,
			AccruedInt,
			Clearing_Destination,
			CTCreator,
			Trd_Dt,
			CTSettled_Date,
			Original_Settle_Date,
			Product,
			ProductGroup,
			Quantity,
			Deal_Proceeds,
			FailCharge,
			ChargeId
		)
		SELECT
			InvNum,
			InvDbId,
			Billing_Code,
			Dealer,
			PeriodId,
			Source,
			Deal_Negotiation_Id,
			Trader_ID,
			Side_Dealer_Perspective,
			Instrument,
			Deal_Security_Name,
			Price_MC,
			DecPrice,
			Principal,
			AccruedInt,
			Clearing_Destination,
			CTCreator,
			Trd_Dt,
			CTSettled_Date,
			Original_Settle_Date,
			Product,
			ProductGroup,
			Quantity,
			Deal_Proceeds,
			FailCharge = SUM (FailCharge),
			ChargeId
			
		FROM	IDB_Billing.dbo.wFailCharges_Staging -- SHIRISH 11/5/2014 -- updating query to use permanent table
	    
		WHERE	PROCESS_ID = @ProcessID

		GROUP BY
			InvNum,
			InvDbId,
			Billing_Code,
			Dealer,
			PeriodId,
			Source,
			Deal_Negotiation_Id,
			Trader_ID,
			Side_Dealer_Perspective,
			Instrument,
			Deal_Security_Name,
			Price_MC,
			DecPrice,
			Principal,
			AccruedInt,
			Clearing_Destination,
			CTCreator,
			Trd_Dt,
			CTSettled_Date,
			Original_Settle_Date,
			Product,
			ProductGroup,
			Quantity,
			Deal_Proceeds,
			ChargeId


		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After Fail Charges',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/*########################### END FAIL CHARGES BLOCK ######################### */

		
		/* NILESH 11/04/2014 - IDB 13217 */
		/* 
		-- NILESH 11/04/2014
		-- Following block is used to calculate the commission credits and amount
		-- for the dealers. Currently since this is applicable only for AMSWP the product
		-- value is hardcoded. Also, this is applicable only for the D & MTD. May be from
		-- next year we can introduce the YTD.
		-- SHIRISH 07/14/2015
		-- Adding a block for calculating AMSWP Streaming credits.
		*/

		CREATE TABLE #DealerCredits
		(
			Dealer varchar(10), 
			BillingCode varchar(16) NULL,
			    
			DailyDV01 float,
			DailyAggressiveDV01 float,
			DailyPassiveDV01 float,

			DailyVolume float,
			DailyAggressiveVolume float,
			DailyPassiveVolume float,

			DailyCredits int, 

			TotalDV01 float, 
			MTDAggressiveDV01 float,
			MTDPassiveDV01 float,

			TotalVolume float,
			MTDAggressiveVolume float,
			MTDPassiveVolume float,

			MTDCredits int, 
			MTDEarnedCreditAmount float, 
			MTDEligibleCreditAmount float,
			
			--RowType varchar(25), --SHIRISH 1/13/2017: This is removed as we are getting E and V combined data from Credits Summary proc
			    
			TS varchar(10)
		)
		
		CREATE TABLE #StreamingCredits
		(
			Dealer varchar(10),
			BillingCode varchar(16) NULL,
			QualifyingInstruments INT
		)

		IF @Owner = 'US' -- IDBBC-16 Only run AMSWP credits section when owner is US.  No need to run this for Owner = UK
		BEGIN
			DECLARE @creditDealer varchar(20)
			DECLARE @StreamingCreditAmount INT = 5000


			IF ((@ProductGroup IS NULL) OR (@ProductGroup = 'AMSWP'))
			BEGIN

    	    
				-- Get the corresponding dealer code in case if the billing code is provided as a parameter.
				SET @creditDealer = NULL
				IF @BillingCode IS NOT NULL
					SELECT @creditDealer = COMPANY_ACRONYM FROM IDB_Billing.dbo.wBillingSchedule WHERE BILLING_CODE = ISNULL(@BillingCode,'') AND PRODUCT_GROUP = 'AMSWP'

				--  SHIRISH 07/22/2019 DIT-18425
				SET @timestamp2 = GETDATE()
				SET @logCount = @logCount + 1
				INSERT INTO IDB_Billing.dbo.BillingSteps 
				VALUES (@today, @ProcessID, @logCount, 'Before GetDealerCommissionCredits',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
				SET @timestamp1 = @timestamp2
	    	
				INSERT INTO #DealerCredits
				(Dealer,DailyDV01,DailyAggressiveDV01,DailyPassiveDV01,DailyVolume,DailyAggressiveVolume,DailyPassiveVolume,DailyCredits,
				 TotalDV01,MTDAggressiveDV01,MTDPassiveDV01,TotalVolume,MTDAggressiveVolume,MTDPassiveVolume,
				  MTDCredits,MTDEarnedCreditAmount,MTDEligibleCreditAmount,TS)
				EXEC IDB_reporting.dbo.GetDealerCommissionCredits @date2, 'IRS', @creditDealer, NULL, 'CS',0 -- IDBBC-42 we need to include both AMSWP and CAD credits for DV01 trading credits.  Updating product to IRS which will include both AMSWP and CAD 

				--  SHIRISH 07/22/2019 DIT-18425
				SET @timestamp2 = GETDATE()
				SET @logCount = @logCount + 1
				INSERT INTO IDB_Billing.dbo.BillingSteps 
				VALUES (@today, @ProcessID, @logCount, 'After GetDealerCommissionCredits',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
				SET @timestamp1 = @timestamp2
			
				-- Update the corresponding billing codes
				UPDATE	DC
				SET		DC.BillingCode = BC.BILLING_CODE
				FROM	#DealerCredits DC
				JOIN	IDB_Billing.dbo.wBillingSchedule BC ON DC.Dealer = BC.COMPANY_ACRONYM -- SHIRISH 11/5/2014 -- updating query to use permanent table
				WHERE	BC.PRODUCT_GROUP = 'AMSWP'
				AND		BC.SOURCE = 'E'
				AND		BC.BILLING_CODE <> 'BAML3V' --GDB-46
				AND		BC.PROCESS_ID = @ProcessID
			
			
				INSERT INTO #CommissionCredits
				(
					InvNum,
					InvDbId,
					Dealer,
					BILLING_CODE,
					PeriodId,
					ProductGroup,
					ChargeId,
					CommissionCredit,
					Source
				)	
				SELECT
					InvNum = ABC.InvNum,
					InvDbId = ABC.InvDbId,
					Dealer = DC.Dealer,
					Billing_Code = DC.BillingCode,
					PeriodId = P.PeriodId,
					ProductGroup = ABC.ProductGroup,
					ChargeID = CT.ChargeId,
					/* SHIRISH 1/13/2017 - No need to apply Max DV01 credit check as its already been applied in GetDealerCommissionCredits*/
					--CommissionCredit = CASE WHEN SUM(DC.MTDEligibleCreditAmount) > @MaxDV01CreditAmount THEN @MaxDV01CreditAmount ELSE SUM(DC.MTDEligibleCreditAmount) END,
					CommissionCredit = DC.MTDEligibleCreditAmount,
					--Source = CASE DC.RowType WHEN 'Electronic' THEN 'E' WHEN 'Voice' THEN 'V' END -- SHIRISH - 1/4/2016 - Added source to distinguish between E and V credits
					Source = 'E' -- SHIRISH - 2/24/2016 - Setting Source = E so that both E & V can be combined into one record so DV01 credit limit can be applied.
				FROM    #DealerCredits DC
				JOIN	IDB_Billing.dbo.wActiveBillingCodes ABC ON DC.BillingCode = ABC.BILLING_CODE 
				--Service periods are stored in unit of months
				JOIN	IDB_Billing.dbo.wPeriodIDs P ON MONTH(@date2) = MONTH(P.PeriodDate) AND YEAR(@date2) = YEAR(P.PeriodDate)
				JOIN	#ChargeType CT ON ChargeType = 'DV01CommissionCredit'
				WHERE	ABC.ProductGroup = 'AMSWP'
				AND		P.PeriodId IS NOT NULL
				AND		ABC.PROCESS_ID = @ProcessID
				AND		P.PROCESS_ID = @ProcessID
				AND		DC.MTDEligibleCreditAmount > 0 -- SHIRISH 1/13/2017: Added new condition to add a record only when there is DV01 credit
				/* SHIRISH 1/13/2017: As we are getting 1 record per dealerthere is no need for grouping */
				--GROUP BY
				--		ABC.InvNum,
				--		ABC.InvDbId,
				--		DC.Dealer,
				--		DC.BillingCode,
				--		P.PeriodId,
				--		ABC.ProductGroup,
				--		CT.ChargeId

				--  SHIRISH 07/22/2019 DIT-18425
				SET @timestamp2 = GETDATE()
				SET @logCount = @logCount + 1
				INSERT INTO IDB_Billing.dbo.BillingSteps 
				VALUES (@today, @ProcessID, @logCount, 'After AMSWP Commission Credits',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
				SET @timestamp1 = @timestamp2
		
				/************ STREAMING CREDITS BLOCK **************/
				-- SHIRISH - 07/14/2015
				-- Get qualified dealers for Streaming Credit
				-- This block is only executed for monthly and yearly
				IF (@ReportMode = 0 OR @Debug = 1)
				BEGIN
					INSERT INTO #StreamingCredits (Dealer, QualifyingInstruments)
					EXEC IDB_Reporting.dbo.GetAMSWPStreamingReport @date1, @date2, @ForBilling=1
				
					-- Update corresponding billing codes
					UPDATE	SC
					SET		SC.BillingCode = BC.BILLING_CODE
					FROM	#StreamingCredits SC
					JOIN	IDB_Billing.dbo.wBillingSchedule BC ON SC.Dealer = BC.COMPANY_ACRONYM
					WHERE	BC.PRODUCT_GROUP = 'AMSWP'
					AND		BC.SOURCE = 'E'
					AND		BC.PROCESS_ID = @ProcessID

					INSERT INTO #CommissionCredits
					(
						InvNum,
						InvDbId,
						Dealer,
						BILLING_CODE,
						PeriodId,
						ProductGroup,
						ChargeId,
						CommissionCredit,
						Source
					)	
					SELECT
						InvNum = ABC.InvNum,
						InvDbId = ABC.InvDbId,
						Dealer = SC.Dealer,
						Billing_Code = SC.BillingCode,
						PeriodId = P.PeriodId,
						ProductGroup = ABC.ProductGroup,
						ChargeID = CT.ChargeId,
						CommissionCredit = @StreamingCreditAmount,
						Source = 'E'
					FROM    #StreamingCredits SC
					JOIN	IDB_Billing.dbo.wActiveBillingCodes ABC ON SC.BillingCode = ABC.BILLING_CODE 
					--Service periods are stored in unit of months
					JOIN	IDB_Billing.dbo.wPeriodIDs P ON MONTH(@date2) = MONTH(P.PeriodDate) AND YEAR(@date2) = YEAR(P.PeriodDate)
					JOIN	#ChargeType CT ON ChargeType = 'StreamingCommissionCredit'
					WHERE	ABC.ProductGroup = 'AMSWP'
					AND		P.PeriodId IS NOT NULL
					AND		ABC.PROCESS_ID = @ProcessID
					AND		P.PROCESS_ID = @ProcessID
					/* SHIRISH 1/12/2017: After 12/1/2016 dealers get either DV01 or Streaming credit but do not get both */
					AND		(CASE WHEN @date1 >= '20161201'
									  AND 
									  EXISTS (SELECT 1 FROM #CommissionCredits CC
											  WHERE CC.BILLING_CODE = SC.BillingCode
											  AND	CC.InvNum = ABC.InvNum)
								THEN 0
								ELSE 1
							END) = 1

				END

				--  SHIRISH 07/22/2019 DIT-18425
				SET @timestamp2 = GETDATE()
				SET @logCount = @logCount + 1
				INSERT INTO IDB_Billing.dbo.BillingSteps 
				VALUES (@today, @ProcessID, @logCount, 'After AMSWP Streaming Credits',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
				SET @timestamp1 = @timestamp2
			
				/************ END STREAMING CREDITS BLOCK **************/
			
				/* Update the #InvoiceInvetory for later use for Invoices */
				INSERT INTO #InvoiceInventory
				(
					InvNum,
					InvDbId,
					DetailNum,
					logon_id,
					ProductGroup,
					Source,
					Billing_Plan_Id,
					instrument_type,
					ItemAmount,
					InvInvTypeId,
					who,
					created,
					periodid,
					ChargeId
				) 
				SELECT 
					InvNum = CC.InvNum,
					InvDbId = CC.InvDbId,
					DetailNum = NULL,
					Logon_Id = @User, 
					ProductGroup = CC.ProductGroup,
					Source = CC.Source, -- SHIRISH 02/09/2016 - Removing source so that DV01 commission can be combined and capped to Max DV01 commission of 20000
					Billing_Plan_Id = NULL, --Commission owed is not broken by billing plan id in invoice inventory. This is the total commission owed by product group
					Instrument_Type = NULL,--Commission collected is not broken by instrument type in invoice inventory. This is the total commission collected by product group
					--Multiply with -1 to negate the value. This gives the effect of crediting Commission Collected so when all the 
					--charges (commission owed, commission collected, fees etc) in the inventory are summed up commission collected is deducted from the total
					ItemAmount = (CC.CommissionCredit * -1),
					--ItemAmount = (CASE WHEN SUM(CC.CommissionCredit) > @MaxDV01CreditAmount THEN @MaxDV01CreditAmount ELSE SUM(CC.CommissionCredit) END) * -1,
					InvInvTypeId = IIT.InvInvTypeId,
					Who = @User ,
					Created = GETDATE(),
					PeriodId = PeriodId,
					ChargeId = CC.ChargeId

				FROM	#CommissionCredits CC
				JOIN	#ChargeType CT ON CT.ChargeId = CC.ChargeId
				--It is ok to use hard coded value CommissionCollected in the join below because this join is specifically for Commission Collected
				-- SHIRISH - 07/14/2015 - Updated this join to accomodate DV01 as well as Streaming credits
				JOIN	IDB_Billing.dbo.InvoiceInventoryType IIT ON IIT.Code = CT.ChargeType AND IIT.Billing_Type IS NULL 

			END
		END --IDBBC-16

		/* END Commission Credits Block */
		 
		SELECT
			FCT.InvNum,
			FCT.InvDbId,
			FCT.Billing_Code,
			FCT.ProductGroup,
			FCT.PeriodId,
			FCT.Source,
			FCT.ChargeId,
			FailCharge = SUM(FCT.FailCharge)

		INTO	#FC

		FROM	#Failed_TRSY_CTs FCT
		JOIN	#FailChargeSchedule FCS ON FCT.ProductGroup = FCS.ProductGroup

		/* NILESH 12/17/2010:
		-- The Fail charges should not be reported on the invoice unless they are
		-- greater than the defined threshold value. However, in all other instances
		-- should be reported. This feature can be used to control if the fail charges
		-- should be reported on an invoice or not while they are getting reported
		-- in the front-end mode or commission summary report.
		*/
		--Fail charges for a clearing trade should be reported only if the charge for the life of fail is greater than or equal to min threshold
		--WHERE	ABS(FailCharge) >= FCS.FailChargeMinThreshold	--NILESH 20101217
		WHERE	(CASE 
				WHEN (@ReportMode = 0 OR @Debug = 1) AND ABS(FailCharge) >= FCS.FailChargeMinThreshold THEN 1 
				WHEN @ReportMode > 0 THEN 1
				ELSE 0 END) = 1	

		GROUP BY FCT.InvNum, FCT.InvDbId, FCT.Billing_Code, FCT.ProductGroup, FCT.PeriodId, FCT.Source, FCT.ChargeId

		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #FC',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/* NILESH -- Tiered Billing */
		-- Temp Table used to regenerate the CommissionSummary
		-- data using the Tier Rates
		SELECT
			FCT.InvNum,
			FCT.InvDbId,
			FCT.Billing_Code,
			FCT.ProductGroup,
			FCT.PeriodId,
			FCT.Trd_Dt,
			FCT.Source,
			FCT.ChargeId,
			FailCharge = SUM(FCT.FailCharge)

		INTO	#FC_Tier

		FROM	#Failed_TRSY_CTs FCT
		JOIN	IDB_CodeBase.dbo.fnProductType() fp ON FCT.ProductGroup = fp.Product
		JOIN	#FailChargeSchedule FCS ON FCT.ProductGroup = FCS.ProductGroup

		/* NILESH 12/17/2010:
		-- The Fail charges should not be reported on the invoice unless they are
		-- greater than the defined threshold value. However, in all other instances
		-- should be reported. This feature can be used to control if the fail charges
		-- should be reported on an invoice or not while they are getting reported
		-- in the front-end mode or commission summary report.
		*/
		--Fail charges for a clearing trade should be reported only if the charge for the life of fail is greater than or equal to min threshold
		--WHERE	ABS(FailCharge) >= FCS.FailChargeMinThreshold	--NILESH 20101217
		WHERE	(CASE 
				WHEN (@ReportMode = 0 OR @Debug = 1) AND ABS(FailCharge) >= FCS.FailChargeMinThreshold THEN 1 
				WHEN @ReportMode > 0 THEN 1
				ELSE 0 END) = 1	
		AND	fp.ProductInvoiceUsesTieredBilling = 'Y'
		
		GROUP BY FCT.InvNum, FCT.InvDbId, FCT.Billing_Code, FCT.ProductGroup, FCT.PeriodId, FCT.Trd_Dt, FCT.Source, FCT.ChargeId

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #FC_Tier',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		--INSERT INVOICE HISTORY (HEADER)
		/* #ActiveBillingCodes will have multiple rows (by product) for each billing code
			InvNum, InvDbId and BILLING_CODE will be the same for all rows of each billing code
		*/
		SELECT	DISTINCT 
			InvNum = ABC.InvNum,
			InvDbId = ABC.InvDbId,
			InvDate = @InvDate,
			billing_code = ISNULL(ABC.MasterBillingCode, ABC.BILLING_CODE),
			InvTypeId = @InvTypeId,
			company_name = AB.COMPANY_NAME,
			legal_name = AB.COMPANY_LEGAL_NAME,
			first_name = AB.BILLING_CONTACT_FIRST_NAME,
			last_name = AB.BILLING_CONTACT_LAST_NAME,
			middle_initial = AB.BILLING_CONTACT_MIDDLE_INITIAL,
			phone = AB.BILLING_CONTACT_PHONE,
			fax = AB.BILLING_CONTACT_FAX,
			address_1 = AB.BILLING_CONTACT_ADDRESS_1,
			address_2 = AB.BILLING_CONTACT_ADDRESS_2,
			city = AB.BILLING_CONTACT_CITY,
			[state] = AB.BILLING_CONTACT_STATE,
			zip = AB.BILLING_CONTACT_ZIP,
			country_code = AB.BILLING_CONTACT_COUNTRY_CODE,
			start_billing_period = ABC.Start_Billing_Period, 
			end_billing_period = ABC.End_Billing_Period, 
			due_date = NULL,
			currency_code = AB.BILLING_CONTACT_CURRENCY_CODE,
			[status] = 'Approved', --Invoices generated by the batch are set as approved by default
			purchase_order = AB.BILLING_CONTACT_PURCHASE_ORDER,
			delivery_acronym = ISNULL(AB.DELIVERY_METHOD_ACRONYM, 'E-mail'), --If delivery acronym is null it should default to e-mail
			[owner] = AB.BILLING_CONTACT_OWNER

		INTO	#InvoiceHistory
		
		FROM	IDB_Billing.dbo.wActiveBilling AB (NOLOCK) -- SHIRISH 11/4/2014 -- updating query to use permanent table
		JOIN	IDB_Billing.dbo.wActiveBillingCodes ABC (NOLOCK) ON AB.BILLING_CODE = ISNULL(ABC.MasterBillingCode, ABC.BILLING_CODE) 
																 AND AB.PROCESS_ID = ABC.PROCESS_ID -- SHIRISH 11/4/2014 -- updating query to use permanent table

		WHERE	AB.PROCESS_ID = @ProcessID -- condition to track records in permanent table

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #InvoiceHistory',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		--INSERT INTO #InvoiceDetails 
		/* The CTE and inserting into temp table #InvoiceDetails is mainly to generate sequential numbers for
		DetailNum; which will be used when inserting corresponding InvoiceInventory
		*/
		;WITH CTE_InvoiceDetails
		(
			InvNum,
			InvDbId,
			DetailNum ,
			productgroup,
			ServiceTypeId ,
			ChargeId,
			PeriodId,
			Source,
			Quantity,
			ItemAmount,
			ItemPrice,
			SalesCode,
			ItemDescr,
			RefInvNum,
			RefInvDbId,
			RefDetailNum,
			who,
			created ,
			Rate,
			InvoiceDescription,
			IsActiveStream
		)
		AS
		(
			SELECT
				InvNum = ABC.InvNum,
				InvDbId = ABC.InvDbId,
				DetailNum = NULL,
				productgroup = NC.productgroup,
				ServiceTypeId = @ServiceTypeId, 
				ChargeId = NC.ChargeId,
				PeriodId = NC.PeriodId,
				Source = NC.Source,
				Quantity = NC.Volume,
				ItemAmount = ISNULL(NC.NetCommission, 0),
				ItemPrice = NULL,
				SalesCode = IDE.SalesCode,
				ItemDescr = IDE.ItemDescr,
				RefInvNum = NULL,
				RefInvDbId = NULL,
				RefDetailNum = NULL,
				who = @User,
				created = GETDATE(),
				Rate = NULL,
				InvoiceDescription = IDE.InvoiceDescription,
				IsActiveStream = CAST(NULL AS BIT)

			FROM	IDB_Billing.dbo.wActiveBillingCodes ABC (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table
			JOIN	#NetCommission NC ON ABC.InvNum = NC.InvNum AND ABC.InvDbId = NC.InvDbId AND ABC.Billing_Code = NC.Billing_Code AND ABC.ProductGroup = NC.ProductGroup
			LEFT JOIN IDB_Billing.dbo.InvoiceDetailsEnrich IDE ON NC.ChargeId = IDE.ChargeId AND NC.ProductGroup = IDE.ProductGroup AND IDE.ServiceTypeId = @ServiceTypeId AND IDE.Source = ABC.InvDetEnrichSource -- IDBBC-165

			--Only get the billing codes (InvNum) that have commission. 
			--If there are no deals for the dealer during the billing period there won't be a commission 
			--don't insert InvoiceDetail line item
			WHERE	ABC.InvNum IS NOT NULL 
				AND NC.PeriodId IS NOT NULL 
				AND NC.ChargeId IS NOT NULL
				AND ABC.PROCESS_ID = @ProcessID

			UNION ALL

			SELECT
				InvNum = ABC.InvNum,
				InvDbId = ABC.InvDbId,
				DetailNum = NULL,
				productgroup = TF.productgroup,
				ServiceTypeId = @ServiceTypeId, 
				ChargeId = TF.ChargeId,
				PeriodId = TF.PeriodId,
				Source = TF.Source,
				Quantity = TF.TradeCt,
				ItemAmount = ISNULL(TF.TracePassThruFees, 0),
				ItemPrice = NULL,
				SalesCode = IDE.SalesCode,
				ItemDescr = IDE.ItemDescr,
				RefInvNum = NULL,
				RefInvDbId = NULL,
				RefDetailNum = NULL,
				who = @User,
				created = GETDATE(),
				Rate = NULL,
				InvoiceDescription = IDE.InvoiceDescription,
				IsActiveStream = CAST(NULL AS BIT)

			FROM	IDB_Billing.dbo.wActiveBillingCodes ABC (NOLOCK)	-- SHIRISH 11/5/2014 -- updating query to use permanent table
			JOIN	#TF TF ON ABC.InvNum = TF.InvNum AND ABC.InvDbId = TF.InvDbId AND ABC.Billing_Code = TF.Billing_Code AND ABC.ProductGroup = TF.ProductGroup
			LEFT JOIN IDB_Billing.dbo.InvoiceDetailsEnrich IDE ON TF.ChargeId = IDE.ChargeId AND TF.ProductGroup = IDE.ProductGroup AND IDE.ServiceTypeId = @ServiceTypeId AND IDE.Source = ABC.InvDetEnrichSource -- IDBBC-165

			--Only get the billing codes (InvNum) that have TRACE charges. 
			--If there are no TRACE eligible deals for the dealer during the billing period there won't be TRACE charges
			--don't insert InvoiceDetail line item
			WHERE	ABC.InvNum IS NOT NULL 
				AND TF.PeriodId IS NOT NULL 
				AND TF.ChargeId IS NOT NULL
				AND ABC.PROCESS_ID = @ProcessID
				
			UNION ALL

			SELECT
				InvNum = ABC.InvNum,
				InvDbId = ABC.InvDbId,
				DetailNum = NULL,
				productgroup = SNF.productgroup,
				ServiceTypeId = @ServiceTypeId, 
				ChargeId = SNF.ChargeId,
				PeriodId = SNF.PeriodId,
				Source = SNF.Source,
				Quantity = SNF.TotalCTs,
				ItemAmount = ISNULL(SNF.Submission_Netting_Fees, 0),
				ItemPrice = NULL,
				SalesCode = IDE.SalesCode,
				ItemDescr = IDE.ItemDescr,
				RefInvNum = NULL,
				RefInvDbId = NULL,
				RefDetailNum = NULL,
				who = @User,
				created = GETDATE(),
				Rate = NULL,
				InvoiceDescription = IDE.InvoiceDescription,
				IsActiveStream

			FROM	IDB_Billing.dbo.wActiveBillingCodes ABC (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table	
			JOIN #SNF SNF ON ABC.InvNum = SNF.InvNum AND ABC.InvDbId = SNF.InvDbId AND ABC.Billing_Code = SNF.Billing_Code AND ABC.ProductGroup = SNF.ProductGroup
			LEFT JOIN IDB_Billing.dbo.InvoiceDetailsEnrich IDE ON SNF.ChargeId = IDE.ChargeId AND SNF.ProductGroup = IDE.ProductGroup AND IDE.ServiceTypeId = @ServiceTypeId AND IDE.Source = ABC.InvDetEnrichSource --IDBBC-165

			--Only get the billing codes (InvNum) that have submission and netting fees. 
			--If there are no deals for the dealer during the billing period there won't be submission and netting fees
			--don't insert InvoiceDetail line item
			WHERE	ABC.InvNum IS NOT NULL 
				AND SNF.PeriodId IS NOT NULL 
				AND SNF.ChargeId IS NOT NULL
				AND ABC.PROCESS_ID = @ProcessID

			UNION ALL

			-- add CAT fees
			SELECT
				InvNum = ABC.InvNum,
				InvDbId = ABC.InvDbId,
				DetailNum = NULL,
				productgroup = CAT.productgroup,
				ServiceTypeId = @ServiceTypeId, 
				ChargeId = CAT.ChargeId,
				PeriodId = CAT.PeriodId,
				Source = CAT.Source,
				Quantity = CAT.TotalCTs,
				ItemAmount = ISNULL(CAT.CATFees, 0),
				ItemPrice = NULL,
				SalesCode = IDE.SalesCode,
				ItemDescr = IDE.ItemDescr,
				RefInvNum = NULL,
				RefInvDbId = NULL,
				RefDetailNum = NULL,
				who = @User,
				created = GETDATE(),
				Rate = NULL,
				InvoiceDescription = IDE.InvoiceDescription,
				IsActiveStream = NULL

			FROM	IDB_Billing.dbo.wActiveBillingCodes ABC (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table	
			JOIN #CATFees CAT ON ABC.InvNum = CAT.InvNum AND ABC.InvDbId = CAT.InvDbId AND ABC.Billing_Code = CAT.Billing_Code AND ABC.ProductGroup = CAT.ProductGroup
			LEFT JOIN IDB_Billing.dbo.InvoiceDetailsEnrich IDE ON CAT.ChargeId = IDE.ChargeId AND CAT.ProductGroup = IDE.ProductGroup AND IDE.ServiceTypeId = @ServiceTypeId AND IDE.Source = ABC.InvDetEnrichSource --IDBBC-165

			--Only get the billing codes (InvNum) that have submission and netting fees. 
			--If there are no deals for the dealer during the billing period there won't be submission and netting fees
			--don't insert InvoiceDetail line item
			WHERE	ABC.InvNum IS NOT NULL 
				AND CAT.PeriodId IS NOT NULL 
				AND CAT.ChargeId IS NOT NULL
				AND ABC.PROCESS_ID = @ProcessID

			UNION ALL

			/* IDBBC-310 - EUREPO Ticket Fees */
			SELECT
				InvNum = ABC.InvNum,
				InvDbId = ABC.InvDbId,
				DetailNum = NULL,
				productgroup = RTF.productgroup,
				ServiceTypeId = @ServiceTypeId, 
				ChargeId = RTF.ChargeId,
				PeriodId = RTF.PeriodId,
				Source = RTF.Source,
				Quantity = RTF.Volume,
				ItemAmount = ISNULL(RTF.TicketFees, 0),
				ItemPrice = NULL,
				SalesCode = IDE.SalesCode,
				ItemDescr = IDE.ItemDescr,
				RefInvNum = NULL,
				RefInvDbId = NULL,
				RefDetailNum = NULL,
				who = @User,
				created = GETDATE(),
				Rate = NULL,
				InvoiceDescription = IDE.InvoiceDescription,
				IsActiveStream = CAST(NULL AS BIT)

			FROM	IDB_Billing.dbo.wActiveBillingCodes ABC (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table
			JOIN	#RepoTicketFeesByInvoice RTF ON ABC.InvNum = RTF.InvNum AND ABC.InvDbId = RTF.InvDbId AND ABC.Billing_Code = RTF.Billing_Code AND ABC.ProductGroup = RTF.ProductGroup
			LEFT JOIN IDB_Billing.dbo.InvoiceDetailsEnrich IDE ON RTF.ChargeId = IDE.ChargeId AND RTF.ProductGroup = IDE.ProductGroup AND IDE.ServiceTypeId = @ServiceTypeId AND IDE.Source = ABC.InvDetEnrichSource -- IDBBC-165

			--Only get the billing codes (InvNum) that have commission. 
			--If there are no deals for the dealer during the billing period there won't be a commission 
			--don't insert InvoiceDetail line item
			WHERE	ABC.InvNum IS NOT NULL 
				AND RTF.PeriodId IS NOT NULL 
				AND RTF.ChargeId IS NOT NULL
				AND ABC.PROCESS_ID = @ProcessID

			/* SHIRISH 04/06/2017: According to Rich on 4/6/2017,  fail charges should not be included on the invoice. 
								   Commenting below section

			UNION ALL

			SELECT
				InvNum = ABC.InvNum,
				InvDbId = ABC.InvDbId,
				DetailNum = NULL,
				productgroup = FC.productgroup,
				ServiceTypeId = @ServiceTypeId, 
				ChargeId = FC.ChargeId,
				PeriodId = FC.PeriodId,
				Source = FC.Source,
				Quantity = NULL,
				ItemAmount = ISNULL(FC.FailCharge, 0),
				ItemPrice = NULL,
				SalesCode = IDE.SalesCode,
				ItemDescr = IDE.ItemDescr,
				RefInvNum = NULL,
				RefInvDbId = NULL,
				RefDetailNum = NULL,
				who = @User,
				created = GETDATE(),
				Rate = NULL,
				InvoiceDescription = IDE.InvoiceDescription
			
			FROM IDB_Billing.dbo.wActiveBillingCodes ABC (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table	
			JOIN #FC FC ON ABC.InvNum = FC.InvNum AND ABC.InvDbId = FC.InvDbId AND ABC.Billing_Code = FC.Billing_Code AND ABC.ProductGroup = FC.ProductGroup
			LEFT JOIN IDB_Billing.dbo.InvoiceDetailsEnrich IDE ON FC.ChargeId = IDE.ChargeId AND FC.ProductGroup = IDE.ProductGroup AND IDE.ServiceTypeId = @ServiceTypeId

			--Only get the billing codes (InvNum) that have fail charges. 
			--If there are no deals for the dealer during the billing period there won't be fail charges
			--don't insert InvoiceDetail line item
			WHERE	ABC.InvNum IS NOT NULL 
				AND FC.PeriodId IS NOT NULL 
				AND FC.ChargeId IS NOT NULL
				AND ABC.PROCESS_ID = @ProcessID

			*/ -- Fail charge section end
				
			/* NILESH 12/29/2014 -- Commission Credits */		
			UNION ALL
				
			SELECT
				InvNum = ABC.InvNum,
				InvDbId = ABC.InvDbId,
				DetailNum = NULL,
				productgroup = CC.productgroup,
				ServiceTypeId = @ServiceTypeId, 
				ChargeId = CC.ChargeId,
				PeriodId = CC.PeriodId,
				Source = CC.Source,
				Quantity = NULL,
				ItemAmount = ISNULL(CC.CommissionCredit, 0) * -1,
				ItemPrice = NULL,
				SalesCode = IDE.SalesCode,
				ItemDescr = IDE.ItemDescr,
				RefInvNum = NULL,
				RefInvDbId = NULL,
				RefDetailNum = NULL,
				who = @User,
				created = GETDATE(),
				Rate = NULL,
				InvoiceDescription = IDE.InvoiceDescription,
				IsActiveStream = CAST(NULL AS BIT)

			FROM IDB_Billing.dbo.wActiveBillingCodes ABC (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table
			JOIN #CommissionCredits CC ON ABC.InvNum = CC.InvNum AND ABC.InvDbId = CC.InvDbId AND ABC.Billing_Code = CC.BILLING_CODE AND ABC.ProductGroup = CC.ProductGroup
			LEFT JOIN IDB_Billing.dbo.InvoiceDetailsEnrich IDE ON CC.ChargeId = IDE.ChargeId AND CC.ProductGroup = IDE.ProductGroup AND IDE.ServiceTypeId = @ServiceTypeId AND IDE.Source = ABC.InvDetEnrichSource -- IDBBC-165

			--Only get the billing codes (InvNum) that have commission. 
			--If there are no deals for the dealer during the billing period there won't be a commission 
			--don't insert InvoiceDetail line item
			WHERE	ABC.InvNum IS NOT NULL 
				AND CC.PeriodId IS NOT NULL 
				AND CC.ChargeId IS NOT NULL	
				AND ABC.PROCESS_ID = @ProcessID		

			UNION ALL

			-- SHIRISH 10/29/2019: IDBBC-7 Add calculated OTR rebate to Invoice Details
			SELECT	InvNum = ABC.InvNum, -- IDBBC-165
					InvDbId = ABC.InvDbId, -- IDBBC-165
					DetailNum = NULL,
					productgroup = ABC.ProductGroup, -- IDBBC-165
					ServiceTypeId = @ServiceTypeId,
					ChargeId = CT.ChargeId,
					PeriodId = P.PeriodId,
					Source = 'E',
					Quantity = NULL,
					ItemAmount = VBC.TotalComm,
					ItemPrice = NULL,
					SalesCode = IDE.SalesCode,
					ItemDescr = IDE.ItemDescr,
					RefInvNum = NULL,
					RefInvDbId = NULL,
					RefDetailNum = NULL,
					who = @User,
					created = GETDATE(),
					Rate = NULL,
					InvoiceDescription = IDE.InvoiceDescription,
					IsActiveStream = CAST(NULL AS BIT)
			FROM	IDB_Billing.dbo.wActiveBillingCodes ABC (NOLOCK) -- replacing #InvoiceHistory with wActiveBillingCodes
			JOIN	#VolumeBasedCommission AS VBC ON VBC.BillingCode = ABC.Billing_Code
			--JOIN	#InvoiceHistory AS IH ON IH.billing_code = VBC.BillingCode
			JOIN	#ChargeType AS CT ON CT.ChargeType = 'OTR Rebate'
			JOIN	IDB_Billing.dbo.wPeriodIDs AS P ON P.PROCESS_ID = @ProcessID	
			LEFT JOIN IDB_Billing.dbo.InvoiceDetailsEnrich IDE (NOLOCK) ON IDE.ChargeId = CT.ChargeId AND IDE.productgroup = VBC.ProductGroup AND IDE.ServiceTypeId = @ServiceTypeId								 
			WHERE	ABC.PROCESS_ID = @ProcessID -- IDBBC-165
			AND		ABC.ProductGroup = 'OTR' -- IDBBC-165
			AND		VBC.InsertUpdate = 0 -- IDBBC-7 (0 = insert, 1 = update)
			
			UNION ALL

			-- IDBBC-75 Insert OTR rebate that is not associated with any volume based commission
			SELECT	InvNum = IH.InvNum,
					InvDbId = IH.InvDbId,
					DetailNum = NULL,
					productgroup = 'OTR',
					ServiceTypeId = @ServiceTypeId,
					ChargeId = CT.ChargeId,
					PeriodId = P.PeriodId,
					Source = 'E',
					Quantity = NULL,
					ItemAmount = II.ItemAmount,
					ItemPrice = NULL,
					SalesCode = IDE.SalesCode,
					ItemDescr = IDE.ItemDescr,
					RefInvNum = NULL,
					RefInvDbId = NULL,
					RefDetailNum = NULL,
					who = @User,
					created = GETDATE(),
					Rate = NULL,
					InvoiceDescription = IDE.InvoiceDescription,
					IsActiveStream = CAST(NULL AS BIT)
			FROM	#InvoiceInventory AS II
			JOIN	#InvoiceHistory AS IH ON IH.InvNum = II.InvNum AND IH.InvDbId = II.InvDbId
			JOIN	IDB_Billing.dbo.InvoiceInventoryType AS IIT (NOLOCK) ON IIT.InvInvTypeId = II.InvInvTypeId
			JOIN	#ChargeType AS CT ON CT.ChargeType = 'OTR Rebate'
			JOIN	IDB_Billing.dbo.wPeriodIDs AS P ON P.PROCESS_ID = @ProcessID	
			LEFT JOIN IDB_Billing.dbo.InvoiceDetailsEnrich IDE (NOLOCK) ON IDE.ChargeId = CT.ChargeId AND IDE.productgroup = 'OTR' AND IDE.ServiceTypeId = @ServiceTypeId								 
			WHERE	IIT.Code = 'OTRREBATE' -- IDBBC-75 OTR Rebate record

		)
		SELECT
				InvNum,
				InvDbId,
				productgroup,
				ServiceTypeId ,
				ChargeId,
				PeriodId,
				Source,
				Quantity = SUM(Quantity),
				ItemAmount = SUM(ItemAmount),
				ItemPrice,
				SalesCode,
				ItemDescr,
				RefInvNum,
				RefInvDbId,
				RefDetailNum,
				who,
				created ,
				Rate,
				InvoiceDescription

		INTO	#InvoiceDetailsGrp

		FROM	CTE_InvoiceDetails
		
		Group By
				InvNum,
				InvDbId,
				productgroup,
				ServiceTypeId ,
				ChargeId,
				PeriodId,
				Source,
				ItemPrice,
				SalesCode,
				ItemDescr,
				RefInvNum,
				RefInvDbId,
				RefDetailNum,
				who,
				created ,
				Rate,
				InvoiceDescription

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #InvoiceInventory',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		-- Insert into #ItemDetails
		SELECT
				InvNum,
				InvDbId,
				DetailNum = ROW_NUMBER() OVER(PARTITION BY InvNum ORDER BY InvNum, InvDbId, productgroup, PeriodId, Source, ChargeId),
				productgroup,
				ServiceTypeId ,
				ChargeId,
				PeriodId,
				Source,
				Quantity,
				ItemAmount,
				ItemPrice,
				SalesCode,
				ItemDescr,
				RefInvNum,
				RefInvDbId,
				RefDetailNum,
				who,
				created ,
				Rate,
				InvoiceDescription,
				SingleSidedPlatformVolume = 0, -- IDBBC-7 adding column that is used by OTR volume based commissions
				[CLOB/DWAS] = CAST(NULL AS VARCHAR(16))
			
		INTO	#InvoiceDetails

		FROM	#InvoiceDetailsGrp

		ORDER BY InvNum, InvDbId, productgroup, PeriodId, Source, ChargeId

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #InvoiceDetails',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		/*************************** XR-ICAP Clearing Fee Calculation **************************/
		-- IDBBC-25 XR has agreed to pay ICAP's clearing fees on the stream with ICAP
		-- We need to reduce the FICC fees on the ICAP invoice by the appropriate amount associated with the XR streams.
		-- Move FICC charges for these trades to XRICAP invoice
		IF @ReportMode = 0 AND EXISTS (SELECT 1 FROM IDB_Billing.dbo.wActiveBilling AS WAB WHERE WAB.BILLING_CODE = 'XRICAP' AND WAB.PROCESS_ID = @ProcessID)
		BEGIN	

			-- Calculate ICAPs FICC fees for trades on streams with XR
			SELECT	Billing_Code = 'XRICAP',
					ProductGroup = 'OTR',
					ChargeType.ChargeId,
					--CT.Source,
					Quantity = SUM(CT.Quantity),
					Submission_Netting_Fees = SUM((CASE WHEN Cancelled = 1 THEN CS.SubmissionFee
														   ELSE CS.SubmissionFee + CS.FixedNettingFee 
													END) + 
												  (CS.VariableNettingFee * CASE WHEN Cancelled = 1 THEN 0 ELSE Quantity END))

			INTO	#XRICAP_Fees
			FROM	#ClearingTrades CT
			JOIN	IDB_Reporting.dbo.IDB_FALCON_DEALS AS IFD ON IFD.DEAL_NEGOTIATION_ID = CT.DEAL_ID
															 AND IFD.DEAL_TRADE_DATE = CT.Trd_Dt
															 -- IDBBC-41 There could be multiple fills for a trade so there could be multiple records for each deal_nego_id 
															 -- in both deals and clearing trades which will cause cartesion product and fees calculated will be incorrect.  
															 -- added below join to avoid this situation
															 AND IFD.DEAL_ID = CT.Trd_Deal_Id 
															 AND IFD.Dealer = 'ICAP'
															 AND IFD.PoolParticipant = 'XRT'
															 AND IFD.IsActiveStream = 1
			--It is ok to use hard coded value Clearing in the join below because this insert is specifically for Submission & Netting Fees
			JOIN	#ChargeType ChargeType ON ChargeType.ChargeType = 'Clearing'
			JOIN	#ClearingSchedule CS ON CT.ProductGroup = CS.ProductGroup AND CS.TradeType = 'DW' -- IDBC-168 Condition was missed in previous release

			WHERE	CT.ProductGroup = 'OTR'
			AND		CT.Trd_Dt BETWEEN CS.EffectiveStartDate AND CS.EffectiveEndDate
			-- SHIRISH 01/15/2019: For non EFP/NAVX products there should not be any record with SECSuffix but adding this condition to ignore nulls
			AND		ISNULL(CT.SECSuffix,'') = '' 
			AND		CT.BILLING_CODE = 'ICAP1'
			GROUP BY
					ChargeType.ChargeId

			--  SHIRISH 07/22/2019 DIT-18425
			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'After #XRICAP Fee calculation',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2

			-- Insert ICAPs Clearing fees into XRICAP invoice details
			INSERT INTO #InvoiceDetails
			(
			    InvNum,
			    InvDbId,
				DetailNum,
			    productgroup,
				ServiceTypeId,
			    ChargeId,
			    PeriodId,
			    Source,
				Quantity,
				ItemAmount,
			    SalesCode,
			    ItemDescr,
				who,
				created,
			    InvoiceDescription,
				SingleSidedPlatformVolume
			)
			SELECT	IH.InvNum,
					IH.InvDbId,
					DetailNum = 1,
					XF.ProductGroup,
					ServiceTypeId = @ServiceTypeId,
					XF.ChargeId,
					PeriodId = WPID.PeriodId,
					Source = 'E',
					XF.Quantity,
					XF.Submission_Netting_Fees,
					SalesCode = IDE.SalesCode,
					ItemDescr = IDE.ItemDescr,
					who = @User,
					created = GETDATE(),
					IDE.InvoiceDescription,
					SingleSidedPlatformVolume = 0
			FROM	#XRICAP_Fees AS XF
			JOIN	#InvoiceHistory AS IH ON IH.billing_code = XF.Billing_Code
			JOIN	IDB_Billing.dbo.wPeriodIDs AS WPID (NOLOCK) ON WPID.PROCESS_ID = @ProcessID
			LEFT JOIN IDB_Billing.dbo.InvoiceDetailsEnrich IDE (NOLOCK) ON IDE.ChargeId = XF.ChargeId AND IDE.productgroup = XF.ProductGroup AND IDE.ServiceTypeId = @ServiceTypeId
			WHERE	IH.billing_code = 'XRICAP'

			DECLARE @ICAPInvNum INT
			SELECT @ICAPInvNum = InvNum FROM #InvoiceHistory AS IH WHERE IH.billing_code = 'ICAP1'

			--IF @Debug=1
			--	SELECT 'ID Before XRICAP Update', * FROM #InvoiceDetails AS ID WHERE ID.InvNum = @ICAPInvNum

			-- Reduce ICAP's clearing fees with the amount charged to XRICAP invoice 
			UPDATE	ID
			SET		ID.ItemAmount = ID.ItemAmount - XF.Submission_Netting_Fees
			FROM	#InvoiceHistory AS IH (NOLOCK)
			JOIN	#ChargeType ChargeType ON ChargeType.ChargeType = 'Clearing'
			JOIN	#InvoiceDetails AS ID (NOLOCK) ON ID.InvNum = IH.InvNum AND ID.InvDbId = IH.InvDbId AND ID.productgroup = 'OTR' AND ID.ChargeId = ChargeType.ChargeId AND ID.Source = 'E'
			CROSS JOIN	#XRICAP_Fees XF
			WHERE	IH.billing_code = 'ICAP1'

			--IF @Debug = 1
			--	SELECT 'ID after XRICAP Update', * FROM #InvoiceDetails AS ID WHERE ID.InvNum = @ICAPInvNum

		END
		/***************************  END XR-ICAP Clearing Fee Calculation **************************/

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #XRICAP Clearing fee insert',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		-- IDBBC-7 Insert OTR rebate record into InoviceInventory table Based on InsertUpdate flag (0 = insert, 1 = update)
		INSERT INTO	#InvoiceInventory
		(
		    InvNum,
		    InvDbId,
		    logon_id,
		    ProductGroup,
		    Source,
		    ItemAmount,
		    InvInvTypeId,
		    who,
		    created,
		    periodid,
		    ChargeId
		)
		SELECT	InvNum = IH.InvNum,
				InvDbId = IH.InvDbId,
				logon_id = @User,
				ProductGroup = 'OTR',
				Source = 'E',
				ItemAmount = VBC.TotalComm,
				InvInvTypeId = IIT.InvInvTypeId,
				who = @User,
				created = GETDATE(),
				Periodid = WPID.PeriodId,
				ChargeId = CT.ChargeId

		FROM	#VolumeBasedCommission AS VBC
		JOIN	#InvoiceHistory AS IH ON IH.billing_code = VBC.BillingCode
		JOIN	IDB_Billing.dbo.InvoiceInventoryType AS IIT (NOLOCK) ON IIT.Code = 'OTRREBATE' AND IIT.Billing_Type IS NULL
		JOIN	IDB_Billing.dbo.wPeriodIDs AS WPID (NOLOCK) ON WPID.PROCESS_ID = @ProcessID
		JOIN	#ChargeType AS CT ON CT.ChargeType = 'OTR Rebate'
		WHERE	VBC.InsertUpdate = 0

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After OTR Rebate Insert',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		--GET DEAL DETAILS FROM THE BASE TABLE. GET BOTH, AGENCY AND THE TREASURY LEGS

		IF (@productgroup IS NULL OR @productgroup = 'AGCY')
			EXEC IDB_Reporting.dbo.GetDealDetails_AGCY @ProcessID

		/* 
		-- IDB-18425 : Added a condition to execute the below code in Invoice 
		-- mode or Debug mode. This is to improve the billing performance.
		-- The below code insert data into wDealDetails table which is used
		-- for InvoiceDealDetails table. The only exception is AGCY which is
		-- used to populate the Deal_Commission table.
		*/			
		IF @ReportMode = 0 OR @Debug = 1
		BEGIN
			IF (@productgroup IS NULL OR @productgroup = 'BILL')
				EXEC IDB_Reporting.dbo.GetDealDetails_BILL @ProcessID

			--  SHIRISH 07/22/2019 DIT-18425
			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'GetDealDetails_BILL',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2
			
			--IF (@productgroup IS NULL OR @productgroup = 'IOS')
			--	EXEC IDB_Reporting.dbo.GetDealDetails_IOS @ProcessID
			
			IF (@productgroup IS NULL OR @productgroup = 'TRSY')
				EXEC IDB_Reporting.dbo.GetDealDetails_TRSY @ProcessID

			--  SHIRISH 07/22/2019 DIT-18425
			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'GetDealDetails_TRSY',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2
			
			IF (@productgroup IS NULL OR @productgroup = 'TIPS')
				EXEC IDB_Reporting.dbo.GetDealDetails_TIPS @ProcessID

			--  SHIRISH 07/22/2019 DIT-18425
			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'GetDealDetails_TIPS',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2
		
			IF (@productgroup IS NULL OR @productgroup = 'EFP')
				EXEC IDB_Reporting.dbo.GetDealDetails_EFP @ProcessID

			--  SHIRISH 07/22/2019 DIT-18425
			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'GetDealDetails_EFP',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2
			
			--IF (@productgroup IS NULL OR @productgroup = 'EQSWP')
			--	EXEC IDB_Reporting.dbo.GetDealDetails_EQSWP @ProcessID
			
			--IF (@productgroup IS NULL OR @productgroup = 'ECDS')
			--	EXEC IDB_Reporting.dbo.GetDealDetails_ECDS @ProcessID
			
			IF (@productgroup IS NULL OR @productgroup = 'AMSWP')
				EXEC IDB_Reporting.dbo.GetDealDetails_AMSWP @ProcessID

			--  SHIRISH 07/22/2019 DIT-18425
			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'GetDealDetails_AMSWP',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2
			
			IF (@productgroup IS NULL OR @productgroup = 'CAD')
				EXEC IDB_Reporting.dbo.GetDealDetails_CAD @ProcessID

			--  SHIRISH 07/22/2019 DIT-18425
			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'GetDealDetails_CAD',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2
			
			--IF (@productgroup IS NULL OR @productgroup = 'UCDS')
			--	EXEC IDB_Reporting.dbo.GetDealDetails_UCDS @ProcessID
			
			--IF (@productgroup IS NULL OR @productgroup = 'CDXEM')
			--	EXEC IDB_Reporting.dbo.GetDealDetails_CDXEM @ProcessID
			
			IF (@productgroup IS NULL OR @productgroup = 'USFRN')
				 EXEC IDB_Reporting.dbo.GetDealDetails_USFRN @ProcessID
			
			--  SHIRISH 07/22/2019 DIT-18425
			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'GetDealDetails_USFRN',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2

			IF (@productgroup IS NULL OR @productgroup = 'NAVX')
				EXEC IDB_Reporting.dbo.GetDealDetails_NAVX @ProcessID
			
			--  SHIRISH 07/22/2019 DIT-18425
			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'GetDealDetails_NAVX',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2

			IF (@productgroup IS NULL OR @productgroup = 'OTR')
				EXEC IDB_Reporting.dbo.GetDealDetails_OTR @ProcessID

			--  SHIRISH 07/22/2019 DIT-18425
			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'GetDealDetails_OTR',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2

			IF (@productgroup IS NULL OR @productgroup = 'USREPO')
				EXEC IDB_Reporting.dbo.GetDealDetails_USREPO @ProcessID

			--  SHIRISH 07/22/2019 DIT-18425
			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'GetDealDetails_USREPO',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2

			---- SM 03/13/2019 DIT-10448 -- Get Deal details for GILTS
			--IF (@productgroup IS NULL OR @productgroup = 'GILTS')
			--	EXEC IDB_Reporting.dbo.GetDealDetails_GILTS @ProcessID

			--VK 08/28/2019 - GDB-99: BTIC-GDB Billing
			IF (@productgroup IS NULL OR @productgroup = 'BTIC')
				EXEC IDB_Reporting.dbo.GetDealDetails_BTIC @ProcessID

			--  SHIRISH 07/22/2019 DIT-18425
			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'GetDealDetails_BTIC',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2

			--SM 05/25/2023 - IDBBC-240: new product BOX
			IF (@productgroup IS NULL OR @productgroup = 'BOX')
				EXEC IDB_Reporting.dbo.GetDealDetails_BOX @ProcessID

			-- DIT-18425
			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'GetDealDetails_BOX',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2

			--SM 10/04/2024 - IDBBC-344: new product REVCON
			IF (@productgroup IS NULL OR @productgroup = 'REVCON')
				EXEC IDB_Reporting.dbo.GetDealDetails_REVCON @ProcessID

			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'GetDealDetails_REVCOM',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2

			--SM 10/08/2024 - IDBBC-344: new product COMBO
			IF (@productgroup IS NULL OR @productgroup = 'COMBO')
				EXEC IDB_Reporting.dbo.GetDealDetails_COMBO @ProcessID

			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'GetDealDetails_COMBO',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2

			-- SM 06/07/2023 - IDBBC-238 new product EUREPO
			IF (@productgroup IS NULL OR @productgroup = 'EUREPO')
				EXEC IDB_Reporting.dbo.GetDealDetails_EUREPO @ProcessID

			-- DIT-18425
			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'GetDealDetails_EUREPO',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2

		END


		-----------------------------------------------------------------------
		
		/***** Deal_Commission AGCY block *****/
		-- IDBBC-178 Only populate Deal_Commission in D mode.
		IF @SummaryType = 'D'
		BEGIN
			INSERT INTO IDB_Billing.dbo.wDeal_Commission
		(
			PROCESS_ID,
			RowNum,
			Deal_Negotiation_Id,
			Dealer,
			Deal_User_Id,
			Deal_Way,
			Source,
			ProductGroup,
			DEAL_PRINCIPAL,
			DEAL_ACCINT,
			DEAL_PROCEEDS,
			Deal_O_Factor,
			Deal_O_FinInt,
			CHARGE_RATE_AGGRESSIVE,
			CHARGE_RATE_PASSIVE,
			DealCommission,
			DealCommission_Override,
			Deal_TradeDate,
			BrokerId,
			SecurityName,
			Quantity,
			TradeType,
			Deal_Price,
			Deal_Aggressive_Passive,
			Deal_Final_Commission,
			Deal_Days_To_Maturity,
			Deal_Id,
			DEAL_EXTRA_COMMISSION,
			DEAL_CHARGED_QTY,
			DEAL_RISK,
			ExchangeRateMultiplier,
			CURRENCY_CODE,
			DEAL_DISCOUNT_RATE,
			TIER_CHARGE_RATE_AGGRESSIVE,
			TIER_CHARGE_RATE_PASSIVE,
			DEAL_SEF_TRADE,
			DEAL_TIM_QTY,
			DEAL_FIRST_AGGRESSED,
			IsActiveStream,
			DEAL_FIRST_AGGRESSOR,
			OVERWRITE,
			DEAL_DATE,
			DEAL_STLMT_DATE,
			DEAL_REPO_START_DATE,
			DEAL_REPO_END_DATE,
			Billing_Code,
			Operator,
			ZeroBrokerCommission,
			FF_DealCommission,
			FF_DealCommission_Override,
			UseGap,
			DEAL_GAP_DV01,
			RepoRecordType
		)
		SELECT	DISTINCT 
				PROCESS_ID = @ProcessID,
				RowNum = 0,	
				Deal_Negotiation_Id = dbs.Deal_Negotiation_Id,
				Dealer = dbs.Dealer,
				Deal_User_Id = dbs.Deal_User_Id,
				Deal_Way = dbs.Deal_Way,
				Source = dbs.Source,
				ProductGroup = dbs.ProductGroup,
				DEAL_PRINCIPAL = d.DEAL_PRINCIPAL,
				DEAL_ACCINT = d.DEAL_ACCINT,
				DEAL_PROCEEDS = d.DEAL_PROCEEDS,
				Deal_O_Factor = dbs.Deal_O_Factor,
				Deal_O_FinInt = NULL, --d.Deal_O_Finint, -- SHIRISH 06/07/2017: Migration STRADE to FALCON
				CHARGE_RATE_AGGRESSIVE = dbs.CHARGE_RATE_AGGRESSIVE,
				CHARGE_RATE_PASSIVE = dbs.CHARGE_RATE_PASSIVE,
				/* NILESH 08/10/2012: 
				-- We need to remove the extra commission as the CommissionOwed_Precap already has this
				-- factored in for the commission summary and invoices. So when generating the deals
				-- for the Deals_Commission we need should to reduce this value to avoid overage in the 
				-- total commission owed by the dealer.
				*/
				DealCommission = CAST(ABS(d.DEAL_COMMISSION) as float) - CAST(ISNULL(d.DEAL_EXTRA_COMMISSION,0) as float),
				DealCommission_Override = CAST(NULL as float),--dap.CommissionOwed_PreCap_Override,
				Deal_TradeDate = d.DEAL_TRADE_DATE,	/* NS:10/12/2011-Added date for future data mining requirements . */	
				/* NS:10/27/2011- Added following new columns to the table for the broker commission summary reports */
				BrokerId = CASE WHEN d.Source = 'HFV' THEN d.BrokerId ELSE NULL END, --d.Broker, -- IDBBC-309
				SecurityName = d.Deal_Security_Name,
				Quantity = d.DEAL_QUANTITY,
				/*NILESH 10/19/2012 - Changed to TradeType2 from TradeType for specific SWAP Security Type */
				TradeType = d.DEAL_TRDTYPE, --.TradeType2,
				Deal_Price = d.Deal_Price,
				Deal_Aggressive_Passive = CASE d.DEAL_IS_AGRESSIVE WHEN 1 THEN 'A' ELSE 'P' END,
				Deal_Final_Commission = CASE d.DEAL_FINAL_COMMISSION WHEN 1 THEN 'Y' ELSE 'N' END,
				-- To be identified whether to use DEAL_TENOR or DEAL_DAYS_TO_MATURITY for TIPS
				Deal_Days_To_Maturity = d.DEAL_DAYS_TO_MATURITY,
				Deal_Id = d.DEAL_ID,
				DEAL_EXTRA_COMMISSION = CAST(ISNULL(d.DEAL_EXTRA_COMMISSION,0) AS float),
				DEAL_CHARGED_QTY = NULL,
				DEAL_RISK = NULL, -- d.DEAL_RISK, -- SHIRISH 06/07/2017: Migration STRADE to FALCON
				ExchangeRateMultiplier = NULL, -- Not applicable for AGCY
				/* ############################################################# */
				/* CHANGE THIS ONCE READY TO ACTIVATE THE CURRENCY DATA IN PROD */
				--CURRENCY_CODE
				/* ############################################################ */
				CURRENCY_CODE = 'USD',
				DEAL_DISCOUNT_RATE = NULL, --d.DEAL_DISCOUNTRATE, -- SHIRISH 06/07/2017: Migration STRADE to FALCON
				/* NILESH 08/01/2013 -- Tiered Billing */
				dbs.TIER_CHARGE_RATE_AGGRESSIVE,
				dbs.TIER_CHARGE_RATE_PASSIVE,
				DEAL_SEF_TRADE = 0,--d.DEAL_SEF_TRADE, -- NEED TO FIND THIS
				DEAL_TIM_QTY = NULL,
				DEAL_FIRST_AGGRESSED = NULL,
				dbs.IsActiveStream,
				d.DEAL_FIRST_AGGRESSOR,
				dbs.OVERWRITE, -- SHIRISH 06/01/2017
				d.DEAL_DATE, -- SHIRISH 08/22/2017
				d.DEAL_STLMT_DATE, -- SHIRISH 08/22/2017
				DEAL_REPO_START_DATE = CAST(NULL as datetime), -- SHIRISH 08/22/2017
				DEAL_REPO_END_DATE = CAST(NULL as datetime), -- SHIRISH 08/22/2017
				dbs.Billing_Code, -- SHIRISH 03/07/2018
				dbs.Operator,
				ZeroBrokerCommission = 0,	/*DIT-11179 - Not applicable for the AGCY product */
				FF_DealCommission = NULL,
				FF_DealCommission_Override = NULL,
				dbs.UseGap, -- IDBBC-132
				DEAL_GAP_DV01 = NULL, -- IDBBC-132
				RepoRecordType = NULL -- IDBBC-234
		
		FROM	IDB_Billing.dbo.wDealDetails wdd (NOLOCK)
		--JOIN	STRADE_AGCY.dbo.TW_DEAL d (NOLOCK) ON d.DEAL_ID = wdd.Deal_Id
		JOIN	IDB_Reporting.dbo.IDB_FALCON_DEALS d (NOLOCK) on d.DEAL_ID = wdd.Deal_Id
		JOIN	IDB_Billing.dbo.wDealBillingSchedule dbs (NOLOCK) ON dbs.PROCESS_ID = wdd.PROCESS_ID
																  AND dbs.ProductGroup = wdd.ProductGroup
																  AND dbs.DEAL_NEGOTIATION_ID = d.DEAL_NEGOTIATION_ID
																  AND dbs.InvNum = wdd.InvNum
																  AND dbs.InvDbId = wdd.InvDbId
																  AND dbs.PeriodId = wdd.PeriodId
																  AND dbs.Source = wdd.Source
		
		WHERE	wdd.ProductGroup = 'AGCY'
		AND		wdd.PROCESS_ID = @ProcessID

			--  SHIRISH 07/22/2019 DIT-18425
			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'After Deals Commission AGCY',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2
		END

		/***** Deal_Commission AGCY block end *****/

		--CREATE COMMISSION SUMMARY
		-- IDBBC-105 There could be multiple Active stream schedules.  In this case we need to add commission calculated for volume based schedule to 
		-- commission calculated for regular active stream schedule.  Updating below code to add commission from RebateAndBillingTracker to original amount instead of replacing it
		SELECT
			StartDate = ABC.Start_Billing_Period ,
			EndDate = ABC.End_Billing_Period,
			ProductGroup = COALESCE(NC.ProductGroup,SNF.ProductGroup,FC.ProductGroup,TF.ProductGroup),
			Billing_Code = ABC.Billing_Code,
			Source = COALESCE(NC.Source,SNF.Source,FC.Source,TF.Source),
			TotalVolume = ISNULL(NC.Volume, 0),
			CommissionWithoutCap = ISNULL(CWC.CommissionWithoutCap, 0) + ISNULL(RABT.RebateAmount, 0), --IDBBC-49 -- IDBBC-105
			CommissionWithoutCapOverride = ISNULL(CWC.CommissionWithoutCapOverride, 0) + ISNULL(RABT.RebateAmount, 0), --IDBBC-49 -- IDBBC-105
			CommissionOwed = ISNULL( NC.CommissionOwed, 0) + ISNULL(RABT.RebateAmount, 0), --IDBBC-49 -- IDBBC-105
			CommissionCollected = ISNULL(NC.CommissionCollected, 0),
			NetCommission = ISNULL(NC.CommissionOwed - NC.CommissionCollected, 0) + ISNULL(RABT.RebateAmount, 0), --IDBBC-49 -- IDBBC-105
			SubmissionAndNettingFees = ISNULL(SNF.Submission_Netting_Fees, 0),
			FailCharge = ISNULL(FC.FailCharge, 0),
			TraceSubmissionFees = ISNULL(TF.TraceSubmissionFees, 0),	/*NILESH 02-23-2010: Trace Submission Fees */
			TracePassThruFees = ISNULL(TF.TracePassThruFees, 0),	/*NILESH 02-23-2010: Trace Submission Fees */
			TraceDeals = ISNULL(TF.TradeCt,0),		/* NILESH 06-07-2011: Trace Deals Count */
			CommissionCredits = CAST(NULL AS FLOAT),
			CommissionCreditAmount = CAST(NULL AS FLOAT),
			--IsActiveStream = ISNULL(NC.IsActiveStream,0) /* SHIRISH 04/20/2016  */
			IsActiveStream = NC.IsActiveStream, --CASE WHEN COALESCE(NC.ProductGroup,SNF.ProductGroup,FC.ProductGroup,TF.ProductGroup)='OTR' THEN ISNULL(NC.IsActiveStream,0) ELSE NULL END
			CWC.Security_Currency,
			RepoTicketFees = ISNULL(CWC.RepoTicketFees,0),
			CATProspectiveFee = ISNULL(CAT.CATProspectiveFee, 0), -- IDBBC-332
			CATHistoricFee = ISNULL(CAT.CATHistoricFee, 0) -- IDBBC-332

		INTO	#CommissionSummary

		FROM	#ActiveBillingCodesWithPeriodIDs ABC
		JOIN	IDB_Billing.dbo.wPeriodIDs AS WPID ON WPID.PeriodId = ABC.PeriodId AND WPID.PROCESS_ID = @ProcessID -- IDBBC-49 Join to get period start dates
		LEFT JOIN #NetCommission NC ON ABC.InvNum = NC.InvNum AND ABC.InvDbId = NC.InvDbId AND ABC.Billing_Code = NC.Billing_Code AND ABC.PeriodId = NC.PeriodId AND ABC.ProductGroup = NC.ProductGroup AND ABC.Source = NC.Source
		LEFT JOIN #CommissionWithoutCap CWC ON ABC.InvNum = CWC.InvNum AND ABC.InvDbId = CWC.InvDbId AND ABC.Billing_Code = CWC.Billing_Code AND ABC.PeriodId = CWC.PeriodId AND ABC.ProductGroup = CWC.ProductGroup AND ABC.Source = CWC.Source AND ISNULL(NC.IsActiveStream,0) = ISNULL(CWC.IsActiveStream,0) AND ISNULL(NC.Security_Currency,'') = ISNULL(CWC.Security_Currency,'')
		-- IDBBC-218 There could be more than one record for a billing code in #SNF.  To avoid cartesian product we need to sum up #SNF records to get only one record per billing code/product/source
		LEFT JOIN
		(
			SELECT	InvNum,
					InvDbId,
					BILLING_CODE,
					PeriodId,
					ProductGroup,
					Source,
					IsActiveStream,
					Submission_Netting_Fees= SUM(Submission_Netting_Fees)
			FROM	#SNF
			GROUP BY 
					InvNum,
					InvDbId,
					BILLING_CODE,
					PeriodId,
					ProductGroup,
					Source,
					IsActiveStream

		) AS SNF ON 
			ABC.InvNum = SNF.InvNum AND ABC.InvDbId = SNF.InvDbId AND ABC.Billing_Code = SNF.Billing_Code AND ABC.PeriodId = SNF.PeriodId AND ABC.ProductGroup = SNF.ProductGroup AND ABC.Source = SNF.Source AND ISNULL(NC.IsActiveStream,0) = ISNULL(SNF.IsActiveStream,0)
		LEFT JOIN #FC FC ON ABC.InvNum = FC.InvNum AND ABC.InvDbId = FC.InvDbId AND ABC.Billing_Code = FC.Billing_Code AND ABC.PeriodId = FC.PeriodId AND ABC.ProductGroup = FC.ProductGroup AND ABC.Source = FC.Source
		LEFT JOIN #TF TF ON ABC.InvNum = TF.InvNum AND ABC.InvDbId = TF.InvDbId AND ABC.Billing_Code = TF.Billing_Code AND ABC.PeriodId = TF.PeriodId AND ABC.ProductGroup = TF.ProductGroup AND ABC.Source = TF.Source	/*NILESH 02-23-2010: Trace Submission Fees */
		LEFT JOIN 
		(
			SELECT	CF.InvNum,
					CF.InvDbId,
					CF.BILLING_CODE,
					CF.PeriodId,
					CF.ProductGroup,
					CF.Source,
					CATProspectiveFee = SUM(CASE WHEN CF.ChargeId = 20 THEN CF.CATFees ELSE 0.0 END),
					CATHistoricFee = SUM(CASE WHEN CF.ChargeId = 21 THEN CF.CATFees ELSE 0.0 END)
			FROM	#CATFees CF
			GROUP BY
					CF.InvNum,
					CF.InvDbId,
					CF.BILLING_CODE,
					CF.PeriodId,
					CF.ProductGroup,
					CF.Source
		) AS CAT ON CAT.InvNum = ABC.InvNum AND CAT.InvDbId = ABC.InvDbId AND CAT.BILLING_CODE = ABC.Billing_Code AND CAT.PeriodId = ABC.PeriodId AND CAT.ProductGroup = ABC.ProductGroup AND CAT.Source = ABC.Source

		-- 2020/03/18 IDBBC-49 WHen SummaryTYpe is MTD or YTD, we need to get commission calculated for Volume Based Commission Schedules from RebateAndBillingTracker
		--			RebateAmount > 0 indicates its commission amount.  This is only applicable to OTR
		--LEFT JOIN IDB_Billing.dbo.RebateAndBillingTracker AS RABT (NOLOCK) ON RABT.Billing_Code = ABC.Billing_Code AND RABT.EffectiveStartDate = WPID.PeriodDate AND ABC.ProductGroup = RABT.ProductGroup AND ABC.Source = 'E' AND ISNULL(RABT.IsActiveStream,0) = ISNULL(CWC.IsActiveStream,0) AND ISNULL(RABT.RebateAmount,0) > 0 AND @SummaryType IN ('MTD','YTD')
		-- IDBBC-196 there could be more than one volume based schedule and can have multiple records in RebateAndBillingTracker.  We need to sum and get only one record to avoid cartesian product
		OUTER APPLY
				(
					SELECT	RebateAmount = SUM(RebateAmount)
					FROM	IDB_Billing.dbo.RebateAndBillingTracker R (NOLOCK)
					JOIN	IDB_Billing.dbo.OverrideSchedules OS (NOLOCK) ON OS.ScheduleId = R.ScheduleId
					WHERE	R.Billing_Code = ABC.Billing_Code
					AND		R.EffectiveStartDate = WPID.PeriodDate
					AND		R.ProductGroup = ABC.ProductGroup
					AND		ABC.Source = 'E'
					AND		ISNULL(R.IsActiveStream,0) = ISNULL(CWC.IsActiveStream,0) 
					AND		ISNULL(R.RebateAmount,0) > 0
					AND		@SummaryType IN ('MTD','YTD')
					AND		ISNULL(OS.Security_Currency,'') = ISNULL(CWC.Security_Currency,'')
				) AS RABT
		WHERE	ABC.InvNum IS NOT NULL 

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After #CommissionSummary Creation',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2

		
		/* Insert the Records which are not invoiced but need to be */
		INSERT INTO #CommissionSummary
		SELECT
			StartDate = @date1 ,
			EndDate = @date2,
			ProductGroup = ProductGroup,
			Billing_Code = Billing_Code,
			Source = Source,
			TotalVolume = 0,
			CommissionWithoutCap = 0,
			CommissionWithoutCapOverride = 0,
			CommissionOwed = 0,
			CommissionCollected = 0,
			NetCommission = 0,
			SubmissionAndNettingFees = 0,
			FailCharge = 0,
			TraceSubmissionFees = ISNULL(TraceSubmissionFees, 0),	/*NILESH 02-23-2010: Trace Submission Fees */
			TracePassThruFees = ISNULL(TracePassThruFees, 0),	/*NILESH 02-23-2010: Trace Submission Fees */
			TraceDeals = ISNULL(TradeCt,0),		/* NILESH 06-07-2011: Trace Deals Count */
			CommissionCredits = CAST(NULL AS FLOAT),
			CommissionCreditAmount = CAST(NULL AS FLOAT),
			IsActiveStream = CASE WHEN ProductGroup='OTR' THEN 0 ELSE NULL END,
			Security_Currency = NULL,
			RepoTicketFees = 0,
			CATProspectiveFee = 0,
			CATHistoricFee = 0
			
		FROM	#TF_COMMREPORT TF
		
		
		/* NILESH -- Tiered Billing */
		-- Need the following data for recreating the commission summary data with the 
		-- new revised commission.
		SELECT
			StartDate = ABC.Start_Billing_Period ,
			EndDate = ABC.End_Billing_Period,
			ProductGroup = NC.ProductGroup, --CASE WHEN NC.ProductGroup IS NULL THEN ISNULL(SNF.ProductGroup, ISNULL(FC.ProductGroup,TF.ProductGroup)) ELSE NC.ProductGroup END,
			Billing_Code = ABC.Billing_Code,
			Source = NC.Source, --CASE WHEN NC.Source IS NULL THEN ISNULL(SNF.Source, ISNULL(FC.Source,TF.Source)) ELSE NC.Source END,
			TotalVolume = ISNULL(NC.Volume, 0),
			CommissionWithoutCap = ISNULL(CWC.CommissionWithoutCap, 0),
			CommissionWithoutCapOverride = ISNULL(CWC.CommissionWithoutCapOverride, 0),
			CommissionOwed = ISNULL(NC.CommissionOwed, 0),
			CommissionCollected = ISNULL(NC.CommissionCollected, 0),
			NetCommission = ISNULL(NC.CommissionOwed - NC.CommissionCollected,0),
			SubmissionAndNettingFees = ISNULL(SNF.Submission_Netting_Fees, 0),
			FailCharge = ISNULL(FC.FailCharge, 0),
			TraceSubmissionFees = ISNULL(TF.TraceSubmissionFees, 0),
			TracePassThruFees = ISNULL(TF.TracePassThruFees, 0),
			TraceDeals = ISNULL(TF.TradeCt,0),
			CommissionCredits = CAST(NULL AS FLOAT),
			CommissionCreditAmount = CAST(NULL AS FLOAT)

		INTO	#CommissionSummary_Tier

		FROM	#ActiveBillingCodesWithPeriodIDs_Tier ABC
		LEFT JOIN #NetCommission_Tier NC ON ABC.InvNum = NC.InvNum AND ABC.InvDbId = NC.InvDbId AND ABC.Billing_Code = NC.Billing_Code AND ABC.PeriodId = NC.PeriodId AND ABC.Start_Billing_Period = NC.TradeDate AND ABC.End_Billing_Period = NC.TradeDate AND ABC.ProductGroup = NC.ProductGroup AND ABC.Source = NC.Source
		LEFT JOIN #CommissionWithoutCap_Tier CWC ON ABC.InvNum = CWC.InvNum AND ABC.InvDbId = CWC.InvDbId AND ABC.Billing_Code = CWC.Billing_Code AND ABC.PeriodId = CWC.PeriodId AND ABC.Start_Billing_Period = CWC.TradeDate AND ABC.End_Billing_Period = CWC.TradeDate AND ABC.ProductGroup = CWC.ProductGroup AND ABC.Source = CWC.Source
		LEFT JOIN #SNF_Tier SNF ON ABC.InvNum = SNF.InvNum AND ABC.InvDbId = SNF.InvDbId AND ABC.Billing_Code = SNF.Billing_Code AND ABC.PeriodId = SNF.PeriodId AND ABC.Start_Billing_Period = SNF.Trd_Dt AND ABC.End_Billing_Period = SNF.Trd_Dt AND ABC.ProductGroup = SNF.ProductGroup AND ABC.Source = SNF.Source
		LEFT JOIN #FC_Tier FC ON ABC.InvNum = FC.InvNum AND ABC.InvDbId = FC.InvDbId AND ABC.Billing_Code = FC.Billing_Code AND ABC.PeriodId = FC.PeriodId AND ABC.Start_Billing_Period = FC.Trd_Dt AND ABC.End_Billing_Period = FC.Trd_Dt AND ABC.ProductGroup = FC.ProductGroup AND ABC.Source = FC.Source
		LEFT JOIN #TF_Tier TF ON ABC.InvNum = TF.InvNum AND ABC.InvDbId = TF.InvDbId AND ABC.Billing_Code = TF.Billing_Code AND ABC.PeriodId = TF.PeriodId AND ABC.Start_Billing_Period = TF.TradeDate AND ABC.End_Billing_Period = TF.TradeDate AND ABC.ProductGroup = TF.ProductGroup AND ABC.Source = TF.Source	/*NILESH 02-23-2010: Trace Submission Fees */

		WHERE	ABC.InvNum IS NOT NULL 

		/* NILESH 12/29/2014 : Update the Commission Credits */
		-- Update the commission summary 
		UPDATE	CS
		SET		CS.CommissionCredits = CASE @SummaryType WHEN 'D' THEN  DC.DailyCredits  WHEN 'PBD' THEN  DC.DailyCredits WHEN 'MTD' THEN  DC.MTDCredits ELSE CAST(0 AS FLOAT) END,
				CS.CommissionCreditAmount = CASE @SummaryType WHEN 'MTD' THEN  DC.MTDEligibleCreditAmount  ELSE CAST(0 AS FLOAT) END 
		FROM	#CommissionSummary CS
		-- SHIRISH 01/05/2016: Adding commission credits and credit amounts for both E and V to AMSWP(E) Billing Code
		JOIN	(SELECT BillingCode, 
						DailyCredits = SUM(DailyCredits), 
						MTDCredits = SUM(MTDCredits), 
						-- SHIRISH 2/9/2016 - Capping DV01 Credit to Max DV01 Credit amount
						-- SHIRISH 1/13/2017 - Cap is already applied when we pull data from GetDealerCommissionSummary
						--MTDEligibleCreditAmount = CASE WHEN SUM(MTDEligibleCreditAmount) > @MaxDV01CreditAmount THEN @MaxDV01CreditAmount ELSE SUM(MTDEligibleCreditAmount) END
						MTDEligibleCreditAmount
				 FROM	#DealerCredits
				 GROUP BY BillingCode,MTDEligibleCreditAmount) AS DC ON CS.Billing_Code = DC.BillingCode
		WHERE	CS.ProductGroup = 'AMSWP'
		AND		CS.SOURCE = 'E'
		AND		CS.StartDate = @date1 AND CS.EndDate = @date2

		--Update the commission summary (Tiered)
		UPDATE	CS
		SET		CS.CommissionCredits = CASE @SummaryType WHEN 'D' THEN  DC.DailyCredits WHEN 'PBD' THEN  DC.DailyCredits WHEN 'MTD' THEN  DC.MTDCredits ELSE CAST(0 AS FLOAT) END,
				CS.CommissionCreditAmount = CASE @SummaryType WHEN 'MTD' THEN  DC.MTDEligibleCreditAmount  ELSE CAST(0 AS FLOAT) END 
		FROM	#CommissionSummary_Tier CS
		-- SHIRISH 01/05/2016: Adding commission credits and credit amounts for both E and V to AMSWP(E) Billing Code
		JOIN	(SELECT BillingCode, 
						DailyCredits = SUM(DailyCredits), 
						MTDCredits = SUM(MTDCredits), 
						-- SHIRISH 2/9/2016 - Capping DV01 Credit to Max DV01 Credit amount
						-- SHIRISH 1/13/2017 - Cap is already applied when we pull data from GetDealerCommissionSummary
						--MTDEligibleCreditAmount = CASE WHEN SUM(MTDEligibleCreditAmount) > @MaxDV01CreditAmount THEN @MaxDV01CreditAmount ELSE SUM(MTDEligibleCreditAmount) END
						MTDEligibleCreditAmount
				 FROM	#DealerCredits
				 GROUP BY BillingCode,MTDEligibleCreditAmount) AS DC ON CS.Billing_Code = DC.BillingCode
		WHERE	CS.ProductGroup = 'AMSWP'
		AND		CS.SOURCE = 'E'
		AND		CS.StartDate = @date1 AND CS.EndDate = @date2

		--  SHIRISH 07/22/2019 DIT-18425
		SET @timestamp2 = GETDATE()
		SET @logCount = @logCount + 1
		INSERT INTO IDB_Billing.dbo.BillingSteps 
		VALUES (@today, @ProcessID, @logCount, 'After Commission Summary',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
		SET @timestamp1 = @timestamp2
		
		/************* TEST QUERIES ****************************/
        
		IF @Debug = 1 
		BEGIN
			
			--SELECT '#ClearingSchedule', * FROM #ClearingSchedule
			--SELECT '#ChargeType', * FROM #ChargeType
			--Select '#PeriodIDs', * From IDB_Billing.dbo.wPeriodIDs where PROCESS_ID = @ProcessID
			Select '#ActiveBilling', * From IDB_Billing.dbo.wActiveBilling where PROCESS_ID = @ProcessID 
			Select '#ActiveBranch', * From IDB_Billing.dbo.wActiveBranch where PROCESS_ID = @ProcessID
			Select '#BillingSchedule', * From IDB_Billing.dbo.wBillingSchedule where PROCESS_ID = @ProcessID order BY BILLING_CODE, INSTRUMENT_TYPE, INSTRUMENT
			--Select '#ActiveBillingCodes', * From IDB_Billing.dbo.wActiveBillingCodes where PROCESS_ID = @ProcessID
			--SELECT '#ActiveBillingCodesWithPeriodIDs', * FROM #ActiveBillingCodesWithPeriodIDs
			--SELECT 'wDateRanges', * From IDB_Billing.dbo.wDateRanges WHERE PROCESS_ID = @ProcessID
			--SELECT '#TraceEligibleProducts', * FROM #TraceEligibleProducts
			Select 'wDeals_AllProd', TradeDate, DealCommission, * from IDB_Billing.dbo.wDeals_AllProd where PROCESS_ID = @ProcessID ORDER BY DEAL_NEGOTIATION_ID
			SELECT '#DBS_Staging', * --, wRank = RANK() OVER(PARTITION BY CASE WHEN ProductGroup = 'NAVX' THEN 'X' ELSE BILLING_TYPE END, Deal_Id ORDER BY Weight DESC) 
			FROM IDB_BIlling.dbo.wDBS_Staging WHERE PROCESS_ID = @ProcessID --AND deal_id IN ('5284695264722946460') 
			ORDER BY DEAL_ID
			--SELECT '#DBS_Rank_Staging', * FROM #DBS_Rank_Staging --WHERE Deal_Id IN (5221081919986339989,5221081919986339050,5221081919986338378,5221081919986338315)
			--SELECT 'wOTRDealsToExclude', * FROM IDB_Billing.dbo.wOTRDealsToExclude WHERE PROCESS_ID = @ProcessID --AND Dealer = @dlr AND IsActiveStream = @IsActiveStream
			--SELECT '#OverrideSchedules', * FROM #OverrideSchedules WHERE Dealer = 'SCOTIA'
			--SELECT '#OverrideSchedules', * FROM #g6750 WHERE Dealer IN ('All','XRT')
			--SELECT '#OverrideSchedules', * FROM #OverrideSchedules WHERE ProcessId = @ProcessID
			--SELECT '#PoolTakerTradesVsOperators', * FROM IDB_BIlling.dbo.wPoolTakerTradesVsOperators WHERE PROCESS_ID = @ProcessId --ORDER BY Deal_Negotiation_Id
			--SELECT 'OverrideScheduleDealBillingSchedule', * FROM IDB_Billing.dbo.wOverrideScheduleDealBillingSchedule WHERE PROCESS_ID = @ProcessId --ORDER BY Deal_Negotiation_Id
			--SELECT '#DBS_Rank_Staging', * FROM #DBS_Rank_Staging
			Select 'wDealBillingSchedule', TradeDate, DEAL_REPO_START_DATE, * From IDB_Billing.dbo.wDealBillingSchedule where PROCESS_ID = @ProcessID ORDER BY DEAL_NEGOTIATION_ID
			--AND ProductGroup = 'OTR' --AND Deal_Negotiation_Id IN ('4A8202B90000047A')
			--ORDER BY BILLING_CODE, ProductGroup, DEAL_NEGOTIATION_ID
			--SELECT '#NAVXCommAdjustment', * FROM #NAVXCommAdjustment ORDER BY DEAL_NEGOTIATION_ID
			Select '#DBS_AllProd', * From IDB_Billing.dbo.wDBS_AllProd WHERE PROCESS_ID = @ProcessID ORDER BY BILLING_CODE, DEAL_NEGOTIATION_ID
			SELECT '#DBS_AllProd_PreCap_RT', * FROM #DBS_AllProd_PreCap_RT --WHERE ProductGroup = 'OTR' --WHERE BILLING_CODE IN ('AT2')
			--Select '#DealBillingSchedule_Staging', * From #DealBillingSchedule_Staging  ORDER BY Dealer, DEAL_NEGOTIATION_ID --WHERE ProductGroup = 'OTR' --WHERE   DEAL_NEGOTIATION_ID IN ('D20190813SB00000006') 
			--SELECT '#DBS_AllProd_Staging', * FROM #DBS_AllProd_Staging  ORDER BY BILLING_CODE
			--SELECT '#RepoTicketFeesByInvoice', * FROM #RepoTicketFeesByInvoice
			--SELECT '#Commissions_AllProd-----', * FROM #Commissions_AllProd AS CAP
			--SELECT '#NAVXCommAdjustment', * FROM #NAVXCommAdjustment
			--SELECT '#ClearingTrades', * FROM #ClearingTrades --WHERE BILLING_CODE = '2SIGM99'
			--SELECT '#CTForNAVX', * FROM #CTForNAVXCommission
			--SELECT '#ClearingSchedule', * FROM #ClearingSchedule
			SELECT '#SNF', * FROM #SNF  --WHERE BILLING_CODE = '2SIGM99'
			--SELECT '#CATFeeSchedule', * FROM #CATFeeSchedule
			-- SELECT '#CAT', * FROM #CATFees
			--SELECT '#InvoiceHistory', * FROM #InvoiceHistory AS IH ORDER BY IH.billing_code
			--SELECT '#ChargeType', * FROM #ChargeType AS CT
			--SELECT '#InvoiceInventory_Staging_AllProd', * FROM #InvoiceInventory_Staging_AllProd AS IISAP --WHERE Produ
			--Select '#InvoiceInventory_Staging', * From #InvoiceInventory_Staging --where ProductGroup = 'OTR' --Where Billing_code = 'DB1'
			--SELECT '#DBS_AllProd_PreCap_RT-----', * FROM #DBS_AllProd_PreCap_RT AS DAPPCR WHERE DAPPCR.BILLING_CODE = 'AT2' ORDER BY DAPPCR.Source, DAPPCR.RowNum

			--SELECT '#ABS_Cap_Floor_AllProd-----', * FROM #ABS_Cap_Floor_AllProd AS ACFAP --WHERE ACFAP.BILLING_CODE = 'DWC_GS1'
			SELECT '#Commissions_AllProd', * FROM #Commissions_AllProd AS CAP --WHERE CAP.BILLING_CODE = 'AT2'

			--SELECT '#CommissionOwed', * From #CommissionOwed AS CO --WHERE CO.BILLING_CODE IN ('AT2','BARCAP8')
			--SELECT '#CommissionCollected', * From #CommissionCollected AS CC --WHERE BILLING_CODE IN ('AT2','BARCAP8')
			--SELECT '#CommissionWithoutCap',* FROM #CommissionWithoutCap
			--SELECT '#NetCommission', * FROM #NetCommission AS NC --WHERE BILLING_CODE IN ('AT2','BARCAP8')
			--SELECT '#VolumeBasedCommission', * FROM #VolumeBasedCommission
			--Select '#InvoiceInventory', * From #InvoiceInventory --WHERE ProductGroup = 'OTR' ORDER BY InvNum
			--SELECT '#InvDtailsGrp',* FROM #InvoiceDetailsGrp AS IDG
			--SELECT '#InvoiceDetails', * From #InvoiceDetails --AS ID WHERE productgroup = 'OTR' ORDER BY ID.InvNum
			SELECT '#DealsCommission', * FROM IDB_Billing.dbo.wDeal_Commission WHERE PROCESS_ID = @ProcessID ORDER by Deal_Negotiation_Id
			--SELECT '#ClearingTrades', * FROM #ClearingTrades CT
			--SELECT '#CTForNAVXCommission', * FROM #CTForNAVXCommission ORDER BY DEAL_ID
			--SELECT 'wDealDetails', * FROM IDB_Billing.dbo.wDealDetails WHERE PROCESS_ID = @ProcessID 
			--SELECT '#ActiveBillingCodesWithPeriodIDs', * FROM #ActiveBillingCodesWithPeriodIDs
			--SELECT '#CommissionWithoutCap', * FROM #CommissionWithoutCap AS CWC
			--SELECT '#CommissionSummary', * FROM #CommissionSummary AS CS
			--SELECT '#TraceDeals', * FROM #TraceDeals AS TD
			--SELECT '#TraceDealsForInvoice', * FROM #TraceDealsForInvoice AS TDFI
			--SELECT '#InvoiceHistory-----',* FROM #InvoiceHistory ORDER BY InvNum


			--SELECT '#Commissions_AllProd_Tier----', * FROM #Commissions_AllProd_Tier

			--SELECT	'#WDetails after change',
			--		InvNum = DD.InvNum,
			--		InvDbId = DD.InvDbId,
			--		PeriodId = DD.PeriodId,
			--		ProductGroup = DD.ProductGroup,
			--		Source = DD.Source,
			--		Deal_Id = DD.DEAL_ID

			--FROM	IDB_Billing.dbo.wDealDetails DD (NOLOCK) -- SHIRISH 11/6/2014 -- updating query to use permanent table
			----JOIN	IDB_Billing.dbo.wDealBillingSchedule AS WDBS ON WDBS.DEAL_ID = DD.Deal_Id AND WDBS.InvNum = DD.InvNum AND WDBS.InvDbId = DD.InvDbId
			--WHERE	DD.PROCESS_ID = @ProcessID
			--AND		DD.DEal_Id IN ('19EVSB00000020')
			----AND		(CASE WHEN DD.ProductGroup <> 'TRSY' THEN 1 WHEN DD.ProductGroup = 'TRSY' AND WDBS.Leg = 'PRI' THEN 1 ELSE 0 END) = 1	


			--SELECT '#InvoiceDetails (test Queries)', * FROM #InvoiceDetails ORDER BY InvNum, productgroup

			--SELECT
			--	'Invoice Deal Details',
			--	InvNum = DD.InvNum,
			--	InvDbId = DD.InvDbId,
			--	DetailNum = ID.DetailNum,
			--	PeriodId = DD.PeriodId,
			--	ProductGroup = DD.ProductGroup,
			--	Source = DD.Source,
			--	Deal_Id = DD.DEAL_ID

			--FROM	IDB_Billing.dbo.wDealDetails DD (NOLOCK) -- SHIRISH 11/6/2014 -- updating query to use permanent table
		 
			--JOIN	#InvoiceDetails ID ON DD.InvNum = ID.InvNum 
			--				AND DD.InvDbId = ID.InvDbId 
			--				AND DD.ChargeId = ID.ChargeId
			--				AND DD.ProductGroup = ID.ProductGroup 
			--				AND DD.Source = ID.Source
		
			---- SHIRISH 07/02/2019: DIT-11311 For AGCY we are getting deals we get synthetic record and for Deal Details we are getting legs. Because of this Deal_ID from wDealDetails
			---- will not match Deal_Id from wDealBillingSchedule.  Making below join a left join so we get AGCY records in InvoiceDealDetails table.
			----LEFT JOIN	IDB_Billing.dbo.wDealBillingSchedule AS WDBS ON WDBS.DEAL_ID = DD.Deal_Id AND DD.PROCESS_ID = WDBS.PROCESS_ID
			--WHERE	DD.PROCESS_ID = @ProcessID
			---- SHIRISH 07/02/2019: DIT-11311 Added below condition to make sure wDealBillingSchedule join is ignored for AGCY trades
			----AND		(DD.ProductGroup = 'AGCY' OR (DD.ProductGroup <> 'AGCY' AND WDBS.DEAL_ID IS NOT NULL))
			--ORDER BY DD.ProductGroup, DD.Deal_Id

		END
		
		/********************** Insert data into billing tables ************************/

		BEGIN TRANSACTION

		IF (@ReportMode = 2)
		BEGIN --ReportMode WILL BE 2 WHEN CALLED FROM FRONT END. RETURN RESULT SET

			SELECT
				StartDate,
				EndDate,
				ProductGroup,
				Billing_Code,
				Source,
				TotalVolume = SUM(TotalVolume),
				CommissionWithoutCap = SUM(CommissionWithoutCap),
				CommissionWithoutCapOverride = SUM(CommissionWithoutCapOverride),
				CommissionOwed = SUM(CommissionOwed),
				CommissionCollected = SUM(CommissionCollected),
				NetCommission = SUM(NetCommission),
				SubmissionAndNettingFees = SUM(SubmissionAndNettingFees),
				FailCharge = SUM(FailCharge),
				TraceSubmissionFees = SUM(TraceSubmissionFees),		/*NILESH 02-23-2010: Trace Submission Fees */
				TracePassThruFees = SUM(TracePassThruFees),	/*NILESH 05-04-2011: Trace Pass-Through Fees */
				TraceDeals = SUM(TraceDeals),		/* NILESH 06-07-2011: Trace Deals Count */
				CommissionCredits = SUM(CommissionCredits),
				CommissionCreditAmount = SUM(CommissionCreditAmount),
				IsActiveStream,
				Security_Currency,
				RepoTicketFees = SUM(RepoTicketFees),
				CATProspectiveFee = SUM(CATProspectiveFee),
				CATHistoricFee = SUM(CATHistoricFee)
				
			FROM	#CommissionSummary

			--Only get the billing codes that have charges. 
			--If there are no deals for the dealer during the billing period there won't be any Commission, Pass Thru Charge or Fail Charge
			--don't return the row
			WHERE	ProductGroup IS NOT NULL

			GROUP BY StartDate, EndDate, ProductGroup, Billing_Code, Source, IsActiveStream, Security_Currency

			/*
			-- NILESH 08/23/2016 
			-- Following UNION Block is to capture any outages reported
			-- on the daily reconciliation report due to the WI trades.
			-- The outage amount is then used as an adjustment during the
			-- EOD commission reports
			-- We are currently using the DWN1 and internal billing code.
			*/
			UNION

			SELECT
				StartDate = @date1,
				EndDate = @date2,
				ProductGroup = ProductGroup,
				Billing_Code = Billing_Code,
				Source = Source,
				TotalVolume = 0,
				CommissionWithoutCap = SUM(OutageAmount),
				CommissionWithoutCapOverride = 0,
				CommissionOwed = SUM(OutageAmount),
				CommissionCollected = SUM(OutageAmount),
				NetCommission = 0, 
				SubmissionAndNettingFees = 0,
				FailCharge = 0,
				TraceSubmissionFees = 0,
				TracePassThruFees = 0,
				TraceDeals = 0,	
				CommissionCredits = 0,
				CommissionCreditAmount = 0,
				IsActiveStream = NULL,
				Security_Currency = NULL,
				RepoTicketFees = 0,
				CATProspectiveFee = 0,
				CATHistoricFee = 0

			FROM	IDB_Billing.dbo.IDB_DailyRecon_Outages
			WHERE	TradeDate between @date1 AND @date2
			AND @Owner = 'US'
			AND	((@ProductGroup IS NULL) OR (@ProductGroup = ProductGroup))
			GROUP BY Billing_Code, Source,ProductGroup
			
		END

		ELSE IF (@ReportMode = 1)
		BEGIN --ReportMode WILL BE 1 WHEN CALLED FOR COMMISSION SUMMARY . INSERT IN COMMISSION SUMMARY
			
			/* #################### SUMMARY TYPE <> "D" BLOCK ######################## */
			IF (@SummaryType <> 'D')
			BEGIN
				
				--Delete existing 
				DELETE	CS
				FROM IDB_Billing.dbo.CommissionSummary CS (NOLOCK)
				/* NILESH 02/28/2013: Added an additional join to ensure that we only delete rows for the billable products */
				JOIN @BILLINGPRODUCTS BP ON CS.ProductGroup = BP.PRODUCT_GROUP

				WHERE	SummaryType = @SummaryType
				AND	((@BillingCode IS NULL) OR (@BillingCode = CS.Billing_Code))
				AND	((@productgroup IS NULL) OR (@productgroup = CS.ProductGroup))

				SELECT @Error = @@ERROR

				IF (@Error <> 0)
				BEGIN
					SET @Msg = 'Error deleting from CommissionSummary'
					GOTO ROLLBACKANDEXIT
				END

				--INSERT SUMMARY
				INSERT INTO IDB_Billing.dbo.CommissionSummary
				(
					SummaryType,
					StartDate,
					EndDate,
					ProductGroup,
					Billing_Code,
					Source,
					TotalVolume,
					CommissionWithoutCap,
					CommissionWithoutCapOverride,
					CommissionOwed,
					CommissionCollected,
					NetCommission,
					SubmissionAndNettingFees,
					FailCharge,
					TraceSubmissionFees,		/*NILESH 02-23-2010: Trace Submission Fees */
					TracePassThruFees,		/*NILESH 05-04-2011: Trace Pass Through Fees */
					TraceDeals,		/* NILESH 06-07-2011: Trace Deals Count */
					/* Next two columns added as part of the Commission Credits to be provided to AMSWP trades */
					CommissionCredits,
					CommissionCreditAmount,
					CreateDate,
					IsActiveStream,
					Security_Currency,  -- IDBBC-310
					RepoTicketFees	-- IDBBC-310
					,CATProspectiveFee -- IDBBC-332
					,CATHistoricalFee -- IDBBC-332
				)

				SELECT
					SummaryType = @SummaryType,
					StartDate,
					EndDate,
					ProductGroup,
					Billing_Code,
					Source,
					TotalVolume = SUM(TotalVolume),
					CommissionWithoutCap = SUM(CommissionWithoutCap),
					CommissionWithoutCapOverride = SUM(CommissionWithoutCapOverride),
					CommissionOwed = SUM(CommissionOwed),
					CommissionCollected = SUM(CommissionCollected),
					--Net commission doesn't make sense for previous business day. Because cap is at a monthly level and 
					--calculation of net commission includes cap levels, displaying net commission for PBD is actually misleading
					NetCommission = CASE WHEN @SummaryType = 'PBD' THEN 0 ELSE SUM(NetCommission) END, 
					SubmissionAndNettingFees = SUM(SubmissionAndNettingFees),
					FailCharge = SUM(FailCharge),
					TraceSubmissionFees = SUM(TraceSubmissionFees),		/*NILESH 02-23-2010: Trace Submission Fees */
					TracePassThruFees = SUM(TracePassThruFees),		/*NILESH 05-04-2011: Trace Pass-Through Fees */
					TraceDeals = SUM(TraceDeals),		/* NILESH 06-07-2011: Trace Deals Count */
					CommissionCredits = SUM(CommissionCredits),
					CommissionCreditAmount = SUM(CommissionCreditAmount),
					CreateDate = GETDATE(),
					IsActiveStream,
					Security_Currency,		-- IDBBC-310
					RepoTicketFees = SUM(RepoTicketFees)		-- IDBBC-310
					,CATProspectiveFee = SUM(CATProspectiveFee)
					,CATHistoricFee = SUM(CATHistoricFee)

				FROM	#CommissionSummary

				--Only get the billing codes that have charges. 
				--If there are no deals for the dealer during the billing period there won't be any Commission, Pass Thru Charge or Fail Charge
				--don't return the row
				WHERE	ProductGroup IS NOT NULL

				GROUP BY StartDate, EndDate, ProductGroup, Billing_Code, Source, IsActiveStream, Security_Currency

				/*
				-- NILESH 08/23/2016 
				-- Following UNION Block is to capture any outages reported
				-- on the daily reconciliation report due to the WI trades.
				-- The outage amount is then used as an adjustment during the
				-- EOD commission reports
				-- We are currently using the DWN1 and internal billing code.
				*/
				UNION

				SELECT
					SummaryType = @SummaryType,
					StartDate = @date1,
					EndDate = @date2,
					ProductGroup = ProductGroup,
					Billing_Code = Billing_Code,
					Source = Source,
					TotalVolume = 0,
					CommissionWithoutCap = SUM(OutageAmount),
					CommissionWithoutCapOverride = 0,
					CommissionOwed = SUM(OutageAmount),
					CommissionCollected = SUM(OutageAmount),
					NetCommission = 0, 
					SubmissionAndNettingFees = 0,
					FailCharge = 0,
					TraceSubmissionFees = 0,
					TracePassThruFees = 0,
					TraceDeals = 0,	
					CommissionCredits = 0,
					CommissionCreditAmount = 0,
					CreateDate = GETDATE(),
					IsActiveStream = NULL,
					Security_Curency = NULL,
					RepoTicketFees = 0
					,CATProspectiveFee = 0
					,CATHistoricFee = 0

				FROM	IDB_Billing.dbo.IDB_DailyRecon_Outages
				WHERE	TradeDate between @date1 AND @date2
				AND		@Owner = 'US'
				AND		((@ProductGroup IS NULL) OR (@ProductGroup = ProductGroup))
				GROUP BY Billing_Code, Source,ProductGroup

				SELECT @RowsAffected = @@ROWCOUNT, @Error = @@ERROR

				IF (@Error <> 0)
				BEGIN
					SET @Msg = 'Error inserting CommissionSummary'
					GOTO ROLLBACKANDEXIT
				END

				/*
				-- NILESH 03/23/2017
				-- Following functionality to extract the REPO MTD commission 
				-- and inserted in IDBRepoCommissionSummary. The table will be
				-- replicated back to the Distribution server for the HF team
				-- to consume for their Commission related requirement.
				*/
				IF @SummaryType = 'MTD' AND @ReportMode = 1 AND (@ProductGroup IS NULL OR @ProductGroup = 'USREPO')
				BEGIN

					/* Delete the existing data */
					DELETE FROM IDB_Billing.dbo.IDBRepoCommissionSummary

					INSERT INTO IDB_Billing.dbo.IDBRepoCommissionSummary
					(StartDate,EndDate,Billing_Code,DealerCode,DealerName,TotalVolume,CommissionOwed,CommissionCap,CreateDate)
					SELECT	StartDate,
							EndDate,
							Billing_Code = CS.Billing_Code,
							DealerCode = B.COMPANY_ACRONYM,
							DealerName = B.COMPANY_NAME,
							TotalVolume = SUM(TotalVolume),
							CommissionOwed = SUM(CommissionWithoutCap),
							CommissionCap = SUM(CASE WHEN ROUND(CommissionWithoutCap,0) <> ROUND(CommissionOwed,0) THEN CommissionOwed ELSE 0 END),
							CreateDate = GETDATE()
					FROM #CommissionSummary CS
					JOIN IDB_Billing.dbo.wActiveBilling B ON CS.Billing_Code = B.Billing_Code
					WHERE CS.ProductGroup = 'USREPO'
					AND B.Process_Id = @ProcessId
					--AND		(CASE WHEN B.BILLING_CODE IN ('WFS1','UBS1','RBSGCM1','BOA1') AND B.SourceDB = 'FALCONDB' THEN 0 ELSE 1 END) = 1

					GROUP BY StartDate,EndDate,CS.Billing_Code,B.COMPANY_ACRONYM,B.COMPANY_NAME
				END

				/*
				* IDBBC-168
				* Calculate Port Charges and Rebate for current month
				*/
				IF @SummaryType = 'MTD' AND @ReportMode = 1 AND (@ProductGroup IS NULL OR @ProductGroup = 'OTR') AND (@BillingCode IS NULL OR RIGHT(@BillingCode, 2) IN ('_C', '_M')) 
				BEGIN

						CREATE TABLE #PortDataSummary
						(
							[Billing_Code] [VARCHAR](16) NULL,
							[OverrideFirmCode] [VARCHAR](30) NULL,
							[FirmCode] [varchar] (30) NULL,
							[FirmName] [varchar] (255) NULL,
							[AccountNo] [VARCHAR](15) NULL,
							[PortName] [VARCHAR] (20) NULL,
							[ChargeId] [INT] NULL,
							[Charge_Type] [varchar] (100) NULL,
							[Protocol_Version] [varchar] (10) NULL,
							[Environment] [varchar] (30) NULL,
							[PortCount] INT NULL,
							[GUIPort] [BIT] NULL,
							[IsMDPort] [BIT] NULL,
							[Charges] DECIMAL(12,2) NULL,
							[Rebate] DECIMAL(12,2) NULL,
							[Tax] DECIMAL(12,2) NULL
						)

						DECLARE @FirmCode VARCHAR(30), @MDPort INT

						IF @BillingCode IS NULL
						BEGIN
							SELECT @FirmCode = NULL, @MDPort = NULL
						END
						ELSE
						BEGIN
							
							SELECT	@FirmCode = COMPANY_ACRONYM
							FROM	IDB_Billing.dbo.wActiveBilling (NOLOCK)
							WHERE	PROCESS_ID = @ProcessID

							IF RIGHT(@BillingCode, 1) = 'M'
								SET @MDPort = 1
							ELSE
								SET @MDPort = 0

						END

						IF @Debug = 1
							SELECT @PeriodStartDate AS StartPeriod, @PeriodEndDate AS EndPeriod, @FirmCode AS FirmCode, @MDPort AS MDPort

						INSERT INTO #PortDataSummary
						EXEC IDB_Billing.dbo.GetMarketPortData @PeriodStartDate, @PeriodEndDate, @FirmCode, @MDPort

						--IF @Debug = 1
						--	SELECT '#PortDataSummary', * FROM #PortDataSummary

						-- Delete existing records
						DELETE	PDS
						FROM	IDB_Billing.dbo.DWC_PortDataSummary PDS (NOLOCK)
						JOIN	IDB_BIlling.dbo.wActiveBilling AB (NOLOCK) ON AB.COMPANY_ACRONYM = ISNULL(PDS.OverrideFirmCode,PDS.FirmCode)
																		  AND AB.BILLING_CODE = PDS.Billing_Code -- IDBBC-179
																		  AND (CASE RIGHT(AB.BILLING_CODE, 2) WHEN '_C' THEN 0 ELSE 1 END) = PDS.IsMDPort
						WHERE	PDS.EffectiveStartDate = @PeriodStartDate
						AND		PDS.EffectiveEndDate = @PeriodEndDate
						AND		RIGHT(AB.BILLING_CODE, 2) IN ('_C', '_M') -- only take connectivity and market data billing codes
						AND		AB.Process_Id = @ProcessID

						-- Insert new charges into PortDataSummary
						INSERT INTO IDB_Billing.dbo.DWC_PortDataSummary
						(
								[Billing_Code],
								[OverrideFirmCode],
								[FirmCode],
								[FirmName],
								[AccountNo],
								[PortName],
								[ChargeId],
								[Charge_Type],
								[Protocol_Version],
								[Environment],
								[PortCount],
								[GUIPort],
								[IsMDPort],
								[Charges],
								[Rebate],
								[Tax],
								[EffectiveStartDate],
								[EffectiveEndDate]
						)
						SELECT 	AB.Billing_Code,
								[OverrideFirmCode],
								[FirmCode],
								[FirmName],
								[AccountNo],
								[PortName],
								[ChargeId],
								[Charge_Type],
								[Protocol_Version],
								[Environment],
								[PortCount],
								[GUIPort],
								[IsMDPort],
								[Charges],
								[Rebate],
								[Tax],
								@PeriodStartDate,
								@PeriodEndDate
						FROM	#PortDataSummary PDS
						JOIN	IDB_BIlling.dbo.wActiveBilling AB (NOLOCK) ON AB.COMPANY_ACRONYM = ISNULL(PDS.OverrideFirmCode,PDS.FirmCode)
																		  AND AB.BILLING_CODE = PDS.Billing_Code -- IDBBC-179
																		  AND (CASE RIGHT(AB.BILLING_CODE, 2) WHEN '_C' THEN 0 ELSE 1 END) = PDS.IsMDPort
						WHERE	AB.Process_Id = @ProcessID
						AND		RIGHT(AB.BILLING_CODE, 2) IN ('_C', '_M') -- only take connectivity and market data billing codes

						DROP TABLE #PortDataSummary
				END

				--  SHIRISH 07/22/2019 DIT-18425
				SET @timestamp2 = GETDATE()
				SET @logCount = @logCount + 1
				INSERT INTO IDB_Billing.dbo.BillingSteps 
				VALUES (@today, @ProcessID, @logCount, 'After Insert <> D Records',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
				SET @timestamp1 = @timestamp2

			END --End of IF (@SummaryType <> 'D')

			/* #################### END SUMMARY TYPE <> "D" BLOCK ######################## */

			--INSERT FOR SUMMARY TYPE "D"
			/* The daily department commission summary report needs commission without cap and floor for different 
				time periods such as prior month and prior MTD.  
				
				Daily commission without cap and floor number will be persisted with SummaryType = 'D'

				Numbers such as CommissionWithoutCap, TotalVolume, CommissionCollected etc do not take cap and floor
				into account and can be persisted daily. All other numbers should be 0 
			*/
			
			/* #################### SUMMARY TYPE "D" BLOCK ######################## */
			IF (@SummaryType = 'D')
			BEGIN

				-- GDB-316 SHIRISH 10/10/2019:
				-- We need to find out if a 2YR WI instrument was traded today.  If yes then we have to see if there is an outage in commission 
				-- and that outage need to be added to IDB_DailyRecon_Outages to make sure this outage does not show up in all dept report
				IF (@ProductGroup IS NULL OR @ProductGroup = 'TRSY') AND (@Owner = 'US')
				BEGIN
					-- find out if 2Yr WI instrument was traded today
					DECLARE @2YrWIInstr VARCHAR(255) = NULL, @NoofTrades INT = 0

					SELECT	@2YrWIInstr =  IFD.INSTRUMENT, 
							@NoofTrades = COUNT(*) 
					FROM	IDB_Reporting.dbo.IDB_FALCON_DEALS AS IFD (NOLOCK)
					JOIN	Instrument.dbo.Security_Master AS SM (NOLOCK) ON SM.std_sec_id = IFD.INSTRUMENT
					JOIN	Instrument.dbo.Security_Type AS ST (NOLOCK) ON ST.sec_type_id = SM.sec_type_id AND ST.product_grp = 'TRSY'
					WHERE	IFD.DEAL_TRADE_DATE = @date2
					AND		IFD.DEAL_STATUS <> 'CANCELLED'
					AND		IFD.DEAL_LAST_VERSION = 1
					AND		IFD.DEAL_WI = 1
					AND		IFD.ProductGroup = 'TRSY'
					AND		SM.issued_as = 24
					AND		SM.sec_id = (SELECT MAX(SM2.Sec_id)
										 FROM	Instrument.dbo.Security_Master AS SM2 (NOLOCK)
										 WHERE	SM2.std_sec_id = SM.std_sec_id AND SM2.sec_type_id = SM.sec_type_id)
					GROUP BY 
							IFD.INSTRUMENT

					IF @Debug = 1
						SELECT 'Instrument', @2YrWIInstr, 'Count', @NoofTrades

					IF (@2YrWIInstr IS NOT NULL AND @NoofTrades > 0)
					BEGIN
						CREATE TABLE #WI2YrOutage
						(
							Commission FLOAT,
                            ExtraCommission FLOAT,
							CommissionAdjustment FLOAT,
							TotalCommission FLOAT,
							Net FLOAT,
							SecurityId VARCHAR(255),
							Diff FLOAT
						)

						-- Find if there is any outage for the instrument
						INSERT INTO #WI2YrOutage
						EXEC IDB_Reporting.dbo.GetDailyCommissionReconciliation_OFTR @date2, @date2, @2YrWIInstr, @SummaryOnly = 1

						IF @Debug=1
							SELECT '#WI2YrOutage', * FROM #WI2YrOutage

						DECLARE @Diff FLOAT
						SELECT @DIff = Diff FROM #WI2YrOutage AS WYO

						-- Add ourage to IDB_DailyRecon_Outages
						IF NOT EXISTS (SELECT  1 FROM IDB_Billing.dbo.IDB_DailyRecon_Outages AS IDRO (NOLOCK) WHERE IDRO.TradeDate = @date2 AND IDRO.Billing_Code = 'DWN1' AND IDRO.Source = 'RC')
							INSERT INTO IDB_Billing.dbo.IDB_DailyRecon_Outages
							(
							    TradeDate,
							    ProductGroup,
							    Billing_Code,
							    Source,
							    OutageAmount,
							    CreateDate,
							    --rowguid,
							    CreatedBy
							)
							VALUES
							(   @date2, -- TradeDate - datetime
							    'TRSY',        -- ProductGroup - varchar(8)
							    'DWN1',        -- Billing_Code - varchar(16)
							    'RC',        -- Source - varchar(8)
							    @Diff * -1,       -- OutageAmount - float
							    GETDATE(), -- CreateDate - datetime
							    --NEWID(),      -- rowguid - uniqueidentifier
							    'BlgUser'         -- CreatedBy - varchar(100)
							)
						ELSE
							UPDATE	IDB_Billing.dbo.IDB_DailyRecon_Outages
							SET		OutageAmount = @Diff * -1,
									CreateDate = GETDATE()
							WHERE	TradeDate = @date2
							AND		Billing_Code = 'DWN1'
							AND		Source = 'RC'

						DROP TABLE #WI2YrOutage --GDB-316 Moved drop table inside loop so code does not fail when there are no WI instruments trades

					END

					--  SHIRISH 07/22/2019 DIT-18425
					SET @timestamp2 = GETDATE()
					SET @logCount = @logCount + 1
					INSERT INTO IDB_Billing.dbo.BillingSteps 
					VALUES (@today, @ProcessID, @logCount, 'After 2YRWIInstr Processing',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
					SET @timestamp1 = @timestamp2

				END	

				/* Insert the Commission at deal level. This was done for the Firm Activity Report for IOS.
				The commission for this product does not get calculated on daily basis and gets charged once
				on the invoice. So there was a desire to see this number on the report.
				*/
				/* IDBBC-178
				Deleting records in batches of 5000 to reduce locking
				*/
				IF EXISTS (SELECT 1 FROM @BILLINGPRODUCTS WHERE PRODUCT_GROUP <> 'USREPO')
				BEGIN
					DECLARE @BatchSize INT = 5000; -- Adjust batch size as needed

					WHILE (1=1)
					BEGIN

						DELETE	TOP (@BatchSize) DC
						FROM	IDB_Reporting.dbo.Deal_Commission DC (NOLOCK)
						JOIN	IDB_Billing.dbo.wActiveBilling AB (NOLOCK) ON AB.PROCESS_ID = @ProcessID -- SHIRISH 11/4/2014 -- updating query to use permanent table
																		   AND AB.BILLING_CODE = DC.Billing_Code -- Switching from Dealer code to billing code as billing code is available in Deal_Commission
						JOIN	@BILLINGPRODUCTS BP ON DC.ProductGroup = BP.PRODUCT_GROUP
						WHERE	DC.Deal_TradeDate BETWEEN @date1 AND @date2
						AND		DC.ProductGroup <> 'USREPO'

						IF @@ROWCOUNT = 0 BREAK; -- Exit when no more rows to delete

					END

				END

				SET @timestamp2 = GETDATE()
				SET @logCount = @logCount + 1
				INSERT INTO IDB_Billing.dbo.BillingSteps 
				VALUES (@today, @ProcessID, @logCount, 'DELETE Deal_Commission for Non USREPO Products',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
				SET @timestamp1 = @timestamp2

				IF EXISTS (SELECT 1 FROM @BILLINGPRODUCTS WHERE PRODUCT_GROUP = 'USREPO')
					DELETE	DC
					FROM	IDB_Reporting.dbo.Deal_Commission DC (NOLOCK)
					JOIN	IDB_Billing.dbo.wActiveBilling AB (NOLOCK) ON AB.PROCESS_ID = @ProcessID -- SHIRISH 11/4/2014 -- updating query to use permanent table
																	   AND AB.BILLING_CODE = DC.Billing_Code -- Switching from Dealer code to billing code as billing code is available in Deal_Commission
					JOIN	@BILLINGPRODUCTS BP ON DC.ProductGroup = BP.PRODUCT_GROUP
					WHERE	DC.DEAL_REPO_START_DATE BETWEEN @date1 AND @date2
					AND		DC.ProductGroup = 'USREPO';

				SET @timestamp2 = GETDATE()
				SET @logCount = @logCount + 1
				INSERT INTO IDB_Billing.dbo.BillingSteps 
				VALUES (@today, @ProcessID, @logCount, 'DELETE Deal_Commission for USREPO',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
				SET @timestamp1 = @timestamp2

				--DELETE	DC
				--FROM	IDB_Reporting.dbo.Deal_Commission DC (NOLOCK)
				--JOIN	IDB_Billing.dbo.wActiveBilling AB (NOLOCK) ON DC.Dealer = AB.Company_Acronym AND AB.PROCESS_ID = @ProcessID -- SHIRISH 11/4/2014 -- updating query to use permanent table
				--JOIN	@BILLINGPRODUCTS BP ON DC.ProductGroup = BP.PRODUCT_GROUP
				--LEFT JOIN IDB_Reporting.dbo.fnREPODateSwitch(@date1,@date2) RDS ON RDS.ProductGroup = 'USREPO'
				
				--WHERE	((@productgroup IS NULL) OR (@productgroup = DC.ProductGroup))
				--/* SHIRISH 04/16/2018: Updating below condition to use correct date field and date range to match schedule based on REPO date switch from TradeDate to repo start date */
				----AND		((DC.ProductGroup <> 'USREPO' AND DC.Deal_TradeDate between @date1 AND @date2) OR (DC.ProductGroup = 'USREPO' AND DEAL_REPO_START_DATE BETWEEN @Date1 AND @Date2))
				--AND		(CASE 
				--			---- For EUREPO use Deal_Repo_Start_Date
				--			--WHEN DC.ProductGroup = 'EUREPO' AND DC.DEAL_REPO_START_DATE BETWEEN @date1 AND @date1 THEN 1
				--			-- For products other than USREPO use tradedate
				--			WHEN DC.ProductGroup <> 'USREPO' AND DC.Deal_TradeDate between @date1 AND @date2 
				--			THEN 1
				--			-- For USREPO when date is before Switch date (3/1/2018) then use tradedate
				--			WHEN DC.ProductGroup = 'USREPO' 
				--				 AND RDS.FieldName = 'TradeDate'
				--				 AND DC.Deal_TradeDate BETWEEN RDS.StartDate AND RDS.EndDate
				--				 --AND DC.Deal_TradeDate between @date1 AND @date2
				--			THEN 1
				--			-- For USREPO when date is on or after switch date (3/1/2018) then use repo start date
				--			WHEN DC.ProductGroup = 'USREPO'
				--				 AND RDS.FieldName = 'StartDate'
				--				 AND DC.DEAL_REPO_START_DATE BETWEEN RDS.StartDate AND RDS.EndDate
				--				 AND DC.Deal_TradeDate >= RDS.SwitchDate
				--			THEN 1
				--			ELSE 0
				--		 END) = 1

				/* Can't us following join as it will ignore the cancels and leave them */
		--		JOIN	#DealsCommission C 
		--			ON DC.Deal_Negotiation_Id = C.Deal_Negotiation_Id
		--			AND DC.Dealer = C.Dealer
		--			AND DC.Deal_User_Id = C.Deal_User_Id
		--			AND DC.Deal_Way = C.Deal_Way
		--			AND DC.Source = C.Source
		--			AND DC.ProductGroup = C.ProductGroup
				
				/* MATCH */
				IF (@ProductGroup IS NULL) OR (@ProductGroup = 'MATCH')
					DELETE	DC
					FROM	IDB_Reporting.dbo.Deal_Commission DC (NOLOCK)
					JOIN	IDB_Billing.dbo.wActiveBilling AB (NOLOCK) ON AB.PROCESS_ID = @ProcessID -- SHIRISH 11/4/2014 -- updating query to use permanent table
																	   AND AB.BILLING_CODE = DC.Billing_Code  -- Switching from Dealer code to billing code as billing code is available in Deal_Commission
					WHERE	DC.ProductGroup = 'MATCH'
					AND DC.Deal_TradeDate between @date1 AND @date2 
					
				SELECT @Error = @@ERROR

				IF (@Error <> 0)
				BEGIN
					SET @Msg = 'Error deleting daily rows from Deal_Commission'
					GOTO ROLLBACKANDEXIT
				END

				SET @timestamp2 = GETDATE()
				SET @logCount = @logCount + 1
				INSERT INTO IDB_Billing.dbo.BillingSteps 
				VALUES (@today, @ProcessID, @logCount, 'DELETE Deal_Commission for MATCH',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
				SET @timestamp1 = @timestamp2


				INSERT INTO IDB_Reporting.dbo.Deal_Commission
				(
					Deal_Negotiation_Id,
					Dealer,
					Deal_User_Id,
					Deal_Way,
					Source,
					ProductGroup,
					Deal_Principal,
					Deal_AccInt,
					Deal_Proceeds,
					Deal_O_Factor,
					Deal_O_Finint,
					Deal_TradeDate,
					CHARGE_RATE_AGGRESSIVE,
					CHARGE_RATE_PASSIVE,
					DealCommission_PreCap,
					DealCommission_PreCap_Override,
					CreatedOn,
					BrokerId,
					SecurityName,
					Deal_Quantity,
					TradeType,
					Deal_Price,
					Deal_Aggressive_Passive,
					Deal_Final_Commission,
					Days_To_Maturity,
					Deal_Id,
					Deal_Extra_Commission,
					DEAL_CHARGED_QTY,
					DEAL_RISK,
					DEAL_CURRENCY,
					DEAL_DISCOUNT_RATE,
					TIER_CHARGE_RATE_AGGRESSIVE,
					TIER_CHARGE_RATE_PASSIVE,
					DEAL_SEF_TRADE,
					DEAL_TIM_QTY,
					DEAL_FIRST_AGGRESSED,
					IsActiveStream,
					DEAL_FIRST_AGGRESSOR,
					OVERWRITE, -- SHIRISH 06/01/2017
					DEAL_DATE, -- SHIRISH 08/22/2017
					DEAL_STLMT_DATE, -- SHIRISH 08/22/2017
					DEAL_REPO_START_DATE, -- SHIRISH 08/22/2017
					DEAL_REPO_END_DATE, -- SHIRISH 08/22/2017
					Billing_Code, -- SHIRISH 03/07/2018
					Operator, -- SHIRISH 04/23/2018 - DWAS 2.5
					ZeroBrokerCommission,	/*DIT-11179*/
					/* DIT-11179 : Commission for fixed fee client */
					FF_DealCommission_PreCap,
					FF_DealCommission_PreCap_Override,
					FF_Charge_Rate_Aggressive,
					UseGap, -- IDBBC-132
					DEAL_GAP_DV01, -- IDBBC-132
					RepoRecordType, -- IDBBC-234
					IsHedgedTrade, -- GDB-2898
					Security_Currency, -- IDBBC-310
					RepoTicketFees	-- IDBBC-310
				)

				SELECT
					Deal_Negotiation_Id,
					D.Dealer,
					Deal_User_Id,
					Deal_Way,
					Source = CASE WHEN Source = 'DIRECT' THEN 'DRECT' ELSE Source END,
					D.ProductGroup,
					Deal_Principal,
					Deal_AccInt,
					Deal_Proceeds,
					Deal_O_Factor,
					Deal_O_FinInt,
					Deal_TradeDate,
					CHARGE_RATE_AGGRESSIVE,
					CHARGE_RATE_PASSIVE,
					DealCommission,
					DealCommission_Override,
					GetDate(),
					BrokerId,
					SecurityName,
					Quantity,
					TradeType,
					Deal_Price,
					Deal_Aggressive_Passive,
					Deal_Final_Commission,
					Deal_Days_To_Maturity,
					Deal_Id,
					Deal_Extra_Commission,
					DEAL_CHARGED_QTY,
					DEAL_RISK,
					CURRENCY_CODE,
					DEAL_DISCOUNT_RATE,
					TIER_CHARGE_RATE_AGGRESSIVE,
					TIER_CHARGE_RATE_PASSIVE,
					DEAL_SEF_TRADE,
					DEAL_TIM_QTY,
					DEAL_FIRST_AGGRESSED,
					IsActiveStream,
					DEAL_FIRST_AGGRESSOR,
					OVERWRITE, -- SHIRISH 06/01/2017
					DEAL_DATE, -- SHIRISH 08/22/2017
					DEAL_STLMT_DATE, -- SHIRISH 08/22/2017
					DEAL_REPO_START_DATE, -- SHIRISH 08/22/2017
					DEAL_REPO_END_DATE, -- SHIRISH 08/22/2017
					D.Billing_Code, -- SHIRISH 03/07/2018
					Operator, -- SHIRISH 04/23/2018 - DWAS 2.5
					ZeroBrokerCommission,	/* DIT-11179 */
					/* DIT-11179 */
					FF_DealCommission,
					FF_DealCommission_Override,
					IFFCR.FixedFeeCommRate,
					D.UseGap, -- IDBBC-132
					D.DEAL_GAP_DV01, -- IDBBC-132
					D.RepoRecordType, -- IDBBC-234
					D.IsHedgedTrade, -- GDB-2898
					D.Security_Currency, -- IDBBC-310
					D.RepoTicketFees	-- IDBBC-310

				FROM	IDB_Billing.dbo.wDeal_Commission AS D
				/* NS:10/12/2011-Generalized for additional products. */
				INNER JOIN IDB_CodeBase.dbo.fnProductType() fp ON D.ProductGroup = fp.Product
				/* NS:10/12/2011-Generalized for additional products. */	
				--WHERE	ProductGroup = 'IOS'
				/* DIT-11179 */
				LEFT JOIN IDB_Reporting.dbo.IDB_FixedFee_CommRate AS IFFCR	ON (D.Deal_TradeDate BETWEEN IFFCR.PeriodStartDate AND IFFCR.PeriodEndDate)
																			AND IFFCR.ProductGroup = CASE WHEN D.Dealer = 'GS' AND D.ProductGroup = 'USFRN' THEN 'TRSY' ELSE D.ProductGroup END -- IDBBC-92 For GS USRN is part of TRSY fixed fees.  We need to apply TRSY fixed fee rate to USFRN
																			AND IFFCR.Dealer = D.Dealer 
																			AND IFFCR.Billing_Code = D.Billing_Code
				WHERE	D.PROCESS_ID = @ProcessID
				AND		fp.NeedCommissionAtDealLevel = 'Y' 
				
				SELECT @RowsAffected = @@ROWCOUNT, @Error = @@ERROR

				IF (@Error <> 0)
				BEGIN
					SET @Msg = 'Error inserting Deal_Commission'
					GOTO ROLLBACKANDEXIT
				END

				--  SHIRISH 07/22/2019 DIT-18425
				SET @timestamp2 = GETDATE()
				SET @logCount = @logCount + 1
				INSERT INTO IDB_Billing.dbo.BillingSteps 
				VALUES (@today, @ProcessID, @logCount, 'After Insert Deal_Commission',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
				SET @timestamp1 = @timestamp2

				/* ******************** END Deal Level Commission Block ********************************* */

				/* Commission Summary Block */
				--Delete Existing
				DELETE	CS
				FROM	IDB_Billing.dbo.CommissionSummary CS (NOLOCK)
				JOIN	#CommissionSummary C ON CONVERT(varchar(8), CS.StartDate, 112) = CONVERT(varchar(8), C.StartDate, 112)
							AND CONVERT(varchar(8), CS.EndDate, 112) = CONVERT(varchar(8), C.EndDate, 112)
							AND C.ProductGroup IS NOT NULL
				/* NILESH 02/28/2013: Added an additional join to ensure that we only delete rows for the billable products */
				JOIN @BILLINGPRODUCTS BP ON CS.ProductGroup = BP.PRODUCT_GROUP
				
				WHERE	CS.SummaryType = 'D'
				AND	((@BillingCode IS NULL) OR (@BillingCode = CS.Billing_Code))
				AND	((@productgroup IS NULL) OR (@productgroup= CS.ProductGroup))
				
				SELECT @Error = @@ERROR

				IF (@Error <> 0)
				BEGIN
					SET @Msg = 'Error deleting daily rows from CommissionSummary'
					GOTO ROLLBACKANDEXIT
				END

				INSERT INTO IDB_Billing.dbo.CommissionSummary
				(
					SummaryType,
					StartDate,
					EndDate,
					ProductGroup,
					Billing_Code,
					Source,
					TotalVolume,
					CommissionWithoutCap,
					CommissionWithoutCapOverride,
					CommissionOwed,
					CommissionCollected,
					NetCommission,
					SubmissionAndNettingFees,
					FailCharge,
					TraceSubmissionFees,
					TracePassThruFees,
					TraceDeals,		
					/* Next two columns added as part of the Commission Credits to be provided to AMSWP trades */
					CommissionCredits,
					CommissionCreditAmount,
					CreateDate,
					IsActiveStream,
					Security_Currency,
					RepoTicketFees,
					CATProspectiveFee,
					CATHistoricalFee
				)

				SELECT
					SummaryType = @SummaryType,
					StartDate,
					EndDate,
					ProductGroup,
					Billing_Code,
					Source,
					TotalVolume = SUM(TotalVolume),
					CommissionWithoutCap = SUM(CommissionWithoutCap),
					CommissionWithoutCapOverride = SUM(CommissionWithoutCapOverride),
					CommissionOwed = 0, --This should be 0. See comments above
					CommissionCollected = SUM(CommissionCollected),
					NetCommission = 0, --This should be 0. See comments above 
					SubmissionAndNettingFees = SUM(SubmissionAndNettingFees),
					FailCharge = SUM(FailCharge),
					TraceSubmissionFees = SUM(TraceSubmissionFees),
					TracePassThruFees = SUM(TracePassThruFees),	
					TraceDeals = SUM(TraceDeals),		/* NILESH 06-07-2011: Trace Deals Count */	
					CommissionCredits = SUM(CommissionCredits),
					CommissionCreditAmount = SUM(CommissionCreditAmount),
					CreateDate = GETDATE(),
					IsActiveStream,
					Security_Currency,
					RepoTicketFees = SUM(RepoTicketFees),
					CATProspectiveFee = SUM(CATProspectiveFee),
					CATHistoricFee = SUM(CATHistoricFee)

				FROM	#CommissionSummary

				--Only get the billing codes that have charges. 
				--If there are no deals for the dealer during the billing period there won't be any Commission, Pass Thru Charge or Fail Charge
				--don't return the row
				WHERE	ProductGroup IS NOT NULL
				AND	@SummaryType = 'D'

				GROUP BY StartDate, EndDate, ProductGroup, Billing_Code, Source, IsActiveStream, Security_Currency

				/*
				-- NILESH 08/23/2016 
				-- Following UNION Block is to capture any outages reported
				-- on the daily reconciliation report due to the WI trades.
				-- The outage amount is then used as an adjustment during the
				-- EOD commission reports
				-- We are currently using the DWN1 and internal billing code.
				*/
				UNION

				SELECT
					SummaryType = @SummaryType,
					StartDate = @date1,
					EndDate = @date2,
					ProductGroup = ProductGroup,
					Billing_Code = Billing_Code,
					Source = Source,
					TotalVolume = 0,
					CommissionWithoutCap = SUM(OutageAmount),
					CommissionWithoutCapOverride = 0,
					CommissionOwed = SUM(OutageAmount),
					CommissionCollected = SUM(OutageAmount),
					NetCommission = 0, 
					SubmissionAndNettingFees = 0,
					FailCharge = 0,
					TraceSubmissionFees = 0,
					TracePassThruFees = 0,
					TraceDeals = 0,	
					CommissionCredits = 0,
					CommissionCreditAmount = 0,
					CreateDate = GETDATE(),
					IsActiveStream = NULL,
					Security_Currency = NULL,
					RepoTicketFees = 0,
					CATProspectiveFee = 0,
					CATHistoricFee = 0

				FROM	IDB_Billing.dbo.IDB_DailyRecon_Outages
				WHERE	TradeDate between @date1 AND @date2
				AND	((@ProductGroup IS NULL) OR (@ProductGroup = ProductGroup))
				AND	@SummaryType = 'D'
				AND @Owner = 'US'
				GROUP BY Billing_Code, Source,ProductGroup

				SELECT @RowsAffected = @@ROWCOUNT, @Error = @@ERROR

				IF (@Error <> 0)
				BEGIN
					SET @Msg = 'Error inserting CommissionSummary'
					GOTO ROLLBACKANDEXIT
				END

				--  SHIRISH 07/22/2019 DIT-18425
				SET @timestamp2 = GETDATE()
				SET @logCount = @logCount + 1
				INSERT INTO IDB_Billing.dbo.BillingSteps 
				VALUES (@today, @ProcessID, @logCount, 'After Insert D Records',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
				SET @timestamp1 = @timestamp2

			END
			/* End Commission Summary Block */
			
			/* #################### END SUMMARY TYPE "D" BLOCK ######################## */
			
			/* #################### SUMMARY TYPE "MTD" TIERED BILLING BLOCK ######################## */
			/* TIERED BILLING BLOCK -- For recreating the data for the month*/
			IF (@SummaryType = 'MTD' AND @UseTieredBilling = 1)
			BEGIN


				/* Insert the Commission at deal level. This was done for the Firm Activity Report for IOS.
				The commission for this product does not get calculated on daily basis and gets charged once
				on the invoice. So there was a desire to see this number on the report.
				*/
				--DELETE	DC
				--FROM	IDB_Reporting.dbo.Deal_Commission DC (NOLOCK)
				--JOIN	IDB_CodeBase.dbo.fnProductType() fp ON DC.ProductGroup = fp.Product
				--JOIN	IDB_Billing.dbo.wActiveBilling AB (NOLOCK) ON DC.Dealer = AB.Company_Acronym AND AB.PROCESS_ID = @ProcessID -- SHIRISH 11/4/2014 -- updating query to use permanent table
				--JOIN	@BILLINGPRODUCTS BP ON DC.ProductGroup = BP.PRODUCT_GROUP
				--LEFT JOIN IDB_Reporting.dbo.fnREPODateSwitch(@date1,@date2) RDS ON RDS.ProductGroup = 'USREPO'
				
				--WHERE	fp.ProductInvoiceUsesTieredBilling = 'Y'
				--AND		((@productgroup IS NULL) OR (@productgroup = DC.ProductGroup))
				--/* SHIRISH 04/16/2018: Updating below condition to use correct date field and date range to match schedule based on REPO date switch from TradeDate to repo start date */
				----AND		((DC.ProductGroup <> 'USREPO' AND DC.Deal_TradeDate between @date1 AND @date2) OR (DC.ProductGroup = 'USREPO' AND DEAL_REPO_START_DATE BETWEEN @Date1 AND @Date2))
				--AND		(CASE 
				--			---- For EUREPO use Deal_Repo_Start_Date
				--			--WHEN DC.ProductGroup = 'EUREPO' AND DC.DEAL_REPO_START_DATE between @date1 AND @date2 THEN 1
				--			-- For products other than USREPO use tradedate
				--			WHEN DC.ProductGroup <> 'USREPO' AND DC.Deal_TradeDate between @date1 AND @date2 
				--			THEN 1
				--			-- For USREPO when date is before Switch date (3/1/2018) then use tradedate
				--			WHEN DC.ProductGroup = 'USREPO' 
				--				 AND RDS.FieldName = 'TradeDate'
				--				 AND DC.Deal_TradeDate BETWEEN RDS.StartDate AND RDS.EndDate
				--				 --AND DC.Deal_TradeDate between @date1 AND @date2
				--			THEN 1
				--			-- For USREPO when date is on or after switch date (3/1/2018) then use repo start date
				--			WHEN DC.ProductGroup = 'USREPO'
				--				 AND RDS.FieldName = 'StartDate'
				--				 AND DC.DEAL_REPO_START_DATE BETWEEN RDS.StartDate AND RDS.EndDate
				--				 AND DC.Deal_TradeDate >= RDS.SwitchDate
				--			THEN 1
				--			ELSE 0
				--		 END) = 1

				DELETE	DC
				FROM	IDB_Reporting.dbo.Deal_Commission DC (NOLOCK)
				JOIN	@BILLINGPRODUCTS BP ON DC.ProductGroup = BP.PRODUCT_GROUP
				JOIN	IDB_CodeBase.dbo.fnProductType() fp ON BP.Product_Group = fp.Product
				JOIN	IDB_Billing.dbo.wActiveBilling AB (NOLOCK) ON DC.Dealer = AB.Company_Acronym AND AB.PROCESS_ID = @ProcessID -- SHIRISH 11/4/2014 -- updating query to use permanent table
				
				WHERE	DC.Deal_TradeDate between @date1 AND @date2
				AND		DC.ProductGroup NOT IN ('USREPO')
				AND		fp.ProductInvoiceUsesTieredBilling = 'Y'
				AND		((@productgroup IS NULL) OR (@productgroup = DC.ProductGroup))

				DELETE	DC
				FROM	IDB_Reporting.dbo.Deal_Commission DC (NOLOCK)
				JOIN	@BILLINGPRODUCTS BP ON DC.ProductGroup = BP.PRODUCT_GROUP
				JOIN	IDB_CodeBase.dbo.fnProductType() fp ON BP.Product_Group = fp.Product
				JOIN	IDB_Billing.dbo.wActiveBilling AB (NOLOCK) ON DC.Dealer = AB.Company_Acronym AND AB.PROCESS_ID = @ProcessID -- SHIRISH 11/4/2014 -- updating query to use permanent table
				
				WHERE	DC.DEAL_REPO_START_DATE BETWEEN @date1 AND @date2
				AND		DC.ProductGroup = 'USREPO'
				AND		fp.ProductInvoiceUsesTieredBilling = 'Y'
				AND		((@productgroup IS NULL) OR (@productgroup = DC.ProductGroup))

				SELECT @Error = @@ERROR

				IF (@Error <> 0)
				BEGIN
					SET @Msg = 'Error deleting daily rows from Deal_Commission for Tiered Billing'
					GOTO ROLLBACKANDEXIT
				END

				INSERT INTO IDB_Reporting.dbo.Deal_Commission
				(
					Deal_Negotiation_Id,
					Dealer,
					Deal_User_Id,
					Deal_Way,
					Source,
					ProductGroup,
					Deal_Principal,
					Deal_AccInt,
					Deal_Proceeds,
					Deal_O_Factor,
					Deal_O_Finint,
					Deal_TradeDate,
					CHARGE_RATE_AGGRESSIVE,
					CHARGE_RATE_PASSIVE,
					DealCommission_PreCap,
					DealCommission_PreCap_Override,
					CreatedOn,
					BrokerId,
					SecurityName,
					Deal_Quantity,
					TradeType,
					Deal_Price,
					Deal_Aggressive_Passive,
					Deal_Final_Commission,
					Days_To_Maturity,
					Deal_Id,
					Deal_Extra_Commission,
					DEAL_CHARGED_QTY,
					DEAL_RISK,
					DEAL_CURRENCY,
					DEAL_DISCOUNT_RATE,
					TIER_CHARGE_RATE_AGGRESSIVE,
					TIER_CHARGE_RATE_PASSIVE,
					DEAL_SEF_TRADE,
					DEAL_TIM_QTY,
					DEAL_FIRST_AGGRESSED,
					IsActiveStream,
					DEAL_FIRST_AGGRESSOR,
					OVERWRITE, -- SHIRISH 06/01/2017
					DEAL_DATE, -- SHIRISH 08/22/2017
					DEAL_STLMT_DATE, -- SHIRISH 08/22/2017
					DEAL_REPO_START_DATE, -- SHIRISH 08/22/2017
					DEAL_REPO_END_DATE, -- SHIRISH 08/22/2017
					Billing_Code, -- SHIRISH 03/07/2018
					Operator, -- SHIRISH 04/23/2018 - DWAS 2.5
					ZeroBrokerCommission,	/* DIT-11179 */
					/* DIT-11179 */
					FF_DealCommission_PreCap,
					FF_DealCommission_PreCap_Override,
					FF_Charge_Rate_Aggressive,
					UseGap, -- IDBBC-132
					DEAL_GAP_DV01, -- IDBBC-132
					IsHedgedTrade,  -- GDB-2898
					Security_Currency,
					RepoTicketFees
				)

				SELECT
					Deal_Negotiation_Id,
					D.Dealer,
					Deal_User_Id,
					Deal_Way,
					Source,
					D.ProductGroup,
					Deal_Principal,
					Deal_AccInt,
					Deal_Proceeds,
					Deal_O_Factor,
					Deal_O_FinInt,
					Deal_TradeDate,
					CHARGE_RATE_AGGRESSIVE,
					CHARGE_RATE_PASSIVE,
					DealCommission,
					DealCommission_Override,
					GetDate(),
					BrokerId,
					SecurityName,
					Quantity,
					TradeType,
					Deal_Price,
					Deal_Aggressive_Passive,
					Deal_Final_Commission,
					Deal_Days_To_Maturity,
					Deal_Id,
					Deal_Extra_Commission,
					DEAL_CHARGED_QTY,
					DEAL_RISK,
					CURRENCY_CODE,
					DEAL_DISCOUNT_RATE,
					TIER_CHARGE_RATE_AGGRESSIVE,
					TIER_CHARGE_RATE_PASSIVE,
					DEAL_SEF_TRADE,
					DEAL_TIM_QTY,
					DEAL_FIRST_AGGRESSED,
					IsActiveStream,
					DEAL_FIRST_AGGRESSOR,
					OVERWRITE, -- SHIRISH 06/01/2017
					DEAL_DATE, -- SHIRISH 08/22/2017
					DEAL_STLMT_DATE, -- SHIRISH 08/22/2017
					DEAL_REPO_START_DATE, -- SHIRISH 08/22/2017
					DEAL_REPO_END_DATE, -- SHIRISH 08/22/2017
					D.Billing_Code,
					Operator, -- SHIRISH 04/23/2018 - DWAS 2.5
					ZeroBrokerCommission,	/* DIT-11179 */
					/* DIT-11179 */
					FF_DealCommission,
					FF_DealCommission_Override,
					IFFCR.FixedFeeCommRate,
					D.UseGap, -- IDBBC-132
					D.DEAL_GAP_DV01, -- IDBBC-132
					D.IsHedgedTrade, -- GDB-2898
					D.Security_Currency,
					D.RepoTicketFees

				FROM	IDB_Billing.dbo.wDeal_Commission D
				/* NS:10/12/2011-Generalized for additional products. */
				INNER JOIN IDB_CodeBase.dbo.fnProductType() fp ON D.ProductGroup = fp.Product
				/* NS:10/12/2011-Generalized for additional products. */	
				--WHERE	ProductGroup = 'IOS'
				/* DIT-11179 */
				LEFT JOIN IDB_Reporting.dbo.IDB_FixedFee_CommRate AS IFFCR	ON (D.Deal_TradeDate BETWEEN IFFCR.PeriodStartDate AND IFFCR.PeriodEndDate)
																			AND IFFCR.ProductGroup = CASE WHEN D.Dealer = 'GS' AND D.ProductGroup = 'USFRN' THEN 'TRSY' ELSE D.ProductGroup END -- IDBBC-92 For GS USRN is part of TRSY fixed fees.  We need to apply TRSY fixed fee rate to USFRN
																			AND IFFCR.Dealer = D.Dealer 
																			AND IFFCR.Billing_Code = D.Billing_Code
				WHERE	D.PROCESS_ID = @ProcessID
				AND		fp.NeedCommissionAtDealLevel = 'Y' 
				AND		fp.ProductInvoiceUsesTieredBilling = 'Y'
				
				SELECT @Error = @@ERROR

				IF (@Error <> 0)
				BEGIN
					SET @Msg = 'Error inserting Deal_Commission in Tiered MTD'
					GOTO ROLLBACKANDEXIT
				END

				/* ******************** END Deal Level Commission Block ********************************* */

				--SELECT '#CommissionSummary', * FROM #CommissionSummary Order By StartDate
				
				/* Commission Summary Block */
				--Delete Existing
				DELETE	CS
				FROM	IDB_Billing.dbo.CommissionSummary CS (NOLOCK)
				INNER JOIN IDB_CodeBase.dbo.fnProductType() fp ON ProductGroup = fp.Product
				JOIN	#CommissionSummary_Tier C ON CONVERT(varchar(8), CS.StartDate, 112) = CONVERT(varchar(8), C.StartDate, 112)
							AND CONVERT(varchar(8), CS.EndDate, 112) = CONVERT(varchar(8), C.EndDate, 112)
							AND C.ProductGroup IS NOT NULL
				/* NILESH 02/28/2013: Added an additional join to ensure that we only delete rows for the billable products */
				JOIN @BILLINGPRODUCTS BP ON CS.ProductGroup = BP.PRODUCT_GROUP
				
				WHERE	fp.NeedCommissionAtDealLevel = 'Y' 
				AND			CS.SummaryType = 'D'
				AND			fp.ProductInvoiceUsesTieredBilling = 'Y'
				AND			((@BillingCode IS NULL) OR (@BillingCode = CS.Billing_Code))
				AND			((@productgroup IS NULL) OR (@productgroup= CS.ProductGroup))
				
				SELECT @Error = @@ERROR

				IF (@Error <> 0)
				BEGIN
					SET @Msg = 'Error deleting daily rows from CommissionSummary'
					GOTO ROLLBACKANDEXIT
				END

				INSERT INTO IDB_Billing.dbo.CommissionSummary
				(
					SummaryType,
					StartDate,
					EndDate,
					ProductGroup,
					Billing_Code,
					Source,
					TotalVolume,
					CommissionWithoutCap,
					CommissionWithoutCapOverride,
					CommissionOwed,
					CommissionCollected,
					NetCommission,
					SubmissionAndNettingFees,
					FailCharge,
					TraceSubmissionFees,
					TracePassThruFees,
					TraceDeals,		
					/* Next two columns added as part of the Commission Credits to be provided to AMSWP trades */
					CommissionCredits,
					CommissionCreditAmount,			
					CreateDate
				)

				SELECT
					SummaryType = 'D',
					StartDate,
					EndDate,
					ProductGroup,
					Billing_Code,
					Source,
					TotalVolume = SUM(TotalVolume),
					CommissionWithoutCap = SUM(CommissionWithoutCap),
					CommissionWithoutCapOverride = SUM(CommissionWithoutCapOverride),
					CommissionOwed = 0, --This should be 0. See comments above
					CommissionCollected = SUM(CommissionCollected),
					NetCommission = 0, --This should be 0. See comments above 
					SubmissionAndNettingFees = SUM(SubmissionAndNettingFees),
					FailCharge = SUM(FailCharge),
					TraceSubmissionFees = SUM(TraceSubmissionFees),
					TracePassThruFees = SUM(TracePassThruFees),	
					TraceDeals = SUM(TraceDeals),		/* NILESH 06-07-2011: Trace Deals Count */	
					CommissionCredits = SUM(CommissionCredits),
					CommissionCreditAmount = SUM(CommissionCreditAmount),
					CreateDate = GETDATE()

				FROM	#CommissionSummary_Tier
				INNER JOIN IDB_CodeBase.dbo.fnProductType() fp ON ProductGroup = fp.Product
				--Only get the billing codes that have charges. 
				--If there are no deals for the dealer during the billing period there won't be any Commission, Pass Thru Charge or Fail Charge
				--don't return the row
				WHERE	ProductGroup IS NOT NULL
				AND	fp.NeedCommissionAtDealLevel = 'Y' 
				AND fp.ProductInvoiceUsesTieredBilling = 'Y'
				AND	(@SummaryType = 'MTD' AND @UseTieredBilling = 1)

				GROUP BY StartDate, EndDate, ProductGroup, Billing_Code, Source

				SELECT @RowsAffected = @@ROWCOUNT + @RowsAffected, @Error = @@ERROR

				IF (@Error <> 0)
				BEGIN
					SET @Msg = 'Error inserting CommissionSummary'
					GOTO ROLLBACKANDEXIT
				END
			END
				/* End Commission Summary Block */
			/* #################### END SUMMARY TYPE "MTD" TIERED BILLING BLOCK ######################## */
			/* END TIERED BILLING BLOCK */

			--  SHIRISH 07/22/2019 DIT-18425
			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'After Insert MTD Records',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2
			
		END

		ELSE IF (@ReportMode = 0)
		BEGIN --ReportMode WILL BE 0 WHEN THE PROC IS CALLED TO GENERATE INVOICES

			--GET CURRENT MAX ID 
			SELECT
				@CurrMaxActiveBillingId = MAX(ActiveBillingId)
			FROM	IDB_Billing.dbo.ActiveBilling --DO NOT USE NOLOCK BECAUSE WE DO WANT TO LOCK TO ENSURE WE GENERATE CORRECT IDs

			INSERT INTO IDB_Billing.dbo.ActiveBilling
			(
				ActiveBillingId,
				InvDate,
				Billing_Code,
				Company_Name,
				Company_Acronym,
				Company_Legal_Name,
				Company_Type,
				Company_MBSCCIDS,
				Company_Authorozed,
				Bill_Contact_Id,
				First_Name,
				Middle_Initial,
				Last_Name,
				Phone,
				Fax,
				Email,
				Address_1,
				Address_2,
				City,
				State,
				Zip,
				Country_Code,
				Timestamp,
				Billing_Contact_CreatedBy,
				Currency_Code,
				Purchase_Order,
				Billing_Contact_Owner,
				Delivery_Method_Description,
				Delivery_Method_Acronym,
				Invoice_Cc_Email_1,
				Invoice_CC_Email_2,
				Invoice_CC_Email_3,
				Invoice_CC_Email_4,
				Invoice_CC_Email_5
			)
			SELECT
				ActiveBillingId = ISNULL(@CurrMaxActiveBillingId, 0) + ROW_NUMBER() OVER (ORDER BY AB.BILLING_CODE),
				InvDate = @InvDate,
				Billing_Code = AB.BILLING_CODE,
				Company_Name = AB.COMPANY_NAME,
				Company_Acronym = AB.COMPANY_ACRONYM,
				Company_Legal_Name = AB.COMPANY_LEGAL_NAME,
				Company_Type = AB.COMPANY_TYPE,
				Company_MBSCCIDS = AB.COMPANY_MBSCCIDS,
				Company_Authorozed = AB.COMPANY_AUTHORIZED,
				Bill_Contact_Id = AB.BILL_CONTACT_ID,
				First_Name = AB.BILLING_CONTACT_FIRST_NAME,
				Middle_Initial = AB.BILLING_CONTACT_MIDDLE_INITIAL,
				Last_Name = AB.BILLING_CONTACT_LAST_NAME,
				Phone = AB.BILLING_CONTACT_PHONE,
				Fax = AB.BILLING_CONTACT_FAX,
				Email = AB.BILLING_CONTACT_EMAIL,
				Address_1 = AB.BILLING_CONTACT_ADDRESS_1,
				Address_2 = AB.BILLING_CONTACT_ADDRESS_2,
				City = AB.BILLING_CONTACT_CITY,
				State = AB.BILLING_CONTACT_STATE,
				Zip = AB.BILLING_CONTACT_ZIP,
				Country_Code = AB.BILLING_CONTACT_COUNTRY_CODE,
				Timestamp = AB.BILLING_CONTACT_TIMESTAMP,
				Billing_Contact_CreatedBy = AB.BILLING_CONTACT_WHO,
				Currency_Code = AB.BILLING_CONTACT_CURRENCY_CODE,
				Purchase_Order = AB.BILLING_CONTACT_PURCHASE_ORDER,
				Billing_Contact_Owner = AB.BILLING_CONTACT_OWNER,
				Delivery_Method_Description = AB.DELIVERY_METHOD_DESCRIPTION,
				Delivery_Method_Acronym = AB.DELIVERY_METHOD_ACRONYM,
				Invoice_Cc_Email_1 = AB.INVOICE_CC_EMAIL_1,
				Invoice_CC_Email_2 = AB.INVOICE_CC_EMAIL_2,
				Invoice_CC_Email_3 = AB.INVOICE_CC_EMAIL_3,
				Invoice_CC_Email_4 = AB.INVOICE_CC_EMAIL_4,
				Invoice_CC_Email_5 = AB.INVOICE_CC_EMAIL_5

			FROM	IDB_Billing.dbo.wActiveBilling AB (NOLOCK) -- SHIRISH 11/4/2014 -- updating query to use permanent table	
			WHERE	AB.PROCESS_ID = @ProcessID -- condition to track records in permanent table

			SELECT @Error = @@ERROR

			IF (@Error <> 0)
			BEGIN
				SET @Msg = 'Error inserting Invoice'
				GOTO ROLLBACKANDEXIT
			END


			--GET CURRENT MAX ID 
			SELECT
				@CurrMaxActiveBranchId = MAX(ActiveBranchId)
			FROM	IDB_Billing.dbo.ActiveBranch --DO NOT USE NOLOCK BECAUSE WE DO WANT TO LOCK TO ENSURE WE GENERATE CORRECT IDs

			INSERT INTO IDB_Billing.dbo.ActiveBranch
			(
				ActiveBranchId,
				InvDate,
				Billing_Code,
				Company_Acronym,
				Bill_Contact_Id,
				Branch_Id,
				Branch_Name,
				Branch_Type,
				Branch_Address,
				Branch_City,
				Branch_State,
				Branch_Zip,
				Branch_Country,
				Branch_Phone_Number,
				Branch_IPS,
				Branch_Authorized
			)

			SELECT
				ActiveBranchId = ISNULL(@CurrMaxActiveBranchId, 0) + ROW_NUMBER() OVER (ORDER BY B.BRANCH_ID),
				InvDate = @InvDate,
				Billing_Code = B.BILLING_CODE,
				Company_Acronym = B.COMPANY_ACRONYM,
				Bill_Contact_Id = B.BILL_CONTACT_ID,
				Branch_Id = B.BRANCH_ID,
				Branch_Name = B.BRANCH_NAME,
				Branch_Type = B.BRANCH_TYPE,
				Branch_Address = B.BRANCH_ADDRESS,
				Branch_City = B.BRANCH_CITY,
				Branch_State = B.BRANCH_STATE,
				Branch_Zip = B.BRANCH_ZIP,
				Branch_Country = B.BRANCH_COUNTRY,
				Branch_Phone_Number = B.BRANCH_PHONE_NUMBER,
				Branch_IPS = B.BRANCH_IPS,
				Branch_Authorized = B.BRANCH_AUTHORIZED

			FROM IDB_Billing.dbo.wActiveBranch B (NOLOCK) -- SHIRISH 11/4/2014 -- updating query to use permanent table
			
			WHERE	B.PROCESS_ID = @ProcessID


			SELECT @Error = @@ERROR

			IF (@Error <> 0)
			BEGIN
				SET @Msg = 'Error inserting Invoice'
				GOTO ROLLBACKANDEXIT
			END


			--GET CURRENT MAX ID 
			SELECT
				@CurrMaxActiveBillingScheduleId = MAX(ActiveBillingScheduleId)
			FROM	IDB_Billing.dbo.ActiveBillingSchedule --DO NOT USE NOLOCK BECAUSE WE DO WANT TO LOCK TO ENSURE WE GENERATE CORRECT IDs

			INSERT 	INTO IDB_Billing.dbo.ActiveBillingSchedule
			(
				ActiveBillingScheduleId,
				InvDate,
				Billing_Code,
				PeriodId,
				Bill_Contact_Id,
				Product_Group,
				Instrument_Type,
				INSTRUMENT, -- SM 05/13/2016: Instrument level billing for NAVX
				Billing_Type,
				Charge_Rate_Passive,
				Charge_Rate_Aggressive,
				Charge_Floor,
				Charge_Cap,
				Settle_Rate_Passive,
				Settle_Rate_Aggressive,
				Effective_Date,
				Expiration_Date,
				Overwrite,
				TRD_TYPE , 
				SOURCE, 
				LEG, 
				MTY_START,
				MTY_END,
				BILLING_PLAN_ID,
				--TIER_BILLING_PLAN_ID,
				--TIER_CHARGE_RATE_AGGRESSIVE,
				--TIER_CHARGE_RATE_PASSIVE
				SUB_INSTRUMENT_TYPE	/* DIT-11312 */
			)
			SELECT
				ActiveBillingScheduleId = ISNULL(@CurrMaxActiveBillingScheduleId, 0) + ROW_NUMBER() OVER (ORDER BY BS.PROCESS_ID),
				InvDate = @InvDate,
				Billing_Code = BS.BILLING_CODE,
				PeriodId = P.PeriodId,		
				Bill_Contact_Id = BS.BILL_CONTACT_ID,
				Product_Group = BS.PRODUCT_GROUP,
				Instrument_Type = BS.INSTRUMENT_TYPE,
				INSTRUMENT = BS.INSTRUMENT, -- SM 05/13/2016: Instrument level billing for NAVX
				Billing_Type = BS.BILLING_TYPE,
				Charge_Rate_Passive = BS.CHARGE_RATE_PASSIVE,
				Charge_Rate_Aggressive = BS.CHARGE_RATE_AGGRESSIVE,
				Charge_Floor = BS.CHARGE_FLOOR,
				Charge_Cap = BS.CHARGE_CAP,
				Settle_Rate_Passive = BS.SETTLE_RATE_PASSIVE,
				Settle_Rate_Aggressive = BS.SETTLE_RATE_AGGRESSIVE,
				Effective_Date = BS.EFFECTIVE_DATE,
				Expiration_Date = BS.EXPIRATION_DATE,
				Overwrite = BS.OVERWRITE,
				TRD_TYPE = BS.TRD_TYPE, 
				SOURCE = BS.SOURCE, 
				LEG = BS.LEG, 
				MTY_START = BS.MTY_START,
				MTY_END = BS.MTY_END,
				BILLING_PLAN_ID = BS.BILLING_PLAN_ID,
				--TIER_BILLING_PLAN_ID = BS.TIER_BILLING_PLAN_ID,
				--TIER_CHARGE_RATE_AGGRESSIVE = BS.TIER_CHARGE_RATE_AGGRESSIVE,
				--TIER_CHARGE_RATE_PASSIVE = BS.TIER_CHARGE_RATE_PASSIVE
				SUB_INSTRUMENT_TYPE = BS.SUB_INSTRUMENT_TYPE	/* DIT-11312 */

			FROM IDB_Billing.dbo.wBillingSchedule BS (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table
			--If there are multiple PeriodIDs, same ActiveBilling info will be repeated for each PeriodID
			CROSS JOIN IDB_Billing.dbo.wPeriodIDs P (NOLOCK) -- SHIRISH 11/5/2014 -- updating query to use permanent table

			WHERE BS.PROCESS_ID = @ProcessID -- condition to track records in a permanent table
			AND   P.PROCESS_ID = @ProcessID -- condition to track records in a permanent table

			SELECT @Error = @@ERROR

			IF (@Error <> 0)
			BEGIN
				SET @Msg = 'Error inserting ActiveBillingSchedule'
				GOTO ROLLBACKANDEXIT
			END

			--  SHIRISH 07/22/2019 DIT-18425
			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'After ReportMode=0 ActiveBilling, Branch and Schedule',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2

			INSERT INTO IDB_Billing.dbo.InvoiceHistory
			(
				InvNum,
				InvDbId,
				InvDate, 
				billing_code,
				InvTypeId,
				company_name,
				legal_name,
				first_name,
				last_name,
				middle_initial,
				phone,
				fax,
				address_1,
				address_2,
				city,
				[state],
				zip,
				country_code,
				start_billing_period,
				end_billing_period,
				due_date,
				currency_code,
				[status],
				purchase_order,
				delivery_acronym,
				[owner]
			)
			SELECT
				InvNum,
				InvDbId,
				InvDate, 
				billing_code,
				InvTypeId,
				company_name,
				legal_name,
				first_name,
				last_name,
				middle_initial,
				phone,
				fax,
				address_1,
				address_2,
				city,
				[state],
				zip,
				country_code,
				start_billing_period,
				end_billing_period,
				due_date,
				currency_code,
				[status],
				purchase_order,
				delivery_acronym,
				[owner]

			FROM #InvoiceHistory

			SELECT @Error = @@ERROR

			IF (@Error <> 0)
			BEGIN
				SET @Msg = 'Error inserting InvoiceHistory'
				GOTO ROLLBACKANDEXIT
			END

			/*********************** REBATE CALCULATION ***************************/
			/*  IDBBC-114
			*	For OTR and NAVX rebate calculation we need to handle below scenarios in records stored in RebateAndBillingTracker.  
			*	We will consider -ve amount as rebate and +ve amount as commission.  ApplyToInvoice determines if we apply amount directly to the invoice amount or create a separate line item
			*	Scenarios:
			*	1. OTR rebate that gets added to invoice as a separate line item (ApplyToInvoice = 0,  RebateAmount is -ve)
			*   2. OTR rebate/commission that gets applied to invoice amount (ApplyToInvoice = 1, treat as rebate if RebateAmount is -ve and Commission if RebateAmount is +ve)
			*	3. NAVX rebate that gets applied to invoice amount (ApplyToInvoice = 1 and RebateAmount is -ve)
			*/

			
			/**************** OTR ****************/
			/*  SM - 2015-04-22 - UPDATE OTR CommissionOwed to account for one time Rebate */
			 
			
			INSERT INTO #REBATE
			SELECT	InvNum = II.InvNum,
					InvDbId = II.InvDbId,
					InvInvTypeId = II.InvInvTypeId,
					Billing_Code = IH.Billing_Code,
					ProductGroup = II.ProductGroup,
					AvailableRebate = ABS(R.RebateAmount),
					Commission = II.ItemAmount,
					CommissionOwed = CASE WHEN (CAST(II.ItemAmount AS DECIMAL(16,2)) - ABS(R.RebateAmount)) < 0.0 THEN 0.0 ELSE CAST(II.ItemAmount AS DECIMAL(16,2)) - ABS(R.RebateAmount) END,
					CommissionCollected = ISNULL(IICC.ItemAmount,0),
					RemainingRebate = (CASE WHEN (ABS(R.RebateAmount) - CAST(II.ItemAmount AS DECIMAL(16,2))) < 0.0 THEN 0.0 ELSE ABS(R.RebateAmount) - CAST(II.ItemAmount AS DECIMAL(16,2)) END) * -1, -- IDBBC-114 Remaining rebate is stored as -ve amount
					StartBillingPeriod = IH.start_billing_period, -- IDBBC-75 new column for start of current billing period
					EndBillingPeriod = IH.End_Billing_Period,
					InvDate = IH.InvDate,
					Who = @User
							
			FROM	#InvoiceInventory II
			JOIN	IDB_Billing.dbo.InvoiceInventoryType IIT (NOLOCK) ON II.InvInvTypeId = IIT.InvInvTypeId
			JOIN	#InvoiceHistory IH ON II.InvNum = IH.InvNum AND II.InvDbId = IH.InvDbId
			JOIN	IDB_Billing.dbo.RebateAndBillingTracker R (NOLOCK) ON II.ProductGroup = R.ProductGroup AND IH.billing_code = R.Billing_Code
			-- Get Commission Collected
			LEFT JOIN #InvoiceInventory IICC ON IICC.InvNum = II.InvNum AND IICC.InvDbId = II.InvDbId AND IICC.ProductGroup = II.ProductGroup AND IICC.InvInvTypeId = 16
			JOIN	IDB_Billing.dbo.InvoiceInventoryType IITCC (NOLOCK) ON IITCC.InvoiceInventoryType = IITCC.InvoiceInventoryType AND IITCC.Code = 'CommissionCollected'
			
			WHERE	IH.End_Billing_Period BETWEEN R.EffectiveStartDate AND R.EffectiveEndDate
			AND		R.RebateAmount < 0.0 -- IDBBC-114 we only need to handle rebates in this section.  -ve RebateAmount indicates rebate.
			AND		IIT.Code = 'CommissionOwed'
			AND		R.ProductGroup = 'OTR'
			AND		R.ApplyToInvoice = 1 -- IDBBC-7 Apply rebate directly to the invoice commission only when ApplyToInvoice is set to 1


			/****************** NAVX *******************/
			/* SM - 2015-12-14 - Calculate NAVX rebate because of rate adjustment and apply existing rebate to commission owed */
			/* IDBBC-75 
			 * Updating below section so it can also be used by OTR
			 * Use commission owed record for NAVX as we are applying available rebate from previous period to current period's commission owed amount. Remaining rebate calculated is stored with Effective start date of next billing period.
			 * For OTR we are using Rebate record as this is only applicable to the current billing period and rebate is displayed on invoice as a separate line item.  And this calculated rebate needs to
			 * stored in RebateAndBillingTracker so that amount can be used in total revenue calculation
			 *
			 * IDBBC-114 Updating below section to only handle NAVX rebates.  OTR rebates are handled by code above
			 */
			INSERT	INTO #REBATE
			SELECT	InvNum = II.InvNum,
					InvDbId = II.InvDbId,
					InvInvTypeId = II.InvInvTypeId,
					Billing_Code = IH.Billing_Code,
					ProductGroup = II.ProductGroup,
					AvailableRebate = ISNULL(ABS(R.RebateAmount),0),
					Commission = II.ItemAmount,
					CommissionOwed = CASE 
										WHEN II.ItemAmount <= ISNULL(ABS(R.RebateAmount),0) THEN 0 -- 0 if rebate amount is more than adjusted commission
										ELSE CAST(II.ItemAmount AS DECIMAL(16,2)) - ISNULL(ABS(R.RebateAmount),0) -- CommissionOwed = Commission - Rebate
									 END,
					CommissionCollected = 0,
					RemainingRebate = CASE 
										WHEN II.ItemAmountForRebate < 0 THEN ISNULL(ABS(R.RebateAmount),0) + CAST(ABS(II.ItemAmountForRebate) AS DECIMAL(16,2)) -- if adjusted commission is -ve then add that to rebate amount
										WHEN II.ItemAmount < ISNULL(ABS(R.RebateAmount),0) THEN ISNULL(ABS(R.RebateAmount),0) - CAST(II.ItemAmount AS DECIMAL(16,2)) -- reduce remaining rebate if available rebate is more than commission
										ELSE 0 -- commission is more than rebate. set remaining rebate to 0
									  END * -1, -- IDBBC-114 Rebate is stored as a -ve number
					StartBillingPeriod = IH.start_billing_period, -- IDBBC-75
					EndBillingPeriod = IH.End_Billing_Period,
					InvDate = IH.InvDate,
					Who = @User
							
			FROM	#InvoiceInventory II
			JOIN	#InvoiceHistory IH ON II.InvNum = IH.InvNum AND II.InvDbId = IH.InvDbId
			JOIN	IDB_Billing.dbo.InvoiceInventoryType AS IIT (NOLOCK) ON IIT.InvInvTypeId = II.InvInvTypeId
			LEFT JOIN	IDB_Billing.dbo.RebateAndBillingTracker R (NOLOCK)  ON II.ProductGroup = R.ProductGroup AND IH.billing_code = R.Billing_Code
																			AND IH.End_Billing_Period BETWEEN R.EffectiveStartDate AND R.EffectiveEndDate
																			AND R.RebateAmount < 0.0 -- IDBBC-114 Rebate is stored as a -ve number
																			AND R.ApplyToInvoice = 1 -- IDBBC-7 Apply rebate directly to the invoice commission only when ApplyToInvoice is set to 1

			
			WHERE	IIT.Code = 'CommissionOwed' 
			AND		II.ProductGroup = 'NAVX' -- CommissionOwed record for NAVX
			

			-- Update rebate table
			-- IDBBC-114 This is applicable to both OTR and NAVX.  Removing filter for NAVX that was added for IDBBC-75
			UPDATE	RT
			SET		EffectiveEndDate = R.EndBillingPeriod,
					BalanceLastUpdate = GETDATE(),
					BalanceLastUpdateBy = @User
			FROM	IDB_Billing.dbo.RebateAndBillingTracker RT (NOLOCK)
			JOIN	#REBATE R ON R.Billing_Code = RT.Billing_Code 
							     AND R.EndBillingPeriod BETWEEN RT.EffectiveStartDate AND RT.EffectiveEndDate
							     AND R.ProductGroup = RT.ProductGroup 

			-- DELETE any existing rebate records for current Invoice date.
			-- This is added in case billing is run multiple times to avoid creating multiple records in rebate tracker
			DELETE	RT
			FROM	IDB_Billing.dbo.RebateAndBillingTracker RT (NOLOCK)
			JOIN	#REBATE R ON R.Billing_Code = RT.Billing_Code
							  -- IDBBC-75 For NAVX we store remaining rebate with effective start Date of next billing period.
							  --          For OTR we need to store calculated rebate with effective date of current billing period
							  -- IDBBC-114 Updating below condition to check InvDate between Effective start and end dates for both NAVX and OTR
							  AND R.InvDate BETWEEN RT.EffectiveStartDate AND RT.EffectiveEndDate
							  AND R.ProductGroup = RT.ProductGroup

			-- Insert New rebate amounts into rebate tracker
			-- IDBBC-114 This is applicable to both OTR and NAVX.  Removing product specific code
			INSERT INTO IDB_Billing.dbo.RebateAndBillingTracker (Billing_Code, ProductGroup, BalanceLastUpdate, BalanceLastUpdateBy, EffectiveStartDate, EffectiveEndDate, RebateAmount, ApplyToInvoice)
			SELECT	Billing_Code, 
					ProductGroup, 
					GETDATE(), 
					@User, 
					EffectiveStartDate = InvDate,
					EffectiveEndDate = '9999-12-31 00:00:00.000',
					RemainingRebate,
					ApplyToInvoice = 1
			FROM	#REBATE
			-- IDBBC-124 We are taking absolute value of Available rebate but calculated remaining rebate is -ve amount so updating condition below to check if Remaining rebate is not 0
			WHERE	(AvailableRebate > 0 OR RemainingRebate <> 0)

			-- Update InvoiceDetails for OTR Rebate
			-- IDBBC-114 this update is applicable to both OTR and NAVX.  Removing productu specific code
			UPDATE	ID
			SET		ItemAmount = R.CommissionOwed + R.CommissionCollected
			FROM	#InvoiceDetails ID
			JOIN	#REBATE R ON ID.InvNum = R.InvNum
							  AND ID.InvDbId = R.InvDbId
							  AND ID.productgroup = R.ProductGroup
							  AND ID.SalesCode = R.ProductGroup + 'COMM'
			WHERE	ID.ChargeId = 1
			
			
			/*********************** END REBATE CALCULATION ***************************/
			
			/* Shirish 07/23/2019 DIT-11517 Commenting this part for now as currently only JPMS falls into this category and we do not want to show Commission Adjustment on their invoice
			/* SHIRISH 09/06/2016
			 * Manual commission adjustments done through commission adjustment scrren on GDB needs to be added to invoice
			 * SHIRISH 10/24/2016 -
			 * Updating below code to add adjustments for only dealers who do not pay at the time of settlement
			 */

			-- Create temp table to determine dealers who do not pay at the time of settlement
			SELECT	DISTINCT 
					B.COMPANY_ACRONYM,
					B.PRODUCT_GROUP
			INTO	#DealersForCommissionAdjustment
			FROM	IDB_Billing.dbo.wBillingSchedule B (NOLOCK)
			LEFT JOIN IDB_Billing.dbo.wBillingSchedule C (NOLOCK) ON B.BILLING_CODE = C.BILLING_CODE
																  AND B.PRODUCT_GROUP = C.PRODUCT_GROUP
																  AND ((ISNULL(C.SETTLE_RATE_AGGRESSIVE, 0) > 0) OR (ISNULL(C.SETTLE_RATE_PASSIVE,0) > 0))
																  AND (
																		(@date1 BETWEEN CONVERT(Varchar(8), C.EFFECTIVE_DATE, 112) AND CONVERT(Varchar(8), C.EXPIRATION_DATE, 112))
																		OR
																		(@date2 BETWEEN CONVERT(Varchar(8), C.EFFECTIVE_DATE, 112) AND CONVERT(Varchar(8), C.EXPIRATION_DATE, 112))
																	  )
			WHERE C.BILLING_CODE IS NULL

			-- Get commission adjustments
			SELECT	IH.InvNum,
					IH.InvDbId,
					EC.ProductGroup,
					Source = EC.Deal_Source,
					PeriodId = P.PeriodId,
					CommmissionAdjustments = SUM(EC.ExtraCommission + EC.CommissionAdjustment)

			INTO	#CommissionAdjustmentsDueToPriceChange

			FROM	#InvoiceHistory IH (NOLOCK)
			JOIN	IDB_Billing.dbo.wActiveBilling AB (NOLOCK) ON IH.billing_code = AB.BILLING_CODE
			--JOIN	IDB_Reporting.dbo.fnGetBillingDBSource() S ON AB.DBSourceID = S.ID
			JOIN	IDB_Billing.dbo.wActiveBillingCodes ABC (NOLOCK) ON AB.BILLING_CODE = ABC.Billing_Code
			-- SHIRISH 07/11/2017: Adding fnProductType so we pick records from correct source
			JOIN	IDB_Codebase.dbo.fnProductType() PT ON PT.Product = ABC.ProductGroup AND PT.SourceDB = AB.SourceDB
			JOIN	IDB_Billing.dbo.wPeriodIDs P (NOLOCK) ON P.PeriodDate BETWEEN ABC.Start_Billing_Period AND ABC.End_Billing_Period
			JOIN	IDB_Reporting.dbo.ExtraBrokerCommDueToPriceChange EC (NOLOCK) ON EC.Dealer = AB.COMPANY_ACRONYM
																				  AND EC.TradeDate BETWEEN ABC.Start_Billing_Period AND ABC.End_Billing_Period
																				  AND EC.ProductGroup = ABC.ProductGroup
			-- SM 2016/10/24 - New join to restrict update only for dealers who do not pay at the time of settlement
			JOIN	#DealersForCommissionAdjustment DCA (NOLOCK) ON EC.Dealer = DCA.COMPANY_ACRONYM
																 AND EC.ProductGroup = DCA.PRODUCT_GROUP

			WHERE	AB.PROCESS_ID = @ProcessID
			AND		ABC.PROCESS_ID = @ProcessID
			AND		p.PROCESS_ID = @ProcessID
			-- SM 2016/10/19: For merged invoices only use STRADE (DBSourceID = 1) record from ActiveBilling
			--AND		S.DBSource = 'STRADE'

			GROUP BY 
					IH.InvNum,
					IH.InvDbId,
					EC.ProductGroup,
					EC.Deal_Source,
					P.PeriodId

			--SELECT '#CommissionAdjustmentsDueToPriceChange', * FROM #CommissionAdjustmentsDueToPriceChange

			/* SHIRISH 09/06/2016
			 * Update InvoiceDetails with CommissionAdjustments
			 */

			--SELECT '#InvoiceDetails Before', * from #InvoiceDetails

			UPDATE	ID
			SET		ItemAmount = ItemAmount + EC.CommmissionAdjustments
			FROM	#InvoiceDetails ID
			JOIN	#CommissionAdjustmentsDueToPriceChange EC ON ID.InvNum = EC.InvNum
															  AND ID.InvDbId = EC.InvDbId
															  AND ID.productgroup = EC.ProductGroup
															  AND ID.Source = EC.Source
			WHERE	ID.SalesCode = ID.productgroup + 'COMM'
			
			--SELECT '#InvoiceDetails After', * from #InvoiceDetails

			*/

			-- Insert data into permanent table
			INSERT INTO IDB_Billing.dbo.InvoiceDetails
			(
				InvNum,
				InvDbId,
				DetailNum ,
				productgroup,
				ServiceTypeId ,
				ChargeId,
				PeriodId,
				Source,
				Quantity,
				ItemAmount,
				ItemPrice,
				SalesCode,
				ItemDescr,
				RefInvNum,
				RefInvDbId,
				RefDetailNum,
				who,
				created ,
				Rate,
				InvoiceDescription,
				SingleSidedPlatFormVolume, -- IDBBC-7
				[CLOB/DWAS] -- IDBBC-7
			)
			SELECT
				InvNum,
				InvDbId,
				DetailNum,
				productgroup,
				ServiceTypeId ,
				ChargeId,
				PeriodId,
				Source,
				Quantity,
				ItemAmount,
				ItemPrice,
				SalesCode,
				ItemDescr,
				RefInvNum,
				RefInvDbId,
				RefDetailNum,
				who,
				created ,
				Rate,
				InvoiceDescription,
				SingleSidedPlatformVolume, -- IDBBC-7
				[CLOB/DWAS] -- IDBBC-7

			FROM	#InvoiceDetails



			-- IDBBC-168 Insert connectivity charges into #InvoiceDetails
			-- IDBBC-187 Separating Charges and tax records
			;WITH CTE_DWC_CONN_MD
			AS
			(
				-- Get Port Charges by dealer
				SELECT	ih.InvNum,
						ih.InvDbId,
						MDInvoice = PDS.IsMdPort,
						ItemAmount = SUM(PDS.Charges),
						C.ChargeId,
						DetailNum = 1

				FROM	#InvoiceHistory ih
				JOIN	IDB_Billing.dbo.wActiveBilling B (NOLOCK) ON B.PROCESS_ID = @ProcessID AND B.BILLING_CODE = ih.billing_code 
				JOIN	IDB_Billing.dbo.DWC_PortDataSummary PDS (NOLOCK) ON PDS.Billing_Code = B.Billing_Code
																		AND PDS.EffectiveStartDate = ih.start_billing_period
				JOIN	#ChargeType C (NOLOCK) ON (PDS.IsMdPort = 0 AND C.ChargeType = 'Connectivity Charges')
												  OR
												  (PDS.IsMdPort = 1 AND C.ChargeType = 'Market Data Charges')
				WHERE	RIGHT(ih.billing_code, 2) IN ('_C','_M')
				AND		PDS.Charges > 0.0
				GROUP BY 		
						ih.InvNum,
						ih.InvDbId,
						PDS.IsMdPort,
						C.ChargeId


				UNION ALL

				-- Get Sales tax
				SELECT	ih.InvNum,
						ih.InvDbId,
						MDInvoice = PDS.IsMdPort,
						ItemAmount = SUM(PDS.Tax),
						C.ChargeId,
						DetailNum = 2

				FROM	#InvoiceHistory ih
				JOIN	IDB_Billing.dbo.wActiveBilling B (NOLOCK) ON B.PROCESS_ID = @ProcessID AND B.BILLING_CODE = ih.billing_code 
				JOIN	IDB_Billing.dbo.DWC_PortDataSummary PDS (NOLOCK) ON PDS.Billing_Code = B.Billing_Code
																		AND PDS.EffectiveStartDate = ih.start_billing_period
				JOIN	#ChargeType C (NOLOCK) ON C.ChargeType = 'SalesTax'
				WHERE	RIGHT(ih.billing_code, 2) IN ('_C','_M')
				AND		PDS.Charges > 0.0
				AND		PDS.Tax > 0.0
				GROUP BY 		
						ih.InvNum,
						ih.InvDbId,
						PDS.IsMdPort,
						C.ChargeId

				UNION ALL

				-- Get Port Rebates by dealer
				SELECT	ih.InvNum,
						ih.InvDbId,
						MDInvoice = PDS.IsMdPort,
						ItemAmount = SUM(PDS.Rebate),
						C.ChargeId,
						DetailNum = 3

				FROM	#InvoiceHistory ih
				JOIN	IDB_Billing.dbo.wActiveBilling B (NOLOCK) ON B.PROCESS_ID = @ProcessID AND B.BILLING_CODE = ih.billing_code 
				JOIN	IDB_Billing.dbo.DWC_PortDataSummary PDS (NOLOCK) ON PDS.Billing_Code = B.Billing_Code
																		AND PDS.EffectiveStartDate = ih.start_billing_period
				JOIN	#ChargeType C (NOLOCK) ON (PDS.IsMdPort = 0 AND C.ChargeType = 'Connectivity Charges')
												  OR
												  (PDS.IsMdPort = 1 AND C.ChargeType = 'Market Data Charges')
				WHERE	RIGHT(ih.billing_code, 2) IN ('_C','_M')
				AND		PDS.Rebate < 0.0
			
				GROUP BY 		
						ih.InvNum,
						ih.InvDbId,
						PDS.IsMdPort,
						C.ChargeId
			)
			INSERT INTO IDB_Billing.dbo.InvoiceDetails
			(
				InvNum,
				InvDbId,
				DetailNum ,
				productgroup,
				ServiceTypeId ,
				ChargeId,
				PeriodId,
				Source,
				Quantity,
				ItemAmount,
				ItemPrice,
				SalesCode,
				ItemDescr,
				RefInvNum,
				RefInvDbId,
				RefDetailNum,
				who,
				created ,
				Rate,
				InvoiceDescription,
				SingleSidedPlatFormVolume,
				[CLOB/DWAS]
			)
			-- Insert total Connectivity charges to Invoice Details
			SELECT
				CM.InvNum,
				CM.InvDbId,
				CM.DetailNum,
				productgroup = 'OTR',
				ServiceTypeId = 1,
				CM.ChargeId,
				P.PeriodId,
				Source = NULL,
				Quantity = 0,
				ItemAmount = SUM(CM.ItemAmount),
				ItemPrice = NULL,
				IDE.SalesCode,
				IDE.ItemDescr,
				RefInvNum = NULL,
				RefInvDbId = NULL,
				RefDetailNum = NULL,
				who = 'BlgUser',
				created = GETDATE(),
				Rate = NULL,
				IDE.InvoiceDescription,
				SingleSidedPlatformVolume = 0,
				[CLOB/DWAS] = NULL

			FROM	CTE_DWC_CONN_MD CM
			JOIN	IDB_Billing.dbo.wPeriodIDs P (NOLOCK) ON P.PROCESS_ID = @ProcessID
			JOIN	IDB_Billing.dbo.InvoiceDetailsEnrich IDE (NOLOCK) ON IDE.ChargeId = CM.ChargeId AND IDE.productgroup = 'OTR'

			GROUP BY 		
				CM.InvNum,
				CM.InvDbId,
				CM.DetailNum,
				CM.ChargeId,
				P.PeriodId,
				IDE.SalesCode,
				IDE.ItemDescr,
				IDE.InvoiceDescription


			SELECT @Error = @@ERROR

			IF (@Error <> 0)
			BEGIN
				SET @Msg = 'Error inserting InvoiceDetails'
				GOTO ROLLBACKANDEXIT
			END

			/* 10-CENT TOLERANCE FOR DIFFERENCE BETWEEN COMMISSION OWED AND COMMISSION COLLECTED
				On some deals there?s a difference of less than 1 cent between Commission Collected (TW_DEAL.DEAL_COMMISSION) 
				and the Commission Owed. As a result, the invoice shows that the dealer owes 1 cent in commission. 

				Compare Commission Collected & Commission Owed and if the difference is within 10-cents, make 
				Commission Owed same as Commission Collected

				RD: 08/20/2010 - Increased 5-cent tolerance to 10-cent
			*/

			UPDATE	II_CO
			SET		II_CO.ItemAmount = ABS(II_CC.ItemAmount)

			FROM	#InvoiceInventory II_CO
			JOIN	IDB_Billing.dbo.InvoiceInventoryType IIT_CO (NOLOCK) ON IIT_CO.InvInvTypeId = II_CO.InvInvTypeId 
					AND IIT_CO.Code = 'CommissionOwed' AND IIT_CO.Billing_Type IS NULL 
			JOIN	#InvoiceInventory II_CC ON
					II_CO.InvNum = II_CC.InvNum
					AND II_CO.InvDbId = II_CC.InvDbId
					AND II_CO.ProductGroup = II_CC.ProductGroup
					AND II_CO.Source = II_CC.Source
					AND II_CO.periodid = II_CC.periodid
			JOIN	IDB_Billing.dbo.InvoiceInventoryType IIT_CC (NOLOCK) ON IIT_CC.InvInvTypeId = II_CC.InvInvTypeId 
					AND IIT_CC.Code = 'CommissionCollected' AND IIT_CC.Billing_Type IS NULL 

			WHERE	ABS(ABS(II_CO.ItemAmount) - ABS(II_CC.ItemAmount)) <= 0.10
			AND		ABS(ABS(II_CO.ItemAmount) - ABS(II_CC.ItemAmount)) > 0

			
			--SELECT 'Before Rebate Update-->',* FROM #InvoiceInventory
			--WHERE ProductGroup = 'NAVX'
			
			-- Update CommissionOwed after rebate
			-- OTR Rebate
			UPDATE	II
			SET		ItemAmount = R.CommissionOwed
			FROM	#InvoiceInventory II
			JOIN	#REBATE R ON II.InvNum = R.InvNum
							  AND II.InvDbId = R.InvDbId
							  AND II.InvInvTypeId = R.InvInvTypeId
							  AND II.ProductGroup = R.ProductGroup
			
			
			--SELECT 'After Rebate Update-->',* FROM #InvoiceInventory
			--WHERE ProductGroup = 'NAVX'

								   
			--SELECT 'InvoiceInventory-After',* from #InvoiceInventory

			
			/* SHIRISH 09/06/2016: 
			 * Manual commission adjustments done through commission adjustment scrren on GDB needs to be added to invoice
			 */
			--INSERT INTO #InvoiceInventory
			--(
			--	InvNum,
			--	InvDbId,
			--	logon_id,
			--	ProductGroup,
			--	Source,
			--	Billing_Plan_Id,
			--	instrument_type,
			--	ItemAmount,
			--	InvInvTypeId,
			--	who,
			--	created,
			--	periodid,
			--	ChargeId
			--) 
			--SELECT	EC.InvNum,
			--		EC.InvDbId,
			--		logon_id = @User,
			--		EC.ProductGroup,
			--		EC.Source,
			--		Billing_Plan_Id = NULL,
			--		InstrumentType = NULL,
			--		ItemAmount = EC.CommmissionAdjustments,
			--		IIT.InvInvTypeId,
			--		Who = @User,
			--		Created = GETDATE(),
			--		PeriodId = EC.PeriodId,
			--		ChargeID = CT.ChargeId

			--FROM	#CommissionAdjustmentsDueToPriceChange EC
			--JOIN	IDB_Billing.dbo.InvoiceInventoryType IIT (NOLOCK) ON IIT.Code = 'CommissionAdjustment' AND IIT.Billing_Type IS NULL
			--JOIN	#ChargeType CT ON CT.ChargeType = 'Commissions'

			/*  SHIRISH - 04/21/2016
			 *	There could be multiple records after adding DWAS Active_Stream flag to #InvoiceInventory_Staging.
			 *  Need to eliminate multiple records from InvoiceInventory by summing ItemAmount
			 */

			;WITH #InvoiceInventorySUM (InvNum,
										InvDBId,
										logon_id,
										ProductGroup,
										Source,
										Billing_Plan_Id,
										instrument_type,
										ItemAmount,
										InvInvTypeId,
										who,
										created,
										periodid,
										ChargeId)
			AS 
			(
				SELECT	InvNum,
						InvDBId,
						logon_id,
						ProductGroup,
						Source,
						Billing_Plan_Id,
						instrument_type,
						SUM(ItemAmount),
						InvInvTypeId,
						who,
						created,
						periodid,
						ChargeId
				FROM	#InvoiceInventory
				GROUP BY 
						InvNum,
						InvDBId,
						logon_id,
						ProductGroup,
						Source,
						Billing_Plan_Id,
						instrument_type,
						InvInvTypeId,
						who,
						created,
						periodid,
						ChargeId
			)
			INSERT INTO IDB_Billing.dbo.InvoiceInventory
			(
				InvNum,
				InvDbId,
				DetailNum,
				logon_id,
				ProductGroup,
				Source,
				Billing_Plan_Id,
				instrument_type,
				ItemAmount,
				InvInvTypeId,
				who,
				created,
				periodid
			) 
			SELECT	
					II.InvNum,
					II.InvDbId,
					DetailNum = ID.DetailNum,
					II.logon_id,
					II.ProductGroup,
					II.Source,
					II.Billing_Plan_Id,
					II.instrument_type,
					II.ItemAmount,
					II.InvInvTypeId,
					II.who,
					II.created,
					II.periodid
	
			FROM	#InvoiceInventorySUM II

			JOIN	#InvoiceDetails ID ON II.InvNum = ID.InvNum 
						AND II.InvDbId = ID.InvDbId 
						AND II.ChargeId = ID.ChargeId 
						AND II.ProductGroup = ID.ProductGroup 
						AND II.Source = ID.Source


			-- IDBBC-168 Added connectivity charges to invoice inventory table
			;WITH CTE_DWC_CONN_INV
			AS
			(
				-- Get Port Charges
				SELECT	ih.InvNum,
						ih.InvDbId,
						iit.InvInvTypeId,
						ItemAmount = SUM(PDS.Charges),
						DetailNum = 1
				FROM	#InvoiceHistory ih
				JOIN	IDB_Billing.dbo.wActiveBilling B (NOLOCK) ON B.PROCESS_ID = @ProcessID AND B.BILLING_CODE = ih.billing_code 
				JOIN	IDB_Billing.dbo.DWC_PortDataSummary PDS (NOLOCK) ON PDS.Billing_Code = B.Billing_Code
																		AND PDS.EffectiveStartDate = ih.start_billing_period
				JOIN	IDB_Billing.dbo.InvoiceInventoryType iit (NOLOCK) ON iit.Code = PDS.Charge_Type
																		 AND (PDS.Environment IS NULL OR iit.Billing_Type = PDS.Environment)
				WHERE	RIGHT(ih.billing_code, 2) IN ('_C','_M')
				AND		PDS.Charges > 0.0  -- IDBBC-185
				GROUP BY 
						ih.InvNum,
						ih.InvDbId,
						iit.InvInvTypeId

				UNION ALL

				-- Get Port count
				SELECT	ih.InvNum,
						ih.InvDbId,
						iit.InvInvTypeId,
						ItemAmount = COUNT(PDS.PortCount),
						DetailNum = 1
				FROM	#InvoiceHistory ih
				JOIN	IDB_Billing.dbo.wActiveBilling B (NOLOCK) ON B.PROCESS_ID = @ProcessID AND B.BILLING_CODE = ih.billing_code 
				JOIN	IDB_Billing.dbo.DWC_PortDataSummary PDS (NOLOCK) ON PDS.Billing_Code = B.Billing_Code
																		AND PDS.EffectiveStartDate = ih.start_billing_period
				JOIN	IDB_Billing.dbo.InvoiceInventoryType iit (NOLOCK) ON iit.Code = PDS.Charge_Type + ' COUNT'
																		 AND (PDS.Environment IS NULL OR iit.Billing_Type = PDS.Environment)
				WHERE	RIGHT(ih.billing_code, 2) IN ('_C','_M')
				AND 	PDS.Charges > 0.0 -- IDBBC-185
				GROUP BY 
						ih.InvNum,
						ih.InvDbId,
						iit.InvInvTypeId

				-- Get Tax
				UNION ALL

				SELECT	ih.InvNum,
						ih.InvDbId,
						iit.InvInvTypeId,
						ItemAmount = SUM(PDS.Tax),
						DetailNum = 2
				FROM	#InvoiceHistory ih
				JOIN	IDB_Billing.dbo.wActiveBilling B (NOLOCK) ON B.PROCESS_ID = @ProcessID AND B.BILLING_CODE = ih.billing_code 
				JOIN	IDB_Billing.dbo.DWC_PortDataSummary PDS (NOLOCK) ON PDS.Billing_Code = B.Billing_Code
																		AND PDS.EffectiveStartDate = ih.start_billing_period
				JOIN	IDB_Billing.dbo.InvoiceInventoryType iit (NOLOCK) ON iit.Code = PDS.Charge_Type + ' TAX'
																		 AND (PDS.Environment IS NULL OR iit.Billing_Type = PDS.Environment)
				WHERE	RIGHT(ih.billing_code, 2) IN ('_C','_M')
				AND 	PDS.Charges > 0.0 -- IDBBC-185
				AND		PDS.Tax > 0.0 -- IDBBC-187
				GROUP BY 
						ih.InvNum,
						ih.InvDbId,
						iit.InvInvTypeId

				UNION ALL

				-- Get Port Rebate
				SELECT	ih.InvNum,
						ih.InvDbId,
						iit.InvInvTypeId,
						ItemAmount = SUM(PDS.Rebate),
						DetailNum = 3
				FROM	#InvoiceHistory ih
				JOIN	IDB_Billing.dbo.wActiveBilling B (NOLOCK) ON B.PROCESS_ID = @ProcessID AND B.BILLING_CODE = ih.billing_code 
				JOIN	IDB_Billing.dbo.DWC_PortDataSummary PDS (NOLOCK) ON PDS.Billing_Code = B.Billing_Code
																		AND PDS.EffectiveStartDate = ih.start_billing_period
																		AND PDS.Rebate < 0
				JOIN	IDB_Billing.dbo.InvoiceInventoryType iit (NOLOCK) ON iit.Code = PDS.Charge_Type
																		 AND (PDS.Environment IS NULL OR iit.Billing_Type = PDS.Environment)
				WHERE	RIGHT(ih.billing_code, 2) IN ('_C','_M')
				AND		PDS.Rebate < 0.0 -- IDBBC-187
				GROUP BY 
						ih.InvNum,
						ih.InvDbId,
						iit.InvInvTypeId
			)
			INSERT INTO IDB_Billing.dbo.InvoiceInventory
			(
				InvNum,
				InvDbId,
				DetailNum,
				logon_id,
				ProductGroup,
				ItemAmount,
				InvInvTypeId,
				who,
				created,
				periodid
			) 
			SELECT	CTE_DWC_CONN_INV.InvNum,
					CTE_DWC_CONN_INV.InvDbId,
					CTE_DWC_CONN_INV.DetailNum,
					logon_id = 'BlgUser',
					ProductGroup = 'OTR',
					CTE_DWC_CONN_INV.ItemAmount,
					CTE_DWC_CONN_INV.InvInvTypeId,
					who = 'BlgUser',
					created = GETDATE(),
					P.periodid

			FROM	CTE_DWC_CONN_INV
			JOIN	IDB_Billing.dbo.wPeriodIDs P (NOLOCK) ON P.PROCESS_ID = @ProcessID



			SELECT @Error = @@ERROR

			IF (@Error <> 0)
			BEGIN
				SET @Msg = 'Error inserting InvoiceInventory'
				GOTO ROLLBACKANDEXIT
			END

			--/*
			-- * DO NOT MOVE THIS UPDATE TO #InvoiceInventory.  THIS HAS TO BE DONE HERE (Reason Below IDBBC-7)

			-- * SHIRISH 2018/01/23: OTR volume based commission section
			-- * SHIRISH 2019/04/29: IDB-18449 Updating below section to accomodate more than one billing code using volume based commission scheme.
			-- *					   Adding section for HUDSON1
			-- * SHIRISH 2019/10/21: IDBBC-7 Updating below section to use a single call to GetVolumeBasedCommission instead of separate calls for each qualifying billing code
			-- *					This needs to be done here on permanent table and not on #invoiceinventory temp table as in temp table there could be multiple records for CommissionOwed (invinvtypeid = 15)
			-- *					in that case we will end up updating both records and doubling final commission.
			-- */
			
			-- IDBBC-7 Update Invoice Details for Volume based commissions
			-- IDBBC-87 Need to remove commission collected from final invoice amount.  This only needs to happen for InvoiceDetails as InvoiceInventory has a separate line item for CommissionCollected
			UPDATE	ID
			SET		ItemAmount = VBC.TotalComm - ISNULL(CC.CommissionCollected,0),
					SingleSidedPlatformVolume = VBC.PlatformTotal/2,
					[CLOB/DWAS] = VBC.PlatFormVolumeDescription
			FROM	IDB_Billing.dbo.InvoiceDetails AS ID (NOLOCK)
			JOIN	#InvoiceHistory AS IH ON IH.InvNum = ID.InvNum AND IH.InvDbId = ID.InvDbId
			JOIN	#VolumeBasedCommission AS VBC ON VBC.BillingCode = IH.billing_code
												 AND VBC.ProductGroup = ID.productgroup -- IDBBC-67 Invoice can have multiple products we need to make sure we only update correct product commission
			-- IDBBC-87 Get commission collected for each billing code in #VolumeBasedCommission.  This is already calculated and present in #CommissionCollected. 
			--			THere could be multiple records in #CommissionCollected so we need to group them to get one amount
			-- IDBBC-102 Updating where clause to use correct fields from #CommissionCollected temp table.
			CROSS APPLY (
							SELECT	CommissionCollected = SUM(c.CommissionCollected)
							FROM	#CommissionCollected c
							WHERE	c.ProductGroup = ID.productgroup -- IDBBC-169 Removing hardcoding for OTR
							AND		c.BILLING_CODE = VBC.BillingCode
						) AS CC

			WHERE	VBC.InsertUpdate = 1
			-- IDBBC-165 adding new sales code used by DW Clob billing codes
			-- IDBBC-51 This condition is needed to make sure we only update commission record and do not update clearing fee record
			AND		(ID.SalesCode = ID.productgroup + 'COMM' OR ID.SalesCode = ID.productgroup + 'COMMEA') -- IDBBC-169 Removing hardcoding for OTR

			UPDATE	II
			SET		ItemAmount = VBC.TotalComm
			FROM	IDB_Billing.dbo.InvoiceInventory AS II (NOLOCK)
			JOIN	#InvoiceHistory AS IH ON IH.InvNum = II.InvNum
										 AND IH.InvDbId = II.InvDbId
			JOIN	#VolumeBasedCommission AS VBC ON VBC.BillingCode = IH.billing_code
												 AND VBC.ProductGroup = II.ProductGroup -- IDBBC-67 Invoice can have multiple products we need to make sure we only update correct product commission
			WHERE	VBC.InsertUpdate = 1
			AND		II.InvInvTypeId = 15

			--/* END Volume based commission section */

			/***** IDBBC-14 SHIRISH 2019/12/16 *****/
			/* Adding fixed Basis fee for dealers set up in table below
			 * Effective dates are applied to the invoice date
			 */
			CREATE TABLE #BasisCommAdjDealers
			(
				BillingCode VARCHAR(15),
				Amount INT,
				EffectiveStartDate DATE, -- InvoiceDate
				EffectiveEndDate DATE -- InvoiceDate
			)
			
			-- IDBBC-76 As per Rich Commission fee for Basis screen has been eliminated.  So we need to update below schedules with the end date of 2020-05-31
			INSERT INTO #BasisCommAdjDealers
			SELECT 'BOA3V', 10000, '2019-02-01','2020-05-31'
			UNION ALL
			SELECT 'CITI3V', 10000, '2019-02-01','2020-05-31'
			UNION ALL
			SELECT 'GS3V', 10000, '2019-02-01','2020-05-31'
			UNION ALL
			SELECT 'WFS3V', 10000, '2019-02-01','2020-05-31'
			UNION ALL
			SELECT 'NSI4V', 10000, '2019-02-01','2019-08-30'

			IF ((@BillingCode IS NULL) OR (@BillingCode IN (SELECT DISTINCT BillingCode FROM #BasisCommAdjDealers AS BCA WHERE @InvDate BETWEEN BCA.EffectiveStartDate AND BCA.EffectiveEndDate)))
			BEGIN

				-- Find max detailnum for invoices.
				SELECT	IH.billing_code, IH.InvNum, IH.InvDbId, ID.PeriodId, BCA.Amount, DetailNum = MAX(ID.DetailNum) + 1
				INTO	#BasisCommAdj
				FROM	#BasisCommAdjDealers AS BCA
				JOIN	#InvoiceHistory AS IH ON IH.billing_code = BCA.BillingCode
				JOIN	#InvoiceDetails AS ID ON ID.InvDbId = IH.InvDbId AND ID.InvNum = IH.InvNum
				WHERE	@InvDate BETWEEN BCA.EffectiveStartDate AND BCA.EffectiveEndDate
				AND		((@BillingCode IS NULL) OR (@BillingCode = IH.billing_code))
				GROUP BY 
						IH.billing_code, IH.InvNum, IH.InvDbId, ID.PeriodId, BCA.Amount

				-- insert records in Invoice Details
				INSERT INTO IDB_Billing.dbo.InvoiceDetails (InvNum, InvDbId, DetailNum, productgroup, ServiceTypeId, ChargeId, PeriodId, ItemAmount, SalesCode, ItemDescr, Who, created, InvoiceDescription, Source)
				SELECT	BCA.InvNum, BCA.InvDbId, BCA.DetailNum, 'AMSWP',1, CT.ChargeId,BCA.PeriodId, BCA.Amount, IDE.SalesCode, IDE.ItemDescr,'BlgUser',GETDATE(), IDE.InvoiceDescription,'E'
				FROM	#BasisCommAdj AS BCA
				JOIN	#ChargeType AS CT ON CT.ChargeType = 'FixedBASISCommission'
				JOIN	IDB_Billing.dbo.InvoiceDetailsEnrich AS IDE (NOLOCK) ON IDE.ChargeId = CT.ChargeId

				-- Insert records into invoice inventory
				INSERT INTO IDB_Billing.dbo.InvoiceInventory (InvNum, InvDbId, DetailNum, logon_id, ProductGroup, ItemAmount, InvInvTypeId, who, created, periodid, Source)
				SELECT	BCA.InvNum, BCA.InvDbId, BCA.DetailNum, 'BlgUser', 'AMSWP', BCA.Amount, IIT.InvInvTypeId, 'BlgUser', GETDATE(), BCA.PeriodId, 'E'
				FROM	#BasisCommAdj AS BCA
				JOIN	IDB_Billing.dbo.InvoiceInventoryType AS IIT (NOLOCK) ON IIT.Code = 'BasisCommissionAdjustment'

			END

			/***** End Fixed Fee Basis Commission Section *****/

			--GET CURRENT MAX ID 
			SELECT
				@CurrMaxInvoiceDealDetailId = MAX(InvoiceDealDetailId)
			FROM	IDB_Billing.dbo.InvoiceDealDetails --DO NOT USE NOLOCK BECAUSE WE DO WANT TO LOCK TO ENSURE WE GENERATE CORRECT IDs

			INSERT INTO IDB_Billing.dbo.InvoiceDealDetails
			(
				InvoiceDealDetailId,
				InvNum,
				InvDbId,
				DetailNum,
				PeriodId,
				ProductGroup,
				Source,
				Deal_Id
			)
			SELECT
				InvoiceDealDetailId = ISNULL(@CurrMaxInvoiceDealDetailId, 0) + ROW_NUMBER() OVER (ORDER BY DD.PROCESS_ID),
				InvNum = DD.InvNum,
				InvDbId = DD.InvDbId,
				DetailNum = ID.DetailNum,
				PeriodId = DD.PeriodId,
				ProductGroup = DD.ProductGroup,
				Source = DD.Source,
				Deal_Id = DD.DEAL_ID

			FROM	IDB_Billing.dbo.wDealDetails DD (NOLOCK) -- SHIRISH 11/6/2014 -- updating query to use permanent table
		 
			JOIN	#InvoiceDetails ID ON DD.InvNum = ID.InvNum 
							AND DD.InvDbId = ID.InvDbId 
							AND DD.ChargeId = ID.ChargeId
							AND DD.ProductGroup = ID.ProductGroup 
							AND DD.Source = ID.Source
		
			-- SHIRISH 07/02/2019: DIT-11311 For AGCY we are getting deals we get synthetic record and for Deal Details we are getting legs. Because of this Deal_ID from wDealDetails
			-- will not match Deal_Id from wDealBillingSchedule.  Making below join a left join so we get AGCY records in InvoiceDealDetails table.
			--LEFT JOIN	IDB_Billing.dbo.wDealBillingSchedule AS WDBS ON WDBS.DEAL_ID = DD.Deal_Id  AND DD.PROCESS_ID = WDBS.PROCESS_ID
			WHERE	DD.PROCESS_ID = @ProcessID
			-- SHIRISH 07/02/2019: DIT-11311 Added below condition to make sure wDealBillingSchedule join is ignored for AGCY trades
			--AND		(DD.ProductGroup = 'AGCY' OR (DD.ProductGroup <> 'AGCY' AND WDBS.DEAL_ID IS NOT NULL))
			--AND		(CASE WHEN DD.ProductGroup <> 'TRSY' THEN 1 WHEN DD.ProductGroup = 'TRSY' AND WDBS.Leg = 'PRI' THEN 1 ELSE 0 END) = 1	/* DIT-11311 */
			SELECT @Error = @@ERROR

			IF (@Error <> 0)
			BEGIN
				SET @Msg = 'Error inserting InvoiceDealDetail'
				GOTO ROLLBACKANDEXIT
			END


			--GET CURRENT MAX ID 
			SELECT
				@CurrMaxInvoiceFailChargeDetailId = MAX(InvoiceFailChargeDetailId)
			FROM	IDB_Billing.dbo.InvoiceFailChargeDetails --DO NOT USE NOLOCK BECAUSE WE DO WANT TO LOCK TO ENSURE WE GENERATE CORRECT IDs

			INSERT INTO IDB_Billing.dbo.InvoiceFailChargeDetails
			(
				InvoiceFailChargeDetailId,
				InvNum,
				InvDbId,
				DetailNum,
				TradeDate,
				Dealer,
				Billing_Code,
				Product,
				Source,
				Deal_Negotiation_Id,
				Trader_ID,
				Side_Dealer_Perspective,
				Instrument,
				Deal_Security_Name,
				Price_MC,
				DecPrice,
				Principal,
				AccruedInt,
				Clearing_Destination,
				CTCreator,
				CTSettled_Date,
				Original_Settle_Date,
				Quantity,
				Deal_Proceeds,
				FailCharge
			)
			SELECT
				InvoiceFailChargeDetailId = FCT.RowNum + ISNULL(@CurrMaxInvoiceFailChargeDetailId, 0),
				InvNum = FCT.InvNum,
				InvDbId = FCT.InvDbId,
				DetailNum = ID.DetailNum,
				TradeDate = FCT.Trd_Dt,
				Dealer = FCT.Dealer,
				Billing_Code = FCT.Billing_Code,
				Product = FCT.Product,
				Source = FCT.Source,
				Deal_Negotiation_Id = FCT.Deal_Negotiation_Id,
				Trader_ID = FCT.Trader_ID,
				Side_Dealer_Perspective = FCT.Side_Dealer_Perspective,
				Instrument = FCT.Instrument,
				Deal_Security_Name = FCT.Deal_Security_Name,
				Price_MC = FCT.Price_MC,
				DecPrice = FCT.DecPrice,
				Principal = FCT.Principal,
				AccruedInt = FCT.AccruedInt,
				Clearing_Destination = FCT.Clearing_Destination,
				CTCreator = FCT.CTCreator,
				CTSettled_Date = FCT.CTSettled_Date,
				Original_Settle_Date = FCT.Original_Settle_Date,
				Quantity = FCT.Quantity,
				Deal_Proceeds = FCT.DEAL_PROCEEDS,
				FailCharge = FCT.FailCharge

			FROM	#Failed_TRSY_CTs FCT

			JOIN	#InvoiceDetails ID ON FCT.InvNum = ID.InvNum 
						AND FCT.InvDbId = ID.InvDbId 
						AND FCT.ChargeId = ID.ChargeId
						AND FCT.ProductGroup = ID.ProductGroup 
						AND FCT.Source = ID.Source

			ORDER BY InvNum, InvDbId, Trd_Dt

			SELECT @Error = @@ERROR

			IF (@Error <> 0)
			BEGIN
				SET @Msg = 'Error inserting InvoiceFailChargeDetails'
				GOTO ROLLBACKANDEXIT
			END

			--  SHIRISH 07/22/2019 DIT-18425
			SET @timestamp2 = GETDATE()
			SET @logCount = @logCount + 1
			INSERT INTO IDB_Billing.dbo.BillingSteps 
			VALUES (@today, @ProcessID, @logCount, 'End Invoice mode',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
			SET @timestamp1 = @timestamp2

		END
		
	EXITOK:
	COMMIT TRAN
	SELECT @RowsAffected as RowsAffected

	END TRY
	BEGIN CATCH
	
		 SELECT
			 ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,ERROR_PROCEDURE() AS ErrorProcedure
			,ERROR_LINE() AS ErrorLine
			,ERROR_MESSAGE() AS ErrorMessage

		GOTO CLEANUP			
	
	END CATCH

	/* SHIRISH 11/4/2014 -- Cleaning up records from permanent table */

	CLEANUP:
	DELETE	IDB_Billing.dbo.wActiveBilling	
	WHERE	PROCESS_ID = @ProcessID

	DELETE	IDB_Billing.dbo.wActiveBranch	
	WHERE	PROCESS_ID = @ProcessID

	DELETE	IDB_Billing.dbo.wBillingSchedule
	WHERE	PROCESS_ID = @ProcessID

	DELETE	IDB_Billing.dbo.wPeriodIDs
	WHERE	PROCESS_ID = @ProcessID

	DELETE	IDB_Billing.dbo.wActiveBillingCodes
	WHERE	PROCESS_ID = @ProcessID

	--DELETE	IDB_Billing.dbo.wDateRanges
	--WHERE	PROCESS_ID = @ProcessID

	DELETE	IDB_Billing.dbo.wDeals_AllProd
	WHERE	PROCESS_ID = @ProcessID

	DELETE  IDB_Billing.dbo.wFailCharges_Staging
	WHERE	PROCESS_ID = @ProcessID

	DELETE  IDB_Billing.dbo.wDealBillingSchedule
	WHERE	PROCESS_ID = @ProcessID

	DELETE  IDB_Billing.dbo.wDealDetails
	WHERE	PROCESS_ID = @ProcessID

	-- IDBBC-26 Need to clean up working table
	DELETE	IDB_Billing.dbo.wOTRDealsToExclude
	WHERE	PROCESS_ID = @ProcessID

	DELETE	IDB_Billing.dbo.wPoolTakerTradesVsOperators
	WHERE	PROCESS_ID = @ProcessID

	DELETE	IDB_Billing.dbo.wOverrideScheduleDealBillingSchedule
	WHERE	PROCESS_ID = @ProcessID

	DELETE	IDB_Billing.dbo.wDBS_Staging
	WHERE	PROCESS_ID = @ProcessID

	DELETE	IDB_Billing.dbo.wDBS_AllProd
	WHERE	PROCESS_ID = @ProcessID

	DELETE	IDB_Billing.dbo.wDeal_Commission
	WHERE	PROCESS_ID = @ProcessID

	-- SHIRISH 03/09/2017: Drop all temp tables
	-- SHIRISH 08/22/2017: Updating drop statements to drop if exists
	IF OBJECT_ID('tempdb.dbo.#ABS_Cap_Floor_AllProd') IS NOT NULL DROP TABLE #ABS_Cap_Floor_AllProd
	--IF OBJECT_ID('tempdb.dbo.#ActiveBilling_Tier') IS NOT NULL DROP TABLE #ActiveBilling_Tier
	IF OBJECT_ID('tempdb.dbo.#ActiveBillingCodesWithPeriodIDs') IS NOT NULL DROP TABLE #ActiveBillingCodesWithPeriodIDs
	IF OBJECT_ID('tempdb.dbo.#ActiveBillingCodesWithPeriodIDs_Tier') IS NOT NULL DROP TABLE #ActiveBillingCodesWithPeriodIDs_Tier
	IF OBJECT_ID('tempdb.dbo.#ActiveBranchTraceInfo') IS NOT NULL DROP TABLE #ActiveBranchTraceInfo
	--IF OBJECT_ID('tempdb.dbo.#BillingSchedule_Tier') IS NOT NULL DROP TABLE #BillingSchedule_Tier
	IF OBJECT_ID('tempdb.dbo.#ChargeType') IS NOT NULL DROP TABLE #ChargeType
	IF OBJECT_ID('tempdb.dbo.#ClearingSchedule') IS NOT NULL DROP TABLE #ClearingSchedule
	IF OBJECT_ID('tempdb.dbo.#ClearingTrades') IS NOT NULL DROP TABLE #ClearingTrades
	IF OBJECT_ID('tempdb.dbo.#CommissionAdjustmentsDueToPriceChange') IS NOT NULL DROP TABLE #CommissionAdjustmentsDueToPriceChange
	IF OBJECT_ID('tempdb.dbo.#CommissionCollected') IS NOT NULL DROP TABLE #CommissionCollected
	IF OBJECT_ID('tempdb.dbo.#CommissionCollected_Tier') IS NOT NULL DROP TABLE #CommissionCollected_Tier
	IF OBJECT_ID('tempdb.dbo.#CommissionCredits') IS NOT NULL DROP TABLE #CommissionCredits
	IF OBJECT_ID('tempdb.dbo.#CommissionOwed') IS NOT NULL DROP TABLE #CommissionOwed
	IF OBJECT_ID('tempdb.dbo.#CommissionOwed_Tier') IS NOT NULL DROP TABLE #CommissionOwed_Tier
	IF OBJECT_ID('tempdb.dbo.#Commissions_AllProd') IS NOT NULL DROP TABLE #Commissions_AllProd
	IF OBJECT_ID('tempdb.dbo.#Commissions_AllProd_Tier') IS NOT NULL DROP TABLE #Commissions_AllProd_Tier
	IF OBJECT_ID('tempdb.dbo.#CommissionScheduleOverride') IS NOT NULL DROP TABLE #CommissionScheduleOverride
	IF OBJECT_ID('tempdb.dbo.#CommissionSummary') IS NOT NULL DROP TABLE #CommissionSummary
	IF OBJECT_ID('tempdb.dbo.#CommissionSummary_Tier') IS NOT NULL DROP TABLE #CommissionSummary_Tier
	IF OBJECT_ID('tempdb.dbo.#CommissionWithoutCap') IS NOT NULL DROP TABLE #CommissionWithoutCap
	IF OBJECT_ID('tempdb.dbo.#CommissionWithoutCap_Tier') IS NOT NULL DROP TABLE #CommissionWithoutCap_Tier
	IF OBJECT_ID('tempdb.dbo.#DBS_AllProd_Daily') IS NOT NULL DROP TABLE #DBS_AllProd_Daily
	IF OBJECT_ID('tempdb.dbo.#DBS_AllProd_PreCap_RT') IS NOT NULL DROP TABLE #DBS_AllProd_PreCap_RT
	IF OBJECT_ID('tempdb.dbo.#DBS_Rank_Staging') IS NOT NULL DROP TABLE #DBS_Rank_Staging
	IF OBJECT_ID('tempdb.dbo.#DealBillingSchedule_Staging') IS NOT NULL DROP TABLE #DealBillingSchedule_Staging
	IF OBJECT_ID('tempdb.dbo.#DealBillingScheduleStaging') IS NOT NULL DROP TABLE #DealBillingScheduleStaging
	IF OBJECT_ID('tempdb.dbo.#DealersForCommissionAdjustment') IS NOT NULL DROP TABLE #DealersForCommissionAdjustment
	IF OBJECT_ID('tempdb.dbo.#DealersWFixedFeesForShrtCpn') IS NOT NULL DROP TABLE #DealersWFixedFeesForShrtCpn
	IF OBJECT_ID('tempdb.dbo.#FailChargeSchedule') IS NOT NULL DROP TABLE #FailChargeSchedule
	IF OBJECT_ID('tempdb.dbo.#Failed_TRSY_CTs') IS NOT NULL DROP TABLE #Failed_TRSY_CTs
	IF OBJECT_ID('tempdb.dbo.#FC') IS NOT NULL DROP TABLE #FC
	IF OBJECT_ID('tempdb.dbo.#FC_Tier') IS NOT NULL DROP TABLE #FC_Tier
	IF OBJECT_ID('tempdb.dbo.#Id') IS NOT NULL DROP TABLE #Id
	IF OBJECT_ID('tempdb.dbo.#InvoiceDetails') IS NOT NULL DROP TABLE #InvoiceDetails
	IF OBJECT_ID('tempdb.dbo.#InvoiceDetailsGrp') IS NOT NULL DROP TABLE #InvoiceDetailsGrp
	IF OBJECT_ID('tempdb.dbo.#InvoiceHistory') IS NOT NULL DROP TABLE #InvoiceHistory
	IF OBJECT_ID('tempdb.dbo.#InvoiceInventory') IS NOT NULL DROP TABLE #InvoiceInventory
	IF OBJECT_ID('tempdb.dbo.#InvoiceInventory_Staging') IS NOT NULL DROP TABLE #InvoiceInventory_Staging
	IF OBJECT_ID('tempdb.dbo.#InvoiceInventory_Staging_AllProd') IS NOT NULL DROP TABLE #InvoiceInventory_Staging_AllProd
	IF OBJECT_ID('tempdb.dbo.#InvoiceInventory_Staging_AllProd_Tier') IS NOT NULL DROP TABLE #InvoiceInventory_Staging_AllProd_Tier
	IF OBJECT_ID('tempdb.dbo.#NetCommission') IS NOT NULL DROP TABLE #NetCommission
	IF OBJECT_ID('tempdb.dbo.#NetCommission_Tier') IS NOT NULL DROP TABLE #NetCommission_Tier
	IF OBJECT_ID('tempdb.dbo.#NOTIONAL_TIER_INFO') IS NOT NULL DROP TABLE #NOTIONAL_TIER_INFO
	IF OBJECT_ID('tempdb.dbo.#ProductsToProcess') IS NOT NULL DROP TABLE #ProductsToProcess
	IF OBJECT_ID('tempdb.dbo.#Range_Cursor') IS NOT NULL DROP TABLE #Range_Cursor
	IF OBJECT_ID('tempdb.dbo.#REBATE') IS NOT NULL DROP TABLE #REBATE
	IF OBJECT_ID('tempdb.dbo.#SNF') IS NOT NULL DROP TABLE #SNF
	IF OBJECT_ID('tempdb.dbo.#SNF_Tier') IS NOT NULL DROP TABLE #SNF_Tier
	IF OBJECT_ID('tempdb.dbo.#Source') IS NOT NULL DROP TABLE #Source
	IF OBJECT_ID('tempdb.dbo.#TF') IS NOT NULL DROP TABLE #TF
	IF OBJECT_ID('tempdb.dbo.#TF_COMMREPORT') IS NOT NULL DROP TABLE #TF_COMMREPORT
	IF OBJECT_ID('tempdb.dbo.#TF_Tier') IS NOT NULL DROP TABLE #TF_Tier
	IF OBJECT_ID('tempdb.dbo.#TieredBillingSchedule') IS NOT NULL DROP TABLE #TieredBillingSchedule
	IF OBJECT_ID('tempdb.dbo.#TieredSchedule') IS NOT NULL DROP TABLE #TieredSchedule
	IF OBJECT_ID('tempdb.dbo.#TotalPlatformNotional') IS NOT NULL DROP TABLE #TotalPlatformNotional
	IF OBJECT_ID('tempdb.dbo.#TraceDeals') IS NOT NULL DROP TABLE #TraceDeals
	IF OBJECT_ID('tempdb.dbo.#TraceDealsForCommReportOnly') IS NOT NULL DROP TABLE #TraceDealsForCommReportOnly
	IF OBJECT_ID('tempdb.dbo.#TraceDealsForInvoice') IS NOT NULL DROP TABLE #TraceDealsForInvoice
	IF OBJECT_ID('tempdb.dbo.#TraceEligibleProducts') IS NOT NULL DROP TABLE #TraceEligibleProducts
	IF OBJECT_ID('tempdb.dbo.#TracePassThruSetup') IS NOT NULL DROP TABLE #TracePassThruSetup
	IF OBJECT_ID('tempdb.dbo.#TraceSchedule') IS NOT NULL DROP TABLE #TraceSchedule
	IF OBJECT_ID('tempdb.dbo.#TraceUpdateList') IS NOT NULL DROP TABLE #TraceUpdateList
	IF OBJECT_ID('tempdb.dbo.#WGT_NOTIONAL_TIER_INFO') IS NOT NULL DROP TABLE #WGT_NOTIONAL_TIER_INFO
	IF OBJECT_ID('tempdb.dbo.#IDB_OFTRDiscountRate') IS NOT NULL DROP TABLE #IDB_OFTRDiscountRate
	IF OBJECT_ID('tempdb.dbo.#OverrideSchedules') IS NOT NULL DROP TABLE #OverrideSchedules
	--IF OBJECT_ID('tempdb.dbo.#TMGRebate') IS NOT NULL DROP TABLE #TMGRebate
	IF OBJECT_ID('tempdb.dbo.#CTForNAVXCommission') IS NOT NULL DROP TABLE #CTForNAVXCommission
	--IF OBJECT_ID('tempdb.dbo.#IDB_FALCON_DEALS') IS NOT NULL DROP TABLE #IDB_FALCON_DEALS
	IF OBJECT_ID('tempdb.dbo.#VolumeBasedCommission') IS NOT NULL DROP TABLE #VolumeBasedCommission
	IF OBJECT_ID('tempdb.dbo.#BasisCommAdj') IS NOT NULL DROP TABLE #BasisCommAdj
	IF OBJECT_ID('tempdb.dbo.#BasisCommAdjDealers') IS NOT NULL DROP TABLE #BasisCommAdjDealers
	IF OBJECT_ID('tempdb.dbo.#ParticipantVolumeAndRebate') IS NOT NULL DROP TABLE #ParticipantVolumeAndRebate
	IF OBJECT_ID('tempdb.dbo.#NAVXCommAdjustment') IS NOT NULL DROP TABLE #NAVXCommAdjustment
	IF OBJECT_ID('tempdb.dbo.#DealerCredits') IS NOT NULL DROP TABLE #DealerCredits
	IF OBJECT_ID('tempdb.dbo.#StreamingCredits') IS NOT NULL DROP TABLE #StreamingCredits
	IF OBJECT_ID('tempdb.dbo.#EUREPOOverrideCommission') IS NOT NULL DROP TABLE #EUREPOOverrideCommission
	IF OBJECT_ID('tempdb.dbo.#RepoTicketFeesByInvoice') IS NOT NULL DROP TABLE #RepoTicketFeesByInvoice
	IF OBJECT_ID('tempdb.dbo.#CATFees') IS NOT NULL DROP TABLE #CATFees
	IF OBJECT_ID('tempdb.dbo.#CATFeeSchedule') IS NOT NULL DROP TABLE #CATFeeSchedule
	
	--  SHIRISH 07/22/2019 DIT-18425
	SET @timestamp2 = GETDATE()
	SET @logCount = @logCount + 1
	INSERT INTO IDB_Billing.dbo.BillingSteps 
	VALUES (@today, @ProcessID, @logCount, 'After Clean-up',DATEDIFF(ms,@timestamp1,@timestamp2), GETDATE())
	SET @timestamp1 = @timestamp2

	RETURN @Error

	ROLLBACKANDEXIT:
	ROLLBACK TRAN
	RAISERROR(@msg,16,-1)
	RETURN @Error

	SET NOCOUNT OFF


-- GO
-- GRANT EXECUTE ON [dbo].[Billing_CreateInvoices] TO [BlgProcesses]
-- GO

