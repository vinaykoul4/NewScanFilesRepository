USE [SEPDB_SCAN_DATA]
GO

SET NOCOUNT ON 
SET XACT_ABORT ON
GO
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
GO

DECLARE @SERVERNAME NVARCHAR(100)
SELECT @SERVERNAME = @@SERVERNAME

IF @SERVERNAME	! = 'PMURTPSDIQC02V2\SQLQ002' --QA
--IF @SERVERNAME	! = 'PMURTPSDID03\SQLD001' --DEV
--IF @SERVERNAME	! = 'PMURTPSDIPC02V2\SQLP002' --PROD   
	BEGIN
		  PRINT 'ERROR: INCORRECT SERVER !!'
	END
ELSE
	BEGIN
	
	DECLARE @ROWCNT1 NVARCHAR(10);

	DECLARE @l_UpdateUser NVARCHAR(20)
	SET @l_UpdateUser	= SUSER_SNAME()

	IF EXISTS (SELECT * FROM tempdb..sysobjects WHERE id=OBJECT_ID('tempdb..#tmpErrors')) DROP TABLE #tmpErrors
	CREATE TABLE #tmpErrors (Error int)

	BEGIN TRANSACTION

	INSERT INTO DBO.SEP_ScanData_SubmitterReporting
		(
				Submitter
				,WeekEndDate
				,ChainSubmittedDate
				,CreateDate
				,CreateUser
				,UpdateDate
				,UpdateUser
				,Active
		)
		SELECT DISTINCT 
				 Submitter
				,WeekEndDate
				,ChainSubmittedDate
				,GETDATE()		AS CreateDate
				,@l_UpdateUser	AS CreateUser
				,GETDATE()		AS UpdateDate
				,@l_UpdateUser	AS UpdateUser
				,Active
		FROM DBO.SEP_ScanData WITH (NOLOCK)
		WHERE Active = 1
		
		INSERT INTO DBO.SEP_ScanData_SubmitterReporting
		(
				Submitter
				,WeekEndDate
				,ChainSubmittedDate
				,CreateDate
				,CreateUser
				,UpdateDate
				,UpdateUser
				,Active
		)
		SELECT DISTINCT 
				 Submitter
				,WeekEndDate
				,ChainSubmittedDate
				,GETDATE()		AS CreateDate
				,@l_UpdateUser	AS CreateUser
				,GETDATE()		AS UpdateDate
				,@l_UpdateUser	AS UpdateUser
				,Active
		FROM DBO.SEP_ScanData_Unmatched WITH (NOLOCK)
		WHERE Active = 1

		SET @ROWCNT1= @@ROWCOUNT;
		
		IF @@ERROR<>0 AND @@TRANCOUNT>0 ROLLBACK TRANSACTION
		IF @@TRANCOUNT=0 BEGIN INSERT INTO #tmpErrors (Error) SELECT 1 BEGIN TRANSACTION END

		IF EXISTS (SELECT * FROM #tmpErrors) ROLLBACK TRANSACTION
		IF @@TRANCOUNT>0 BEGIN
		PRINT 'The database update succeeded with count of rows: ' 
		PRINT 'SEP_ScanData_SubmitterReporting:'+ @ROWCNT1
		
		COMMIT TRANSACTION
		END
		ELSE PRINT 'The database update failed'
		
		IF OBJECT_ID('tempdb..#tmpErrors') IS NOT NULL
              DROP TABLE #tmpErrors
              
SET NOCOUNT OFF
  
END	



