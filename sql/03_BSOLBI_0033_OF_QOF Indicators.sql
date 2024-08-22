    

/*=================================================================================================
 ID 212 - Stroke: QOF prevalence (all ages)			
=================================================================================================*/


  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '212'
         ,'2019-20'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_201920
		 ,Listsize_201920
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Stroke and transient ischaemic attack'
     AND Register_201920 IS NOT NULL  
      )

  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '212'
         ,'2020-21'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_202021
		 ,Listsize_202021
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Stroke and transient ischaemic attack'
     AND Register_202021 IS NOT NULL  
      )

  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '212'
         ,'2021-22'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_202122
		 ,Listsize_202122
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Stroke and transient ischaemic attack'
     AND Register_202122 IS NOT NULL  
      )

  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '212'
         ,'2022-23'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_202223
		 ,Listsize_202223
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Stroke and transient ischaemic attack'
     AND Register_202223 IS NOT NULL  
      )

/*=================================================================================================
 ID 219 - Hypertension: QOF prevalence (all ages)		
=================================================================================================*/


  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '219'
         ,'2019-20'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_201920
		 ,Listsize_201920
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Hypertension'
     AND Register_201920 IS NOT NULL  
      )

  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '219'
         ,'2020-21'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_202021
		 ,Listsize_202021
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Hypertension'
     AND Register_202021 IS NOT NULL  
      )

  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod]
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '219'
         ,'2021-22'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_202122
		 ,Listsize_202122
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Hypertension'
     AND Register_202122 IS NOT NULL  
      )

  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '219'
         ,'2022-23'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_202223
		 ,Listsize_202223
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Hypertension'
     AND Register_202223 IS NOT NULL  
      )

/*=================================================================================================
 ID 262 - Heart Failure: QOF prevalence (all ages)		
=================================================================================================*/


  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '262'
         ,'2019-20'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_201920
		 ,Listsize_201920
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Heart Failure'
     AND Register_201920 IS NOT NULL  
      )

  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '262'
         ,'2020-21'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_202021
		 ,Listsize_202021
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Heart Failure'
     AND Register_202021 IS NOT NULL  
      )

  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '262'
         ,'2021-22'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_202122
		 ,Listsize_202122
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Heart Failure'
     AND Register_202122 IS NOT NULL  
      )

  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '262'
         ,'2022-23'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_202223
		 ,Listsize_202223
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Heart Failure'
     AND Register_202223 IS NOT NULL  
      )

/*=================================================================================================
 ID 273 - CHD: QOF prevalence (all ages)		
=================================================================================================*/


  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '273'
         ,'2019-20'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_201920
		 ,Listsize_201920
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Coronary Heart Disease'
     AND Register_201920 IS NOT NULL  
      )

  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '273'
         ,'2020-21'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_202021
		 ,Listsize_202021
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Coronary Heart Disease'
     AND Register_202021 IS NOT NULL  
      )

  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '273'
         ,'2021-22'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_202122
		 ,Listsize_202122
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Coronary Heart Disease'
     AND Register_202122 IS NOT NULL  
      )

  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '273'
         ,'2022-23'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_202223
		 ,Listsize_202223
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Coronary Heart Disease'
     AND Register_202223 IS NOT NULL  
      )


/*=================================================================================================
 ID 280 - Atrial fibrillation: QOF prevalence (all ages)	
=================================================================================================*/


  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '280'
         ,'2019-20'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_201920
		 ,Listsize_201920
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Atrial Fibrilation'
     AND Register_201920 IS NOT NULL  
      )

  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '280'
         ,'2020-21'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_202021
		 ,Listsize_202021
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Atrial Fibrilation'
     AND Register_202021 IS NOT NULL  
      )

  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod]
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '280'
         ,'2021-22'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_202122
		 ,Listsize_202122
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Atrial Fibrilation'
     AND Register_202122 IS NOT NULL  
      )

  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '280'
         ,'2022-23'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_202223
		 ,Listsize_202223
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Atrial Fibrilation'
     AND Register_202223 IS NOT NULL  
      )

/*=================================================================================================
 ID 90647 - Prevalence of depression and anxiety in adults
=================================================================================================*/


  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '90647'
         ,'2019-20'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_201920
		 ,Listsize_201920
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Depression'
     AND Register_201920 IS NOT NULL  
      )

  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '90647'
         ,'2020-21'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_202021
		 ,Listsize_202021
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Depression'
     AND Register_202021 IS NOT NULL  
      )

  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '90647'
         ,'2021-22'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_202122
		 ,Listsize_202122
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Depression'
     AND Register_202122 IS NOT NULL  
      )

  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '90647'
         ,'2022-23'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_202223
		 ,Listsize_202223
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Depression'
     AND Register_202223 IS NOT NULL  
      )

/*=================================================================================================
 ID 253 - Reduce the prevalence of COPD
=================================================================================================*/


  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '253'
         ,'2019-20'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_201920
		 ,Listsize_201920
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'COPD'
     AND Register_201920 IS NOT NULL  
      )

  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '253'
         ,'2020-21'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_202021
		 ,Listsize_202021
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'COPD'
     AND Register_202021 IS NOT NULL  
      )

  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '253'
         ,'2021-22'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_202122
		 ,Listsize_202122
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'COPD'
     AND Register_202122 IS NOT NULL  
      )

  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '253'
         ,'2022-23'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_202223
		 ,Listsize_202223
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'COPD'
     AND Register_202223 IS NOT NULL  
      )

/*=================================================================================================
 ID 90933 - Reduce prevalence of Asthma (6ys+)
=================================================================================================*/


  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '90933'
         ,'2019-20'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_201920
		 ,Listsize_201920
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Asthma'
     AND Register_201920 IS NOT NULL  
      )

  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '90933'
         ,'2020-21'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_202021
		 ,Listsize_202021
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Asthma'
     AND Register_202021 IS NOT NULL  
      )

  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '90933'
         ,'2021-22'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_202122
		 ,Listsize_202122
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Asthma'
     AND Register_202122 IS NOT NULL  
      )

  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
       [ReferenceID] 
      ,[TimePeriod] 
	  ,[TimePeriodDesc]
      ,[GP_Practice] 
      ,[Numerator] 
	  ,[Denominator] 
	  )
	  (
  SELECT '90933'
         ,'2022-23'
		 ,'Financial Year'
         ,Practicecode
		 ,Register_202223
		 ,Listsize_202223
    FROM [EAT_Reporting_BSOL].[Reference].[QOF_Prevalence]
   WHERE DATASET = 'Asthma'
     AND Register_202223 IS NOT NULL  
      )

/*=================================================================================================
 ID 93790 - Increase patients with asthma with a review in last 12 months			
=================================================================================================*/

  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
         [ReferenceID] 
        ,[TimePeriod] 
	    ,[TimePeriodDesc]
        ,[GP_Practice] 
        ,[Numerator] 
	    ,[Denominator] 
	    )
		(
  SELECT '93790'
        ,[FiscalYear]
		,'Financial Year'
        ,[PracticeCode]
        ,[Numerator]
        ,[Denominator]

   FROM [AnalystGlobal].[Performance].[QOFIndicatorsByPracticeStandardised] T1
  INNER JOIN EAT_Reporting_BSOL.Reference.BSOL_ICS_PracticeMapped T2
     ON T1.PracticeCode = T2.GPPracticeCode_Original
  WHERE IndicatorCode = 'AST007'
    AND T2.ICS_2223 = 'BSOL'
	      )


/*=================================================================================================
  Update Grouping columns			
=================================================================================================*/

  UPDATE [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator]
     SET IndicatorID = T2.IndicatorID
	FROM [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] T1
   INNER JOIN [EAT_Reporting_BSOL].[OF].[IndicatorList] T2
      ON T1.ReferenceID = T2.ReferenceID

  UPDATE [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator]
     SET PCN = T2.PCN
	    ,Locality_Reg = T2.Locality
    FROM [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] T1
   INNER JOIN EAT_Reporting_BSOL.Reference.BSOL_ICS_PracticeMapped T2
      ON T1.GP_Practice = T2.GPPracticeCode_Original
   WHERE ICS_2223 = 'BSOL'
   

/*=================================================================================================
  Delete Cape Hill Medical Centre			
=================================================================================================*/

  DELETE T1
    FROM [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] T1
   WHERE GP_Practice = 'M88006'



/*=================================================================================================
 91215 - Percentage of dementia care plans reviewed in the last 12 months 		
=================================================================================================*/

  INSERT INTO [EAT_Reporting_BSOL].[OF].[IndicatorDataPredefinedDenominator] (
        [IndicatorID]
       ,[ReferenceID]
       ,[TimePeriod]
       ,[TimePeriodDesc]
       ,[GP_Practice]
       ,[PCN]
       ,[Locality_Reg]
       ,[Numerator]
       ,[Denominator]
       ,[Indicator_Level]
       ,[LSOA_2011]
       ,[LSOA_2021]
       ,[Ethnicity_Code]
	   )
	   (
  SELECT '107'
        ,'91215'
        ,T1.FinancialYear
        ,'Financial Year'
        ,T2.GPPracticeCode_Original
        ,T2.PCN
		,T2.Locality as Locality_Reg
		,T1.Numerator
		,T1.Denominator
		,'Practice Level'
		,NULL
		,NULL
		,NULL
    FROM [AnalystGlobal].[Performance].[QOFIndicatorsByPractice] T1
   INNER JOIN EAT_Reporting_BSOL.Reference.BSOL_ICS_PracticeMapped T2
      ON T1.PracticeCode = T2.GPPracticeCode_Original
   WHERE IndicatorCode = 'DEM004'
     AND T2.ICS_2223 = 'BSOL'
	    )



  