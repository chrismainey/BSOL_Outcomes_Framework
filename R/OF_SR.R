library(tidyverse)
library(janitor)
library(DBI)
library(odbc)
library(IMD)
library(PHEindicatormethods)
library(readxl)

rm(list = ls())

# Start the timer
start_time <- Sys.time()

#1. DB connection --------------------------------------------------------------

con <-
  dbConnect(
    odbc(),
    Driver = "SQL Server",
    Server = "MLCSU-BI-SQL",
    Database = "EAT_Reporting_BSOL",
    Trusted_Connection = "True"
  )

#2. Reference tables -----------------------------------------------------------

##2.1 LSOA 2021 to Ward 2022 to LAD 2022 lookup --------------------------------
lsoa_ward_lad_map<-read.csv("data/Lower_Layer_Super_Output_Area_(2021)_to_Ward_(2022)_to_LAD_(2022)_Lookup_in_England_and_Wales_v3.csv")
lsoa_ward_lad_map <- lsoa_ward_lad_map %>% 
  filter(LAD22CD %in% c('E08000025', 'E08000029'))

# Get unique pairs of WD22CD and LAD22CD
Ward_LAD_unique <- lsoa_ward_lad_map %>% 
  select(WD22CD, LAD22CD) %>% 
  distinct()

##2.2 Ward to Locality lookup --------------------------------------------------
ward_locality_map <- read.csv("data/ward_to_locality.csv", header = TRUE, check.names = FALSE)
ward_locality_map <- ward_locality_map %>% 
  rename(LA = ParentCode,
         WardCode = AreaCode,
         WardName = AreaName)

# Get unique pairs of Ward and Locality
ward_locality_unique <- ward_locality_map %>% 
  rename(WD22CD = WardCode,
         WD22NM = WardName) %>% 
  select(WD22CD, WD22NM, Locality) %>% 
  distinct()

##2.3 Ethnicity code mapping ---------------------------------------------------
ethnicity_map <- dbGetQuery(
  con,
  "SELECT [NHSCode]
  ,       [NHSCodeDefinition]
  ,       [LocalGrouping]
  ,       [CensusEthnicGroup]
  ,       [ONSGroup]
  FROM [EAT_Reporting_BSOL].[OF].[lkp_ethnic_categories]") %>% 
  as_tibble()

##2.4 Population estimates by Ward ---------------------------------------------
popfile_ward <- read.csv("data/c21_a18_e20_s2_ward.csv", header = TRUE, check.names = FALSE) %>% 
  clean_names()

#3. Get numerator data ---------------------------------------------------------
#3.1 Load the indicator data from the warehouse --------------------------------

# Insert which indicator IDs to extract
indicator_ids <- c(10,11,13,19, 24, 26, 49, 50, 51, 59, 104, 109, 114, 115, 124, 129)

# Convert the indicator IDs to a comma-separated string
indicator_ids_string <- paste(indicator_ids, collapse = ", ")

query <- paste0("SELECT * FROM EAT_Reporting_BSOL.[OF].IndicatorData
          where IndicatorID IN (", indicator_ids_string, ")")

# Execute the SQL query
indicator_data <- dbGetQuery(con, query) %>% 
  as_tibble() %>% 
  mutate(Ethnicity_Code = trimws(Ethnicity_Code))   # Remove trailing spaces

#4. Data preparation -----------------------------------------------------------
##4.1 Function 1: Create aggregated data ---------------------------------------

# Parameters:
# data: Numerator or denominator data used to create aggregated data
# agg_years: Aggregation years for creating the aggregated data
# type: Specifies which aggregated data needs to be created; default is "numerator"

create_aggregated_data <- function(data, agg_years = c(3, 5), type = "numerator") {
  
  aggregated_data = list()
  
  # Get the minimum available fiscal year start
  min_fiscal_year <- as.numeric(substr(min(data$FiscalYear), 1, 4))
  
  
  for (year in agg_years){
    # Initial filter based on the aggregation year
    if(year == 3){
      aggregated_data[[paste0(year, "YR_data")]] <- data %>%
        mutate(
          FiscalYearStart = as.numeric(substr(FiscalYear, 1, 4)),
          PeriodStart = FiscalYearStart - ((FiscalYearStart - min_fiscal_year) %% year),
          FiscalYear = paste0(PeriodStart, "/", PeriodStart + 3),  # 3-year rolling period
          AggYear = year
        ) 
    }
    else if (year == 5) {
      aggregated_data[[paste0(year, "YR_data")]] <- data %>%
        mutate(
          FiscalYearStart = as.numeric(substr(FiscalYear, 1, 4)),
          # Ensure rolling years are consecutive
          PeriodStart = min_fiscal_year + floor((FiscalYearStart - min_fiscal_year) / year) * year,
          FiscalYear = paste0(PeriodStart, "/", PeriodStart + 5),  # 5-year rolling period
          AggYear = year
        )
    }
    
    # Filter based on the aggregated data type 
    if(type == "numerator"){
      aggregated_data[[paste0(year, "YR_data")]] <- aggregated_data[[paste0(year, "YR_data")]] %>% 
        group_by(
          FiscalYear, AggYear, EthnicityCode, LAD22CD, WD22CD, WD22NM, Locality, AgeBandCode, AgeBandCategory
        ) %>%
        summarise(
          Numerator = sum(Numerator, na.rm = TRUE), .groups = 'drop'
        ) 
    } else{
      aggregated_data[[paste0(year, "YR_data")]] <- aggregated_data[[paste0(year, "YR_data")]] %>% 
        group_by(
          ElectoralWardsAndDivisionsCode, ElectoralWardsAndDivisions,EthnicGroup20CategoriesCode, NHSCode, 
          NHSCodeDefinition, ONSGroup, Quantile, AgeB18CategoriesCode,AgeB18Categories, FiscalYear, AggYear
        ) %>%
        summarise(
          Denominator = sum(Denominator, na.rm = TRUE), # Get the sum of denominator
          .groups = 'drop'
        ) 
    }
    
  }
  # Combine both 3- and 5-year aggregated data
  output <- bind_rows(aggregated_data)
  
  return(output)
}


##4.2 Function 2: Create numerator dataset -------------------------------------

# Parameters:
# indicator_data: Main indicator data
# indicator_id: Indicator ID
# reference_id: (Optional) Fingertips ID if available

get_numerator <- function(indicator_data, indicator_id, reference_id = NA, min_age = NA, max_age = NA) {
  
  # Initial filter based on Indicator ID and optional Reference ID
  if (!is.na(reference_id)) {
    filtered_data <- indicator_data %>%
      filter(IndicatorID == indicator_id & ReferenceID == reference_id)
  } else {
    filtered_data <- indicator_data %>%
      filter(IndicatorID == indicator_id)
  }
  
  # Apply age filters if provided
  if (!is.na(min_age) & !is.na(max_age)) {
    filtered_data <- filtered_data %>%
      filter(Age >= min_age & Age <= max_age)
  } else if (!is.na(min_age)) {
    filtered_data <- filtered_data %>%
      filter(Age >= min_age)
  } else if (!is.na(max_age)) {
    filtered_data <- filtered_data %>%
      filter(Age <= max_age)
  }
  
  
  filtered_output <- filtered_data %>%
    group_by(IndicatorID, ReferenceID, Ethnicity_Code, LSOA_2021, Age, Financial_Year) %>% 
    summarise(Numerator = sum(Numerator, na.rm = TRUE), .groups = 'drop')
  
  # Create 5-year age bands
  # Define the labels for the age bands
  age_labels <- c(
    "Aged 4 years and under",
    "Aged 5 to 9 years",
    "Aged 10 to 14 years",
    "Aged 15 to 19 years",
    "Aged 20 to 24 years",
    "Aged 25 to 29 years",
    "Aged 30 to 34 years",
    "Aged 35 to 39 years",
    "Aged 40 to 44 years",
    "Aged 45 to 49 years",
    "Aged 50 to 54 years",
    "Aged 55 to 59 years",
    "Aged 60 to 64 years",
    "Aged 65 to 69 years",
    "Aged 70 to 74 years",
    "Aged 75 to 79 years",
    "Aged 80 to 84 years",
    "Aged 85 years and over"
  )
  
  # Create age bands and assign labels
  output <- filtered_output %>%
    mutate(
      AgeBandCode = cut( Age,
                         breaks = c(seq(0, 85, by = 5), Inf), # Ensures the last break is Inf for ages 85 and over
                         labels = FALSE,                      # No labels assigned to the age band codes
                         right = FALSE,                       # Ensures that intervals are left-closed (e.g., [0, 5) means 0 <= Age < 5).
                         include.lowest = TRUE),              # Includes the lowest value in the first interval, covering ages 0 to 4.
      AgeBandCategory = age_labels[AgeBandCode]               # Uses the code to look up the corresponding label
    )
  
  
  # Process the data to generate the output
  output <- output %>%
    left_join(ethnicity_map, by = c("Ethnicity_Code" = "NHSCode")) %>%
    left_join(lsoa_ward_lad_map, by = c("LSOA_2021" = "LSOA21CD")) %>%
    left_join(ward_locality_map, by = c("WD22NM" = "WardName")) %>%
    group_by(Ethnicity_Code, Financial_Year, LAD22CD, WD22CD, WD22NM, Locality, AgeBandCode, AgeBandCategory) %>%
    summarise(Numerator = as.numeric(sum(Numerator, na.rm = TRUE)), .groups = 'drop') %>%
    rename(Fiscal_Year = Financial_Year) %>%
    mutate(Fiscal_Year = str_replace(Fiscal_Year, "-", "/20"),
           AggYear = 1) %>%
    clean_names(case = "upper_camel", abbreviations = c("WD", "LAD", "CD", "NM", "ONS"))
  
  # Get the aggregated numerator data for 3- and 5-year rolling periods
  aggregated_data <- create_aggregated_data(output, agg_years = c(3, 5), type = "numerator")
  
  # Combine all data into one dataframe
  output <- bind_rows(output,aggregated_data)
  
  return(output)
  
}

# Example
my_numerator <- get_numerator(indicator_data = indicator_data,
                              indicator_id = 109,
                              reference_id = "90808",
                              min_age = 15,
                              max_age = 24)


##4.3 Function 3: Create denominator dataset  ----------------------------------

# Parameters:
# pop_estimates: Population file by Ward
# numerator_data: Numerator dataset to get the available periods

get_denominator <- function(pop_estimates, numerator_data){
  
  # Map the population estimates to the unique Wards and LADs
  pop_estimates <- pop_estimates %>% 
    # Ensures the denominator data contains the age-specific bands for the indicator 
    filter(age_b_18_categories_code %in% unique(numerator_data$AgeBandCode)) %>% 
    inner_join(Ward_LAD_unique, 
               by = c("electoral_wards_and_divisions_code" = "WD22CD")) %>% 
    group_by(electoral_wards_and_divisions_code, electoral_wards_and_divisions,
             ethnic_group_20_categories_code, ethnic_group_20_categories, 
             age_b_18_categories_code,age_b_18_categories) %>% # Added age band codes & categories
    summarise(observation = sum(observation), .groups = 'drop') 
  
  
  # Add the IMD quintiles by Ward
  imd_england_ward <- IMD::imd_england_ward %>%
    select(ward_code, Score) %>%
    phe_quantile(Score, nquantiles = 5L, invert = TRUE) %>%
    select(ward_code, quantile) %>%
    mutate(quantile = paste0("Q", quantile))
  
  # Get the available unique periods in the numerator dataset
  periods <- numerator_data %>% 
    filter(AggYear == 1) %>% 
    select(FiscalYear) %>% 
    distinct()
  
  # Enrich the population estimates with quintiles and ethnicity descriptions
  output <- pop_estimates %>%
    left_join(imd_england_ward,
              by = c("electoral_wards_and_divisions_code" = "ward_code")) %>%
    left_join(ethnicity_map, 
              by = c("ethnic_group_20_categories_code" = "CensusEthnicGroup" )) %>%
    group_by(electoral_wards_and_divisions_code, electoral_wards_and_divisions,
             ethnic_group_20_categories_code, NHSCode, NHSCodeDefinition, ONSGroup, quantile, age_b_18_categories_code,age_b_18_categories) %>% 
    summarise(Denominator = as.numeric(sum(observation, na.rm = TRUE)), .groups = 'drop') %>%
    cross_join(periods) %>%
    clean_names(case = "upper_camel", abbreviations = c("NHS", "ONS", "ID")) %>% 
    mutate(AggYear = 1, DataQualityID = 1) %>% 
    filter(!is.na(ONSGroup))
  
  # Get the aggregated numerator data for 3- and 5-year rolling periods
  aggregated_data <- create_aggregated_data(output, agg_years = c(3, 5), type = "denominator")
  
  # Combine all data into one dataframe
  output <- bind_rows(output,aggregated_data)
  
  return(output)
} 

# Example
# my_denominator <- get_denominator(pop_estimates = popfile_ward,
#                                   numerator_data = my_numerator)

#5. Age-standardised rates calculation -----------------------------------------

# Create a standard 5-year age band populations to calculate age-standardised rates
standard_pop <- popfile_ward %>% 
  select(
    AgeBandCode = age_b_18_categories_code,
    AgeBandCategory = age_b_18_categories
  ) %>% 
  distinct() %>%
  mutate(
    Population = case_when(
      AgeBandCode < 18 ~ esp2013[AgeBandCode],
      AgeBandCode == 18 ~ sum(esp2013[18:19]) # sums up the last two age band populations to match the Census's 18 age bands groups
    )
  )

#5.1 Function 4: Calculate age-standardised rates ------------------------------

# Parameters:
# indicator_id: Indicator ID for which the rates are calculated
# numerator_data: Numerator dataset
# denominator_data: Denominator dataset
# aggID: Geographic levels, e.g., c('BSOL', 'LAD22CD', 'Locality', 'WD22NM')
# genderGrp: The gender for which the indicator was measured, e.g., c('Persons', 'Male', 'Female')
# ageGrp: The age group for which the indicator was measured, e.g., c('All ages')
# multiplier: The scale at which the rates are calculated, e.g., 100000 by default

# Helper function to determine grouping columns based on rate type
get_grouping_columns <- function(rate_type) {
  base_group_vars <- c("FiscalYear", "DataQualityID")
  
  switch(rate_type,
         "overall" = base_group_vars,
         "ethnicity" = c(base_group_vars, "ONSGroup"),
         "deprivation" = c(base_group_vars, "Quantile"),
         "ethnicity_deprivation" = c(base_group_vars, "ONSGroup", "Quantile"),
         stop("Invalid rate type specified.")
  )
}

# Helper function to summarize numerator and denominator data with the correct 'DataQualityID'
get_summarized_data <- function(id, group_vars, year, denominator_data, numerator_data) {
  
  
  summarized_data <- denominator_data %>%
    filter(AggYear == year) %>%
    left_join(numerator_data %>% filter(AggYear == year),
              by = c("ElectoralWardsAndDivisionsCode" = "WD22CD",
                     "NHSCode" = "EthnicityCode",
                     "FiscalYear" = "FiscalYear",
                     "AgeB18CategoriesCode" = "AgeBandCode"))
  
  if (id == "WD22NM") {
    summarized_data <- summarized_data %>%
      left_join(ward_locality_unique, by = c("ElectoralWardsAndDivisionsCode" = "WD22CD")) %>%
      group_by(across(all_of(c(group_vars, "WD22NM.y"))))
  } else if (id == "LAD22CD") {
    summarized_data <- summarized_data %>%
      left_join(Ward_LAD_unique, by = c("ElectoralWardsAndDivisionsCode" = "WD22CD")) %>%
      group_by(across(all_of(c(group_vars, "LAD22CD.y"))))
  } else if (id == "Locality") {
    summarized_data <- summarized_data %>%
      left_join(ward_locality_unique, by = c("ElectoralWardsAndDivisionsCode" = "WD22CD")) %>%
      group_by(across(all_of(c(group_vars, "Locality.y"))))
  } else if (id == "BSOL ICB") {
    summarized_data <- summarized_data %>%
      group_by(across(all_of(group_vars)))
  }
  
  summarized_data <- summarized_data %>%
    summarise(Numerator = sum(Numerator, na.rm = TRUE),
              Denominator = sum(Denominator),
              .groups = 'drop') %>%
    mutate(DataQualityID = ifelse(Denominator == 0, 5, 1))
  
  return(summarized_data)
}

# Main function to calculate age-standardised rate
calculate_age_std_rate <- function(indicator_id, denominator_data, numerator_data, aggID, genderGrp, ageGrp, multiplier = 100000) {
  
  
  # Aggregation years to calculate age-standardised rates for 1, 3 and 5 rolling periods
  AggYears <- c(1, 3, 5)
  
  # Initialize an empty list to store results
  results <- list()
  
  for(year in AggYears){
    
    # Helper function to calculate rates
    calculate_rate <- function(id, group_vars) {
      
      joined_data <- denominator_data %>% 
        filter(AggYear == year) %>%
        left_join(numerator_data %>% 
                    filter(AggYear == year), 
                  by = c("ElectoralWardsAndDivisionsCode" = "WD22CD",
                         "NHSCode" = "EthnicityCode",
                         "FiscalYear" = "FiscalYear",
                         "AgeB18CategoriesCode" = "AgeBandCode")) # Added age band
      
      # Conditional operations for different levels of aggregations
      if (id == "WD22NM") {
        joined_data <- joined_data %>%
          left_join(ward_locality_unique, by = c("ElectoralWardsAndDivisionsCode" = "WD22CD")) %>%
          group_by(across(all_of(c(group_vars, "WD22NM.y")))) # Grouping by WD22NM.y
        
      } else if (id == "LAD22CD") {
        joined_data <- joined_data %>%
          left_join(Ward_LAD_unique, by = c("ElectoralWardsAndDivisionsCode" = "WD22CD")) %>%
          group_by(across(all_of(c(group_vars, "LAD22CD.y")))) # Grouping by LAD22CD.y
        
      } else if (id == "Locality") {
        joined_data <- joined_data %>%
          left_join(ward_locality_unique, by = c("ElectoralWardsAndDivisionsCode" = "WD22CD")) %>%
          group_by(across(all_of(c(group_vars, "Locality.y")))) # Grouping by Locality.y
        
      } else if (id == "BSOL ICB") {
        joined_data <- joined_data %>%
          group_by(across(all_of(group_vars))) # No need to left join when id == BSOL
      }
      
      # Summarize and aggregate the data to calculate age-standardized rates
      summarized_data <- joined_data %>%
        summarise(
          Numerator = sum(Numerator, na.rm = TRUE),
          Denominator = sum(Denominator),
          .groups = 'keep'
        ) %>%
        mutate(
          Numerator = ifelse(is.na(Numerator) | Denominator == 0, 0, Numerator),
          Denominator = ifelse(Denominator == 0, 1, Denominator)  # Handle cases where Denominator == 0 to avoid errors
        ) %>%
        mutate(
          Gender = genderGrp,
          AgeGroup = ageGrp,
          IMD = ifelse("Quantile" %in% group_vars, Quantile, NA_character_),  
          EthnicityCode = as.character(ifelse("ONSGroup" %in% group_vars, ONSGroup, NA_character_)), 
          DataQualityID = 1, # Added DataQualityID
          AggregationLabel = ifelse(id == "BSOL ICB", id, !!sym(paste0(id, ".y"))),
          AggregationType = case_when(
            id == "BSOL ICB" ~ "ICB",
            id == "WD22NM"   ~ "Ward",
            id == "LAD22CD"  ~ "Local Authority",
            TRUE ~ "Locality (resident)"
          )
        ) %>%
        group_by(AggregationType, AggregationLabel, Gender, AgeGroup, IMD, EthnicityCode, FiscalYear, DataQualityID) %>%
        left_join(standard_pop, by = c("AgeB18CategoriesCode"="AgeBandCode")) %>% 
        phe_dsr(x=Numerator,
                n=Denominator,
                stdpop= Population,
                stdpoptype = "field",
                type = "standard",
                multiplier = multiplier) %>%
        rename(
          Numerator = total_count,
          Denominator = total_pop,
          IndicatorValue = value,
          LowerCI95 = lowercl,
          UpperCI95 = uppercl
        )
      
      return(summarized_data)
    }
    
    for (id in aggID) {
      # Overall indicator rate
      overall_rate <- calculate_rate(id = id, group_vars = c("FiscalYear", "AgeB18CategoriesCode", "AgeB18Categories")) %>%
        mutate(IndicatorValueType = paste0(year, "-year Overall Age-Standardised Rate")) %>% 
        left_join(get_summarized_data(id = id,
                                      group_vars = get_grouping_columns(rate_type = "overall"),
                                      year = year,
                                      denominator_data = denominator_data,
                                      numerator_data = numerator_data),
                  by = if(id == "BSOL ICB"){
                    c("FiscalYear" = "FiscalYear")
                  } else{
                    c("AggregationLabel" = paste0(id, ".y"),
                      "FiscalYear" = "FiscalYear")
                  }) %>%
        select(-Numerator.x, -Denominator.x, -DataQualityID.x) %>%
        rename_with(~ str_replace(., "\\.y$", ""))
      
      # Ethnicity indicator rate
      ethnicity_rate <- calculate_rate(id = id, group_vars = c("FiscalYear", "AgeB18CategoriesCode", "AgeB18Categories", "ONSGroup")) %>% 
        mutate(IndicatorValueType = paste0(year, "-year Ethnicity Age-Standardised Rate")) %>% 
        left_join(get_summarized_data(id = id,
                                      group_vars = get_grouping_columns(rate_type = "ethnicity"),
                                      year = year,
                                      denominator_data = denominator_data,
                                      numerator_data = numerator_data),
                  by = if(id == "BSOL ICB"){
                    c("FiscalYear" = "FiscalYear", 
                      "EthnicityCode" = "ONSGroup")
                  } else{
                    c("AggregationLabel" = paste0(id, ".y"),
                      "FiscalYear" = "FiscalYear",
                      "EthnicityCode" = "ONSGroup")
                  }) %>%
        select(-Numerator.x, -Denominator.x, -DataQualityID.x) %>%
        rename_with(~ str_replace(., "\\.y$", ""))
      
      # IMD indicator rate
      imd_rate <- calculate_rate(id = id, group_vars = c("FiscalYear", "AgeB18CategoriesCode", "AgeB18Categories", "Quantile")) %>%
        mutate(IndicatorValueType = paste0(year, "-year IMD Age-Standardised Rate")) %>% 
        left_join(get_summarized_data(id = id,
                                      group_vars = get_grouping_columns(rate_type = "deprivation"),
                                      year = year,
                                      denominator_data = denominator_data,
                                      numerator_data = numerator_data),
                  by = if(id == "BSOL ICB"){
                    c("FiscalYear" = "FiscalYear", 
                      "IMD" = "Quantile")
                  } else{
                    c("AggregationLabel" = paste0(id, ".y"),
                      "FiscalYear" = "FiscalYear",
                      "IMD" = "Quantile")
                  }) %>%
        select(-Numerator.x, -Denominator.x, -DataQualityID.x) %>%
        rename_with(~ str_replace(., "\\.y$", ""))
      
      # Ethnicity by IMD indicator rate
      ethnicity_imd_rate <- calculate_rate(id = id, group_vars = c("FiscalYear", "AgeB18CategoriesCode", "AgeB18Categories", "ONSGroup", "Quantile")) %>% 
        mutate(IndicatorValueType = paste0(year, "-year EthnicityXIMD Age-Standardised Rate")) %>% 
        left_join(get_summarized_data(id = id,
                                      group_vars = get_grouping_columns(rate_type = "ethnicity_deprivation"),
                                      year = year,
                                      denominator_data = denominator_data,
                                      numerator_data = numerator_data),
                  by = if(id == "BSOL ICB"){
                    c("FiscalYear" = "FiscalYear",
                      "EthnicityCode" = "ONSGroup",
                      "IMD" = "Quantile")
                  } else{
                    c("AggregationLabel" = paste0(id, ".y"),
                      "FiscalYear" = "FiscalYear",
                      "EthnicityCode" = "ONSGroup",
                      "IMD" = "Quantile")
                  }) %>%
        select(-Numerator.x, -Denominator.x, -DataQualityID.x) %>%
        rename_with(~ str_replace(., "\\.y$", ""))
      
      # Combine all rates into a single data frame
      results[[paste0(id, "_", year, "YR")]] <- bind_rows(
        overall_rate,
        ethnicity_rate,
        imd_rate,
        ethnicity_imd_rate
      ) %>% mutate(EthnicityCode = as.character(EthnicityCode)) # Ensure EthnicityCode is character before binding
    }
    
    # Bind all results together into a single data frame
    final_results <- bind_rows(results)
    
    # Add the rest of the variables as per the OF data model
    output <- final_results %>%
      # filter(FiscalYear != '2013/2014') %>%
      mutate(
        IndicatorID = indicator_id,
        InsertDate = today(),
        IndicatorStartDate = as.Date(ifelse(is.na(FiscalYear), NA, paste0(substring(FiscalYear, 1, 4), '-04-01'))),
        IndicatorEndDate = as.Date(ifelse(is.na(FiscalYear), NA, paste0('20', substring(FiscalYear, 8, 9), '-03-31'))),
        StatusID = as.integer(1) # current
      ) %>%
      clean_names(case = "upper_camel", abbreviations = c("ID", "IMD", "CI")) %>% 
      select(
        IndicatorID, InsertDate, Numerator, Denominator, IndicatorValue, IndicatorValueType,
        LowerCI95, UpperCI95, AggregationType, AggregationLabel, FiscalYear, Gender, AgeGroup, IMD, EthnicityCode, 
        StatusID, DataQualityID, IndicatorStartDate, IndicatorEndDate
      )
  }
  
  return(output)
}


#6. Process all parameters -----------------------------------------------------

##6.1 Optional: Process one indicator at a time --------------------------------
# Can use the following codes directly  if you already know which indicator
# you want to process, and the parameters for that indicator

# Requirements:
#1. Must use indicator_data, containing the indicator you want to process (see Step 3: Get numerator data)
#2. Specify the indicator id parameter
#3. Specify the reference id parameter
#4. Specify the min age group parameter
#5. Specify the max age group parameter

## Use 'result' variable to write the data into database (Step 7)

# my_numerator <- get_numerator(indicator_data = indicator_data,
#                               indicator_id = 109,
#                               reference_id = 90808,
#                               min_age = 15,
#                               max_age = 24)
# 
# my_denominator <- get_denominator(pop_estimates = popfile_ward,
#                                   numerator_data = my_numerator)
# 
# result <- calculate_age_std_rate(
#   indicator_id = 109,
#   denominator_data = my_denominator,
#   numerator_data = my_numerator,
#   aggID = c("BSOL ICB", "WD22NM", "LAD22CD", "Locality"),
#   genderGrp = "Persons",
#   ageGrp = "15-24 yrs",
#   multiplier = 100000
# )


# Use 'result' variable to write the data into the database (Step 7)

##6.2 Process several indicators altogether ------------------------------------

# Read the Excel file to get the available indicators
parameter_combinations <- readxl::read_excel("data/parameter_combinations.xlsx", 
                                             sheet = "standardised_indicators")

indicators_params <- parameter_combinations %>% 
  filter(StandardizedIndicator == 'Y' & PredeterminedDenominator == "N") %>% # Ensure we're taking the correct indicators
  filter(IndicatorID %in% indicator_ids) # Ensure we're extracting parameters ONLY for indicators we've specified in the beginning, otherwise, error will occur.


## Apply functions to specific parameter combinations 

# Parameter:
# row: the row of parameter combinations data

process_parameters <- function(row) {
  # Try to calculate age-standardized rates
  tryCatch(
    {
      print(paste("Processing numerator data for indicator ID:", row$IndicatorID, "& reference ID:", row$ReferenceID))
      
      numerator_data <- get_numerator(
        indicator_data = indicator_data,
        indicator_id = row$IndicatorID,
        reference_id = row$ReferenceID
      )
      
      print(paste("Processing denominator data for indicator ID:", row$IndicatorID, "& reference ID:", row$ReferenceID))
      
      denominator_data <- get_denominator(
        pop_estimates = popfile_ward,
        numerator_data = numerator_data
      )
      
      print(paste("Calculating age-standardised rates for indicator ID:", row$IndicatorID, "& reference ID:", row$ReferenceID))
      
      final_output <- calculate_age_std_rate(
        indicator_id = row$IndicatorID,
        denominator_data = denominator_data,
        numerator_data = numerator_data,
        aggID = c("BSOL ICB", "WD22NM", "LAD22CD", "Locality"),
        genderGrp = row$GenderCategory,
        ageGrp = row$AgeCategory
      )
      
      print(paste("Process completed!"))
      
      
    },
    # Error handling
    error = function(e) {
      message(
        paste0(
          "Error occurred for IndicatorID: ", row$IndicatorID,
          ", ReferenceID: ", row$ReferenceID,
          "\nDetails: ", e
        )
      )
      
      # Create an empty dataframe with the correct column types and NA values
      final_output <- tibble(
        IndicatorID = NA_integer_,
        InsertDate = as.Date(NA),
        Numerator = NA_real_,
        Denominator = NA_real_,
        IndicatorValue = NA_real_,
        LowerCI95 = NA_real_,
        UpperCI95 = NA_real_,
        AggregationType = NA_character_,
        AggregationLabel = NA_character_,
        FiscalYear = NA_character_,
        Gender = NA_character_,
        AgeGroup = NA_character_,
        IMD = NA_character_,
        EthnicityCode = NA_character_,
        NHSCodeDefinition = NA_character_,
        DataQualityID = NA_integer_,
        IndicatorStartDate = as.Date(NA),
        IndicatorEndDate = as.Date(NA),
        IndicatorValueType = NA_character_
      )
    }
  )
  
  return(final_output)
}

# Apply the function to each row of the parameter combinations
results <- indicators_params %>% # Filtered to indicators requiring age-standardisation
  rowwise() %>% # This ensures each row is treated as a separate set of inputs
  do(process_parameters(.)) %>%  # Apply the function to each row
  ungroup() #Remove the rowwise grouping, so the output is a simple tibble

# Remove the '2024/2025' Fiscal Year from the final output and future dates
results <- results %>% 
  filter(FiscalYear != '2024/2025') %>% 
  filter(!(as.Date(IndicatorEndDate, format = "%Y-%m-%d") > as.Date("2024-04-01", format = "%Y-%m-%d")))

#7. Write into database ----------------------------------------------------------

sql_connection <- dbConnect(
  odbc(),
  Driver = "SQL Server",
  Server = "MLCSU-BI-SQL",
  Database = "Working",
  Trusted_Connection = "True"
)

# Overwrite the existing table
dbWriteTable(
  sql_connection,
  Id(schema = "dbo", table = "BSOL_0033_OF_Age_Standardised_Rates"),
  results, # Processed dataset
  # append = TRUE # Append the data to the existing table
  overwrite = TRUE
)

# End the timer
end_time <- Sys.time()

# Calculate the time difference
time_taken <- end_time - start_time

# Print the time taken
print(paste("Time taken to run the script:", time_taken))