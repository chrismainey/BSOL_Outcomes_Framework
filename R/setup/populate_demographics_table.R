library(tidyverse)  
library(IMD)

gender <-  data.frame (
  Gender = c("Persons", "Female", "Male")
)

agegrp <-  data.frame (
  AgeGrp = c("All ages",
             "<28 days",
             "6-8 weeks",
             "0 yrs",
             "1 yr",
             "2 yrs",
             "2-3 yrs",
             "0-4 yrs",
             "0-5 yrs",
             "4-5 yrs",
             "6+ yrs",
             "10+ yrs",
             "10-11 yrs",
             "10-24 yrs",
             "0-17 yrs",
             "1-17 yrs",
             "0-18 yrs",
             "12+ yrs",
             "12-17 yrs",
             "15+ yrs",
             "15-24 yrs",
             "16+ yrs",
             "16-64 yrs",
             "18+ yrs",
             "18-74 yrs",
             "19+ yrs",
             "35+ yrs",
             "40-74 yrs",
             "40-64 yrs",
             "53-70 yrs",
             "60-74 yrs",
             "65+ yrs",
             "65 yrs",
             "<75 yrs",
             NA)
)

imd5 <-  data.frame (
  IMD = c(1,2,3,4,5,NA))


#imd10 <-  data.frame (
#  IMD = c(1,2,3,4,5,6,7,8,9,10,NA))

#demo <- read.csv("demographicID.txt")
#imd <- read.csv("imdid.txt")

ethnic_codes <- read.csv("data/nhs_ethnic_categories.csv", header=TRUE, check.names=FALSE)
ethnic_codes <- 
  ethnic_codes %>% 
  distinct(ONSGroup)

#demo, setting up the blank table 
# Age and gender
demo <- agegrp %>% 
  cross_join(gender) 

demoG <- 
  demo %>% 
  mutate(Label =  paste0(Gender,': ', AgeGrp),
         Gender = Gender,
         AgeGrp = AgeGrp,
         IMD = NA,
         Ethnicity = NA) %>% 
  select(Label, Gender, AgeGrp, IMD, Ethnicity)

# demoG <-# demo  %>%
#   #mutate(
#     
#     data.frame(
#         Label = paste0(Gender,': ', AgeGrp),
#          Gender = gender$Gender,
#          AgeGrp = AgeAgeGrp,
#          IMD = NA,
#          Ethnicity = NA
#   )

# Age, gender, IMD
demoi5 <- demo %>% cross_join(imd5) %>%
  mutate(Label = paste0(Gender,': ', AgeGrp,': IMD Quintile',IMD),
         Gender = Gender,
         AgeGrp = AgeGrp,
         IMD = paste0('Q',IMD),
         Ethnicity = NA 
  ) 
# demoi10 <- demo %>% cross_join(imd) %>%
#   mutate(Label = paste0(Gender,': ', AgeGrp,': IMD Decile',IMD),
#          Gender = Gender,
#          AgeGrp = AgeGrp,
#          IMD = paste0('D',IMD),
#          Ethnicity = NA  
#   ) 
# Age Gender Ethnicity
demoE <- demo %>% cross_join(ethnic_codes) %>%
  mutate(Label = paste0(Gender,': ', AgeGrp,': ',ONSGroup) ,
         Gender = Gender,
         AgeGrp = AgeGrp,
         IMD = NA,
         Ethnicity = ONSGroup
  ) %>%
  select(Label, Gender, AgeGrp, Ethnicity, IMD)

# Local grouping suspended at this point.
# demoEgrp <- demo %>% cross_join(ethnic_codes) %>%
#   group_by(Gender, AgeGrp, LocalGrouping) %>%
#   summarise(count = n()) %>%
#   mutate(Label = paste0(Gender,': ', AgeGrp,': ',LocalGrouping) ,
#          Gender = Gender,
#          AgeGrp = AgeGrp,
#          IMD = NA,
#          Ethnicity = LocalGrouping 
#   ) %>%
#   select(- LocalGrouping, -count)

# Age gender, IMD, Ethnicity
demoEi5 <- demoE %>% 
  select(-IMD) %>%
  cross_join(imd5) %>%
  #filter(IMD < 6)  %>%
  mutate(Label = paste0(Label,': IMD Quintile', IMD) ,
         Gender = Gender,
         AgeGrp = AgeGrp,
         IMD = paste0('Q',IMD),
         Ethnicity = Ethnicity 
  ) 

a <- demoEi5 %>%  distinct()
b <- demoE %>%  distinct()

anti_join(a, demoEi5)
# demoEi10 <- demoE %>% 
#   select(-IMD) %>%
#   cross_join(imd) %>%
#   mutate(Label = paste0(Label,': IMD Decile',IMD) ,
#          Gender = Gender,
#          AgeGrp = AgeGrp,
#          IMD = paste0('D',IMD),
#          Ethnicity = Ethnicity 
#   ) 

# # Age gender, IMD ethnicity
# demoEgrpi5 <- demoEgrp %>% 
#   select(-IMD) %>%
#   cross_join(imd5) %>%
#   #filter(IMD < 6)  %>%
#   mutate(Label = paste0(Label,': IMD Quintile',IMD) ,
#          Gender = Gender,
#          AgeGrp = AgeGrp,
#          IMD = paste0('Q',IMD),
#          Ethnicity = Ethnicity
#   ) 

demo_table <- rbind(demoG, 
                    demoi5,
                    #demoi10,
                    demoE, 
                    #demoEgrp,
                    demoEi5
                    #demoEi10,
                    #demoEgrpi5
)

demo_table <- rename(demo_table, DemographicLabel = Label)
#demo_table$DemographicID <- 1:nrow(demo_table)
demo_table %>%  distinct() %>%  count()
# demo_table %>% 
#   select(DemographicID, Label, Gender, AgeGrp, IMD, Ethnicity)


# Manuel addition for NDTMS indicators.  Appended to not mess up previous order and keys.
# Manually added to SQL server, but included here for rebuild.

ndtms_age <- c("18-29 yrs", "30-49 yrs", "50+ yrs")

ndtms_append <-
  data.frame(DemographicLabel = paste0("Persons: ", ndtms_age)
             , Gender = "Persons"
             , AgeGrp = ndtms_age
             , IMD = NA
             , Ethnicity = NA)


library(DBI)
con <- dbConnect(odbc::odbc(), .connection_string = "Driver={SQL Server};server=MLCSU-BI-SQL;database=EAT_Reporting_BSOL", 
                 timeout = 10)

# Write the table back
# We have to use ID function to explain the schema 'OF' to dbWriteTable, else it
# writes to 'dbo', the default schema.  
out_tbl_demo <- Id("OF","Demographic")  
DBI::dbExecute(con, "TRUNCATE TABLE [OF].[Demographic]")
DBI::dbWriteTable(con, out_tbl_demo, demo_table, overwrite = FALSE, append = TRUE)


