# Import libraries
import pandas as pd
import sqlalchemy as sql
import os
import shutil
import logging

# set to suppress use of sceintific notation on large numbers for import
pd.set_option('display.float_format', lambda x: '%.12f' % x)

#set up logging and sql engine
logging.basicConfig(filename='sqlalchemy_log.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s')

logger = logging.getLogger('sqlalchemy.engine')
logger.setLevel(logging.INFO)

engine = sql.create_engine('mssql+pyodbc://@' + 'MLCSU-BI-SQL' + '/' + 'EAT_REPORTING_BSOL' + '?trusted_connection=Yes&driver=ODBC+Driver+17+for+SQL+Server')


# Loop through folder finding filenames and inserting into SQL Server OF.External_file_staging
# To do:  ad try catch for individual files so others carry on.
folder_path = "Z:\\Reports\\01_Adhoc\\BSOLBI_0033 Outcome Framework\\Final\\"
folder_path2 = "Z:\\Reports\\01_Adhoc\\BSOLBI_0033 Outcome Framework\\Final\\loaded\\"
for filename in os.listdir(folder_path):
    if filename.endswith(".csv"):
        file_path = os.path.join(folder_path, filename)
        df = pd.read_csv(file_path, dtype=str)

        df = df.rename(columns={'Unnamed: 0': 'Column_0'})
        df = df.rename(columns={'ValueID': 'Column_0'})
        df = df.rename(columns={'ID': 'Column_0'})
        
        df.to_sql('External_file_staging', engine, schema='OF', if_exists="append", index=False)

        file_path2 = os.path.join(folder_path2, filename)

        shutil.move(file_path, file_path2)

# Update text fields with potential bad values for correct NULL
fields = ['numerator', 'denominator', 'IndicatorValue', 'lowerCI95', 'upperCI95']

for i in fields:
    sql1 = """
    Update [EAT_Reporting_BSOL].[OF].[External_file_staging] 
    SET """ + i + """ = NULL 
    WHERE """ + i + """ in ('Inf', '-Inf', 'NA', 'Null', 'NULL', 'Nan', 'NaN', '' )
    """
    logger.info('Cleaning ' + i + ' column.')
    with engine.connect() as connection: 
        results = connection.execute(sql.text(sql1))
