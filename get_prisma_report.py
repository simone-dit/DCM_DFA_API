import argparse
import pandas as pd
import sys
import pyodbc
import warnings

warnings.filterwarnings('ignore')

# Connect to PRISMA Azure SQL DB Server
server = 'danreportingproddw02.database.windows.net'
database = 'DAN_Reporting_DW_Prisma'
username = 'dw02readonlyuser'
password = 'vd2ZF3m9Ve'
driver = '{ODBC Driver 13 for SQL Server}'

cnxn_prisma = pyodbc.connect(driver=driver, server=server, user=username, password=password, database=database)

argparser = argparse.ArgumentParser(add_help=False)
argparser.add_argument(
    'client_name', type=str,
    help='The name of the client for media plans'
)
argparser.add_argument(
    'planned_month', type=str,
    help='The month of media plans')

client_name = sys.argv[1]
planned_month = sys.argv[2]
#########################################################################

# Temp tables 
temp_all = """
IF OBJECT_ID('tempdb..##all') IS NOT NULL DROP TABLE ##all
SELECT *
INTO ##all
FROM PrismaBulkAPI.v_Amplifi_all_Placements
WHERE client_name = '{client_name}'
AND month_Year = '{planned_month}'
AND media_name = 'Interactive'
"""
temp_all = temp_all.format_map(vars())

temp_package = """
IF OBJECT_ID('tempdb..##package') IS NOT NULL DROP TABLE ##package
SELECT 
month_Year,
adserver_publisher_name,
product_name,
campaign_name,
placement_name,
placement_number,
placement_cost_method,
package_type,
package_id,
adserver_placement_id,
PlacementId,
MONTHLY_planned_amount,
MONTHLY_planned_impressions,
placement_rate
INTO ##package
FROM ##all
where package_type = 'Package'
"""
temp_package = temp_package.format_map(vars())

temp_nopackage = """
IF OBJECT_ID('tempdb..##nopackage') IS NOT NULL DROP TABLE ##nopackage
SELECT 
month_Year,
adserver_publisher_name,
product_name,
campaign_name,
placement_name,
placement_number,
placement_cost_method,
package_type,
package_id,
adserver_placement_id,
PlacementId,
MONTHLY_planned_amount,
MONTHLY_planned_impressions,
placement_rate
INTO ##nopackage
FROM ##all
where package_type != 'Package'
"""
temp_nopackage = temp_nopackage.format_map(vars())

update_line = """
UPDATE ##nopackage 
SET ##nopackage.MONTHLY_planned_amount = ##package.MONTHLY_planned_amount,
       ##nopackage.MONTHLY_planned_impressions = ##package.MONTHLY_planned_impressions,
       ##nopackage.placement_cost_method = ##package.placement_cost_method,
       ##nopackage.placement_rate = ##package.placement_rate
FROM ##package
WHERE ##nopackage.package_id = ##package.PlacementId
"""

#########################################################################

# Execute Temp Table queries
cnxn_prisma.autocommit = True
cursor = cnxn_prisma.cursor()

cursor.execute(temp_all)
cursor.execute(temp_package)
cursor.execute(temp_nopackage)

cursor.execute(update_line)

cnxn_prisma.commit()

# Get final PRISMA table 
prisma_line = """
SELECT
FORMAT(getdate(), 'yyyy-MM-dd') AS Date,
FORMAT(month_Year, 'MMM-yyyy') AS Month ,
adserver_publisher_name AS Publisher,
product_name AS Product,
campaign_name AS Campaign,
placement_name AS Placement,
placement_number AS Placenemt_Number,
placement_cost_method AS Cost_Method,
package_type AS Package_Type,
package_id AS Package_ID,
adserver_placement_id AS DCM_Placement_ID,
MONTHLY_planned_amount AS Planned_Cost,
MONTHLY_planned_impressions AS Planned_Impressions,
placement_rate AS Placement_Rate
FROM ##nopackage
"""

prisma_line = prisma_line.format_map(vars())

df_prisma = pd.read_sql_query(prisma_line, cnxn_prisma)

print(df_prisma.head())

# EXPORT DF to File
file_name = 'Monthly_' + planned_month[:7] + '_PRISMA_' + pd.to_datetime('today').strftime('%Y-%m-%d') + '.csv'
df_prisma.to_csv(file_name, sep=',', index=False)
print('File ' + file_name + ' has been saved to your current drive.')
