import mysql.connector
import pandas

import datetime
from datetime import datetime, timedelta

HOST='104.154.180.62'
DATABASE='diageo'
USER='root'
PASSWORD='Xy19891020'

print('1. Configuring MySQL connection.')
print()
cnxn = mysql.connector.connect(user=USER, password=PASSWORD,
                       host=HOST, database=DATABASE)

temp_child = """
CREATE TEMPORARY TABLE child_number
SELECT
Package_ID,
COUNT(*) AS child_num
FROM
diageo.prisma
GROUP BY Package_ID;
"""

temp_prisma = """
CREATE TEMPORARY TABLE temp_prisma
select
p.Month as prisma_month,
p.publisher as prisma_publisher,
p.product as prisma_product,
p.Campaign as prisma_campaign,
p.Placement as prisma_placement,
p.Placement_Number as prisma_placement_number,
p.Cost_Method as prisma_cost_method,
p.Package_Type as prisma_package_type,
p.Package_ID as prisma_package_id,
p.DCM_Placement_ID as prisma_dcm_placement_id,
p.Planned_Cost as prisma_planned_cost,
p.Planned_Impressions as prisma_planned_impressions,
p.Placement_Rate as prisma_placement_rate,
child_number.child_num
from diageo.prisma p
left join child_number
on p.Package_ID = child_number.Package_ID
WHERE DCM_Placement_ID IS NOT NULL;
"""

temp_dcm = """
CREATE TEMPORARY TABLE dcm_new
SELECT Date,
  DATE_FORMAT(Date, "%b-%Y") AS Month,
  Campaign_ID,
  Campaign,
  Placement_ID,
  Placement,
  Placement_Cost_Structure,
  Placement_Rate,
  SUM(Impressions) as Impressions,
  SUM(Viewable_Impressions) as Viewable_Impressions,
  SUM(Clicks) as Clicks,
  SUM(Video_Plays) as Video_Plays,
  SUM(Video_Completions) as Video_Completions,
  SUM(Media_Cost) as Media_Cost
FROM diageo.dcm
  GROUP BY Date,
  Month,
  Campaign_ID,
  Campaign,
  Placement_ID,
  Placement,
  Placement_Cost_Structure,
  Placement_Rate;
"""

temp_dcm_prisma = """
CREATE TEMPORARY TABLE dcm_prisma
select d.Date as dcm_date,
d.Month as dcm_month,
d.Campaign_ID as dcm_campaign_id,
d.Campaign as dcm_Campaign,
d.Placement_ID as dcm_placement_id,
d.Placement as dcm_placement,
d.Placement_Cost_Structure as dcm_placement_cost_structure,
d.Placement_Rate as dcm_placement_rate,
d.Impressions as dcm_impressions,
d.Viewable_Impressions as dcm_viewable_impressions,
d.Clicks as dcm_clicks,
d.Video_Plays as dcm_video_plays,
d.Video_Completions as dcm_video_completions,
d.Media_Cost as dcm_media_cost,
temp_prisma.*
from dcm_new d
left join temp_prisma
on d.Placement_ID = temp_prisma.prisma_dcm_placement_id
AND d.Month = temp_prisma.prisma_month;
"""

cnxn.autocommit = True
cursor = cnxn.cursor()

print()
cursor.execute(temp_child)
cursor.execute(temp_prisma)
cursor.execute(temp_dcm)
cursor.execute(temp_dcm_prisma)

cnxn.commit()

# FINAL JOIN
result_line = """
select *,
case when prisma_planned_cost = 0 then 0
when dcm_placement_cost_structure = 'CPA' then 0
when dcm_placement_cost_structure = 'CPM' then dcm_impressions * prisma_placement_rate / 1000
when prisma_cost_method = 'Flat' and prisma_package_type = 'Child' then prisma_planned_cost / DAY(LAST_DAY(dcm_date)) / child_num
when prisma_cost_method = 'Flat' and prisma_package_type = 'Standalone' then prisma_planned_cost / DAY(LAST_DAY(dcm_date))
end as delivered_cost
from dcm_prisma
WHERE dcm_Campaign NOT LIKE '%DART Search%';
"""

df = pandas.read_sql_query(result_line, cnxn)

end_date = datetime.strftime(datetime.now() - timedelta(1), '%Y%m%d')
df.to_csv("PRISMA_DCM_Cost_Match_" + end_date + '.csv', index=False, encoding='utf8')