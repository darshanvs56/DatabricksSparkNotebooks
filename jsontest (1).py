# Databricks notebook source
from pyspark.sql.functions import *
spark.conf.set("spark.sql.caseSensitive", "true")
df1 = spark.read.format("json").load("dbfs:/FileStore/shared_uploads/darshan.vs@icloud.com/json.json")
#df1 = spark.read.format("json").load("dbfs:/FileStore/shared_uploads/darshan.vs@icloud.com/finalfinaljson.json")
#df1 = spark.read.format("json").load("dbfs:/FileStore/shared_uploads/darshan.vs@icloud.com/final.json")
#df1 = spark.read.format("json").load("dbfs:/FileStore/shared_uploads/darshan.vs@icloud.com/thejson-1.json")
df1.show()
df1.persist()

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

#explanation
#ssot_incident = df1.select("ssot_incident", "ssot_incident")
#ssot_incident.printSchema()
tickets = df1.select("ssot_incident.tickets")
#tickets.printSchema()
#tickets.show()
df2 = df1.select("ssot_incident.tickets", explode("ssot_incident.tickets").alias("ticketsarray")).drop("ssot_incident.tickets")
#df2.show()
#df2.printSchema()
df3=df2.select("ticketsarray",explode("ticketsarray").alias("ticketinfo")).drop("ticketsarray")
#df3.show()
df4=df3.select("ticketinfo.id","ticketinfo.ticketInfo.rootCauseCD","ticketinfo.ticketInfo.adjTimeToRepair","ticketinfo.ticketInfo.managingOrg","ticketinfo.ticketInfo.subRootcauseCD")
df4.show()


# COMMAND ----------

from pyspark.sql.utils import AnalysisException
from pyspark.sql import Row


def has_column(df, col):
    try:
        df[col]
        return True
    except AnalysisException:
        return False
      

# COMMAND ----------

#modelscores,
#need to add data missing fields at the beginning 
if(has_column(df1,"ssot_incident.modelscores")):
  modelscoresdf2 = df1.select("_id","ssot_incident.modelscores")
  if df1.select("ssot_incident.modelscores"):
    model_scores = df1.select("_id","ssot_incident.modelscores", explode("ssot_incident.modelscores").alias("modelarray")).drop("ssot_incident.modelscores")
    #model_scores.printSchema()
    #initial few fields missing
    model_score_df=model_scores.select("_id","modelarray.deviceLocations","modelarray.dslamCardWithAlarmingPort","modelarray.features.numAlarmingDslamPorts","modelarray.alarmingDslamCards","modelarray.alarmingDslamPorts","modelarray.alarmingDslamDevices")
    model_score_df.show()
  else:
    print("Model data was empty")


# COMMAND ----------

#customers,
#need to validate data fields exist
#added if else check
if(has_column(df1,"ssot_incident.customers")):
  customers2 = df1.select("_id","ssot_incident.customers")
  if df1.select("ssot_incident.customers"):
    print("Customer data was empty")
  else:
    customers = df1.select("_id","ssot_incident.customers", explode("ssot_incident.customers").alias("customerarray")).drop("ssot_incident.customers")
    if(has_column(customers,"customerarray.customers_data")):
      customers=customers.select("_id,customerarray.customers_data",explode("customerarray.customers_data").alias("customer_data"))
      impactedServiceList=customers.select("_id,customerarray.customers_data").alias("customer_data")
    if(has_column(customers,"customer_data.contactList")):
      customers=customers.select("_id,customer_data",explode("customer_data.contactList").alias("contactList"))
      customers_df=customers.select("_id","customer_data.customerName","customer_data.customerIdType","customer_data.customerIdvalue","customer_data.customerRegion","customer_data.customerRevenueImpact","contactList.contactType","contactList.contactPhone","contactList.contactId","contactList.contactEmail","contactList.contactPhone")
    model_score_df.show()



# COMMAND ----------

#impacted list

impactedServiceListdf=impactedServiceList
if(has_column(impactedServiceList,"customer_data.impactServiceList")):
   if impactedServiceList.select("customer_data.impactServiceList"):
    print("Customer data was empty")
  else:
    impactedServiceListdf = impactedServiceList.select("_id","customer_data.impactServiceList", explode("customer_data.impactServiceList").alias("impactedservicelistarray")).drop("ssot_incident.customers")
      impactedServiceListdf=impactedServiceListdf.select("_id,"impactedServiceListdf.feeder_cable_idli")
  

# COMMAND ----------

#topologies
#customers,
#incident list
#need to add other fields check and assign


if has_column(df1,"ssot_incident.topologies"):
  if df1.select("ssot_incident.topologies"):
    print("Topology data was empty")
  else:
    topologies2 = df1.select("_id","ssot_incident.topologies",explode("ssot_incident.topologies").alias("topologyarray"))
    topologies_df=topologies2.select("_id","topologyarray.feeder_cable_idl","topologyarray.deployment_status_cd")
    

# COMMAND ----------

#dispatches
#need to validate other fields
#banId does not exists "dispatchinfo.col.dispatchInfo.banID"
dispatchesdf2 = df1.select("_id","ssot_incident.dispatches", explode("ssot_incident.dispatches").alias("dispatchedArray")).drop("ssot_incident.dispatches").select("_id","dispatchedArray",explode("dispatchedArray")).alias("dispatchinfo").drop("dispatchedArray").select("_id","dispatchinfo.col.wrId","dispatchinfo.col.dispatchInfo.source","dispatchinfo.col.dispatchInfo.dispatchCreatedDateTime","dispatchinfo.col.dispatchInfo.dispatchAsset","dispatchinfo.col.dispatchInfo.dispatchStatus","dispatchinfo.col.dispatchInfo.techAssigned","dispatchinfo.col.dispatchInfo.dispatchWrid")
dispatchesdf2.show()
dispatchesdf2.printSchema()

# COMMAND ----------

#tickets
#need to add other fields
ticketsdf2 = df1.select("_id","ssot_incident.tickets", explode("ssot_incident.tickets").alias("ticketsarray")).drop("ssot_incident.tickets").select("_id","ticketsarray",explode("ticketsarray").alias("ticketinfo")).drop("ticketsarray").select("_id","ticketinfo.id","ticketinfo.ticketInfo.rootCauseCD","ticketinfo.ticketInfo.adjTimeToRepair","ticketinfo.ticketInfo.managingOrg","ticketinfo.ticketInfo.subRootcauseCD")
ticketsdf2.show()


# COMMAND ----------



# COMMAND ----------

#userupdates
#need to validate other fields
#addedcomments 
#updated

userupdatesdf= df1.select("_id","ssot_incident.userupdates", explode("ssot_incident.userupdates").alias("userupdatesarray")).drop("ssot_incident.userupdates")
if(has_column(userupdatesdf,"userupdatesarray.comments")):
  if df1.select("userupdatesarray.comments"):
    print("Customer data was empty")
  else:
    userupdatesdf=userupdatesdf.select("_id","userupdatesarray",explode("userupdatesarray").alias("commentsdata"))
    userupdatesdf=userupdatesdf.select("_id","userupdatesarray","userupdatesarray._id","userupdatesarray._class","userupdatesarray.incident_id","userupdatesarray.updatedBy","userupdatesarray.lastUpdateTime","commentsdata.userId","commentsdata.createdTime","commentsdata.comment").drop("userupdatesarray")
else:
  userupdatesdf=userupdatesdf.select("_id","userupdatesarray","userupdatesarray._id","userupdatesarray._class","userupdatesarray.incident_id","userupdatesarray.updatedBy","userupdatesarray.lastUpdateTime").drop("userupdatesarray")
userupdatesdf.show()
userupdatesdf.printSchema()

# COMMAND ----------

#matchedalarams
#need to add other fields
df3 = df1.select("_id","ssot_incident.matchedAlarms", explode("ssot_incident.matchedAlarms").alias("matchedarray")).drop("ssot_incident.matchedAlarms").select("_id","matchedarray",explode("matchedarray").alias("matchedalaram")).drop("matchedarray").select("_id","matchedalaram.classification","matchedalaram.domain","matchedalaram.level_value")
df3.show()
df3.printSchema()

# COMMAND ----------

#outcome
outcomedf = df1.select("_id","ssot_incident.outcomes", explode("ssot_incident.outcomes").alias("outcomesarray")).drop("ssot_incident.outcomes").select("_id","outcomesarray",explode("outcomesarray").alias("outcomes")).drop("outcomesarray").select("_id","outcomes.rootCause","outcomes.affectedAsset.*","outcomes.affectedDispatchAsset.*","outcomes.affectedEquipments.*","outcomes.long_millis","outcomes.mlLib_method","outcomes.outcome_timestamp")
outcomedf.show()

# COMMAND ----------

#incident 
#validate fields
incidentdf = df1.select("_id","ssot_incident.incident.region","ssot_incident.incident.rootCause","ssot_incident.incident.revenueImpacted","ssot_incident.incident.incidentRole","ssot_incident.incident.incidentDurationInMinutes","ssot_incident.incident.incident_state","ssot_incident.incident.autoClosed","ssot_incident.incident.incidentSeverity","ssot_incident.incident.autoIncident","ssot_incident.incident.clli","ssot_incident.incident.incident_status","ssot_incident.incident.created_timestamp","ssot_incident.incident.updated_timestamp","ssot_incident.incident.predicated_MTTR","ssot_incident.incident.incidentStateTimestamps","ssot_incident.incident.incidentSeverity","ssot_incident.incident.locationInfo")
incidentdf.show()

# COMMAND ----------

df4.write.format("csv").option("delimiter","|").saveAsTable("dummy")

# COMMAND ----------

# Read location and name
dfReadSpecificStructure = explodeArrarDF.select("Exp_RESULTS.user.location.*","Exp_RESULTS.user.name.*")
dfReadSpecificStructure.show(truncate=False)
