import pyspark

from pyspark.sql import SparkSession

from pyspark.sql.types import StructField, StructType, IntegerType, BooleanType, FloatType,  StringType, LongType

from pyspark.sql import functions as F

import pandas as pd

import matplotlib.pyplot as plt


spark = SparkSession.builder.getOrCreate()

job_events_schema = StructType([

     StructField("time",IntegerType(),True),

     StructField("missing_info",IntegerType(),True),

     StructField("job_id",IntegerType(),True),

     StructField("event_type",IntegerType(),True),

     StructField("user",StringType(),True),

     StructField("scheduling_class",IntegerType(),True),

     StructField("job_name",StringType(),True),

     StructField("logical_job_name",StringType(),True)

])

job_events = spark.read.format("csv").option("header",False).schema(job_events_schema).load("job_events/data")

#job_events.show(2)

task_events_schema = StructType([

     StructField("time",IntegerType(),True),

     StructField("missing_info",IntegerType(),True),

     StructField("job_id",IntegerType(),True),

     StructField("task_index",IntegerType(),True),

     StructField("machine_id",IntegerType(),True),

     StructField("event_type",IntegerType(),True),

     StructField("user",StringType(),True),

     StructField("scheduling_class",IntegerType(),True),

     StructField("priority",IntegerType(),True),

     StructField("cpu_request",FloatType(),True),

     StructField("memory_request",FloatType(),True),

     StructField("disk_space_request",FloatType(),True),

  StructField("different_machine_restriction",BooleanType(),True)

])

task_events = spark.read.format("csv").option("header",False).schema(task_events_schema).load("task_event/data")

#task_events.show(2)

machine_events_schema = StructType([

         StructField("time",IntegerType(),True),

         StructField("machine_id",IntegerType(),True),

         StructField("event_type",IntegerType(),True),

         StructField("platform_id",StringType(),True),

         StructField("cpus",FloatType(),True),

         StructField("memory",FloatType(),True)

])

machine_events = spark.read.format("csv").option("header",False).schema(machine_events_schema).load("machine_event/data")

#machine_events.show(2)

machine_attributes_schema = StructType([

         StructField("time",IntegerType(),True),

         StructField("machine_id",IntegerType(),True),

         StructField("attribute_name",StringType(),True),

         StructField("attribute_value",StringType(),True),

         StructField("attribute_deleted",BooleanType(),True)

])

machine_attributes = spark.read.format("csv").option("header",False).schema(machine_attributes_schema).load("machine_attributes/data")

#machine_attributes.show(2)

task_constraints_schema = StructType([

         StructField("time",IntegerType(),True),

         StructField("job_id",IntegerType(),True),

         StructField("task_index",IntegerType(),True),

         StructField("comparison_operator",IntegerType(),True),

         StructField("attribute_name",StringType(),True),

         StructField("attibute_value",StringType(),True)

])

task_constraints = spark.read.format("csv").option("header",False).schema(task_constraints_schema).load("task_constraints/data")

task_usage_schema = StructType([

         StructField("start_time",IntegerType(),True),

         StructField("end_time",IntegerType(),True),

         StructField("job_id",IntegerType(),True),

         StructField("task_index",IntegerType(),True),

         StructField("machine_index",IntegerType(),True),

         StructField("cpu_rate",FloatType(),True),

         StructField("canonical_memory_usage",FloatType(),True),

         StructField("assigned_memory_usage",FloatType(),True),

         StructField("unmapped_page_cache",FloatType(),True),

         StructField("total_page_cache",FloatType(),True),

         StructField("maximummemory_usage",FloatType(),True),

         StructField("disk_io_time",FloatType(),True),

         StructField("local_disk_space_usage",FloatType(),True),

         StructField("maximum_cpu_rate",FloatType(),True),

         StructField("maximum_disk_io_time",FloatType(),True),

         StructField("cycles_per_instruction",FloatType(),True),

         StructField("memory_accesses_per_instruction",FloatType(),True),

         StructField("sample_portion",FloatType(),True),

         StructField("aggregation_type",BooleanType(),True),

         StructField("sampled_cpu_usage",FloatType(),True)

])

task_usage = spark.read.format("csv").option("header",False).schema(task_usage_schema).load("task_usage/data")

#task_usage.show(2)
#Data Clean

def drop_unused_column(dataframe_object,list_column):
 return dataframe_object.drop(*list_column)

def filter_data(dataframe,list_column):
 condition = ""
 nb_col = len(list_column)
 for line in list_column:
  nb_col -= 1
  condition += str(line) + " IS NOT NULL"
  if nb_col>0:
   condition += " AND "
 return dataframe.filter(condition)
#job_event
list_column = ['time','missing_info','user','job_name','logical_job_name']
list_filter_col = ['job_id']
job_events = drop_unused_column(job_events,list_column)
job_events = filter_data(job_events,list_filter_col)
job_events.show(2)
#machine_event
list_column = ['time','platform_id']
machine_events = drop_unused_column(machine_events,list_column)
machine_events.show(2)
#task_event
list_column = ['time','missing_info','user','different_machine_restriction']
list_filter_col = ['job_id','task_index','machine_id']
task_events = drop_unused_column(task_events,list_column)
task_events = filter_data(task_events,list_filter_col)
task_events.show(2)
#task_usage
list_column = ['sampled_cpu_usage','aggregation_type','sample_portion','memory_accesses_per_instruction',"cycles_per_instruction","maximum_disk_io_time","local_disk_space_usage","disk_io_time","maximummemory_usage"]
list_filter_col = ['job_id','task_index','machine_index']
task_usage = drop_unused_column(task_usage,list_column)
task_usage = filter_data(task_usage,list_filter_col)
task_usage.show(2)

#Operation 1: Calcul de la repartition des machines en fonction de leur CPU

repartition_cpu = machine_events.select("machine_id",

"cpus").where(F.col("cpus").isNotNull()).groupBy("cpus").count()



repartition_cpu.show()

pa = repartition_cpu.toPandas()

pa.plot(x="cpus",y="count", kind='bar')

plt.grid()

plt.xticks([0,0.25,0.5,0.75,1])

plt.show()





#Operation 2: Calcul du nombre moyen de taches par job

nb_job = task_events.select("job_id","task_index").distinct().groupBy("job_id").count()



avg_job = nb_job.select(F.mean("count").alias("Moyenne")).collect()[0]["Moyenne"]



# print("Le nombre moyen de taches par job: ",round(avg_job))



# nb_job = task_events.select("job_id","task_index").distinct().where(F.col("job_id").isNotNull() & F.col("task_index").isNotNull()).limit(200).groupBy("job_id").count()

# pa_job = nb_job.toPandas()

# pa_job.plot(x="job_id",y="count", kind='bar')

# plt.grid()

# plt.show()





#Operation 3: Calcul de la distribution du nombre de jobs/taches par classe d'ordonnancement

job_per_scheduler = task_events.select("job_id","task_index","scheduling_class").groupBy("scheduling_class").count().sort("scheduling_class")

job_per_scheduler.show()



# Operation 4: Faible priorite , forte probabilite d'etre evince?

task_per_type = task_events.select("job_id","task_index","event_type","priority").where(F.col("event_type")==2).groupBy("priority").count().withColumnRenamed("count","Evince")

task_per_priority = task_events.select("job_id","task_index","priority").groupBy("priority").count().withColumnRenamed("count","Total")



task_per_type.show()

task_per_priority.show()



statistic_eviction = task_per_priority.join(task_per_type,["priority"])

percentage_eviction = statistic_eviction.withColumn("Probabilite",(F.col("Evince")/F.col("Total"))*100).sort("priority")

percentage_eviction.show()

#Operation 5: Verifions si en general si les taches d'un meme job s'executent sur la meme machine

tache_localisation = task_events.select("job_id","machine_id").distinct().where(F.col("job_id").isNotNull()).groupBy("job_id").count().sort("job_id")

tache_location_one = tache_localisation.where(F.col("count")==1)

percentage = (tache_location_one.count() / tache_localisation.count()) * 100

print("Le pourcentage de taches qui s'executent sur la meme machine est : ",str(percentage))



# Operation 6: Consommation des cpu en fonction des demandes

nb_requested_cpu_mem = task_events.select(F.concat_ws("_",F.col("job_id"), F.col("task_index")).alias("task_id"),"cpu_request","memory_request").groupby("task_id").agg(F.max("cpu_request").alias("cpu_request"), F.max("memory_request").alias("memory_request")).distinct()

nb_used_cpu_mem = (task_usage.select(F.concat_ws("_",F.col("job_id"),F.col("task_index")).alias("task_id"),"cpu_rate","canonical_memory_usage").groupBy("task_id").agg(F.sum("cpu_rate").alias("total_cpu_utilise"),F.sum("canonical_memory_usage").alias("total_memoire_utilise"))).distinct()

requestedConsumed= nb_requested_cpu_mem.join(nb_used_cpu_mem,['task_id'])



memory_info = (requestedConsumed.select("task_id","memory_request","total_memoire_utilise").groupBy("memory_request").agg(F.avg("total_memoire_utilise").alias("Moyenne"))).sort("memory_request")

cpu_info = (requestedConsumed.select("task_id","cpu_request","total_cpu_utilise").groupBy("cpu_request").agg(F.avg("total_cpu_utilise").alias("Moyenne"))).sort("cpu_request")



memory_info.show()

cpu_info.show()





# Operation 7: Correlation entre consommation ressource et eviction de taches

task_per_scheduling_class = task_events.select("job_id","scheduling_class").distinct().where(F.col("job_id").isNotNull() & F.col("scheduling_class").isNotNull()).sort("job_id")



task_per_scheduling_priority = task_events.select("job_id","scheduling_class","priority").distinct().where(F.col("scheduling_class").isNotNull() & F.col("priority").isNotNull() & F.col("job_id").isNotNull()).sort("job_id")



correlation = (task_per_scheduling_priority.drop("job_id").agg(F.corr(F.col("scheduling_class"), F.col("priority"))).alias("Correlation"))



correlation.show()



# Operation 8: Relation entre les ressources consommees par une tache et leur priorite



priority_per_task = (task_events.select(F.concat_ws("_",F.col("job_id"), F.col("task_index")).alias("task_id"), "priority").limit(2000).groupBy("task_id").agg(F.max("priority").alias("priority")))



cpu_used_per_task = (task_usage.select(F.concat_ws("_",F.col("job_id"), F.col("task_index")).alias("task_id"),"cpu_rate").groupBy("task_id").agg(F.sum("cpu_rate").alias("total_cpu_utilise")))



priority_cpu_used = (priority_per_task.join(cpu_used_per_task).groupBy("priority").agg(F.mean("total_cpu_utilise").alias("Moyenne")).sort("priority"))



priority_cpu_used.show()

*********************************************************
# ************************
# Pourcentage de puissance de taches perdues en raison de la maintenance
maxTime = machine_events.select(F.max("timestamp").alias("Temps maximal")).collect()[0]["Temps maximal"]
eventPerMachineDF = machine_events.where(F.col("event_type")!= 2).select("machine_id", "cpus", F.struct("event_type", "timestamp").alias("Type evenement par timestamp"))
eventPerMachineDF = eventPerMachineDF.groupBy("machine_id", "cpus").agg(F.collect_list("Type evenement par timestamp").alias("evenements"))

# Fonction pour calculer le temps d'arret
def calculateDowntime(machineEvents):
    totalDownTime = 0
    global maxTime
    for event in machineEvents:
        if event[0] == "0":
            totalDownTime += int(event[1])
        elif event[0] == "1":
            totalDownTime -= int(event[1])
        if event[0] == "1" and event is machineEvents[-1]:
            totalDownTime += maxTime
    return totalDownTime


# On calcule le temps total d'arret par machine
totalDowntimePerMachineDf = (
eventPerMachineDF.withColumn("total_down_time",
calculateDowntime(F.col("evenements"))).where(F.col("total_down_time").isNotNull()))

# On calcule la capacite ideale
idealComputationCapacity = machine_events.where(F.col("capacity_cpu").isNotNull()).rdd.map(lambda x: x["capacity_cpu"] * maxTime).sum()

# On calcule la capacite perdue 
lostComputationCapacity = totalDowntimePerMachineDf.rdd.map(lambda x: x["capacity_cpu"] * x["total_down_time"]).sum()

# On calcule le pourcentage 
print( "The percentage of computational power lost due to maintanance is:", round((lostComputationCapacity / idealComputationCapacity) * 100, 2), "%",)