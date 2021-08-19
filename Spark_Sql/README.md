FINDING AVERAGE HOURS AND IDLE HOURS FOR USER FROM CSV FILE

Start hadoop daemons $ start-all.sh

Start spark daemons $ ./sbin/start-all.sh

Open pyspark $ pyspark

Load csv files in dataframe Df= spark.read.format("csv").option("header","true").load("hdfs://localhost:9000/Spark/CpuLogData/*.csv")

Take necessary columns in on df Df.select("user_name","DateTime","boot_time","keyboard","mouse").show()

Create Temp view Df.createOrReplaceTempView("log_data")

Total entries for each user avg_hr_df= spark.sql("select user_name ,count('') as total from log_data where keyboard !=0.0 or mouse!=0.0 group by user_name")

To show output of above query avg_hr_df.show()

Again create temp view of above df avg_hr_df.createOrReplaceTempView("avg_hr")

Calculate avg sec for each person hrs_df=spark.sql("select user_name, ((((total-1)*5)*60)/6) as avg_hrs from avg_hr")

Convert seconds in min and hrs final_avg_hr=hrs_df.withColumn("Minutes", round((col("avg_hrs")/60),2))
.withColumn("Hours", floor((col("Minutes")/60)))
.withColumn("hourmin", floor(col("Minutes")-(col("Hours").cast("int") * 60)))
.withColumn("Days", floor((col("Hours")/24)))
.withColumn("Days2", col("Days")*24)
.withColumn("Time", when((col("Hours")==0) &(col("Days")==0), concat(col("hourmin"),lit("min"))).when((col("Hours")!=0)&(col("Days")==0), concat(col("Hours"),lit("hr "),col("hourmin"),lit("min"))).when(col("Days")!=0, concat(col("Days"),lit("d "),(col("Hours")-col("Days2")),lit("hr "),col("hourmin"),lit("min"))))
.drop("Minutes","Hours","hourmin","Days","Days2","avg_hrs")
.sort(desc("Time")).show()

Query to get idle hr entries idle_count = spark.sql("select user_name, DateTime from log_data where keyboard=0 and mouse=0").groupBy("user_name").count()

Idle entries count idle_count.sort('count').show()

Creating temp view idle_count.createOrReplaceTempView("idle_time")

Getting idle time in seconds idle_df=spark.sql("select user_name, (((count-1)560)/6) as idle_min from idle_time")

Converting into hrs and min idle_hrs=idle_df.withColumn("Minutes", round((col("idle_min")/60),2))
.withColumn("Hours", floor((col("Minutes")/60)))
.withColumn("hourmin", floor(col("Minutes")-(col("Hours").cast("int") * 60)))
.withColumn("Days", floor((col("Hours")/24)))
.withColumn("Days2", col("Days")*24)
.withColumn("Idle_Hrs", when((col("Hours")==0) &(col("Days")==0), concat(col("hourmin"),lit("min"))).when((col("Hours")!=0)&(col("Days")==0), concat(col("Hours"),lit("hr "),col("hourmin"),lit("min"))).when(col("Days")!=0, concat(col("Days"),lit("d "),(col("Hours")-col("Days2")),lit("hr "),col("hourmin"),lit("min"))))
.drop("Minutes","Hours","hourmin","Days","Days2",'idle_min')
.sort(asc("Idle_Hrs")).show()
Visualization with matplotlib

Import matplotlib

import matplotlib.pyplot as plt import pandas

Create pandas df idle = idle_count.toPandas()

Take columns we want to plot username= idle['user_name'] hrs=idle['count']

Select colours for each slice colors =['Red', 'Blue', 'Green','Grey','Brown','Red','Blue','Black'] explode = (0.1, 0, 0, 0, 0, 0, 0, 0)

Plot the pie chart plt.pie(hrs, labels=username, explode=explode, colors=colors, autopct='%1.1f%%', shadow=True) plt.title("Idle hours by person") plt.show()
