# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('MiniProject')
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/StudentData.csv')
headers = rdd.first()

# COMMAND ----------

rdd2 = rdd.filter(lambda x: x!=headers).map(lambda x: x.split(','))

# COMMAND ----------

rdd2.collect()

# COMMAND ----------

# Question 1) Show the number of students in the file.
##rdd2.map(lambda x: x[2]).distinct().count()
rdd2.count()

# COMMAND ----------

# Question 2) Show the total marks achieved by Female and Male students
rdd2.map(lambda x: (x[1],int(x[5]))).reduceByKey(lambda x , y : (x + y)).collect()

# COMMAND ----------

# Question 3) Show the total number of students that have passed and failed. 50+ marks are required to pass the course.
passed = rdd2.filter(lambda x: int(x[5])>50).count()
failed = rdd2.filter(lambda x: int(x[5])<=50).count()
failed2 = rdd2.count() - passed

print(passed, failed)

# COMMAND ----------

# Question 4) Show the total number of students enrolled per course.
rdd2.map(lambda x: (x[3],1)).reduceByKey(lambda x,y: x+y ).collect()

# COMMAND ----------

# Question 5) Show the total marks that student achieved per course.
rdd2.map(lambda x: (x[3],int(x[5]))).reduceByKey(lambda x,y: x+y ).collect()

# COMMAND ----------

# Question 6) Show the average marks that student achieved per course.
##rdd2.map(lambda x: (x[3],(int(x[5]),1))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).map(lambda x: (x[0], x[1][0] / x[1][1])).collect()
rdd2.map(lambda x: (x[3],(int(x[5]),1))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).mapValues(lambda x: (x[0] / x[1])).collect()

# COMMAND ----------

# Question 7) Show the minimum and maximum marks that student achieved per course.
print(rdd2.map(lambda x: (x[3],int(x[5]))).reduceByKey(lambda x,y : x if x>y else y).collect())
print(rdd2.map(lambda x: (x[3],int(x[5]))).reduceByKey(lambda x,y : x if x<y else y).collect())

# COMMAND ----------

# Question 8) Show the average age of male and female students.
##rdd2.map(lambda x: (x[1],(int(x[0]),1))).reduceByKey(lambda x , y: (x[0]+y[0],x[1]+y[1])).map(lambda x: (x[0],x[1][0]/x[1][1])).collect()
rdd2.map(lambda x: (x[1],(int(x[0]),1))).reduceByKey(lambda x , y: (x[0]+y[0],x[1]+y[1])).mapValues(lambda x: x[0]/x[1]).collect()
rdd2.map(lambda x: (x[1],(int(x[0]),1))).reduceByKey(lambda x , y: (x[0]+y[0],x[1]+y[1])).mapValues(lambda x: x).collect()
