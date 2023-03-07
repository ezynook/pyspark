# Download Offline Package (Linux) <hr>
[-> PySpark 3.1.3](https://softnix365-my.sharepoint.com/:u:/g/personal/pasit_softnix_co_th/ESjyjPEsY-dJuPNyl7eoiXwBM4F9rYMNiO3uA4atHdQ82g?e=Z8SHDQ)

[-> Spark 3.1.3](https://softnix365-my.sharepoint.com/:u:/g/personal/pasit_softnix_co_th/EVmPpLHGex9DsbLwxLHqtAsB8Mt5bG8PXaxlZc5G3qO2Pw?e=DSWoaI)
# วิธีติดตั้ง PySpark Offline Linux
```bash
#online installer
yum -y install java-1.8.0-openjdk #CentOS
apt install default-jdk scala git -y #Ubuntu
wget https://dlcdn.apache.org/spark/spark-3.1.3/spark-3.1.3-bin-hadoop2.7.tgz
tar xvf spark-3.1.3-bin-hadoop2.7.tgz
sudo mv spark-3.1.3-bin-hadoop2.7/ /opt/spark 
vim ~/.bashrc
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME:$PATH
export PYSPARK_PYTHON=/root/anaconda3/bin/python
vim ~/.profile
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON=/usr/bin/python3
source ~/.bashrc
source ~/.profile
pip install pyspark==3.1.3
```
```bash
#Offline installer
#Install Jdk and jre 8 before install this step
yum -y install java-1.8.0-openjdk #CentOS
apt install default-jdk scala git -y #Ubuntu
tar xvf ./spark-3.1.3.tgz
sudo mv spark-3.1.3.tgz/ /opt/spark 
vim ~/.bashrc
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME:$PATH
export PYSPARK_PYTHON=/root/anaconda3/bin/python
vim ~/.profile
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON=/usr/bin/python3
source ~/.bashrc
source ~/.profile
pip install ./pyspark-3.1.3.tar.bz2
```
# ติดตั้ง Apache Iceberg
```bash
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-{version same PySpark}/1.1.0/iceberg-spark-{version same PySpark}-1.1.0.jar
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-hive-runtime/1.1.0/iceberg-hive-runtime-1.1.0.jar
mv iceberg-spark-{version same PySpark}-1.1.0.jar /opt/spark/jars/
mv iceberg-hive-runtime-1.1.0.jar /opt/spark/jars/
su hive
$ add jar /opt/spark/jars/iceberg-spark-{version same PySpark}-1.1.0.jar
$ add jar /opt/spark/jars/iceberg-hive-runtime-1.1.0.jar
```
# ตัวอย่างการใช้งาน Iceberg
```py
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark
import pandas as pd
import os
import requests
from datetime import datetime
#-----------------รูปแบบการ Connection Context แบบที่ 1 คือ ใช้งานผ่าน Linux Localfile
LOCAL_PATH="iceberg_local_db"
conf = (
    pyspark.SparkConf()
        .setAppName('Application_Name')
        .set('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.0.0,software.amazon.awssdk:bundle:2.17.178,software.amazon.awssdk:url-connection-client:2.17.178')
        .set('spark.jars', '/opt/spark/iceberg-hive-runtime-1.1.0.jar')
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
        .set('spark.sql.catalog.iceberg', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.iceberg.type', 'hadoop')
        .set(f'spark.sql.catalog.iceberg.warehouse', '{LOCAL_PATH}')
)
spark = SparkSession.builder.config(conf=conf).getOrCreate()
##-----------------รูปแบบการ Connection Context แบบที่ 2 คือ ใช้งานผ่าน HDFS
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr/lib/jvm/jre-1.8.0-openjdk'
os.environ['HADOOP_USER_NAME']='hive'
os.environ['PYSPARK_PYTHON'] ='/root/anaconda3/bin/python'

HDFS_PATH="hdfs:/spark/iceberg/default"
HDFS_METASTORE="thrift://nook.bigdata:9083"

conf = pyspark.SparkConf().setAll([
     ('spark.sql.catalog.default', 'org.apache.iceberg.spark.SparkCatalog'),
     ('spark.sql.catalog.default.type', 'hadoop'),
     ('spark.sql.catalog.default.warehouse', f'{HDFS_PATH}'),
     ('spark.jars', '/opt/spark/jars/iceberg-hive-runtime-1.1.0.jar'),
     ('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.0.0,software.amazon.awssdk:bundle:2.17.178,software.amazon.awssdk:url-connection-client:2.17.178'),
     ('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions'),
     ('spark.driver.memory', '2g'),
     ('spark.sql.repl.eagerEval.enabled','true'),
     ('hive.strict.managed.tables','false'),
     ('hive.metastore.uris', f'{HDFS_METASTORE}'),
     ('spark.sql.warehouse.dir', f'{HDFS_PATH}'),
     ('metastore.client.capability.check','false'),
    ])
spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Application_Name") \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()
     
## สร้าง Table แบบ Manual
spark.sql("CREATE TABLE iceberg.table1 (name string) USING iceberg;")
## เพิ่มข้อมูลได้โดยใช้คำสั่ง INSERT INTO
spark.sql("INSERT INTO iceberg.table1 VALUES ('Nook'), ('Pasit'), ('Nooky')")
## สร้าง Query ได้ผ่าน SELECT / Schema.Table
df = spark.sql("SELECT * FROM iceberg.table1").show()
df = spark.read.table("iceberg.table1").show()
#เพิ่มข้อมูลโดยใช้ DataFrame จาก Pandas หรือ PySpark
schema = StructType([
    StructField("year", StringType(), True),
    StructField("weeknum", StringType(), True),
    StructField("province", StringType(), True),
    StructField("new_case", IntegerType(), True),
    StructField("total_case", IntegerType(), True),
    StructField("new_case_excludeabroad", IntegerType(), True),
    StructField("total_case_excludeabroad", IntegerType(), True),
    StructField("new_death", IntegerType(), True),
    StructField("total_death", IntegerType(), True),
    StructField("update_date", StringType(), True),
])
df = spark.createDataFrame(pd.read_json("https://covid19.ddc.moph.go.th/api/Cases/today-cases-by-provinces"), schema)
#ตัวอย่างการ Where ด้วย Function Filter
df = df.filter("new_case != 0").filter("province != 'ทั้งประเทศ'").sort("new_case", ascending=False)
#เขียนข้อมูลลง HDFS
df.writeTo("iceberg.covid2").using("iceberg").createOrReplace() #create(), append(), replace()
#แบบสามารถเปลี่ยน Format ได้
df.writeTo("prod.db.table")
    .tableProperty("write.format.default", "orc") #parquet, orc, avro
    .partitionedBy("column name")
    .createOrReplace()
```
#### Schema ที่ใช้งานได้
```bash
org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
```
#### หากใช้ไม่ได้ให้ใช้คำสั่งนี้
```bash
ALTER TABLE SCHEMA.TABLE SET SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe';
```
# Simple Code
```py
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark
import pandas as pd
import os
#########กำหนดค่า pySpark Environment
#แบบที่ 1
spark = SparkSession.builder \
        .master('local[*]') \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.sql.warehouse.dir","/user/hive/warehouse/default") \
        .appName('myappname') \
        .enableHiveSupport() \
        .getOrCreate()
#แบบที่ 2 ส่วนใหญ่ใช้แบบนี้
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr/local/jdk8u222-b10'
os.environ['HADOOP_USER_NAME']='hive'
os.environ['PYSPARK_PYTHON'] ='/root/anaconda3/bin/python'
conf = pyspark.SparkConf().setAll([
     ('spark.driver.maxResultSize', '0'),
     ('spark.driver.memory', '2g'), #ตามสเปคของเครื่องการทำงานรูปแบบ Memory (Lazy parallelize) อาจจะทำให้ stuck ได้
     ('spark.sql.repl.eagerEval.enabled','true'),
     ('hive.strict.managed.tables','false'),
     ('spark.sql.warehouse.dir','/user/hive/warehouse/default/covid_orc'),
     ('hive.metastore.uris', 'thrift://nn01.bigdata:9083'),
     ('metastore.client.capability.check','false')
    ])
spark = SparkSession.builder \
        .master("local[*]") \
        .appName("NookTest") \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate();
#กำหนด Schema
#สามารถดูรูปแบบ Data Type ได้จากที่นี้ https://sparkbyexamples.com/pyspark/pyspark-sql-types-datatype-with-examples/
schema = StructType([
    StructField("year", StringType(), True),
    StructField("weeknum", IntegerType(), True),
    StructField("province", StringType(), True),
    StructField("new_case", IntegerType(), True),
    StructField("total_case", IntegerType(), True),
    StructField("new_case_excludeabroad", IntegerType(), True),
    StructField("total_case_excludeabroad", IntegerType(), True),
    StructField("new_death", IntegerType(), True),
    StructField("total_death", IntegerType(), True),
    StructField("update_date", StringType(), True)])
#อ่านข้อมูลจากตัวอย่าง API
df = spark.createDataFrame(pd.read_json("https://covid19.ddc.moph.go.th/api/Cases/today-cases-by-provinces"), schema)
#เขียนข้อมูลลง Hive
df.write \
    .format("orc, parquet")
    .mode("overwrite") \ #ถ้า Add New ใน ETL ใช้ append 
    .option("path", "/user/hive/warehouse/default/covid_orc") \ #External Table
    .saveAsTable("default.tbl_covid")

#Query
query = spqrk.sql("select * from db.tb").toPandas()
#or
query = spqrk.read.table("default.tbl_covid").toPandas()
#insert
query = spqrk.sql("insert into db.tb (col) values (col)").show()
#Parguet type นี้ไม่สามารถใช้คำสั่ง DELETE & UPDATE ได้
```
<b>By Pasit Y. @DataEngineer | Softnix Co.,Ltd | 2023-02-03</b>
