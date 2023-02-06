#Download Offline Package (Linux)
[PySpark 3.1.3](https://softnix365-my.sharepoint.com/:u:/g/personal/pasit_softnix_co_th/ESjyjPEsY-dJuPNyl7eoiXwBM4F9rYMNiO3uA4atHdQ82g?e=Z8SHDQ)

[Spark 3.1.3](https://softnix365-my.sharepoint.com/:u:/g/personal/pasit_softnix_co_th/EVmPpLHGex9DsbLwxLHqtAsB8Mt5bG8PXaxlZc5G3qO2Pw?e=DSWoaI)
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
#### Schema ที่ใช้งานได้
```bash
org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
```
#### หากใช้ไม่ได้ให้ใช้คำสั่งนี้
```bash
ALTER TABLE potential_plant SET SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe';
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
        .config("spark.sql.warehouse.dir","/users/hive/warehouse/") \
        .appName('myappname') \
        .enableHiveSupport() \
        .getOrCreate()
#แบบที่ 2 ส่วนใหญ่ใช้แบบนี้
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr/local/jdk8u222-b10'
os.environ['HADOOP_USER_NAME']='hive'
os.environ['PYSPARK_PYTHON'] ='/root/anaconda3/bin/python'
conf = pyspark.SparkConf().setAll([
#    ('spark.sql.hive.metastore.version', '2.3.9'),
     ('spark.driver.maxResultSize', '0'),
     ('spark.driver.memory', '2g'),
     ('spark.sql.repl.eagerEval.enabled','true'),
     ('hive.strict.managed.tables','false'),
     ('hive.metastore.uris', 'thrift://nn01.bigdata:9083'),
     ('metastore.client.capability.check','false')
    ])
spark = SparkSession.builder.master("local[*]").appName("NookTest").config(conf=conf).enableHiveSupport().getOrCreate();
#กำหนด Schema
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
    StructField("update_date", TimestampType(), True)])
#อ่านข้อมูลจากตัวอย่าง API
df = pd.read_json("https://covid19.ddc.moph.go.th/api/Cases/today-cases-by-provinces")
df.to_csv('/tmp/tbl_covid_0.csv', sep=";", index=False)
#เขียนข้อมูลลง Hive
df.write.mode("overwrite").saveAsTable("nook.tbl_covid")

#Query
query = spqrk.sql("select * from db.tb").show()
#insert
query = spqrk.sql("insert into db.tb (col) values (col)").show()
#Parguet type นี้ไม่สามารถใช้คำสั่ง DELETE & UPDATE ได้
```
<b>By Pasit Y. @DataEngineer | Softnix Co.,Ltd</b>
