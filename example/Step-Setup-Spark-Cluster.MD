# Setup Spark Cluster

### Java Installation:

```bash
sudo yum install java
```
### Configure Hosts
```sudo vim /etc/hosts```
```bash
#Example
192.168.1.101   spark-master
192.168.1.102   spark-worker1
```
### SSH Configuration
```bash
ssh-keygen -t rsa
ssh-copy-id user@spark-worker1
```
###  Install Spark
```bash
wget https://downloads.apache.org/spark/spark-x.x.x/spark-x.x.x-bin-hadoopx.x.tgz
tar -xvzf spark-x.x.x-bin-hadoopx.x.tgz
sudo mv spark-x.x.x-bin-hadoopx.x /opt/spark
```
**Set the SPARK_HOME and PATH variables in the ```~/.bashrc``` file:**
```bash
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.bashrc
source ~/.bashrc
```
### Configure Spark Cluster
**Edit the Spark configuration file (`spark-env.sh`) on the master node:**
```bash
cp /opt/spark/conf/spark-env.sh.template /opt/spark/conf/spark-env.sh
nano /opt/spark/conf/spark-env.sh
```
**Add the following lines:**
```bash
export SPARK_MASTER_HOST=spark-master
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
export SPARK_WORKER_MEMORY=4g
export SPARK_WORKER_CORES=2
```
### Start Spark Master
**Start the Spark master on the master node:**
```bash
/opt/spark/sbin/start-master.sh
```
**Access the Spark Web UI at `http://spark-master:8080` to verify the master is running.**
### Start Spark Worker
**Start the Spark worker on each worker node:**
```bash
/opt/spark/sbin/start-worker.sh spark://spark-master:7077
```
_Verify that the workers are registered on the Spark Web UI._