การใช้ SQL บน PySpark สามารถทำได้โดยใช้ `SparkSession` และ `DataFrame` ซึ่ง PySpark รองรับ SQL query ผ่าน `sql()` method และ `createOrReplaceTempView()` เพื่อสร้าง temporary table

* * *

## 🔹 **1. เริ่มต้นใช้งาน PySpark SQL**

PySpark SQL ต้องใช้ `SparkSession` เป็นจุดเริ่มต้นในการทำงาน

### ✅ **ติดตั้ง PySpark (ถ้ายังไม่มี)**

```bash
pip install pyspark
```

### ✅ **สร้าง SparkSession**

```python
from pyspark.sql import SparkSession

# สร้าง SparkSession
spark = SparkSession.builder.appName("PySparkSQLExample").getOrCreate()
```

* * *

## 🔹 **2. การสร้าง DataFrame และ Query ด้วย SQL**

เราจะใช้ `createOrReplaceTempView()` เพื่อสร้างตารางชั่วคราวจาก DataFrame

### ✅ **ตัวอย่าง 1: สร้าง DataFrame และใช้ SQL Query**

```python
from pyspark.sql import Row

# ข้อมูลตัวอย่าง
data = [
    Row(id=1, name="Alice", age=25, salary=3000),
    Row(id=2, name="Bob", age=30, salary=4000),
    Row(id=3, name="Charlie", age=28, salary=3500)
]

# สร้าง DataFrame
df = spark.createDataFrame(data)

# สร้าง Temporary Table
df.createOrReplaceTempView("employees")

# ใช้ SQL Query
result = spark.sql("SELECT * FROM employees WHERE age > 26")
result.show()
```

### 🔍 **ผลลัพธ์**

```
+---+-------+---+------+
| id|   name|age|salary|
+---+-------+---+------+
|  2|    Bob| 30|  4000|
|  3|Charlie| 28|  3500|
+---+-------+---+------+
```

* * *

## 🔹 **3. การใช้ SQL Aggregation Functions**

PySpark SQL รองรับฟังก์ชันทางคณิตศาสตร์ เช่น `SUM()`, `AVG()`, `COUNT()`, `MAX()`, `MIN()`

### ✅ **ตัวอย่าง 2: คำนวณเงินเดือนเฉลี่ย**

```python
avg_salary = spark.sql("SELECT AVG(salary) as avg_salary FROM employees")
avg_salary.show()
```

### 🔍 **ผลลัพธ์**

```
+----------+
|avg_salary|
+----------+
|    3500.0|
+----------+
```

* * *

## 🔹 **4. การใช้ GROUP BY และ ORDER BY**

PySpark SQL รองรับ `GROUP BY` และ `ORDER BY` เหมือน SQL ทั่วไป

### ✅ **ตัวอย่าง 3: จัดกลุ่มข้อมูลและเรียงลำดับ**

```python
grouped = spark.sql("""
    SELECT age, COUNT(*) as count 
    FROM employees 
    GROUP BY age 
    ORDER BY age DESC
""")
grouped.show()
```

### 🔍 **ผลลัพธ์**

```
+---+-----+
|age|count|
+---+-----+
| 30|    1|
| 28|    1|
| 25|    1|
+---+-----+
```

* * *

## 🔹 **5. การ JOIN ตารางใน PySpark SQL**

เราสามารถใช้ SQL เพื่อ JOIN ข้อมูลระหว่าง DataFrame ได้

### ✅ **ตัวอย่าง 4: INNER JOIN**

```python
# ข้อมูลตำแหน่งงาน
job_data = [
    Row(id=1, job="Engineer"),
    Row(id=2, job="Manager"),
    Row(id=3, job="Analyst")
]

# สร้าง DataFrame
job_df = spark.createDataFrame(job_data)

# สร้าง Temporary Table
job_df.createOrReplaceTempView("jobs")

# ทำ INNER JOIN
join_result = spark.sql("""
    SELECT employees.id, employees.name, employees.age, jobs.job
    FROM employees
    INNER JOIN jobs ON employees.id = jobs.id
""")
join_result.show()
```

### 🔍 **ผลลัพธ์**

```
+---+-------+---+--------+
| id|   name|age|     job|
+---+-------+---+--------+
|  1|  Alice| 25|Engineer|
|  2|    Bob| 30| Manager|
|  3|Charlie| 28| Analyst|
+---+-------+---+--------+
```

* * *

## 🔹 **6. การใช้ CASE WHEN (เงื่อนไข IF-ELSE)**

PySpark SQL รองรับการใช้ `CASE WHEN` เพื่อตรวจสอบเงื่อนไข

### ✅ **ตัวอย่าง 5: กำหนดระดับเงินเดือน**

```python
salary_level = spark.sql("""
    SELECT name, salary,
    CASE 
        WHEN salary >= 4000 THEN 'High'
        WHEN salary >= 3000 THEN 'Medium'
        ELSE 'Low'
    END AS salary_level
    FROM employees
""")
salary_level.show()
```

### 🔍 **ผลลัพธ์**

```
+-------+------+------------+
|   name|salary|salary_level|
+-------+------+------------+
|  Alice|  3000|     Medium|
|    Bob|  4000|       High|
|Charlie|  3500|     Medium|
+-------+------+------------+
```

* * *

## 🔹 **7. การใช้ Window Functions (เช่น RANK, DENSE_RANK)**

Window Functions ใช้คำนวณค่าภายในกลุ่มข้อมูล เช่น จัดอันดับ (`RANK()`)

### ✅ **ตัวอย่าง 6: จัดอันดับเงินเดือน**

```python
rank_salary = spark.sql("""
    SELECT name, salary, 
    RANK() OVER (ORDER BY salary DESC) as rank
    FROM employees
""")
rank_salary.show()
```

### 🔍 **ผลลัพธ์**

```
+-------+------+----+
|   name|salary|rank|
+-------+------+----+
|    Bob|  4000|   1|
|Charlie|  3500|   2|
|  Alice|  3000|   3|
+-------+------+----+
```

* * *

## 🔹 **8. การใช้ UDF (User Defined Function) ใน SQL**

เราสามารถสร้างฟังก์ชัน Python และใช้ใน SQL ได้โดยใช้ `udf()`

### ✅ **ตัวอย่าง 7: แปลงชื่อเป็นตัวพิมพ์ใหญ่**

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# ฟังก์ชัน Python
def to_upper(name):
    return name.upper()

# สร้าง UDF
upper_udf = udf(to_upper, StringType())

# ใช้ UDF ใน SQL
df.withColumn("name_upper", upper_udf(df.name)).show()
```

### 🔍 **ผลลัพธ์**

```
+---+-------+---+------+----------+
| id|   name|age|salary|name_upper|
+---+-------+---+------+----------+
|  1|  Alice| 25|  3000|    ALICE|
|  2|    Bob| 30|  4000|      BOB|
|  3|Charlie| 28|  3500|  CHARLIE|
+---+-------+---+------+----------+
```

* * *

## 🔥 **สรุป**

✅ PySpark SQL ใช้งานเหมือน SQL ทั่วไป  
✅ รองรับฟังก์ชัน `AVG()`, `SUM()`, `GROUP BY`, `ORDER BY`, `JOIN`  
✅ ใช้ `createOrReplaceTempView()` เพื่อสร้างตารางชั่วคราว  
✅ รองรับ Window Functions (`RANK()`, `DENSE_RANK()`)  
✅ สามารถสร้าง UDF เพื่อใช้ใน SQL Query ได้