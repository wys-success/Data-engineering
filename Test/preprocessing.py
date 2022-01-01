from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, when, split, posexplode
 
conf = SparkConf().setMaster("local").setAppName("preprocessing")
sc = SparkContext(conf = conf)
sc.setLogLevel('WARN') # 减少不必要的log输出
spark = SparkSession.builder \
                    .config(conf = SparkConf()) \
                    .getOrCreate()
 
rawFile = 'file:///home/hadoop/HBING/earthquake.csv'
rawData = spark.read.format('csv') \
                    .options(header='true', inferschema='true') \
                    .load(rawFile)
rawData.printSchema() # 查看数据结构
print('total count: %d' % rawData.count()) # 打印总行数
# 查看每列的非空行数
rawData.agg(*[count(c).alias(c) for c in rawData.columns]).show()
 
# 提取数据
newData = rawData.select('Date', 'Time', 'Latitude',
                         'Longitude', 'Type', 'Depth',
                         'Magnitude', 'Magnitude Type',
                         'ID', 'Source','Location Source', 
                         'Magnitude Source', 'Status')
# 拆分'Date'到'Month',' Day', 'Year'
newData = newData.withColumn('Split Date', split(newData.Date, '/'))
attrs = sc.parallelize(['Month',' Day', 'Year']).zipWithIndex().collect()
for name, index in attrs:
    newColumn = newData['Split Date'].getItem(index)
    newData = newData.withColumn(name, newColumn)
newData = newData.drop('Split Date')
newData.show(5)
 
# 上传文件至HDFS
newData.write.csv('earthquakeData.csv', header='true')