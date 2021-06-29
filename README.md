# spark-lineage-parent
跟踪Spark-sql中的字段血缘关系

具体使用文档请参考wiki

#注意：另外本代码不保证用createdataframe生成的df可以定位最初的字段，但如果是从源表读出来的，一定会定位到终端字段

#所谓源表就是带有表头信息的，如果单纯定义df出现的关系图谱不一定看的懂：如下：
val df = spark.createDataFrame(Seq(("1","test"))).toDf("id","name")

#这样的方式创建dataframe后续追踪的关系图谱就不一定看的懂，但是按照以下方式创建却是正常的：
case class schema(id:String,name:String)
val df = spark.createDataFrame(Seq(("1","test"))).toDf("id","name").as[schema]

其他的类似于orc,parquet 格式都是本身自带表头信息的，直接用就行

![image](https://user-images.githubusercontent.com/26522622/123764351-47ea6f00-d8f7-11eb-9599-0d9ca1313661.png)

