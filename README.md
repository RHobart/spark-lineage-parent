# spark-lineage-parent
跟踪Spark-sql中的字段血缘关系

## 可以直接下载jar包进行spark字段关系进行处理，处理方法如下:
val relObj = new new sparkLineageImplV1(spark)
relObj.prettyRslt(df) 或者relObj.commRslt(df) 都可以获取到结果

具体使用文档请参考wiki

#注意：另外本代码不保证用createdataframe生成的df可以定位最初的字段，但如果是从源表读出来的，一定会定位到终端字段

#所谓源表就是带有表头信息的，如果单纯定义df出现的关系图谱不一定看的懂：如下：
val df = spark.createDataFrame(Seq(("1","test"))).toDf("id","name")


字段关系流转图
![image](https://user-images.githubusercontent.com/26522622/123889277-baa32b00-d987-11eb-9b2c-3af8e53e443e.png)


