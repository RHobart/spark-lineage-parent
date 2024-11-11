如果需要解答，请加wetchat:OutOfTimeSpace
# spark-lineage-parent
跟踪Spark-sql中的字段血缘关系

## 使用方法如下:
val relObj = new new sparkLineageImpl(spark)
relObj.prettyRslt(df) 或者 relObj.prettyRsltV2(df) 都可以

具体使用文档请参考wiki

#注意：另外本代码不保证用createdataframe生成的df可以定位最初的字段，但如果是从源表读出来的，一定会定位到终端字段

字段关系流转图
![image](https://user-images.githubusercontent.com/26522622/123889277-baa32b00-d987-11eb-9b2c-3af8e53e443e.png)


