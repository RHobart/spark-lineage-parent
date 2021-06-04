# spark-lineage-parent
跟踪Spark-sql中的字段血缘关系

# 步骤如下:
step1. 将sparkLineageImpl.scala 添加到工程中的任何一个位置
step2. 实例化一个对象,需要传入两个参数有,分别为:(DataFrame,SparkSession)
setp3. 调用对象中的getRslt()方法就可以获取到字段的字段端到端的关系图

# 测试样例一：
val spark  = SparkSession.builder().master("local[*]").enableHiveSupport().getOrCreate()
spark.createDataFrame(Seq(("1","2","3"))).toDF("id","id2","id3").createOrReplaceTempView("uuu")
val df = spark.sql("select * from uuu")
val tx = new sparkLineageImpl(df,spark)
print(tx.getRslt())

# 结果如下:
Map(id3#2434 -> List((_3#2428,uuu)), id2#2433 -> List((_2#2427,uuu)), id#2432 -> List((_1#2426,uuu)))

# 测试样例二：

val df2 = spark.createDataFrame(Seq(("1",92.1))).toDF("id","score")
val df3 = spark.createDataFrame(Seq(("1",92.3))).toDF("id","score")
df3.createOrReplaceTempView("tx")
df2.createOrReplaceTempView("tx2")

val df2 = spark.sql("""select * from (select * from tx union all select * from tx2) tmp""")
val tx2 = new sparkLineageImpl(df2,spark)
print(tx2.getRslt())

# 结果如下：
Map(id#12 -> List((_1#8,tx)), score#13 -> List((_2#9,tx)))

*注意：对于正常从表来的数据是可以正常显示全部的字段关系
