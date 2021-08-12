package com.yj

import org.apache.log4j._
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Project, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.json4s.{Formats, NoTypeHints}

import scala.collection.mutable._
import scala.util.control.Breaks.{break, breakable}

class SparkLineageImpl(spark:SparkSession){
  val logger = Logger.getLogger("SparkLineageImplV3")
  def getLineageStream(df:DataFrame):String ={
    val retErrorMsg = ""
    try {
      var targetFields: List[String] = List()
      val srcTabFields: Map[String, List[String]] = Map()
      val fieldProcess: Map[String, List[String]] = Map()
      val retRslt: Map[String, List[(String, String)]] = Map() //结果集

      var tmpTargetFields: List[String] = List()
      val tmpProjectFields: ListBuffer[List[String]] = ListBuffer()
      val tmpFieldProcess: ListBuffer[(String, String, List[String])] = ListBuffer()
      val tmpDfCols:List[String] = df.columns.toList
      var totalTables:List[String] = List()

      // 获取所有表名包括创建的临时表名或者临时视图
      totalTables = spark.catalog.listTables().collect().map(row=>row.database+"#"+row.name).toList

      // 获取字段流转过程
      df.queryExecution.analyzed.collect {
        case ctas: CreateHiveTableAsSelectCommand => {
          tmpTargetFields = ctas.outputColumnNames.toList
        }
        case itht: InsertIntoHiveTable => {
          tmpTargetFields = itht.outputColumnNames.toList
        }
        case agg: Aggregate => {
          val aList = agg.aggregateExpressions.map(a => a.name + "#" + a.exprId.id).toList
          tmpProjectFields.append(aList)
          agg.aggregateExpressions.foreach(a => tmpFieldProcess.append((a.name + "#" + a.exprId.id.toString, a.prettyName, a.references.toList.map(_.toString()))))
        }
        case proj: Project => {
          val plist = proj.projectList.map(p => p.name + "#" + p.exprId.id).toList
          tmpProjectFields.append(plist)
          proj.projectList.foreach(p => tmpFieldProcess.append((p.name + "#" + p.exprId.id.toString, p.prettyName, p.references.toList.map(_.toString))))
        }
        case htr: HiveTableRelation => {
          val dbname = htr.tableMeta.identifier.database.getOrElse("")
          val tbname = htr.tableMeta.identifier.table
          srcTabFields += (dbname + "#" + tbname -> htr.output.map(_.toString()).toList)
        }
        case sa: SubqueryAlias => {
          sa.child.collect {
            case lr:LogicalRelation => {
              if(lr.argString.startsWith("JDBCRelation")){
                val tb = lr.argString.split(",")(0).split("\\s")(0)
                  .replace("JDBCRelation","")
                  .replace("(","")
                  .replace(")","")
                srcTabFields += ("#"+tb->lr.output.map(_.toString()).toList)
              }else{
                totalTables.foreach(s => {if (s.split("#")(1).equals(sa.alias)) srcTabFields += (s -> sa.output.map(_.toString()).toList)})
              }
            }
          }
        }
        case lr:LogicalRelation => {
          if(lr.argString.startsWith("JDBCRelation")){
            val tb = lr.argString.split(",")(0).split("\\s")(0)
              .replace("JDBCRelation","")
              .replace("(","")
              .replace(")","")
            srcTabFields += ("#"+tb->lr.output.map(_.toString()).toList)
          }
        }
      }

      // 确定其目标字段
      targetFields = if (!tmpTargetFields.isEmpty && tmpProjectFields.head.map(e => e.split("#")(0)).equals(tmpTargetFields)
        && tmpProjectFields.head.map(e => e.split("#")(0)).equals(tmpDfCols)) {
        tmpProjectFields.head
      }else if(tmpTargetFields.isEmpty && !tmpDfCols.isEmpty){
        val flatProject:ListBuffer[String] = ListBuffer()
        tmpProjectFields.foreach(child=>child.foreach(flatProject.append(_)))
        if(flatProject.toList.take(tmpDfCols.length).map(_.split("#")(0)).equals(tmpDfCols)){
          flatProject.toList.take(tmpDfCols.length)
        }else{
          List()
        }
      }else {
        List()
      }

      // 确定字段流转过程
      tmpFieldProcess.foreach(lvl1 => {
        tmpFieldProcess.foreach(lvl2 => {
          if (lvl1._1 == lvl2._1) {
            if (lvl1._1.equals("alias")) fieldProcess += (lvl1._1 -> lvl1._3)
          } else fieldProcess += (lvl1._1 -> lvl1._3)
        })
      })

      // 输出结果
      targetFields.foreach(tf => {
        val detailProcess: ListBuffer[(String, String)] = ListBuffer()
        searchLineage(tf, detailProcess, fieldProcess, srcTabFields)
        retRslt += (tf -> detailProcess.toList.distinct)
      })

      // 返回结果值
      prettyRsltV2(retRslt)
    }catch {
      case ex:Exception => {
        logger.error("血缘关系无法解析:"+ex.getCause)
        retErrorMsg
      }
    }
  }


  // 查询目标字段与源表字段的关系
  private def searchLineage(tf:String,rdp:ListBuffer[(String,String)],fieldProcess:Map[String,List[String]],srcTabFields:Map[String,List[String]]): Unit ={
    fieldProcess.getOrElse(tf,None) match {
      case None => logger.warn("...None value:"+tf)
      case _ => fieldProcess.get(tf).foreach(childList=>{
        childList.foreach(f=>{
          var iterflg = true
          srcTabFields.foreach(t=>{
            if(t._2.contains(f)){
              rdp.append((f,t._1))
              iterflg = false
            }
          })
          breakable({
            if(tf==f) break()
            if(iterflg == true) searchLineage(f,rdp,fieldProcess,srcTabFields)
          })
        })
      })
    }
  }


  // 格式化结果
  private def prettyRslt(rslt:Map[String,List[(String,String)]]):String = {
    val tmp:Map[String,List[List[String]]] = Map()
    rslt.foreach(e=>{tmp += (e._1.split("#")(0)->e._2.map(f=>List(f._1.split("#")(0),f._2)))})
    val ret: Predef.Map[String, List[List[String]]] = tmp.toMap
    implicit val formats:AnyRef with Formats = Serialization.formats(NoTypeHints)
    write(ret)
  }

  private def prettyRsltV2(rslt:Map[String,List[(String,String)]]):String = {
    val tmp:Map[String,List[List[String]]] = Map()
    val tmp2:Map[String,List[String]] = Map()
    rslt.foreach(e=>{tmp2 += (e._1.split("#")(0)->e._2.map(f=>f._2+"."+f._1.split("#")(0)))})
    val ret: Predef.Map[String, List[String]] = tmp2.toMap
    implicit val formats:AnyRef with Formats = Serialization.formats(NoTypeHints)
    write(ret)
  }

}
