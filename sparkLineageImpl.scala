package com.yj

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LocalRelation, Project, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.{ListBuffer, Map}

class sparkLineageImpl(df:DataFrame, spark:SparkSession) {
  private val targetListSchemas = df.columns.toList
  private var targetField:List[String] = List()  // 目标字段
  private val fieldRelation:Map[String,List[String]] = Map() // 字段过程
  private val tbFieldRelation:Map[String,List[String]] = Map() // 字段与表的关系

  private val logicalMap:Map[String,List[String]] = Map()
  private val lb:ListBuffer[(String,List[String],String)] = ListBuffer() // 存放字段过程,样例{"f1"->["f2","f3"],"alias"}
  private val tableList:ListBuffer[String] = ListBuffer()
  /*
   获取目标到源字段以及表之间的对应关系
   */
  private def fieldLiveLine(tf:String,cl:ListBuffer[(String,String)]): Unit ={
    fieldRelation.get(tf).foreach(childList=>{
      childList.foreach(f=>{
        var flg = 1
        tbFieldRelation.foreach(t=>{
          if(t._2.contains(f)){
            cl.append((f,t._1))
            flg = 0
          }
        })
        if(flg == 1) fieldLiveLine(f,cl)
      })
    })
  }

  /*
    追踪目标字段是由哪些源字段计算得来的
   */
  private def traceFieldLineMap(df:DataFrame): Unit = {
    // 获取表名
    df.queryExecution.logical.collect{
      case u:UnresolvedRelation => {
        tableList.append(u.tableIdentifier.table)
      }
    }

    var count = 0
    df.queryExecution.analyzed.collect{
      case ag: Aggregate => {
        count = count + 1
        val tmp1 = ag.aggregateExpressions.map(r=>(r.verboseString,r.references.toList.map(_.toString()),r.prettyName))
        val ot = ag.output.map(_.toString())
        if(ot.map(_.split("#")(0)).equals(targetListSchemas) && count == 1) targetField = ot.toList
        ot.foreach(o=>{
          tmp1.foreach(t=>{
            if(t._1.contains(o)) lb.append((o,t._2,t._3)) //确定结果字段来源于多个源字段
          })
        })
      }
      case proj:Project => {
        count = count + 1
        val tmp1 = proj.projectList.toList.map(r=>(r.verboseString,r.references.toList.map(_.toString()),r.prettyName))
        val ot = proj.output.map(_.toString())
        if(ot.map(_.split("#")(0)).equals(targetListSchemas) && count == 1) targetField = ot.toList
        ot.foreach(o=>{
          tmp1.foreach(t=>{
            if(t._1.contains(o)) lb.append((o,t._2,t._3)) //确定结果字段来源于多个源字段
          })
        })
      }
      case sa:SubqueryAlias => {
        sa.child.collect {
          case lr:LocalRelation => {
            tableList.foreach{r=>{
              if(r.equals(sa.alias)) tbFieldRelation += (sa.alias->lr.output.map(_.toString()).toList)
            }}
          }
          case ds:LogicalRelation => {
            tableList.foreach{r=>{
              if(r.equals(sa.alias)) tbFieldRelation += (sa.alias->ds.output.map(_.toString()).toList)
            }}
          }
        }
      }
    }
  }

  def getRslt():Map[String,List[(String,String)]] ={
    val retRslt:Map[String,List[(String,String)]] = Map() // 返回结果
    traceFieldLineMap(df)
    lb.foreach(l=>{
      lb.foreach(c=>{
        if(l._1 == c._1){
          if(l._3.equals("alias")) fieldRelation += (l._1->l._2) // 如果key:字段名称 相等，则去alias的目标字段
        }else{
          fieldRelation += (l._1->l._2)
        }
      })
    })
    targetField.foreach(e=>{
      val ls:ListBuffer[(String,String)] = ListBuffer()
      fieldLiveLine(e,ls)
      retRslt += (e->ls.toList.distinct)
    })
    retRslt
  }

  def getVar(): Unit ={
    traceFieldLineMap(df)
    lb.foreach(l=>{
      lb.foreach(c=>{
        if(l._1 == c._1){
          if(l._3.equals("alias")) fieldRelation += (l._1->l._2)
        }else{
          fieldRelation += (l._1->l._2)
        }
      })
    })
    println("----------------targetField-------------------")
    targetField.foreach(r=>println("targetField:"+r))
    println("----------------tableNameField-------------------")
    tbFieldRelation.foreach(r=>println("tableNameField:"+r))
    println("----------------fieldName-------------------")
    fieldRelation.foreach(r=>println("fieldName:"+r))
  }
}
