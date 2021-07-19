package com.yj

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LocalRelation, Project, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.json4s.{Formats, NoTypeHints}

import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.mutable.{ListBuffer, Map}
import scala.util.control.Breaks.{break, breakable}


class sparkLineageImpl(spark:SparkSession) {
  private var targetListSchemas:List[String] = List()

  private var targetField:List[String] = List()
  private val fieldRelation:Map[String,List[String]] = Map()
  private val tbFieldRelation:Map[String,List[String]] = Map()

  private val recordFieldProcess:ListBuffer[(String,List[String],String)] = ListBuffer() // store field process
  private val tableList:ListBuffer[String] = ListBuffer() // target tables

  clearTmpRslt() // 每次先清空数据结构，防止上次结果集驻留


  /*
   自动寻找字段关系
   */
  private def searchLineage(tf:String,cl:ListBuffer[(String,String)]): Unit ={
    fieldRelation.getOrElse(tf,None) match {
      case None => println("None value:"+tf)
      case _ => fieldRelation.get(tf).foreach(childList=>{
        childList.foreach(f=>{
          var iterflg = 1
          tbFieldRelation.foreach(t=>{
            if(t._2.contains(f)){
              cl.append((f,t._1))
              iterflg = 0
            }
          })
          breakable({
            if(tf==f) break()
            if(iterflg == 1) searchLineage(f,cl)
          })
        })
      })
    }
  }

  /*
    记录字段流转过程
   */
  private def traceFieldLineMap(df:DataFrame): Unit = {
    // 获取表名
    df.queryExecution.logical.collect{
      case u:UnresolvedRelation => {
        if(u.tableName.contains(".")) tableList.append(u.tableName.split('.')(1)) else tableList.append(u.tableName)
      }
    }

    var count = 0
    df.queryExecution.analyzed.collect{
      case crt:CreateHiveTableAsSelectCommand => {
        if(count == 0) targetListSchemas = crt.outputColumnNames.toList
      }
      case ins:InsertIntoHiveTable => {
        if(count == 0) targetListSchemas = ins.table.schema.fields.toList.map(f=>f.name)
      }
      case ag:Aggregate => {
        count = count + 1
        ag.aggregateExpressions.foreach{a=>{recordFieldProcess.append((a.name+"#"+a.exprId.id.toString,a.references.map(_.toString()).toList,a.prettyName))}

        val ot = ag.output.map(_.toString())
        if(ot.map(_.split("#")(0)).equals(targetListSchemas) && count == 1) targetField = ot.toList
        }
      }
      case proj:Project => {
        count = count + 1
        proj.projectList.toList.foreach{r=>recordFieldProcess.append((r.name+"#"+r.exprId.id.toString,r.references.toList.map(_.toString()),r.prettyName))}
        val ot = proj.output.map(_.toString())
        if(ot.map(_.split("#")(0)).equals(targetListSchemas) && count == 1) targetField = ot.toList
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
          case hivetable:HiveTableRelation => {
            tableList.foreach { r=>{
              if(r.equals(sa.alias)) tbFieldRelation += (sa.alias->hivetable.output.map(_.toString()).toList)
            }
            }
          }
        }
      }
    }
  }

  private def getLineageRel(df:DataFrame):Map[String,List[(String,String)]] ={
    targetListSchemas = df.columns.toList
    val retRslt:Map[String,List[(String,String)]] = Map() // 返回结果
    traceFieldLineMap(df)
    recordFieldProcess.foreach(l=>{
      recordFieldProcess.foreach(c=>{
        if(l._1 == c._1){
          if(l._3.equals("alias")) fieldRelation += (l._1->l._2) // 如果key:字段名称 相等，则去alias的目标字段
        }else{
          fieldRelation += (l._1->l._2)
        }
      })
    })
    targetField.foreach(e=>{
      val ls:ListBuffer[(String,String)] = ListBuffer()
      searchLineage(e,ls)
      retRslt += (e->ls.toList.distinct)
    })
    retRslt
  }

  def prettyRslt(df:DataFrame):String = {
    val rslt = getLineageRel(df)
    val tmp:Map[String,List[List[String]]] = Map()
    rslt.foreach(m=>tmp+=(m._1.split("#")(0)->m._2.map(e=>List(e._1.split("#")(0),e._2))))
    val ret: Predef.Map[String, List[List[String]]] = tmp.toMap
    implicit val formats:AnyRef with Formats = Serialization.formats(NoTypeHints)
    write(ret)
  }

  def commRslt(df:DataFrame):Map[String,List[String]] = {
    val rslt = getLineageRel(df)
    val ret:Map[String,List[String]] = Map()
    rslt.foreach(m=>ret+=(m._1.split("#")(0)->m._2.map(e=>e._2+"."+e._1.split("#")(0))))
    ret
  }

  def getVar(df:DataFrame): Unit ={
    traceFieldLineMap(df)
    recordFieldProcess.foreach(l=>{
      recordFieldProcess.foreach(c=>{
        if(l._1 == c._1){
          if(l._3.equals("alias")) fieldRelation += (l._1->l._2)
        }else{
          fieldRelation += (l._1->l._2)
        }
      })
    })
    println("target Field:------------------------->")
    targetField.foreach(r=>println("targetField:"+r))
    println("source table Field:------------------------->")
    tbFieldRelation.foreach(r=>println("tableNameField:"+r))
    println("field Relation :------------------------->")
    fieldRelation.foreach(r=>println("fieldName:"+r))
  }

  private def clearTmpRslt(): Unit ={
    targetListSchemas clear()
    targetField.clear()
    fieldRelation.clear()
    tbFieldRelation.clear()
    recordFieldProcess.clear()
    tableList.clear()
  }
}
