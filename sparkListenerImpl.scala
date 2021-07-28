import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, InsertIntoTable, LocalRelation, Project, SubqueryAlias}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.util.QueryExecutionListener
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.apache.log4j.Logger
import scala.collection.mutable._
import scala.util.control.Breaks.{break, breakable}

class sparkListenerImpl extends QueryExecutionListener{
  val log = Logger.getLogger("sparkListenerImpl")
  private var targetFields:List[String] = List() //目标字段
  private val fieldProcess:Map[String,List[String]] = Map() //记录字段流转过程
  private val srcTabFields:Map[String,List[String]] = Map() //源表以及字段
  private val retRslt:Map[String,List[(String,String)]] = Map() //结果集

  private val tmpFieldProcess:ListBuffer[(String,String,List[String])] = ListBuffer()
  private val tmpTargetFields:ListBuffer[String] = ListBuffer()
  private val tmpProjectFields:ListBuffer[List[String]] = ListBuffer()


  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    getLineageProcess(qe:QueryExecution)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = ???

  private def getLineageProcess(qe:QueryExecution) = {
      log.info("Starting recording field process!")
      qe.analyzed.collect{
        case proj:Project => {
          tmpProjectFields.append(proj.output.toList.map(f=>f.name+"#"+f.exprId.id))
          proj.projectList.foreach(p=>tmpFieldProcess.append((p.name+"#"+p.exprId.id.toString,p.prettyName,p.references.toList.map(_.toString))))
        }
        case agg:Aggregate => {
          agg.aggregateExpressions.foreach(a=>tmpFieldProcess.append((a.name+"#"+a.exprId.id.toString,a.prettyName,a.references.toList.toList.map(_.toString()))))
        }
        case htb:HiveTableRelation => {
          val tbname = htb.tableMeta.identifier.database.get+"#"+htb.tableMeta.identifier.table
          val fields = htb.dataCols.map(f=>(f.name+"#"+f.exprId.id)).toList
          srcTabFields += (tbname->fields)
        }
        case lgr:LogicalRelation => {
          val tbname = lgr.catalogTable.get.identifier.database.get + "#" +lgr.catalogTable.get.identifier.table
          val fields = lgr.output.map(f=>(f.name+"#"+f.exprId.id)).toList
          srcTabFields += (tbname->fields)
        }
        case sa:SubqueryAlias => {
          val tbname = sa.alias
          sa.child.collect{
            case lr:LocalRelation => {
              srcTabFields += (tbname -> lr.output.map(_.toString()).toList)
            }
          }
        }
      }

    // 如果存在Aias，则取Aias对应的数据，否则取Reference对象的数据
    removeDuplicateFieldProcess(tmpFieldProcess)

    // 如果临时目标字段集合为空，则取投影集合中的第一个元素。
    if(!tmpTargetFields.isEmpty){
      targetFields = tmpTargetFields.toList
    }else{
      targetFields = if(!tmpProjectFields.isEmpty) tmpProjectFields.head else List()
    }

    // 输出字段血缘分析结果
    targetFields.foreach(tf=>{
      val detailProcess:ListBuffer[(String,String)] = ListBuffer()
      searchLineage(tf,detailProcess)
      retRslt += (tf->detailProcess.toList.distinct)
    })
    log.info(prettyRslt(retRslt))
  }

  /**
   *
   * @param fp:field process
   */
  private def removeDuplicateFieldProcess(fp:ListBuffer[(String,String,List[String])])={
    fp.foreach(f1=>{
      fp.foreach(f2=>{
        if(f1._1.equals(f2._1)){
          if(f1._2.equals("alias")) fieldProcess += (f1._1->f1._3)
        }else{
          fieldProcess += (f1._1->f1._3)
        }
      })
    })
  }

  /**
   *
   * @param tf:target field
   * @param rdp:record detail process
   */
  private def searchLineage(tf:String,rdp:ListBuffer[(String,String)]): Unit ={
    fieldProcess.getOrElse(tf,None) match {
      case None => log.warn("None value:"+tf)
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
            if(iterflg == true) searchLineage(f,rdp)
          })
        })
      })
    }
  }

  def prettyRslt(rslt:Map[String,List[(String,String)]]):String = {
    val tmp:Map[String,List[List[String]]] = Map()
    rslt.foreach(e=>{
      tmp += (e._1.split("#")(0)->e._2.map(f=>List(f._1.split("#")(0),f._2)))
    })
    val ret: Predef.Map[String, List[List[String]]] = tmp.toMap
    implicit val formats:AnyRef with Formats = Serialization.formats(NoTypeHints)
    write(ret)
  }



  def getVar()={
    println("tmpProjectFields:"+tmpProjectFields)
    println("srcTabFields:"+srcTabFields)
    println("targetFields:"+targetFields)
    println("fieldProcess:"+fieldProcess)
    println("tmpFieldProcess:"+tmpFieldProcess)
  }

}
