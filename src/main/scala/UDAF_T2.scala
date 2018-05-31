package Jingwei

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class UDAF_T2() extends UserDefinedAggregateFunction {

  val bucketSize = 1000
  override def inputSchema: StructType = {
//    S_dimension, T_indice, gain, S_name, S_class
    StructType(StructField("S_name", StringType)
      :: StructField("T_indice", StringType)
      ::Nil )
  }

  override def bufferSchema: StructType = {
    StructType(StructField("Collect_T", ArrayType(StructType(
        StructField("T_name", StringType)
        ::StructField("T_indice", StringType)
        ::Nil)))::Nil)
  }

  override def dataType: DataType = {
    StructType(StructField("Collect_T", ArrayType(StructType(
      StructField("T_name", StringType)
        ::StructField("T_indice", StringType)
        ::Nil)))::Nil)
  }

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Seq[(String, String)]()
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //Add input row into Array
    val new_data = (input.getString(0), input.getString(1))
    buffer(0) =  buffer.getAs[Seq[(String, String)]](0) :+ new_data
  }

  // Merge two partial aggregates
  // Same as the functionality of "AggregateByKey(value_init)(fonc1, fonc2)" in Spark, where
  // "value_init => initialize(), fonc1 => update(), fonc2 => merge()"
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Seq[(String, String)]](0) ++ buffer2.getAs[Seq[(String, String)]](0)
  }

  override def evaluate(buffer: Row)=
    buffer
//    Row(buffer.getInt(0)), for (i <- 0 until num) yield buffer.getSeq[String](i):_*)
//    Row(buffer.getInt(0), buffer.getSeq[String](1), buffer.getSeq[String](2), buffer.getSeq[String](3))
}
