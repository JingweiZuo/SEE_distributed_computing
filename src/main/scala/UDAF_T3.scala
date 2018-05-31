package Jingwei

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructField, StructType, StringType, ArrayType, IntegerType}

class UDAF_T3() extends UserDefinedAggregateFunction {

  val bucketSize = 1000
  override def inputSchema: StructType = {
    //    S_dimension, T_indice, gain, S_name, S_class
    StructType(StructField("S_dimension", StringType)
      :: StructField("T_indice", StringType)
      :: StructField("gain", StringType)
      :: StructField("S_name", StringType)
      :: StructField("S_class", StringType)
      ::Nil )
  }

  override def bufferSchema: StructType = {
    StructType(StructField("Collect_Seq", ArrayType(StructType(
      StructField("S_dimension", StringType)
        ::StructField("T_indice", StringType)
        ::StructField("gain", StringType)
        ::StructField("S_name", StringType)
        ::StructField("S_class", StringType)
        ::Nil)))::Nil)
  }

  override def dataType: DataType = {
    StructType(StructField("Collect_Seq", ArrayType(StructType(
      StructField("S_dimension", StringType)
        ::StructField("T_indice", StringType)
        ::StructField("gain", StringType)
        ::StructField("S_name", StringType)
        ::StructField("S_class", StringType)
        ::Nil)))::Nil)
  }

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Seq[(String, String, String, String, String)]()
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //Add input row into Array
    val new_data = (input.getString(0), input.getString(1), input.getString(2), input.getString(3),input.getString(4))
    buffer(0) =  buffer.getAs[Seq[(String, String, String, String, String)]](0) :+ new_data
  }

  // Merge two partial aggregates
  // Same as the functionality of "AggregateByKey(value_init)(fonc1, fonc2)" in Spark, where
  // "value_init => initialize(), fonc1 => update(), fonc2 => merge()"
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Seq[(String, String, String, String, String)]](0) ++ buffer2.getAs[Seq[(String, String, String, String, String)]](0)
  }

  override def evaluate(buffer: Row)=
    buffer
  //    Row(buffer.getInt(0)), for (i <- 0 until num) yield buffer.getSeq[String](i):_*)
  //    Row(buffer.getInt(0), buffer.getSeq[String](1), buffer.getSeq[String](2), buffer.getSeq[String](3))
}
