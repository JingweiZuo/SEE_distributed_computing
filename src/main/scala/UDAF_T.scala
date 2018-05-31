package Jingwei

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructType}

class UDAF_T(schema:StructType, num:Int) extends UserDefinedAggregateFunction {

  val bucketSize = 1000
  override def inputSchema: org.apache.spark.sql.types.StructType = this.schema

  override def bufferSchema: StructType = inputSchema

/*  override def dataType: DataType = StructType(
      StructField("Order", IntegerType, true) ::
      StructField("T1", ArrayType(StringType, true)) ::
      StructField("T2", ArrayType(StringType, true)) ::
      StructField("T3", ArrayType(StringType, true)) :: Nil)*/
  override def dataType: DataType = inputSchema

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    var i = 0
    //buffer.update(0, 0)
    buffer(0) = 0
    for (i <-1 until num){
      //buffer.update(i, Seq[Int](bucketSize))
      buffer(i)= Seq[String]()
    }
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var i = 0
    //Every shapelet appears one time in a sequence, in order to calculate how many shapelet in a sequence
    buffer(0) = buffer.getAs[Int](0) + 1
    for (i <-1 until num) {
      //if the indices contain 'x', which means the shapelet doesn't exist in the sequence, then -> do nothing, just merge the indices
      if(input.getAs[Seq[Int]](i).contains("x") ){
        //Pattern non found
        if (buffer(i) == null){
          buffer(i) = input.getAs[Seq[String]](i)
        }
        else{
          buffer(i) = buffer.getAs[Seq[String]](i) ++ input.getAs[Seq[String]](i)
        }
      }
      else{
          //put the entire list of the indice prefixed into the buffer
          var input_prefixed = Seq[String]()
          input.getAs[Seq[String]](i).foreach(a => {
            //input.getAs[Int](0) is the order of shapelet in the sequence
            //input_prefixed format: order_indice
            //a: [1,2,3,4,5,...], we need to change the format for all elements in 'a'
            //input_prefixed: [indice_1,indice_2,indice_3,indice_4,indice_5,...]
            a.split(", ").foreach(a_new => {
              input_prefixed = (input.getAs[Int](0).toString + "_" + a_new) +: input_prefixed
            })
          })
          if (buffer(i) == null){
            buffer(i) = input_prefixed
          }
          else{
            buffer(i) = buffer.getAs[Seq[String]](i) ++ input_prefixed
          }
      }
    }
  }

  // Merge two partial aggregates
  // Same as the functionality of "AggregateByKey(value_init)(fonc1, fonc2)" in Spark, where
  // "value_init => initialize(), fonc1 => update(), fonc2 => merge()"
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var i = 0
    //Statistical number of shapelets in a sequence
    buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
    for (i <-1 until num){
      buffer1(i) = buffer1.getAs[Seq[String]](i) ++ buffer2.getAs[Seq[String]](i)
    }
  }

  override def evaluate(buffer: Row)=
    buffer
//    Row(buffer.getInt(0)), for (i <- 0 until num) yield buffer.getSeq[String](i):_*)
//    Row(buffer.getInt(0), buffer.getSeq[String](1), buffer.getSeq[String](2), buffer.getSeq[String](3))
}
