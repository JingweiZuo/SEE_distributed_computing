package Jingwei

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
//import org.apache.spark.{SparkConf, SparkContext}


object SparkPi extends java.io.Serializable{

  //use .setMaster("local") to run locally with one thread, local[4] to run locally with 4 cores
  //or .setMaster("spark://master:7077") to run on a Spark standalone cluster
  //    val conf = new SparkConf().setAppName("SparkPi").setMaster("spark://s0.adam.uvsq.fr:7077").setJars(List("/home/jingwei/jar/see_distributed_computing_2.11-0.1.jar"))

  //    val conf = new SparkConf().setAppName("SparkPi").setMaster("spark://s0.adam.uvsq.fr:7077").setJars(List("/Users/Jingwei/IdeaProjects/SEE_distributed_computing/out/artifacts/see_distributed_computing_jar/see_distributed_computing.jar"))
  //  val conf = new SparkConf().setAppName("SparkPi").setMaster("local").setJars(List("/Users/Jingwei/IdeaProjects/SEE_distributed_computing/out/artifacts/see_distributed_computing_jar/see_distributed_computing.jar"))
  //  val sc = new SparkContext(conf)
  //  sc.addJar("/home/jingwei/jar/see_distributed_computing_2.11-0.1.jar")

  //  sc.addJar("/Users/Jingwei/IdeaProjects/SEE_distributed_computing/out/artifacts/see_distributed_computing_jar/see_distributed_computing.jar")
  /*  val path = "/Users/Jingwei/Desktop/Stage_DAVID/use_see-master/Wafer/testing_wafer_train/"
    val rdd_dataset = sc.textFile(path + "csv_dataset/dataset_test.csv")
    val rdd_shapelet = sc.textFile(path + "csv_shapelet/shapelet_test.csv")*/


  val spark: SparkSession = SparkSession.builder.appName("SEE").master("spark://s0.adam.uvsq.fr:7077").getOrCreate()
  //  val spark: SparkSession = SparkSession.builder.appName("SparkPi").master("local").getOrCreate

  import spark.implicits._

  //  spark.sparkContext.addJar("/home/jingwei/jar/see_distributed_computing_2.11-0.1.jar")
  //  val path = "/Users/Jingwei/Desktop/Stage_DAVID/use_see-master/Wafer/testing_wafer_train_complet/"
  val path = "JingweiZUO/testing_wafer_train_complet/"
  val rdd_dataset = spark.sparkContext.textFile(path + "csv_dataset/dataset_test.csv")
  val rdd_shapelet = spark.sparkContext.textFile(path + "csv_shapelet/shapelet_test_modified_v2.csv").cache()
  //  val rdd_dataset = sc.textFile(path + "csv_dataset/dataset_test.csv")
  //  val rdd_shapelet = sc.textFile(path + "csv_shapelet/shapelet_test_modified_v2.csv")
  val accuracy_threshold = 0.8

  case class class_sequences(class_s:String, sequences:List[List[(String, String, String, String, String)]])
  case class class_dataset(T_name: String, class_d:String)



  //Using RDD to manipulate the data
  //pairRDD, key = T_name, value = class

  val dataset_v1 = rdd_dataset.map(_.split(';')).map(a => (a(0), a(1)))
  val dataset_v2 = rdd_dataset.map(_.split(';')).map(a => class_dataset(a(0), a(1))).toDF()

  /***************************************************************************************************************************/
  /****************************************** Step 0: Timeseries.encodage ****************************************************/
  /***************************************************************************************************************************/

  /*************Integrate the class of Timeseries into the table**************/
  case class shapelet_raw(S_name:String, S_origin:String, S_dimension:String, S_class:String, T_name:String, T_indice: String, gain:String, sub_sequence:String, distance:String)
  val shapelet_raw_df = rdd_shapelet.map(_.split(';')).map(a => shapelet_raw(a(0), a(1), a(2), a(3), a(4), a(5), a(6), a(7), a(8))).toDF()
  shapelet_raw_df.createOrReplaceTempView("shapelet_raw_df")
  val shapelet_indice_df = spark.sql("SELECT S_dimension, S_name, S_class, T_name, T_indice FROM shapelet_raw_df")
  val shapelet_indice_Tclass_df = shapelet_indice_df.join(dataset_v2, dataset_v2("T_name")===shapelet_indice_df("T_name"), "left_outer").drop(shapelet_indice_df("T_name"))
  /***************************************************************************/

  //  val shapelet_indice_non_null = rdd_shapelet.map(_.split(';')).filter(s=> s(5)!="[]").cache()
  val shapelet_indice_non_null = spark.sql("SELECT * FROM shapelet_raw_df WHERE T_indice!=\"[]\"")
  shapelet_indice_non_null.createOrReplaceTempView("shapelet_indice_non_null")
  //  pairRDD, key = (S_class, T_name), value = (S_dimension, T_indice, gain, S_name, S_class)
  //  val t_encode = shapelet_indice_non_null.rdd.map(a => ((a(3), a(4)),(a(2).toString, a(5).toString, a(6).toString, a(0).toString, a(3).toString))).groupByKey().cache()

  val agg_indice_table_T1 = new UDAF_T1()
  val t_encode = shapelet_indice_non_null.groupBy("S_class", "T_name").agg(agg_indice_table_T1(col("S_dimension"), col("T_indice"), col("gain"), col("S_name"), col("S_class")).as("udaf_T1"))
  /*************Group the Timeseries' indices for each shapelet **************/
  //pairRDD, key = (S_dimension, S_name, S_class), value= (T_name, T_indice), the only way here is using groupByKey(), not reduceByKey nor aggregateByKey
  //  val shapelet_T_map = shapelet_indice_Tclass_df.rdd.map(a => ((a(0).toString, a(1).toString, a(2).toString), (a(3).toString, a(4).toString))).groupByKey()
  val agg_indice_table_T2 = new UDAF_T2()
  val shapelet_T_map_df = shapelet_indice_Tclass_df.groupBy("S_dimension","S_name","S_class").agg(agg_indice_table_T2(col("T_name"), col("T_indice")).as("udaf_T2"))

  /***************************************************************************/

  /***************************************************************************************************************************/
  /****************************************** Step 1: Timeseries.permutation *************************************************/
  /***************************************************************************************************************************/
  //Do permutations for shapelet list
  val parse_list:(Row=>List[Seq[(String, String, String, String, String)]]) =
  (shap_list:Row) =>{
    val lists = shap_list.getSeq(0).asInstanceOf[Seq[Row]]
    var dimension : List[String] = List()
    lists.foreach(f => {
      dimension = f.getString(0) +: dimension
    })
    var lists_d_filtered : List[(String, String, String, String, String)] = List()

    lists.foreach(f => {
      if(dimension.contains(f.getString(0))){
        dimension = dimension.filter(d => d!= f.getString(0))
        lists_d_filtered = (f.getString(0),f.getString(1),f.getString(2),f.getString(3),f.getString(4)) +: lists_d_filtered
      }
    })
    val len = lists_d_filtered.size
    var i = 0
    var list_com :List[Seq[(String, String, String, String, String)]]= List()
    //    var list_per :List[List[(String, String, String, String, String)]]= List()
    // do combinations of different length
    for (i <- 1 to len){
      val list_com_i = lists_d_filtered.combinations(i).toList
      /*
         * ordering the shapelets by their indices,
         * which means the sequence exist in the original Timeseries
         */
      /********************************* à vérifier!!!!!!!!!!!!!!!!!!!!!!!!!******************************/
      //list_com_i:List[List[(String, String, String, String, String)]]
      //_._2:[index1, index2, ...]
      val list_com_i_sorted = list_com_i.map(_.sortBy(_._2.split(",")(0)))
      list_com = List.concat(list_com, list_com_i_sorted)
    }
    list_com.distinct
  }
  val udf_parse_list = udf(parse_list)
  val t_permutation = t_encode.withColumn("udf_parse_list", udf_parse_list(t_encode.col("udaf_T1"))).drop("udaf_T1")

  /*  def parse_list(lists: List[(String, String, String, String, String)]): List[List[(String, String, String, String, String)]] ={
        //take 1 shapelet for each dimension
          var dimension : List[String] = List()
          lists.foreach(f => {
            dimension = f._1 +: dimension
          })

          var lists_d_filtered : List[(String, String, String, String, String)] = List()

          lists.toList.foreach(f => {
            if(dimension.contains(f._1)){
              dimension = dimension.filter(d => d!= f._1)
              lists_d_filtered = f +: lists_d_filtered
            }
          })
          val len = lists_d_filtered.size
          var i = 0
          var list_com :List[List[(String, String, String, String, String)]]= List()
          //    var list_per :List[List[(String, String, String, String, String)]]= List()
          // do combinations of different length
          for (i <- 1 to len){
            list_com = List.concat(list_com, lists_d_filtered.toList.combinations(i).toList)
            // do permutations for each combinations
            for (j <- list_com){
              println("TEST 4")
              //list_per = List.concat(list_per, j.permutations.toList)
              /*
                * ordering the shapelets by their indices,
                * which means the sequence exist in the original Timeseries
                *
              ********************************* à vérifier!!!!!!!!!!!!!!!!!!!!!!!!!******************************/
              list_com = list_com.sortBy(_.head._2)
            }
          }
          return list_com.distinct
      }*/

  /* We can also choose the critirons to filter the sequences in the same class*/
  /*  //"a.getAs[Row](2).getSeq[Row](0).map{...}" is used to process the struct generated by "UDAF_T1"
    //    val t_map_to_Iter = t_encode.rdd.map(a=> (a.getAs[(String)](0), a.getAs[(String)](1), a.getAs[Row](2).getSeq[Row](0).map{case Row(a:String, b:String, c:String, d:String, e:String) =>(a,b,c,d,e)}))
    val t_map_to_Iter = t_encode.rdd.map(a=> ((a.getAs[(String)](0), a.getAs[(String)](1)), a.getAs[Row](2)))
    val t_map_to_Iter_temp = t_map_to_Iter.mapValues(_.getSeq[Row](0).map(row=> (row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4)))).cache()
    val t_permutation = t_map_to_Iter_temp.mapValues(_.toList).mapValues(parse_list(_))
    //MapReduce computing for each Timeseries T
    def seq0(element1:Iterable[List[(String, String, String, String, String)]], element2:Iterable[List[(String, String, String, String, String)]]):Iterable[List[(String, String, String, String, String)]] ={
      return element1 ++ element2
    }
    //Data after doing permutations; key = (class, T_name), value = List(Sequences), Sequences: List(Shapelets)
    val aggr_init = List[List[(String, String, String, String, String)]]()
    val rdd_class_seq = t_permutation.map(a => ((a.getString(0), a.getString(1)), a.getList(2))).aggregateByKey(aggr_init)(seq0, seq0).mapValues(_.toList.distinct)
  */
  //group the sequences by class, and filter the repetitive sequence in the same class
  //df_class_seq: ("S_class", "T_name", "sequences")
  //"sequences":List[List[Seq[(String, String, String, String, String)]]]
  val df_class_seq = t_permutation.groupBy("S_class").agg(collect_list("udf_parse_list") as "sequences" )
  //d.getList[List[Seq[Row]]](1): foreach: List[Seq[Row]], list of sequences

  val flattenList:(Seq[Seq[Seq[(String, String, String, String, String)]]]=> Seq[Seq[(String, String, String, String, String)]]) = (arr:Seq[Seq[Seq[(String, String, String, String, String)]]])=> {arr.flatten}
  val udf_flattenList = udf(flattenList)
  val df_class_seq_flat = df_class_seq.withColumn("sequences_flat", udf_flattenList($"sequences")).drop("sequences")

  /***************************************************************************************************************************/
  /***************************************** Step 2: Get Time Window and Accuracy*********************************************/
  /***************************************************************************************************************************/

  /***************************************************************************************************************************/
  /*****************************************Get Time Window and Accuracy(DataFrame)*******************************************/
  /***************************************************************************************************************************/

  //  import org.apache.spark.sql.functions.monotonically_increasing_id
  val df_class_seq_expl  = df_class_seq_flat.withColumn("sequence", explode($"sequences_flat")).drop("sequences_flat")
  // Join "shapelet_T_map" and "conversion(df_class_seq_expl)"
  //1. Transfer "df_class_seq_expl"
  //data:Row = (S_dimension, T_indice, S_gain, S_name, S_class)
  val shap_name :(Row => String) = (data:Row) => {
    data.get(3).toString
  }
  val shap_order :(Row => Int) = (data:Row) => {
    //indice: [A,B] -> A,B
    val indices = data.get(1).toString
    //first_indice: A
    val first_indice = indices.slice(1, indices.size - 1 ).split(",")(0)
    first_indice.toInt
  }
  val extract_shap_name = udf(shap_name)
  val extract_shap_order = udf(shap_order)
  val df_class_seq_expl_id = df_class_seq_expl.withColumn("sequence_id", monotonically_increasing_id)
  val df_class_seq_shap = df_class_seq_expl_id.withColumn("shapelet", explode($"sequence"))
  val df_class_seq_shap_id_noOrder = df_class_seq_shap.drop("sequence").withColumn("shapelet_name",extract_shap_name(df_class_seq_shap.col("shapelet")) )
  val df_class_seq_shap_id = df_class_seq_shap_id_noOrder.withColumn("shapelet_order", extract_shap_order($"shapelet"))

  //Transfer shapelet-T_indice into several columns with individual indice for each Timeseries
  def add_T_col(dataset:Broadcast[Array[(String, String)]], join_table_para:DataFrame):DataFrame ={
    var join_table_new:DataFrame = join_table_para
    dataset.value.foreach(a=> {
      val indice_val :(Row => List[String]) = (data_input:Row) => {
        var result_list:List[String] = List()
        // 'data' here is a sequence of tuple: Seq[(T_name, indices)]
        val data = data_input.getSeq[Row](0)
        data.foreach(d => {
          if(d.getString(0) == a._1){
            if (d.get(1) == "[]"){
              result_list = "x" +: result_list
            }
            else{
              val temp = d.get(1).toString.slice(1, d.get(1).toString.size -1)
              result_list = temp +: result_list
            }
          }
        })
        result_list
      }//end indice_val
      val extract_indice_val = udf(indice_val)
      join_table_new = join_table_new.withColumn(a._1 + "_" + a._2 , extract_indice_val($"udaf_T2"))
    })//end dataset_v2.foreach
    join_table_new = join_table_new.drop( "udaf_T2")
    return join_table_new
  }

  //2. Join "shapelet-T-indice" table with "Sequence-Shapelet" table
  val join_table = df_class_seq_shap_id.drop("shapelet", "S_class").join(shapelet_T_map_df, df_class_seq_shap_id("shapelet_name") === shapelet_T_map_df("S_name"), "left_outer")
  val ds_v1_collect = dataset_v1.collect()
  val dataset_broadcast = spark.sparkContext.broadcast(ds_v1_collect)
  val join_table_new = add_T_col(dataset_broadcast, join_table).cache()



  def join_T_shapelet(join_table:DataFrame): DataFrame ={
    val new_join_table = join_table.drop("shapelet_name", "S_dimension", "S_name")
    val struct_schema = join_table.drop("sequence_id", "shapelet_name", "S_dimension", "S_name", "S_class").schema
    //struct_schema: shapemet_order, T_name1, T_name2, ... T_nameN
    println("This is the struct_schema: " )
    println(struct_schema)
    println("This is the schema_size: " )
    println(struct_schema.size)
    val agg_indice_table = new UDAF_T(struct_schema, struct_schema.size)
    //Select the columns' names for aggregation
    //Méthode 1
    val col_table = struct_schema.fieldNames
    //    new_join_table.show()

    val test_result = new_join_table.groupBy("sequence_id", "S_class").agg(agg_indice_table(col_table.map(c => col("`"+c+"`")):_*).as("udaf_t"))
    //Méthode 2
    //    val col_table = new_join_table.drop("sequence_id","class_shapelet")
    //    val test_result = new_join_table.groupBy("sequence_id", "class_shapelet").agg(agg_indice_table((for (i <- col_table.columns) yield col("`"+i+"`")) :_*).as("udaf_t"))
    return  test_result
  }

  //3. Calculating Accuracy and Time window

  //fields = fields.drop(0)
  val join_table_new_T_shap = join_T_shapelet(join_table_new).cache()
  val dataset_fields = join_table_new_T_shap.schema.filter(c => c.name == "udaf_t").flatMap(_.dataType.asInstanceOf[StructType].fields).map(_.name)
//  val ds_fields_broadcast = spark.sparkContext.broadcast(dataset_fields)
  val acc_tas :((String, Row) => (Double, Seq[Int])) = (S_class:String, data:Row) => {
    // 1. get field's name,
    // 2. map field's name to dataset's value,
    // 3. compare class value between the sequence and Timeseries,
    // 4. define tp/tn/fp/fn
    // 5. calculte the accuracy
    //     Not working for the following cmds
    //     val test_result = join_T_shapelet()
    //     var fields = test_result.schema.filter(c => c.name == "udaf_t").flatMap(_.dataType.asInstanceOf[StructType].fields).map(_.name)

    var TP, TN, FP, FN = 0
    var i = 0
    var TAS_fin = Seq[Int]()
    // for each Timeseries, check the existence of pattern and calculate Time window
    //fields: (shapelet_order, T_name1, T_name2, ...)

    for(i<- 1 until dataset_fields.size){
      var pattern_found = true
      var TAS = Seq[Int]()
      val T_name = dataset_fields(i)
      //Broadcast dataset_v2.toDF()
      val T_class = T_name.split("_")(2)
      //E.g: Sort [52_4, 51_1, 53_3, 53_5] -> [51_1, 52_4, 53_3, 53_5]
      var time_window = data.getSeq[String](i).sorted
      if(time_window.contains("x")){
        //pattern not found
        pattern_found = false
      }
      else{
        //E.g: [51_1, 52_4, 53_3, 53_5] -> [0_1, 1_4, 2_3, 2_5]
        //indexOf(d) starts from 0
        time_window = time_window.map(d => time_window.indexOf(d).toString + "_" + d.split("_")(1))
        //get the first time indice of "0" as the init indice
        //init_indice_element: 0_indice
        if(TAS.size == 0){
          //time_window(0): order_indice
          //if input Row is the first shapelet in the sequence, then take the minimal indice of the shapelet in every Timeseries
          TAS = time_window(0).split("_")(1).toInt +: TAS
        }
        else{
          //take the indice which is greater than the maximal value in previous built "Time_Window"
          var order= 1
          //check that there are still elements in "time_window" list, check by ascending order
          //'indice_element' is a list which contains several indice elements, as a shapelet can appear several times in the same Timeseries with different orders
          //E.g: [0_1, 1_4, 2_3, 2_5] -> 'indice_element': [2_3, 2_5], with order =2
          var indice_element = time_window.filter(t => t.slice(0,1) == order.toString)
          //Itérer tous les indices dans 'time_window'
          while( indice_element!= null){
            var indice = Seq[Int]()
            //take all indices of the same shapelet exiting in a Timeseries
            //E.g: 'indice_element': [2_3, 2_5] -> indice = [3, 5]
            indice_element.foreach(e => indice = e.split("_")(1).toInt +: indice)
            //if no indice in the list is greater than the max value in TAS, then the pattern is not found
            //E.g: TAS = [1, 4], indice = [3, 5] -> pattern_found = true, TAS = [1, 4, 5]
            if(indice.forall(_ < TAS.max)){
              //pattern not found
              pattern_found = false
            }
            else{
              //pattern found, add the indice into TAS
              TAS = indice.filter(_ >= TAS.max).min +: TAS
            }
            order += 1
            indice_element = time_window.filter(t => t.slice(0,1) == order.toString)
          }//end while
        }
      }//end adding elements into 'TAS' for one Timeseries
      //Check shapelet's status and decide the final TAS of the shapelet
      if(T_class == S_class){
        if(pattern_found){
          TP += 1
          var TAS_temp = Seq[Int]()
          if(TAS_fin.isEmpty){
            TAS_fin = TAS
          }
          else{
            for ((a,b) <- TAS_fin.zip(TAS)){
              TAS_temp = math.max(a, b) +: TAS_temp
            }
            TAS_fin = TAS_temp
          }
        }
        else{
          FN += 1
        }
      }
      else{
        if(pattern_found){
          FP += 1
        }
        else{
          TN += 1
        }
      }
    }//end for
    var accuracy = (TP + TN).toDouble/((TP + TN + FP + FN).toDouble)
    (accuracy, TAS_fin)
  }

  val udf_acc_tas = udf(acc_tas)
  /***************************************************************************************************************************/
  /*****************************************Get Time Window and Accuracy(RDD)*************************************************/
  /***************************************************************************************************************************/

  def main(args: Array[String]) {
    //Step 0
    //    shapelet_indice_non_null.saveAsTextFile("/Users/Jingwei/Desktop/Scala_file_temp/shapelet_indice_non_null.csv")
    //    t_encode.saveAsTextFile("/Users/Jingwei/Desktop/Scala_file_temp/t_encode.csv")
    //Step 1
    /*    println("t_encode is ")
        t_encode.printSchema()
        t_encode.show()
        */
    /*    println("t_permutation is ")
        t_permutation.printSchema()
        t_permutation.show()
        println("df_class_seq is ")
        df_class_seq.printSchema()
        df_class_seq.show()
        println("df_class_seq_flat is ")
        df_class_seq_flat.printSchema()
        df_class_seq_flat.show()
        //Step 2: Join "shapelet-T-indice" table with "Sequence-Shapelet" table
        println("This is 'df_class_seq_expl':")
        df_class_seq_expl.printSchema()
        df_class_seq_expl.show()
        println("This is 'df_class_seq_expl_id':")
        df_class_seq_expl_id.printSchema()
        df_class_seq_expl_id.show()
        println("This is 'df_class_seq_shap':")
        df_class_seq_shap.printSchema()
        df_class_seq_shap.show()
        println("This is 'df_class_seq_shap_id_noOrder':")
        df_class_seq_shap_id_noOrder.printSchema()
        df_class_seq_shap_id_noOrder.show()
        println("This is 'df_class_seq_shap_id':")
        df_class_seq_shap_id.show()

        println("This is 'shapelet_T_map_df':")
        shapelet_T_map_df.show()
        println("This is Joined Table:")
        join_table.printSchema()
        join_table.take(1).foreach(println(_))
        join_table.show()*/

    println("This is Joined Table After Adding Timeseries Columns:")
    join_table_new.show()
    //    val test_result = join_T_shapelet(join_table_new)
    println("This is join_table_new_T_shap:")
    join_table_new_T_shap.printSchema()
    join_table_new_T_shap.take(1).foreach(println(_))
    join_table_new_T_shap.show()
    //Step 2: Computing the accuracy

    //Execution in the master/driver, inavoidable for the communication cost
    //    dataset_fields = join_table_new_T_shap.schema.filter(c => c.name == "udaf_t").flatMap(_.dataType.asInstanceOf[StructType].fields).map(_.name)
    //    println("This is dataset_fields:")
    //    println(dataset_fields)

    //    val ds_fields_broadcast = spark.sparkContext.broadcast(dataset_fields)
    val result_accuracy = join_table_new_T_shap.withColumn("Accuracy_TAS", udf_acc_tas($"S_class", $"udaf_t"))
    println("This is result_accuracy:")
    result_accuracy.printSchema()
    result_accuracy.show()
    result_accuracy.select($"sequence_id", $"S_class", $"udaf_t", $"Accuracy_TAS._1".as("Accuracy"), $"Accuracy_TAS._2".as("TAS"))

    /*    val test_result = join_T_shapelet()
        //Execution in the master/driver, inavoidable for the communication cost
        fields = test_result.schema.filter(c => c.name == "udaf_t").flatMap(_.dataType.asInstanceOf[StructType].fields).map(_.name)
        println(fields)
        test_result.show()
        test_result.withColumn("Accuracy_TAS", udf_acc_tas($"class_shapelet", $"udaf_t")).select($"sequence_id", $"class_shapelet", $"udaf_t", $"Accuracy_TAS._1".as("Accuracy"), $"Accuracy_TAS._2".as("TAS")).show()
    */
  }
}

