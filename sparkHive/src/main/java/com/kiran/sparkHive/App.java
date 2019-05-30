package com.kiran.sparkHive;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        
        SparkSession session = SparkSession
        		.builder()
        		.appName("Spark sql Hive")
        		.master("local[3]")
        		.enableHiveSupport()
        		.getOrCreate();
        
 /*       session.sql("CREATE TABLE IF NOT EXISTS src1 (key INT, value STRING) USING hive");
       
        session.sql("LOAD DATA LOCAL INPATH '/Users/KP/Documents/workspace-sts-3.9.0.RELEASE/spark-hive-files/in/kv1.txt' INTO TABLE src1");
        
        
        session.sql("INSERT OVERWRITE LOCAL DIRECTORY '/Users/KP/Documents/workspace-sts-3.9.0.RELEASE/spark-hive-files/out' \r\n" + 
        		"ROW FORMAT DELIMITED \r\n" + 
        		"FIELDS TERMINATED BY ',' \r\n" + 
        		"select * from src1");
        
        session.sql("SELECT * FROM src").show();
   */     
  /*      
        session.sql("CREATE TABLE IF NOT EXISTS T100KSALES1 (Region STRING, Country STRING, ItemType STRING, SalesChannel STRING, OrderPriority STRING, OrderDate STRING, OrderID STRING, ShipDate STRING, UnitsSold STRING, UnitPrice STRING, UnitCost STRING, TotalRevenue STRING, TotalCost STRING, TotalProfit STRING) row format delimited fields terminated by ','");
        
        session.sql("LOAD DATA LOCAL INPATH '/Users/KP/Documents/workspace-sts-3.9.0.RELEASE/spark-hive-files/in/100000 Sales Records.csv' INTO TABLE T100KSALES1");
        
        session.sql("SELECT * FROM T100KSALES1").show();
  */
        
        Dataset<Row> salesDF = session.sql("SELECT * FROM T100KSALES1 limit 10");
        
        Dataset<String> salesDS = salesDF.toJSON();
        
        salesDS.show();
        
        salesDF.rdd().saveAsTextFile("/Users/KP/Documents/workspace-sts-3.9.0.RELEASE/spark-hive-files/out1");
        
        
        /*
        session.sql("SELECT Count(*) FROM T100KSALES1").show();
        
        session.sql("INSERT OVERWRITE LOCAL DIRECTORY '/Users/KP/Documents/workspace-sts-3.9.0.RELEASE/spark-hive-files/out' \r\n" + 
        		"ROW FORMAT DELIMITED \r\n" + 
        		"FIELDS TERMINATED BY '|' \r\n" + 
          		"select * from T100KSALES1");
      */
        
        session.close();
    }
}
