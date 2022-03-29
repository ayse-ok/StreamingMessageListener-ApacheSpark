package com.apache.spark.StreamingListener;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class App {
    public static void main( String[] args ) throws StreamingQueryException{
    	System.setProperty("hadoop.home.dir","C:\\hadoop-common-2.2.0-bin-master");
    	
    	SparkSession session = SparkSession.builder().appName("StreamingMessageListener").master("local").getOrCreate();
    	Dataset<Row> rawData = session.readStream().format("socket").option("host", "localhost").option("port", 8005).load();
    	Dataset<String> data = rawData.as(Encoders.STRING());
    	
    	//mesajdaki her satır data içinde bir string. 
    	Dataset<String> dataset = data.flatMap(new FlatMapFunction<String,String>() {		
			private static final long serialVersionUID = 1L;

			public Iterator<String> call(String s) throws Exception {	
			//	string datayı kelimelerini ayırıyoruz
				return Arrays.asList(s.split(" ")).iterator();
			}
		},Encoders.STRING()); 
    	
    	Dataset<Row> groupedData = dataset.groupBy("value").count();
    	StreamingQuery start = groupedData.writeStream().outputMode("complete").format("console").start();  // complete olunca console a yaz
    	start.awaitTermination();  // programı kapatana kadar ekrana yazar
    }
}
