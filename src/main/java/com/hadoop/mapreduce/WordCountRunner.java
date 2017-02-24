package com.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountRunner {
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin-master");
		System.setProperty("HADOOP_USER_NAME", "root");
		Configuration conf = new Configuration();
		try {
			Job wcjob = Job.getInstance(conf);
			conf.set("mapreduce.job.jar", "wcount.jar");
			
			//设置wcjob中的资源所在的jar包
			wcjob.setJarByClass(WordCountRunner.class);
			
			
			//wcjob要使用哪个mapper类
			wcjob.setMapperClass(WordCountMapper.class);
			//wcjob要使用哪个reducer类
			wcjob.setReducerClass(WordCountReduce.class);
			
			//wcjob的mapper类输出的kv数据类型
			wcjob.setMapOutputKeyClass(Text.class);
			wcjob.setMapOutputValueClass(LongWritable.class);
			
			//wcjob的reducer类输出的kv数据类型
			wcjob.setOutputKeyClass(Text.class);
			wcjob.setOutputValueClass(LongWritable.class);
			
			//指定要处理的原始数据所存放的路径
			FileInputFormat.setInputPaths(wcjob, "/tmp/test2.txt");
		
			//指定处理之后的结果输出到哪个路径
			FileOutputFormat.setOutputPath(wcjob, new Path("/tmp/outputa.txt"));
			
			boolean res = wcjob.waitForCompletion(true);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
