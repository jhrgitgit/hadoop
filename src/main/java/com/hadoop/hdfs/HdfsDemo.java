package com.hadoop.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class HdfsDemo {
	private FileSystem fs;
	public void begin(){
		System.setProperty("HADOOP_USER_NAME", "root");
		Configuration conf = new Configuration();
		try {
			fs = FileSystem.get(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	@After
	public void end(){
		try {
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void mkdir(){
		Path path = new Path("/tmp5");
		try {
			boolean b = fs.mkdirs(path);
			System.out.println(b);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void upload(){
		Path path = new Path("/tmp/test2");
		try {
			FSDataOutputStream fSDataOutputStream = fs.create(path);
			FileUtils.copyFile(new File("D://a.txt"), fSDataOutputStream);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		HdfsDemo hdfsDemo = new HdfsDemo();
		hdfsDemo.begin();
		hdfsDemo.upload();
		hdfsDemo.end();
	}
	
}
