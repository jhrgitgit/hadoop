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
	FileSystem fs;
	@Before
	public void begin(){
		System.setProperty("HADOOP_USER_NAME", "root");
		Configuration conf = new Configuration();
//		conf.set("dfs.nameservices", "harlan");
//		conf.set("dfs.ha.namenodes.harlan", "nn1,nn2");
//		conf.set("dfs.namenode.rpc-address.harlan.nn1", "192.168.132.128:8020");
//		conf.set("dfs.namenode.rpc-address.harlan.nn2", "192.168.132.137:8020");
//		conf.set("dfs.namenode.http-address.harlan.nn1", "192.168.132.128:50070");
//		conf.set("dfs.namenode.http-address.harlan.nn2", "192.168.132.137:50070");
//		conf.set("fs.defaultFS", "hdfs://harlan");
//		conf.set("hadoop.tmp.dir", "/opt/hadoop");
		//conf.set("ha.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
//		conf.set("ha.zookeeper.quorum", "hdfs://harlan");
		
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
			// TODO Auto-generated catch block
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
		Path path = new Path("/tmp/test");
		try {
			FSDataOutputStream fSDataOutputStream = fs.create(path);
			FileUtils.copyFile(new File("D://a11.xlsx"), fSDataOutputStream);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	
}
