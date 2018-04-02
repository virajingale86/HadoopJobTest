package com.hadoop.hdfs.services;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import com.hadoop.exception.HadoopException;

public class HDFSUtility {

	private static final Logger logger = Logger.getLogger("com.hadoop.hdfs.services.HDFSUtility");
	private static Properties props = null;

	static{
		try {
			props = loadProperties();
		} catch (HadoopException e) {
			logger.error("Failed to load properties",e);
		}
	}

	/**
	 * Get Properties object
	 * @return Properties
	 * @throws HadoopException
	 */
	public Properties getProperties() throws HadoopException{
		if(props != null){
			return props;
		}else{
			try {
				props = loadProperties();
				return props;
			} catch (HadoopException e) {
				logger.error("Failed to load properties",e);
				throw e;
			}
		}
	}

	public static void main(String[] args) {
		HDFSUtility dl = new HDFSUtility();
		try {
			FileSystem fs = dl.getHDFSFileSystem();
			if(fs != null){
				System.out.println("Ok");
			}
		} catch (HadoopException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * Connect to HDFS server and get FileSystem object
	 * @return FileSystem
	 * @throws HadoopException
	 * @exception IOException
	 */
	public FileSystem getHDFSFileSystem() throws HadoopException {

		logger.info("Loading HDFS file system");

		String hdfsuri = props.getProperty("hdfs.namenode.url");
		String hadoopUserName = props.getProperty("hadoop.user.name");
		String hadoopHomeDir = props.getProperty("hadoop.home.dir");

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsuri);
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		System.setProperty("HADOOP_USER_NAME", hadoopUserName);
		System.setProperty("hadoop.home.dir", hadoopHomeDir);

		//Get the file system - HDFS
		FileSystem fs = null;
		try {
			fs = FileSystem.get(URI.create(hdfsuri), conf);

			return fs;
		} catch (IOException e) {
			logger.error("Failed to load HDFS file system");
			throw new HadoopException("Failed to load HDFS file system", e);
		}
	}

	/**
	 * Read properties file and return Properties object
	 * @return Properties
	 * @throws HadoopException 
	 * @exception FileNotFoundException IOException
	 */
	private static Properties loadProperties() throws HadoopException{
		logger.info("Loading Properties");
		Properties props = null;
		try {
			props = new Properties();
			props.load(new FileInputStream("src/test/resources/test.properties"));

			return props;
		} catch (FileNotFoundException e) {
			logger.error("Failed to load properties",e);
			throw new HadoopException("Failed to load properties", e);
		} catch (IOException e) {
			logger.error("Failed to load properties",e);
			throw new HadoopException("Failed to load properties", e);
		}
	}

}
