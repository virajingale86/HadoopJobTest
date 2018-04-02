package com.hadoop.job.service;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import com.hadoop.exception.HadoopException;
import com.hadoop.hdfs.services.HDFSUtility;

public class WordCountJob {

	private static final Logger logger = Logger.getLogger("com.hadoop.job.service.WordCountJob");

	public static void main(String[] args) throws HadoopException {

		HDFSUtility dataLoader = new HDFSUtility();

		FileSystem fs = null;
		try {
			fs = dataLoader.getHDFSFileSystem();
		} catch (HadoopException e) {
			logger.error("Failed to get HDFS file system", e);
			throw e;
		}

		if(fs != null){
			String path = "/wordCountJob";
			Path newFolderPath= new Path(path);
			Properties props = dataLoader.getProperties();
			String inputFileName = props.getProperty("word.count.job.input.file.name");
			String jarFileName = props.getProperty("word.count.job.input.jar.name");
			InputStream in = null;
			FSDataOutputStream outputStream = null;
			try {
				if(!fs.exists(newFolderPath)) {
					fs.mkdirs(newFolderPath);
					logger.info("Path "+path+" created.");
				}
				
				logger.info("Begin Write input file into hdfs");
				Path inputPath = new Path(newFolderPath + "/input/" + inputFileName);
				outputStream = fs.create(inputPath);
				in = new BufferedInputStream(new FileInputStream("src/test/resources/"+inputFileName));
				IOUtils.copyBytes(in, outputStream, 4096, true);
				logger.info("End Write input file into hdfs");
				
				IOUtils.closeStream(in);
				IOUtils.closeStream(outputStream);
				
				logger.info("Begin Write Jar file into hdfs");
				Path jarPath = new Path(newFolderPath + "/" + jarFileName);
				outputStream = fs.create(jarPath);
				in = new BufferedInputStream(new FileInputStream("src/test/resources/"+jarFileName));
				IOUtils.copyBytes(in, outputStream, 4096, true);
				logger.info("End Write Jar file into hdfs");
				
			} catch (IOException e) {
				logger.error("Failed to create directory in HDFS", e);
				throw new HadoopException("Failed to create directory in HDFS", e);
			}finally {
				IOUtils.closeStream(in);
				IOUtils.closeStream(outputStream);
			}
		}
	}
}
