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

public class AcsDataJob {
	private static final Logger logger = Logger.getLogger("com.hadoop.job.service.AcsDataJob");
	
	public static void putToHDFS() throws HadoopException {

		HDFSUtility dataLoader = new HDFSUtility();

		FileSystem fs = null;
		try {
			fs = dataLoader.getHDFSFileSystem();
		} catch (HadoopException e) {
			logger.error("Failed to get HDFS file system", e);
			throw e;
		}

		if(fs != null){
			String path = "/acsdata";
			Path newFolderPath= new Path(path);
			Properties props = dataLoader.getProperties();
			String acsFile0 = props.getProperty("acs.data.job.input.file0");
			String acsFile1 = props.getProperty("acs.data.job.input.file1");
			InputStream in = null;
			FSDataOutputStream outputStream = null;
			try {
				if(!fs.exists(newFolderPath)) {
					fs.mkdirs(newFolderPath);
					logger.info("Path "+path+" created.");
				}

				logger.info("Begin Write ACS File0 into hdfs");
				Path inputPath = new Path(newFolderPath + "/input/20180422/" + acsFile0);
				outputStream = fs.create(inputPath);
				in = new BufferedInputStream(new FileInputStream("src/test/resources/acsdata/"+acsFile0));
				IOUtils.copyBytes(in, outputStream, 4096, true);
				logger.info("End Write ACS File0 into hdfs");

				IOUtils.closeStream(in);
				IOUtils.closeStream(outputStream);

				logger.info("Begin Write ACS File1 into hdfs");
				Path jarPath = new Path(newFolderPath + "/input/20180423/" + acsFile1);
				outputStream = fs.create(jarPath);
				in = new BufferedInputStream(new FileInputStream("src/test/resources/acsdata/"+acsFile1));
				IOUtils.copyBytes(in, outputStream, 4096, true);
				logger.info("End Write ACS File1 into hdfs");

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
