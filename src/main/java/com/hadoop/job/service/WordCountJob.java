package com.hadoop.job.service;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import com.hadoop.exception.HadoopException;
import com.hadoop.hdfs.services.HDFSUtility;

public class WordCountJob {

	private static final Logger logger = Logger.getLogger("com.hadoop.job.service.WordCountJob");

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

	public static void getFromHDFS() throws HadoopException{
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
			Properties props = dataLoader.getProperties();
			String hdfsuri = props.getProperty("hdfs.namenode.url");
			try {

				RemoteIterator<LocatedFileStatus> fileStatus = fs.listFiles(new Path(hdfsuri+path+"/output/"), true);

				List<String> fileList = new ArrayList<String>();

				while (fileStatus.hasNext()) {
					LocatedFileStatus locatedFileStatus = fileStatus.next();
					System.out.println(locatedFileStatus.getPath().toString());
					fileList.add(locatedFileStatus.getPath().toString());
				}

				boolean isSuccess = fileList.stream().anyMatch(p -> p.contains("_SUCCESS")); 

				if(!isSuccess){
					logger.error("WordCount Job Failed..");
					throw new HadoopException("WordCount Job Failed");
				}else{
					Map<String,Integer> countMap = new HashMap<String, Integer>();
					for(String filePath : fileList){
						if(!filePath.contains("_SUCCESS")){
							Path inputPath = new Path(filePath);
							FSDataInputStream inputStream = fs.open(inputPath);
							BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
							String line = "";
							while((line = br.readLine()) != null){
								System.out.println(line);
								String[] lineContaint = line.split("\t");
								countMap.put(lineContaint[0], Integer.parseInt(lineContaint[1]));
							}
						}
					}

				}

			} catch (IOException e) {
				logger.error("Failed to create directory in HDFS", e);
				throw new HadoopException("Failed to create directory in HDFS", e);
			}finally {
				//IOUtils.closeStream(in);
				//IOUtils.closeStream(outputStream);
			}
		}
	}
}
