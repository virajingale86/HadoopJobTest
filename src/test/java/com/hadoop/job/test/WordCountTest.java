package com.hadoop.job.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Test;

import com.hadoop.exception.HadoopException;
import com.hadoop.job.service.WordCountJob;

import junit.framework.TestCase;

public class WordCountTest extends TestCase {

	@Test(expected = HadoopException.class)
	public void testHDFSDataLoading() throws HadoopException {
		WordCountJob.putToHDFS();
	}
	
	@Test
	public void testResults() throws HadoopException{
		
		WordCountJob.getFromHDFS();
		/*File file = new File("test.txt");
		FileReader fileReader = new FileReader(file);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		StringBuffer stringBuffer = new StringBuffer();
		String line;
		while ((line = bufferedReader.readLine()) != null) {
			stringBuffer.append(line);
			stringBuffer.append("\n");
		}
		fileReader.close();
		System.out.println("Contents of file:");
		System.out.println(stringBuffer.toString());*/
	}
}
