package com.hadoop.job.test;

import com.hadoop.exception.HadoopException;
import com.hadoop.job.service.WordCountJob;

import junit.framework.TestCase;

public class WordCountTest extends TestCase {

	public void testHDFSDataLoading() throws HadoopException{
		WordCountJob.main(null);
	}
}
