package com.hadoop.job.test;

import static org.junit.Assert.*;

import org.junit.Test;

import com.hadoop.exception.HadoopException;
import com.hadoop.job.service.AcsDataJob;

public class AcsDataJobTest {

	@Test
	public void test() throws HadoopException {
		AcsDataJob.putToHDFS();
	}

}
