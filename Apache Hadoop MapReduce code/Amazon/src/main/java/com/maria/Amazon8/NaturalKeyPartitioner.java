package com.maria.Amazon8;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<CompositeKeyWritable, IntWritable> {

	// partitioning based on Product ID
	public int getPartition(CompositeKeyWritable key, IntWritable value, int numPartitions) {

		return key.getProductID().hashCode() % numPartitions;
	}

}
