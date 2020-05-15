package com.maria.Amazon8;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitionerTop extends Partitioner<CompositeKeyWritableTop, IntWritable> {

	// partitioning based on Product Category
	public int getPartition(CompositeKeyWritableTop key, IntWritable value, int numPartitions) {

		return key.getProductCategory().hashCode() % numPartitions;
	}

}