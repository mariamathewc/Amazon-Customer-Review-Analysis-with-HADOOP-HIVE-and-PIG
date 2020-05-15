package com.maria.Amazon8;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class App {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try {
			Job job = Job.getInstance(conf, " TOP 5 PRODUCTS FROM 'DIGITAL SOFTWARE' and 'PERSONAL CARE' DEPARTMENT");

			// set driver class
			job.setJarByClass(App.class);

			// set Natural Key PartitionerClass
			job.setPartitionerClass(NaturalKeyPartitioner.class);
			// set Natural Key GroupingComparatorClass
			job.setGroupingComparatorClass(NaturalKeyGroupComparator.class);
			
			
			
			// set MapperClass
			job.setMapperClass(ProductMapper.class);
			// set CombinerClass
			job.setCombinerClass(ProductReducer.class);
			// set ReducerClass
			job.setReducerClass(ProductReducer.class);

			// set InputFormatClass
			job.setInputFormatClass(TextInputFormat.class);
			// set OutputFormatClass
			job.setOutputFormatClass(TextOutputFormat.class);

			// set OutputKeyClass
			job.setOutputKeyClass(CompositeKeyWritable.class);
			// set OutputValueClass
			job.setOutputValueClass(IntWritable.class);

			// set path
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			try {
				job.waitForCompletion(true);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} catch (IOException e) {

			e.printStackTrace();
		}

		Configuration conf1 = new Configuration();
		try {
			Job job1 = Job.getInstance(conf1, " TOP 5 PRODUCTS FROM 'DIGITAL SOFTWARE' and 'PERSONAL CARE' DEPARTMENT");

			// set driver class
			job1.setJarByClass(App.class);

			// set Natural Key PartitionerClass
			job1.setPartitionerClass(NaturalKeyPartitionerTop.class);
			// set Natural Key GroupingComparatorClass
			job1.setGroupingComparatorClass(NaturalKeyGroupComparatorTop.class);
			// set SortComparatorClass
			job1.setSortComparatorClass(SecondarySortComparatorTop.class);

			// set MapperClass
			job1.setMapperClass(TopProductMapper.class);
			// set ReducerClass
			job1.setReducerClass(TopProductReducer.class);

			// set InputFormatClass
			job1.setInputFormatClass(TextInputFormat.class);
			// set OutputFormatClass
			job1.setOutputFormatClass(TextOutputFormat.class);

			// set OutputKeyClass
			job1.setOutputKeyClass(CompositeKeyWritableTop.class);
			// set setOutputValueClass
			job1.setOutputValueClass(Text.class);

			// set path
			FileInputFormat.addInputPath(job1, new Path(args[1]));
			FileOutputFormat.setOutputPath(job1, new Path(args[2]));
			try {
				System.exit(job1.waitForCompletion(true) ? 0 : 1);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} catch (IOException e) {

			e.printStackTrace();
		}
	}
}
