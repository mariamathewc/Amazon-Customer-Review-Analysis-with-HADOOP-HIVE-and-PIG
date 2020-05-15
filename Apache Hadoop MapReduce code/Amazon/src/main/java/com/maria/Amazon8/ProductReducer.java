package com.maria.Amazon8;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProductReducer extends Reducer<CompositeKeyWritable, IntWritable, CompositeKeyWritable, IntWritable> {

	// The reduce function emits a composite key('product_id' and
	// 'product_category') and sum of ratings of each Product ID.
	@Override
	public void reduce(CompositeKeyWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sum = 0;

		for (IntWritable i : values) {

			sum += i.get();

		}

		IntWritable count = new IntWritable(sum);

		context.write(key, count);
	}
}
