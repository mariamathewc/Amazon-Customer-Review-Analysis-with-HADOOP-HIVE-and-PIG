package com.maria.Amazon8;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProductMapper extends Mapper<LongWritable, Text, CompositeKeyWritable, IntWritable> {

	// The map function process each line of tsv file and emit a composite
	// key('product_id' and 'product_category') and rating of each Product ID.
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// Convert text to string
		String str = value.toString();
		// Skipping header
		if (!(value.toString().contains("marketplace"))) {

			String[] word = str.split("\\t");

			if (word.length == 15) {

				IntWritable rating = new IntWritable();

				rating.set(Integer.parseInt(word[7]));

				// Compositekey consist of 'product_id' and 'product_category'
				CompositeKeyWritable obj = new CompositeKeyWritable(word[3], word[6]);

				context.write(obj, rating);
			}
		}

	}
}
