package com.maria.Amazon8;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TopProductMapper extends Mapper<LongWritable, Text, CompositeKeyWritableTop, Text> {

	// tree map for finding local top5 products from each mapper in each department
	private TreeMap<Long, String> tmap_PersonalCare;
	private TreeMap<Long, String> tmap_Software;

	// Initializing two tree map. One for personal care department and another for
	// Digital Software department
	@Override
	public void setup(Context context) throws IOException, InterruptedException {

		tmap_PersonalCare = new TreeMap<Long, String>();
		tmap_Software = new TreeMap<Long, String>();

	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String str = value.toString();

		String[] word = str.split("\\s+");
		if (word.length == 3) {

			// Finding local top 5 from each mapper in Personal Care Appliance department
			if (word[1].equals("Personal_Care_Appliances")) {

				long total_rating = Long.parseLong(word[2]);

				if (!tmap_PersonalCare.containsKey(total_rating)) {

					tmap_PersonalCare.put(total_rating, word[0]);

				} else {

					String product_list = tmap_PersonalCare.get(total_rating);
					product_list += " " + word[0];
					tmap_PersonalCare.put(total_rating, product_list);
				}

				if (tmap_PersonalCare.size() > 5) {

					tmap_PersonalCare.remove(tmap_PersonalCare.firstKey());
				}

			}
			// Finding local top 5 from each mapper in Digital Software department
			else if (word[1].equals("Digital_Software")) {

				long total_rating = Long.parseLong(word[2]);

				if (!tmap_Software.containsKey(total_rating)) {

					tmap_Software.put(total_rating, word[0]);

				} else {

					String product_list = tmap_Software.get(total_rating);
					product_list += " " + word[0];
					tmap_Software.put(total_rating, product_list);

				}

				if (tmap_Software.size() > 5) {

					tmap_Software.remove(tmap_Software.firstKey());
				}

			}
		}

	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		Text products = new Text();

		// Emitting local top 5 key value from each mapper in Personal_Care_Appliances
		// department
		int counter = 0;
	

		for (int ptr = 5; ptr > 0; ptr--) {

			if (tmap_PersonalCare.size() == 0) {
				break;
			}

			long mykey = tmap_PersonalCare.lastKey();
			String prd_list = tmap_PersonalCare.get(mykey);

			String[] productlist_array = prd_list.split(" ");

			for (String product : productlist_array) {

				if (counter < 5) {

					products.set(product);

					String category = "Personal_Care_Appliances";
					String prd_ratings = "" + mykey;

					CompositeKeyWritableTop obj = new CompositeKeyWritableTop(category, prd_ratings);

					context.write(obj, products);

					counter++;

				} else {
					break;
				}

			}
			if (counter >= 5) {
				break;
			} else {
				tmap_PersonalCare.remove(tmap_PersonalCare.lastKey());
			}
		}

		// Emitting local top 5 key value from each mapper in Digital Software department
		counter = 0;

		for (int ptr = 5; ptr > 0; ptr--) {

			if (tmap_Software.size() == 0) {
				break;
			}

			long mykey = tmap_Software.lastKey();
			String prd_list = tmap_Software.get(mykey);

			String[] productlist_array = prd_list.split(" ");

			for (String product : productlist_array) {

				if (counter < 5) {

					products.set(product);

					String category = "Digital_Software";
					String prd_ratings = "" + mykey;

					CompositeKeyWritableTop obj = new CompositeKeyWritableTop(category, prd_ratings);

					context.write(obj, products);

					counter++;

				} else {
					break;
				}

			}
			if (counter >= 5) {
				break;
			} else {
				tmap_Software.remove(tmap_Software.lastKey());
			}
		}
	}

}
