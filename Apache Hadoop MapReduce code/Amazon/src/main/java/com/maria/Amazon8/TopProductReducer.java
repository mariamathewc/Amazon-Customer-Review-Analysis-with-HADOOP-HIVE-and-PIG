package com.maria.Amazon8;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TopProductReducer extends Reducer<CompositeKeyWritableTop, Text, CompositeKeyWritable, LongWritable> {

	// tree map for finding top5 products from each department
	private TreeMap<Long, String> tmap_PersonalCare;
	private TreeMap<Long, String> tmap_Software;

	// Initializing two tree map. One for personal care department and another for
	// digital software department
	@Override
	public void setup(Context context) throws IOException, InterruptedException {

		tmap_PersonalCare = new TreeMap<Long, String>();
		tmap_Software = new TreeMap<Long, String>();
	}

	@Override
	public void reduce(CompositeKeyWritableTop key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		for (Text product_id : values) {

			long total_rating = Long.parseLong(key.getRating());

			// Finding top 5 product id from Personal Care Appliance department
			if (key.getProductCategory().equals("Personal_Care_Appliances")) {

				if (!tmap_PersonalCare.containsKey(total_rating)) {

					tmap_PersonalCare.put(total_rating, product_id.toString());

				} else {

					String product_list = tmap_PersonalCare.get(total_rating);
					product_list += " " + product_id.toString();
					tmap_PersonalCare.put(total_rating, product_list);

				}

				if (tmap_PersonalCare.size() > 5) {

					tmap_PersonalCare.remove(tmap_PersonalCare.firstKey());

				}

				// Finding top 5 product id from Digital Software department
			} else if (key.getProductCategory().equals("Digital_Software")) {

				if (!tmap_Software.containsKey(total_rating)) {

					tmap_Software.put(total_rating, product_id.toString());

				} else {

					String product_list = tmap_Software.get(total_rating);
					product_list += " " + product_id.toString();
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
		LongWritable FinalRank = new LongWritable();

		// Emitting top 5 product id and its ratings from Personal_Care_Appliances
		// department

		int counter = 0;
		long rank =1;

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
					FinalRank.set(rank++);

					String Product_Id = products.toString();
					String Product_Category = "Personal_Care_Appliances";

					CompositeKeyWritable obj = new CompositeKeyWritable(Product_Id, Product_Category);

					context.write(obj, FinalRank);

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

		// Emitting top 5 product id and its ratings from Digital Software department
		counter = 0;
		rank =1;

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
					FinalRank.set(rank++);

					String Product_Id = products.toString();
					String Product_Category = "Digital_Software";

					CompositeKeyWritable obj = new CompositeKeyWritable(Product_Id, Product_Category);

					context.write(obj, FinalRank);

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
