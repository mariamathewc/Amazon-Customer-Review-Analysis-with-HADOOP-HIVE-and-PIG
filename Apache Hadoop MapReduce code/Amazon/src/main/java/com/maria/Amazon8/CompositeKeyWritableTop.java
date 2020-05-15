package com.maria.Amazon8;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CompositeKeyWritableTop implements WritableComparable {

	public CompositeKeyWritableTop() {

	}

	public CompositeKeyWritableTop(String product_Category, String Total_Ratings) {
		super();
		RATINGS = Total_Ratings;
		PRD_CAT = product_Category;
	}

	String PRD_CAT;
	String RATINGS;

	// get Rating
	public String getRating() {
		return RATINGS;
	}

	// set Rating
	public void setRating(String Total_rating) {
		RATINGS = Total_rating;
	}

	// get Product Category
	public String getProductCategory() {
		return PRD_CAT;
	}

	// set Product Category
	public void setProductCategory(String product_cat) {
		PRD_CAT = product_cat;
	}

	public void readFields(DataInput in) throws IOException {

		RATINGS = in.readUTF();
		PRD_CAT = in.readUTF();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(RATINGS);
		out.writeUTF(PRD_CAT);

	}

	@Override
	public String toString() {

		return PRD_CAT + " " + RATINGS;
	}

	public int compareTo(Object o) {
		CompositeKeyWritableTop ck = (CompositeKeyWritableTop) o;
		String thisvalue = this.getProductCategory();
		String othervalue = ck.getProductCategory();
		int result = thisvalue.compareTo(othervalue);
		return (result < 0 ? -1 : (result == 0 ? 0 : 1));

	}

}
