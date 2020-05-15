package com.maria.Amazon8;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CompositeKeyWritable implements WritableComparable {

	public CompositeKeyWritable() {

	}

	public CompositeKeyWritable(String product_ID, String product_Category) {
		super();
		PRD_ID = product_ID;
		PRD_CAT = product_Category;
	}

	String PRD_ID;
	String PRD_CAT;

	// get Product ID
	public String getProductID() {
		return PRD_ID;
	}

	// set Product ID
	public void setProductID(String product_id) {
		PRD_ID = product_id;
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

		PRD_ID = in.readUTF();
		PRD_CAT = in.readUTF();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(PRD_ID);
		out.writeUTF(PRD_CAT);

	}

	@Override
	public String toString() {

		return PRD_ID + " " + PRD_CAT;
	}

	public int compareTo(Object o) {
		CompositeKeyWritable ck = (CompositeKeyWritable) o;
		String thisvalue = this.getProductID();
		String othervalue = ck.getProductID();
		int result = thisvalue.compareTo(othervalue);
		return (result < 0 ? -1 : (result == 0 ? 0 : 1));

	}

}
