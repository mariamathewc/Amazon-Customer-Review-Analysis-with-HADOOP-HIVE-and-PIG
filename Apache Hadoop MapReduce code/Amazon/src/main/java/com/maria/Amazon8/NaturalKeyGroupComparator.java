package com.maria.Amazon8;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NaturalKeyGroupComparator extends WritableComparator {

	public NaturalKeyGroupComparator() {
		super(CompositeKeyWritable.class, true);
	}

	// grouping based on Product ID
	public int compare(WritableComparable a, WritableComparable b) {
		CompositeKeyWritable ck1 = (CompositeKeyWritable) a;
		CompositeKeyWritable ck2 = (CompositeKeyWritable) b;

		int result = ck1.getProductID().compareTo(ck2.getProductID());

		return result;
	}

}
