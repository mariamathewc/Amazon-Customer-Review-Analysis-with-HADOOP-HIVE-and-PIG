package com.maria.Amazon8;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NaturalKeyGroupComparatorTop extends WritableComparator {

	public NaturalKeyGroupComparatorTop() {
		super(CompositeKeyWritableTop.class, true);
	}

	// grouping based on Product Rating
	public int compare(WritableComparable a, WritableComparable b) {
		CompositeKeyWritableTop ck1 = (CompositeKeyWritableTop) a;
		CompositeKeyWritableTop ck2 = (CompositeKeyWritableTop) b;

		Integer num1 = Integer.parseInt(ck1.getRating());
		Integer num2 = Integer.parseInt(ck2.getRating());

		int result = num1.compareTo(num2);

		return result;
	}

}