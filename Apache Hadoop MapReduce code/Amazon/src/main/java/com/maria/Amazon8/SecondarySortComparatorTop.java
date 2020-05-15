package com.maria.Amazon8;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortComparatorTop extends WritableComparator {

	public SecondarySortComparatorTop() {
		super(CompositeKeyWritableTop.class, true);
	}

	// The composite key is sorted in descending order based on total rating
	public int compare(WritableComparable a, WritableComparable b) {

		CompositeKeyWritableTop ck1 = (CompositeKeyWritableTop) a;
		CompositeKeyWritableTop ck2 = (CompositeKeyWritableTop) b;

		Integer num1 = Integer.parseInt(ck1.getRating());
		Integer num2 = Integer.parseInt(ck2.getRating());

		int result = -1 * num1.compareTo(num2);

		return result;
	}
}