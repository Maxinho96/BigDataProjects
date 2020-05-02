package es1.customSorts;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IntWritableDescendingSort extends WritableComparator {

    private static final IntWritable.Comparator INTWRITABLE_COMPARATOR = new IntWritable.Comparator();

    public IntWritableDescendingSort() {
        super(IntWritable.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return (-1)* INTWRITABLE_COMPARATOR.compare(b1, s1, l1, b2, s2, l2);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        if (a instanceof IntWritable && b instanceof IntWritable) {
            return (-1)*(((IntWritable) a).compareTo((IntWritable) b));
        }
        return super.compare(a, b);
    }

}


