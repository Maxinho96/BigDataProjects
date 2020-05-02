package es1.customSorts;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FloatWritableDescendingSort extends WritableComparator {

    private static final FloatWritable.Comparator FLOATWRITABLE_COMPARATOR = new FloatWritable.Comparator();

    public FloatWritableDescendingSort() {
        super(FloatWritable.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return (-1)* FLOATWRITABLE_COMPARATOR.compare(b1, s1, l1, b2, s2, l2);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        if (a instanceof FloatWritable && b instanceof FloatWritable) {
            return (-1)*(((FloatWritable) a).compareTo((FloatWritable) b));
        }
        return super.compare(a, b);
    }

}


