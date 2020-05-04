package es3.customWritables;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

import java.util.StringJoiner;

public class FloatArrayWritable extends ArrayWritable{

    public FloatArrayWritable() {
        super(FloatWritable.class);
    }

    public FloatArrayWritable(float[] floats) {
        super(FloatWritable.class);
        FloatWritable[] floatWritables = new FloatWritable[floats.length];
        for (int i = 0; i < floats.length; i++) {
            floatWritables[i] = new FloatWritable(floats[i]);
        }
        set(floatWritables);
    }

    @Override
    public FloatWritable[] get() {
        return (FloatWritable[]) super.get();
    }

    @Override
    public String toString() {

        StringJoiner stringJoiner = new StringJoiner(",");

        FloatWritable[] values = get();

        for (FloatWritable val : values) {
            stringJoiner.add(val.toString());
        }

        return stringJoiner.toString();
    }
}
