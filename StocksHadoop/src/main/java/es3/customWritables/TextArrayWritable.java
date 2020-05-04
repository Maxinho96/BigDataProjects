package es3.customWritables;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

import java.util.StringJoiner;

public class TextArrayWritable extends ArrayWritable {

    public TextArrayWritable() {
        super(Text.class);
    }

    public TextArrayWritable(String[] strings) {
        super(Text.class);
        Text[] texts = new Text[strings.length];
        for (int i = 0; i < strings.length; i++) {
            texts[i] = new Text(strings[i]);
        }
        set(texts);
    }

    @Override
    public Text[] get() {
        return (Text[]) super.get();
    }

    @Override
    public String toString() {

        StringJoiner stringJoiner = new StringJoiner(",");

        Text[] values = get();

        for (Text val : values) {
            stringJoiner.add(val.toString());
        }

        return stringJoiner.toString();
    }
}
