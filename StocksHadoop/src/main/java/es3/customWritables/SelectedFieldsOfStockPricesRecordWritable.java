package es3.customWritables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class SelectedFieldsOfStockPricesRecordWritable implements Writable {

    float close;
    String date;

    public SelectedFieldsOfStockPricesRecordWritable() {}

    public SelectedFieldsOfStockPricesRecordWritable(float close, String date) {
        this.close = close;
        this.date = date;
    }

    public float getClose() {
        return this.close;
    }

    public String getDate() {
        return this.date;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(this.close);
        dataOutput.writeUTF(this.date);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.close = dataInput.readFloat();
        this.date = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return this.close + "," + this.date;
    }
}
