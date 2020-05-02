package es1.customWritables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class SelectedFieldsOfStockPricesRecordWritable implements Writable {

    float close;
    float volume;
    String date;

    public SelectedFieldsOfStockPricesRecordWritable() {}

    public SelectedFieldsOfStockPricesRecordWritable(float close, float volume, String date) {
        this.close = close;
        this.volume = volume;
        this.date = date;
    }

    public float getClose() {
        return this.close;
    }

    public float getVolume() {
        return this.volume;
    }

    public String getDate() {
        return this.date;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(this.close);
        dataOutput.writeFloat(this.volume);
        dataOutput.writeUTF(this.date);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.close = dataInput.readFloat();
        this.volume = dataInput.readFloat();
        this.date = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return this.close + "," + this.volume + "," + this.date;
    }
}
