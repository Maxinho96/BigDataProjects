package es1.customWritables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TickerDataWithoutDiffPercQuotWritable implements Writable {

    private String ticker;
    private float minClose;
    private float maxClose;
    private float avgVolume;

    public TickerDataWithoutDiffPercQuotWritable() {
    }

    public TickerDataWithoutDiffPercQuotWritable(String ticker, float minClose, float maxClose, float avgVolume) {
        this.ticker = ticker;
        this.minClose = minClose;
        this.maxClose = maxClose;
        this.avgVolume = avgVolume;
    }

    public String getTicker() {
        return ticker;
    }

    public float getMinClose() {
        return minClose;
    }

    public float getMaxClose() { return maxClose; }

    public float getAvgVolume() {
        return avgVolume;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.ticker);
        dataOutput.writeFloat(this.minClose);
        dataOutput.writeFloat(this.maxClose);
        dataOutput.writeFloat(this.avgVolume);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.ticker = dataInput.readUTF();
        this.minClose = dataInput.readFloat();
        this.maxClose = dataInput.readFloat();
        this.avgVolume = dataInput.readFloat();
    }

    @Override
    public String toString() {
        return this.ticker + "," + this.minClose + "," + this.maxClose + "," + this.avgVolume;
    }
}
