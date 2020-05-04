package es3.domainObjects;

import java.util.Date;

public class StockPricesRecord {

    private String ticker;
    private float open;
    private float close;
    private float adj_close;
    private float low;
    private float high;
    private float volume;
    private Date date;

    public StockPricesRecord(String ticker, float open, float close, float adj_close, float low, float high, float volume, Date date) {
        this.ticker = ticker;
        this.open = open;
        this.close = close;
        this.adj_close = adj_close;
        this.low = low;
        this.high = high;
        this.volume = volume;
        this.date = date;
    }

    public String getTicker() {
        return ticker;
    }

    public float getOpen() {
        return open;
    }

    public float getClose() {
        return close;
    }

    public float getAdj_close() {
        return adj_close;
    }

    public float getLow() {
        return low;
    }

    public float getHigh() {
        return high;
    }

    public float getVolume() {
        return volume;
    }

    public Date getDate() {
        return date;
    }

}
