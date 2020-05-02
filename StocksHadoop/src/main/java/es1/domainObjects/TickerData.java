package es1.domainObjects;

public class TickerData{

    private String ticker;
    private int diffPercQuotRoundedDown;
    private float minClose;
    private float maxClose;
    private float avgVolume;

    public TickerData() {
    }

    public TickerData(String ticker, int diffPercQuotRoundedDown, float minClose, float maxClose, float avgVolume) {
        this.ticker = ticker;
        this.diffPercQuotRoundedDown = diffPercQuotRoundedDown;
        this.minClose = minClose;
        this.maxClose = maxClose;
        this.avgVolume = avgVolume;
    }

    public String getTicker() {
        return ticker;
    }

    public int getDiffPercQuotRoundedDown() { return diffPercQuotRoundedDown; }

    public float getMinClose() {
        return minClose;
    }

    public float getMaxClose() {
        return maxClose;
    }

    public float getAvgVolume() {
        return avgVolume;
    }

}
