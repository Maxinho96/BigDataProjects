package es3.customExceptions;

public class ParseStockPricesRecordException extends Exception {
    public ParseStockPricesRecordException(String errorMessage) {
        super(errorMessage);
    }
}
