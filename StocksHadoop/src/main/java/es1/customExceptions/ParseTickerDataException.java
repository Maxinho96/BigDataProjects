package es1.customExceptions;

public class ParseTickerDataException extends Exception {
    public ParseTickerDataException(String errorMessage) {
        super(errorMessage);
    }
}
