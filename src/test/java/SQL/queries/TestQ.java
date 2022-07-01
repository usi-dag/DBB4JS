package SQL.queries;

import SQL.dataset.TPCHStreamDB;

public interface TestQ {
    TPCHStreamDB tpchStreamDB = TPCHStreamDB.get();
    Number threshold = 0.0001;

    void testCorrectness();

    default boolean isARoundingError(double a, double b) {
        return ((b-a)/a) < threshold.doubleValue();
    }

    default boolean isARoundingError(long a , long b) {
        return ((b-a)/a) < threshold.longValue();
    }
}
