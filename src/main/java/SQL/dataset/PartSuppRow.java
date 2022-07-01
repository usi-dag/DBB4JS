package SQL.dataset;

public interface PartSuppRow {
    long ps_partkey();

    long ps_suppkey();

    long ps_availqty();

    double ps_supplycost();

    String ps_comment();

    record Record(long ps_partkey, long ps_suppkey, long ps_availqty, double ps_supplycost,
                  String ps_comment) implements PartSuppRow {
    }

}
