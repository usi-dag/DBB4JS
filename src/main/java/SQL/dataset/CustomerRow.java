package SQL.dataset;

public interface CustomerRow {
    long c_custkey();

    String c_name();

    String c_address();

    int c_nationkey();

    String c_phone();

    double c_acctbal();

    String c_mktsegment();

    String c_comment();

    record Record(long c_custkey, String c_name, String c_address,
                  int c_nationkey, String c_phone, double c_acctbal, String c_mktsegment,
                  String c_comment) implements CustomerRow {
    }

}
