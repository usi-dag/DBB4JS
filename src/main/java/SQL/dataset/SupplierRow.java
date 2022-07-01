package SQL.dataset;

public interface SupplierRow {
    long s_suppkey();

    String s_name();

    String s_address();

    int s_nationkey();

    String s_phone();

    double s_acctbal();

    String s_comment();

    record Record(long s_suppkey, String s_name, String s_address, int s_nationkey, String s_phone, double s_acctbal,
                  String s_comment) implements SupplierRow {
    }

}
