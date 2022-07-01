package SQL.dataset;

import SQL.datatypes.Date;

public interface OrdersRow {
    long o_orderkey();

    long o_custkey();

    String o_orderstatus();

    double o_totalprice();

    Date o_orderdate();

    String o_orderpriority();

    String o_clerk();

    int o_shippriority();

    String o_comment();

    record Record(long o_orderkey, long o_custkey, String o_orderstatus, double o_totalprice, Date o_orderdate,
                  String o_orderpriority,
                  String o_clerk, int o_shippriority, String o_comment) implements OrdersRow {
    }
}


