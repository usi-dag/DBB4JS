package SQL.dataset;

import SQL.datatypes.Date;

public interface LineItemRow {
    long l_orderkey();

    long l_partkey();

    long l_suppkey();

    long l_linenumber();

    double l_quantity();

    double l_extendedprice();

    double l_discount();

    double l_tax();

    String l_returnflag();

    String l_linestatus();

    Date l_shipdate();

    Date l_commitdate();

    Date l_receiptdate();

    String l_shipinstruct();

    String l_shipmode();

    String l_comment();

    record Record(long l_orderkey, long l_partkey, long l_suppkey, long l_linenumber,
                  double l_quantity, double l_extendedprice, double l_discount,
                  double l_tax, String l_returnflag, String l_linestatus,
                  Date l_shipdate, Date l_commitdate, Date l_receiptdate,
                  String l_shipinstruct, String l_shipmode, String l_comment) implements LineItemRow {

    }

}
