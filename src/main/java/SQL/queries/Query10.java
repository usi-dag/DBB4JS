package SQL.queries;

import SQL.dataset.*;
import SQL.datatypes.Date;
import SQL.sql_utils.SQLStreamUtils;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Query10 implements Query {

    record CustOrder(long c_custkey, String c_name, double c_acctbal, String c_address, String c_phone,
                     String c_comment, int c_nationkey, long o_orderkey) {
    }

    record NationCustOrder(long c_custkey, String c_name, double c_acctbal, String n_name, String c_address,
                           String c_phone, String c_comment, long o_orderkey) {
    }

    record LineItemNationCustOrder(long c_custkey, String c_name, double c_acctbal, String n_name, String c_address,
                                   String c_phone, String c_comment, double l_extendedprice, double l_discount) {
    }

    record Q10Key(long c_custkey, String c_name, double c_acctbal, String c_phone, String n_name, String c_address,
                  String c_comment) {
    }

    static class Result {
        long c_custkey;
        String c_name;
        double revenue;
        double c_acctbal;
        String n_name;
        String c_address;
        String c_phone;
        String c_comment;

        static void aggregate(Result r, LineItemNationCustOrder l) {
            r.c_custkey = l.c_custkey();
            r.c_name = l.c_name();
            r.revenue += (l.l_extendedprice() * (1 - l.l_discount()));
            r.c_acctbal = l.c_acctbal();
            r.n_name = l.n_name();
            r.c_address = l.c_address();
            r.c_phone = l.c_phone();
            r.c_comment = l.c_comment();
        }

        @Override
        public String toString() {
            return "Result{" +
                    "c_custkey=" + c_custkey +
                    ", c_name='" + c_name + '\'' +
                    ", revenue=" + revenue +
                    ", c_acctbal=" + c_acctbal +
                    ", n_name='" + n_name + '\'' +
                    ", c_address='" + c_address + '\'' +
                    ", c_phone='" + c_phone + '\'' +
                    ", c_comment='" + c_comment + '\'' +
                    '}';
        }
    }

    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1993-10-01");
        Date max = new Date("1994-01-01");

        // Cust and orders join
        var custOrderJoin = SQLStreamUtils.streamJoin(
                db.orders().filter(o -> o.o_orderdate().fromTo(min, max)),
                OrdersRow::o_custkey,
                db.customer(),
                CustomerRow::c_custkey,
                (o, c) -> new CustOrder(c.c_custkey(), c.c_name(), c.c_acctbal(), c.c_address(), c.c_phone(), c.c_comment(), c.c_nationkey(), o.o_orderkey())
        );

        // cust order nation join
        var custOrderNationJoin = SQLStreamUtils.streamJoin(
                db.nation(),
                NationRow::n_nationkey,
                custOrderJoin,
                CustOrder::c_nationkey,
                (n, c) -> new NationCustOrder(c.c_custkey(), c.c_name(), c.c_acctbal(), n.n_name(), c.c_address(), c.c_phone(), c.c_comment(), c.o_orderkey())
        );

        Comparator<Result> comparator = Comparator.comparing(result -> -result.revenue);

        // custOrderNation and LineItem join
        return SQLStreamUtils.streamJoin(
                        custOrderNationJoin,
                        NationCustOrder::o_orderkey,
                        db.lineitem().filter(lineItemRow -> lineItemRow.l_returnflag().equals("R")),
                        LineItemRow::l_orderkey,
                        (con, l) -> new LineItemNationCustOrder(con.c_custkey(), con.c_name(), con.c_acctbal(), con.n_name(), con.c_address(), con.c_phone(),
                                con.c_comment(), l.l_extendedprice(), l.l_discount())
                )
                .collect(Collectors.groupingBy(c -> new Q10Key(c.c_custkey(), c.c_name(), c.c_acctbal(), c.c_phone(), c.n_name(), c.c_address(), c.c_comment()),
                        Collector.of(
                                Result::new, Result::aggregate, (a, b) -> null
                        )
                ))
                .values()
                .stream()
                .sorted(comparator)
                .limit(20)
                .toList();

    }

    public static void main(String[] args) {
        new Query10().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
