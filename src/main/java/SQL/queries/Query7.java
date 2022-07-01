package SQL.queries;

import SQL.dataset.*;
import SQL.datatypes.Date;
import SQL.sql_utils.SQLStreamUtils;
import SQL.sql_utils.tuple_types.LongIntPair;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Query7 implements Query {

    record NationJoined(String n1_name, String n2_name, int n1_nationkey, int n2_nationkey) {
    }

    record NationCustomerJoined(String n1_name, String n2_name, int n1_nationkey, int n2_nationkey, long c_custkey) {
        public NationCustomerJoined(NationJoined n, long c_custkey) {
            this(n.n1_name(), n.n2_name(), n.n1_nationkey(), n.n2_nationkey(), c_custkey);
        }
    }

    record NationCustomerOrderJoined(String n1_name, String n2_name, int n1_nationkey, int n2_nationkey,
                                     long o_orderkey) {
        public NationCustomerOrderJoined(NationCustomerJoined n, long o_orderkey) {
            this(n.n1_name(), n.n2_name(), n.n1_nationkey(), n.n2_nationkey(), o_orderkey);
        }
    }

    record NationCustomerOrderLineItemJoined(String n1_name, String n2_name, int n1_nationkey, long l_suppkey,
                                             Date l_shipdate,
                                             double l_extendedprice, double l_discount) {
        public NationCustomerOrderLineItemJoined(NationCustomerOrderJoined n, long l_suppkey, Date l_shipdate, double l_extendedprice, double l_discount) {
            this(n.n1_name(), n.n2_name(), n.n1_nationkey(), l_suppkey, l_shipdate, l_extendedprice, l_discount);
        }
    }

    static class Result {
        String supp_nation;
        String cust_nation;
        int l_year;
        double revenue;

        public void aggregate(NationCustomerOrderLineItemJoined n) {
            supp_nation = n.n1_name();
            cust_nation = n.n2_name();
            l_year = n.l_shipdate().getYears();
            revenue += n.l_extendedprice() * (1 - n.l_discount());
        }

        @Override
        public String toString() {
            return "Result{" +
                    "supp_nation='" + supp_nation + '\'' +
                    ", cust_nation='" + cust_nation + '\'' +
                    ", l_year=" + l_year +
                    ", revenue=" + revenue +
                    '}';
        }
    }

    record GroupBy(String supp_nation,
                   String cust_nation,
                   int l_year) {
    }

    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1995-01-01");
        Date max = new Date("1996-12-31");

        var nationsJoin = db.nation()
                .flatMap(n1 -> db.nation().filter(n2 -> (n1.n_name().equals("GERMANY") && n2.n_name().equals("FRANCE")) || (n1.n_name().equals("FRANCE") && n2.n_name().equals("GERMANY")))
                        .map(n2 -> new NationJoined(n1.n_name(), n2.n_name(), n1.n_nationkey(), n2.n_nationkey()))
                );


        var nationsCustomerJoin = SQLStreamUtils.streamJoin(
                nationsJoin,
                NationJoined::n2_nationkey,
                db.customer(),
                CustomerRow::c_nationkey,
                (n, c) -> new NationCustomerJoined(n, c.c_custkey())
        );

        var nationsCustomerOrdersJoin = SQLStreamUtils.streamJoin(
                nationsCustomerJoin,
                NationCustomerJoined::c_custkey,
                db.orders(),
                OrdersRow::o_custkey,
                (n, o) -> new NationCustomerOrderJoined(n, o.o_orderkey())
        );

        var nationsCustomerOrdersLineItemJoin = SQLStreamUtils.streamJoin(
                nationsCustomerOrdersJoin,
                NationCustomerOrderJoined::o_orderkey,
                db.lineitem().filter(l -> l.l_shipdate().compareTo(min) >= 0 && l.l_shipdate().compareTo(max) <= 0),
                LineItemRow::l_orderkey,
                (n, o) -> new NationCustomerOrderLineItemJoined(n, o.l_suppkey(), o.l_shipdate(), o.l_extendedprice(), o.l_discount())
        );

        Comparator<Result> c1 = Comparator.comparing(r -> r.supp_nation);
        Comparator<Result> c2 = Comparator.comparing(r -> r.cust_nation);
        Comparator<Result> c3 = Comparator.comparing(r -> r.l_year);
        Comparator<Result> comparator = c1.thenComparing(c2).thenComparing(c3);

        return SQLStreamUtils.streamJoin(
                        db.supplier(),
                        s -> new LongIntPair(s.s_suppkey(), s.s_nationkey()),
                        nationsCustomerOrdersLineItemJoin,
                        n -> new LongIntPair(n.l_suppkey(), n.n1_nationkey()),
                        (s, n) -> n
                )
                .collect(Collectors.groupingBy(n -> new GroupBy(n.n1_name(), n.n2_name(), n.l_shipdate().getYears()),
                        Collector.of(
                                Result::new, Result::aggregate, (a, b) -> null
                        )))
                .values()
                .stream()
                .sorted(comparator)
                .toList();

    }


    public static void main(String[] args) {
        new Query7().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }

}
