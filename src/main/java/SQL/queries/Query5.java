package SQL.queries;

import SQL.dataset.*;
import SQL.datatypes.Date;
import SQL.sql_utils.SQLStreamUtils;
import SQL.sql_utils.tuple_types.LongIntPair;

import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

// not working 
public class Query5 implements Query {

    record NationCustomer(String n_name, long c_custkey, int c_nationkey) {
    }

    record NationCustomerOrder(String n_name, int c_nationkey, long o_orderkey) {
    }

    record NationLine(String n_name, int c_nationkey, long l_suppkey, double l_extended_price, double l_discount) {
    }


    static class Result {
        String n_name;
        double revenue = 0;

        static void aggregate(Result r, NationLine nl) {
            r.n_name = nl.n_name();
            r.revenue += (nl.l_extended_price() * (1.d - nl.l_discount()));
        }

        @Override
        public String toString() {
            return "Result{" +
                    "n_name='" + n_name + '\'' +
                    ", revenue=" + revenue +
                    '}';
        }
    }

    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1994-01-01");
        Date max = new Date("1995-01-01");

        // region and nation join
        var regionNationJoin = SQLStreamUtils.streamJoin(
                db.region().filter(regionRow -> regionRow.r_name().equals("ASIA")),
                RegionRow::r_regionkey,
                db.nation(),
                NationRow::n_regionkey,
                (r, n) -> n
        );

        // nation and customer join
        var nationCustomerJoin = SQLStreamUtils.streamJoin(
                regionNationJoin,
                NationRow::n_nationkey,
                db.customer(),
                CustomerRow::c_nationkey,
                (n, c) -> new NationCustomer(n.n_name(), c.c_custkey(), c.c_nationkey())
        );


        // nation order join
        var nationOrderJoin = SQLStreamUtils.streamJoin(
                nationCustomerJoin,
                NationCustomer::c_custkey,
                db.orders().filter(ordersRow -> ordersRow.o_orderdate().fromTo(min, max)),
                OrdersRow::o_custkey,
                (nc, o) -> new NationCustomerOrder(nc.n_name(), nc.c_nationkey(), o.o_orderkey())
        );

        // nation and lineItem join
        var nationLineItemJoin = SQLStreamUtils.streamJoin(
                nationOrderJoin,
                NationCustomerOrder::o_orderkey,
                db.lineitem(),
                LineItemRow::l_orderkey,
                (nco, l) -> new NationLine(nco.n_name(), nco.c_nationkey(), l.l_suppkey(), l.l_extendedprice(), l.l_discount())
        );

        Comparator<Result> comparator = Comparator.comparing(r -> -r.revenue);


        // supp Nation Line join + groupBy + sort
        return SQLStreamUtils.streamJoin(
                        db.supplier(),
                        (s) -> new LongIntPair(s.s_suppkey(), s.s_nationkey()),
                        nationLineItemJoin,
                        (nl) -> new LongIntPair(nl.l_suppkey(), nl.c_nationkey()),
                        (s, nl) -> nl
                ).collect(
                        Collectors.groupingBy(NationLine::n_name, Collector.of(
                                Result::new, Result::aggregate, (a, b) -> null
                        ))
                )
                .values()
                .stream()
                .sorted(comparator)
                .toList();
    }

    public static void main(String[] args) {
        new Query5().execute(TPCHStreamDB.get()).forEach(System.out::println);

    }
}
