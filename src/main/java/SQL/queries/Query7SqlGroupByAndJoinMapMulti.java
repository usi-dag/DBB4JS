package SQL.queries;

import SQL.dataset.*;
import SQL.datatypes.Date;
import SQL.sql_utils.GroupByCollector;
import SQL.sql_utils.SQLCollectors;
import SQL.sql_utils.SQLStreamUtils;
import SQL.sql_utils.tuple_types.LongIntPair;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public class Query7SqlGroupByAndJoinMapMulti implements Query {
    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1995-01-01");
        Date max = new Date("1996-12-31");

        Stream<Query7.NationJoined> nationsJoin = db.nation()
                .mapMulti((n1, consumer) -> db.nation()
                        .filter(n2 -> (n1.n_name().equals("GERMANY") && n2.n_name().equals("FRANCE")) || (n1.n_name().equals("FRANCE") && n2.n_name().equals("GERMANY")))
                        .forEach(n2 -> consumer.accept(new Query7.NationJoined(n1.n_name(), n2.n_name(), n1.n_nationkey(), n2.n_nationkey()))));


        var nationsCustomerJoin = SQLStreamUtils.streamJoinMapMulti(
                nationsJoin,
                Query7.NationJoined::n2_nationkey,
                db.customer(),
                CustomerRow::c_nationkey,
                (n, c) -> new Query7.NationCustomerJoined(n, c.c_custkey())
        );

        var nationsCustomerOrdersJoin = SQLStreamUtils.streamJoinMapMulti(
                nationsCustomerJoin,
                Query7.NationCustomerJoined::c_custkey,
                db.orders(),
                OrdersRow::o_custkey,
                (n, o) -> new Query7.NationCustomerOrderJoined(n, o.o_orderkey())
        );

        var nationsCustomerOrdersLineItemJoin = SQLStreamUtils.streamJoinMapMulti(
                nationsCustomerOrdersJoin,
                Query7.NationCustomerOrderJoined::o_orderkey,
                db.lineitem().filter(l -> l.l_shipdate().compareTo(min) >= 0 && l.l_shipdate().compareTo(max) <= 0),
                LineItemRow::l_orderkey,
                (n, o) -> new Query7.NationCustomerOrderLineItemJoined(n, o.l_suppkey(), o.l_shipdate(), o.l_extendedprice(), o.l_discount())
        );

        Comparator<Query7Imperative.Result> c1 = Comparator.comparing(r -> r.supp_nation);
        Comparator<Query7Imperative.Result> c2 = Comparator.comparing(r -> r.cust_nation);
        Comparator<Query7Imperative.Result> c3 = Comparator.comparing(r -> r.l_year);
        Comparator<Query7Imperative.Result> comparator = c1.thenComparing(c2).thenComparing(c3);

        return SQLStreamUtils.streamJoinMapMulti(
                        db.supplier(),
                        s -> new LongIntPair(s.s_suppkey(), s.s_nationkey()),
                        nationsCustomerOrdersLineItemJoin,
                        n -> new LongIntPair(n.l_suppkey(), n.n1_nationkey()),
                        (s, n) -> n
                )
                .collect(SQLCollectors.groupingBy(n -> new Query7.GroupBy(n.n1_name(), n.n2_name(), n.l_shipdate().getYears()),
                        GroupByCollector.of(
                                (Query7.NationCustomerOrderLineItemJoined n) -> new Query7Imperative.Result(n.n1_name(), n.n2_name(), n.l_shipdate().getYears()),
                                (a, b) -> a.aggregate(b.l_extendedprice(), b.l_discount()),
                                (a, b) -> null
                        )))
                .values()
                .stream()
                .sorted(comparator)
                .toList();
    }

    public static void main(String[] args) {
        new Query7SqlGroupByAndJoinMapMulti().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
