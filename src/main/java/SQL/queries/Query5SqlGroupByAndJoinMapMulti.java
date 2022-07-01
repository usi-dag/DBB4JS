package SQL.queries;

import SQL.dataset.*;
import SQL.datatypes.Date;
import SQL.sql_utils.GroupByCollector;
import SQL.sql_utils.SQLCollectors;
import SQL.sql_utils.SQLStreamUtils;
import SQL.sql_utils.tuple_types.LongIntPair;

import java.util.Comparator;
import java.util.List;

public class Query5SqlGroupByAndJoinMapMulti implements Query {
    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1994-01-01");
        Date max = new Date("1995-01-01");

        // region and nation join
        var regionNationJoin = SQLStreamUtils.streamJoinMapMulti(
                db.region().filter(regionRow -> regionRow.r_name().equals("ASIA")),
                RegionRow::r_regionkey,
                db.nation(),
                NationRow::n_regionkey,
                (r, n) -> n
        );

        // nation and customer join
        var nationCustomerJoin = SQLStreamUtils.streamJoinMapMulti(
                regionNationJoin,
                NationRow::n_nationkey,
                db.customer(),
                CustomerRow::c_nationkey,
                (n, c) -> new Query5.NationCustomer(n.n_name(), c.c_custkey(), c.c_nationkey())
        );


        // nation order join
        var nationOrderJoin = SQLStreamUtils.streamJoinMapMulti(
                nationCustomerJoin,
                Query5.NationCustomer::c_custkey,
                db.orders().filter(ordersRow -> ordersRow.o_orderdate().fromTo(min, max)),
                OrdersRow::o_custkey,
                (nc, o) -> new Query5.NationCustomerOrder(nc.n_name(), nc.c_nationkey(), o.o_orderkey())
        );

        // nation and lineItem join
        var nationLineItemJoin = SQLStreamUtils.streamJoinMapMulti(
                nationOrderJoin,
                Query5.NationCustomerOrder::o_orderkey,
                db.lineitem(),
                LineItemRow::l_orderkey,
                (nco, l) -> new Query5.NationLine(nco.n_name(), nco.c_nationkey(), l.l_suppkey(), l.l_extendedprice(), l.l_discount())
        );

        Comparator<Query5Imperative.Result> comparator = Comparator.comparing(r -> -r.revenue);


        // supp Nation Line join + groupBy + sort
        return SQLStreamUtils.streamJoinMapMulti(
                        db.supplier(),
                        (s) -> new LongIntPair(s.s_suppkey(), s.s_nationkey()),
                        nationLineItemJoin,
                        (nl) -> new LongIntPair(nl.l_suppkey(), nl.c_nationkey()),
                        (s, nl) -> nl
                ).collect(SQLCollectors.groupingBy(
                        Query5.NationLine::n_name, GroupByCollector.of(
                                (Query5.NationLine nl) -> new Query5Imperative.Result(nl.n_name()),
                                (a, b) -> a.aggregate(b.l_extended_price(), b.l_discount()),
                                (a, b) -> null

                        )))
                .values()
                .stream()
                .sorted(comparator)
                .toList();
    }
}
