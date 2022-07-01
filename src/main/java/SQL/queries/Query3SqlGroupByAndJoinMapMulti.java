package SQL.queries;

import SQL.dataset.*;
import SQL.datatypes.Date;
import SQL.sql_utils.GroupByCollector;
import SQL.sql_utils.SQLCollectors;
import SQL.sql_utils.SQLStreamUtils;

import java.util.Comparator;
import java.util.List;

public class Query3SqlGroupByAndJoinMapMulti implements Query {
    final Date o_orderdateMax = new Date("1995-03-15");
    final Date l_shipdateMin = new Date("1995-03-15");

    @Override
    public List<?> execute(TPCHStreamDB db) {

        var customersStream = db.customer();
        var ordersStream = db.orders();
        var lineitemStream = db.lineitem();

        var customerOrderJoinedStream = SQLStreamUtils.streamJoinMapMulti(
                customersStream.filter(customer -> customer.c_mktsegment().equals("BUILDING")),
                CustomerRow::c_custkey,
                ordersStream.filter(order -> order.o_orderdate().compareTo(o_orderdateMax) < 0),
                OrdersRow::o_custkey,
                (customer, order) -> order);

        var cusordLineiteimJoinedStream = SQLStreamUtils.streamJoinMapMulti(
                customerOrderJoinedStream,
                OrdersRow::o_orderkey,
                lineitemStream.filter(row -> row.l_shipdate().compareTo(l_shipdateMin) > 0),
                LineItemRow::l_orderkey,
                (order, lineitem) -> new Query3.Q3JoinedRow(
                        lineitem.l_orderkey(),
                        lineitem.l_extendedprice(),
                        lineitem.l_discount(),
                        order.o_orderdate(),
                        order.o_shippriority()));


        // comparator
        Comparator<Query3Imperative.Result> c1 = Comparator.comparing(row -> -row.revenue);
        Comparator<Query3Imperative.Result> c2 = Comparator.comparing(row -> row.o_orderdate);
        Comparator<Query3Imperative.Result> comparator = c1.thenComparing(c2);


        return cusordLineiteimJoinedStream
                .collect(SQLCollectors.groupingBy(
                        x -> new Query3.Q3Key(x.l_orderkey(), x.o_orderdate(), x.o_shippriority()),
                        GroupByCollector.of(
                                (Query3.Q3JoinedRow l) -> new Query3Imperative.Result(l.l_orderkey(), l.o_orderdate(), l.o_shippriority()),
                                (a, b) -> a.aggregate(b.l_extendedprice(), b.l_discount()),
                                Query3Imperative.Result::combine
                        )))
                .values()
                .stream()
                .sorted(comparator)
                .limit(10)
                .toList();
    }
}
