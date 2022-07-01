package SQL.queries;

import SQL.dataset.*;
import SQL.datatypes.Date;
import SQL.sql_utils.SQLStreamUtils;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Query3JoinMapMulti implements Query {

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

        Comparator<Query3.Result> c1 = Comparator.comparing(row -> -row.revenue);
        Comparator<Query3.Result> c2 = Comparator.comparing(row -> row.o_orderdate);
        Comparator<Query3.Result> comparator = c1.thenComparing(c2);

        return SQLStreamUtils.streamJoinMapMulti(
                        customerOrderJoinedStream,
                        OrdersRow::o_orderkey,
                        lineitemStream.filter(row -> row.l_shipdate().compareTo(l_shipdateMin) > 0),
                        LineItemRow::l_orderkey,
                        (order, lineitem) -> new Query3.Q3JoinedRow(
                                lineitem.l_orderkey(),
                                lineitem.l_extendedprice(),
                                lineitem.l_discount(),
                                order.o_orderdate(),
                                order.o_shippriority()))
                .collect(Collectors.groupingBy(
                        q3JoinedRow -> new Query3.Q3Key(q3JoinedRow.l_orderkey(), q3JoinedRow.o_orderdate(), q3JoinedRow.o_shippriority()),
                        Collector.of(
                                Query3.Result::new,
                                Query3.Result::accumulate,
                                (a, b) -> null)))

                .values()
                .stream()
                .sorted(comparator)
                .limit(10)
                .toList();
    }
}
