package SQL.queries;

import SQL.dataset.*;
import SQL.datatypes.Date;
import SQL.sql_utils.SQLStreamUtils;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Query3 implements Query {
    record Q3JoinedRow(long l_orderkey, double l_extendedprice, double l_discount, Date o_orderdate,
                       int o_shippriority) {
    }

    record Q3Key(long l_orderkey, Date o_orderdate, int o_shippriority) {
    }

    static class Result {
        long l_orderkey;
        double revenue;
        Date o_orderdate;
        int o_shippriority;

        void accumulate(Q3JoinedRow row) {
            l_orderkey = row.l_orderkey;
            o_orderdate = row.o_orderdate;
            o_shippriority = row.o_shippriority;
            revenue += row.l_extendedprice * (1 - row.l_discount);
        }

        @Override
        public String toString() {
            return "Result{" +
                    "l_orderkey=" + l_orderkey +
                    ", revenue=" + revenue +
                    ", o_orderdate=" + o_orderdate +
                    ", o_shippriority=" + o_shippriority +
                    '}';
        }
    }

    final Date o_orderdateMax = new Date("1995-03-15");
    final Date l_shipdateMin = new Date("1995-03-15");

    public List<?> execute(TPCHStreamDB db) {
        var customersStream = db.customer();
        var ordersStream = db.orders();
        var lineitemStream = db.lineitem();

        var customerOrderJoinedStream = SQLStreamUtils.streamJoin(
                customersStream.filter(customer -> customer.c_mktsegment().equals("BUILDING")),
                CustomerRow::c_custkey,
                ordersStream.filter(order -> order.o_orderdate().compareTo(o_orderdateMax) < 0),
                OrdersRow::o_custkey,
                (customer, order) -> order);

        Comparator<Result> c1 = Comparator.comparing(row -> -row.revenue);
        Comparator<Result> c2 = Comparator.comparing(row -> row.o_orderdate);
        Comparator<Result> comparator = c1.thenComparing(c2);

        return SQLStreamUtils.streamJoin(
                        customerOrderJoinedStream,
                        OrdersRow::o_orderkey,
                        lineitemStream.filter(row -> row.l_shipdate().compareTo(l_shipdateMin) > 0),
                        LineItemRow::l_orderkey,
                        (order, lineitem) -> new Q3JoinedRow(
                                lineitem.l_orderkey(),
                                lineitem.l_extendedprice(),
                                lineitem.l_discount(),
                                order.o_orderdate(),
                                order.o_shippriority()))
                .collect(Collectors.groupingBy(
                        q3JoinedRow -> new Q3Key(q3JoinedRow.l_orderkey, q3JoinedRow.o_orderdate, q3JoinedRow.o_shippriority),
                        Collector.of(
                                Result::new,
                                Result::accumulate,
                                (a, b) -> null)))

                .values()
                .stream()
                .sorted(comparator)
                .limit(10)
                .toList();
    }

    public static void main(String[] args) {
        new Query3().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
