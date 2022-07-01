package SQL.queries;

import SQL.dataset.LineItemRow;
import SQL.dataset.OrdersRow;
import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;
import SQL.datatypes.Date;
import SQL.sql_utils.SQLStreamUtils;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Query4 implements Query {

    static class Result {
        String o_orderpriority;
        int order_count;

        void aggregate(OrdersRow o) {
            o_orderpriority = o.o_orderpriority();
            order_count += 1;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "o_orderpriority='" + o_orderpriority + '\'' +
                    ", order_count=" + order_count +
                    '}';
        }
    }


    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1993-07-01");
        Date max = new Date("1993-10-01");
        Comparator<Result> comparator = Comparator.comparing(r -> r.o_orderpriority);
        // lineItemOrdersRightSemiJoin
        return SQLStreamUtils.streamLeftSemiJoin(
                        db.orders().filter(l -> l.o_orderdate().fromTo(min, max)),
                        OrdersRow::o_orderkey,
                        db.lineitem().filter(l -> l.l_commitdate().compareTo(l.l_receiptdate()) < 0),
                        LineItemRow::l_orderkey
                )
                .collect(Collectors.groupingBy(OrdersRow::o_orderpriority, Collector.of(
                        Result::new, Result::aggregate, (a, b) -> null)))
                .values()
                .stream()
                .sorted(comparator)
                .toList();
    }

    public static void main(String[] args) {
        new Query4().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
