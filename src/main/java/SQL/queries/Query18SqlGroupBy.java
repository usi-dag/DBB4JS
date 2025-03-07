package SQL.queries;

import SQL.dataset.*;
import SQL.sql_utils.GroupByCollector;
import SQL.sql_utils.SQLCollectors;
import SQL.sql_utils.SQLStreamUtils;

import java.util.Comparator;
import java.util.List;

public class Query18SqlGroupBy implements Query {

    static class InnerSelect {
        final long l_orderkey;
        double quantity;

        public InnerSelect(long l_orderkey) {
            this.l_orderkey = l_orderkey;
        }

        public void aggregate(LineItemRow other) {
            quantity += other.l_quantity();
        }

        public long getL_orderkey() {
            return l_orderkey;
        }

        public double getQuantity() {
            return quantity;
        }
    }

    @Override
    public List<?> execute(TPCHStreamDB db) {
        //filtered LineItem
        var filteredLineItem = db.lineitem()
                .collect(SQLCollectors.groupingBy(
                        LineItemRow::l_orderkey,
                        GroupByCollector.of(
                                (LineItemRow l) -> new InnerSelect(l.l_orderkey()),
                                InnerSelect::aggregate,
                                (a, b) -> null
                        )))
                .values()
                .stream()
                .filter(row -> row.quantity > 300);


        // right semi join orders and lineitem

        var joinOrdersAndLine = SQLStreamUtils.streamRightSemiJoin(
                filteredLineItem,
                InnerSelect::getL_orderkey,
                db.orders(),
                OrdersRow::o_orderkey
        );


        var custAndOrdersJoin = SQLStreamUtils.streamJoin(
                db.customer(),
                CustomerRow::c_custkey,
                joinOrdersAndLine,
                OrdersRow::o_custkey,
                (c, o) -> new Query18.CustomerOrder(c.c_name(), c.c_custkey(), o.o_orderkey(), o.o_orderdate(), o.o_totalprice())
        );

        Comparator<Query18Imperative.Result> comparator = SQLStreamUtils.comparator(row -> -row.o_totalprice, row -> row.o_orderdate);


        // join lineitem and after with custAndJoin
        return SQLStreamUtils.streamJoin(
                        custAndOrdersJoin,
                        Query18.CustomerOrder::o_orderkey,
                        db.lineitem(),
                        LineItemRow::l_orderkey,
                        (custAndOrder, lineitem) -> new Query18.LastRecord(custAndOrder.c_name(),
                                custAndOrder.c_custkey(),
                                lineitem.l_orderkey(),
                                custAndOrder.o_orderdate(),
                                custAndOrder.o_totalprice(),
                                lineitem.l_quantity()
                        )
                )
                .collect(SQLCollectors.groupingBy(
                        row -> new Query18.CustomerOrder(row.c_name(), row.c_custkey(), row.o_orderkey(), row.o_orderdate(), row.o_totalprice())
                        ,
                        GroupByCollector.of(
                                (Query18.LastRecord lr) -> new Query18Imperative.Result(lr.c_name(), lr.c_custkey(), lr.o_orderkey(), lr.o_orderdate(), lr.o_totalprice()),
                                (a, b) -> a.quantity += b.quantity(),
                                Query18Imperative.Result::combine)
                ))
                .values()
                .stream()
                .sorted(comparator)
                .limit(100)
                .toList();
    }


    public static void main(String[] args) {
        new Query18SqlGroupBy().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }


}
