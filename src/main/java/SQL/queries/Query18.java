package SQL.queries;

import SQL.dataset.*;
import SQL.datatypes.Date;
import SQL.sql_utils.SQLStreamUtils;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Query18 implements Query {


    static class InnerSelect {
        long l_orderkey;
        double quantity;

        public void aggregate(LineItemRow other) {
            l_orderkey = other.l_orderkey();
            quantity += other.l_quantity();
        }

        public long getL_orderkey() {
            return l_orderkey;
        }

        public double getQuantity() {
            return quantity;
        }
    }


    record CustomerOrder(String c_name, long c_custkey, long o_orderkey, Date o_orderdate, double o_totalprice) {
    }


    record LastRecord(String c_name, long c_custkey, long o_orderkey, Date o_orderdate, double o_totalprice,
                      double quantity) {
    }

    static class Accumulator {

        String c_name;
        long o_custkey;
        long o_orderkey;
        Date o_orderdate;
        double o_totalprice;
        double quantity;

        public void aggregate(LastRecord other) {
            c_name = other.c_name();
            o_custkey = other.c_custkey();
            o_orderkey = other.o_orderkey();
            o_orderdate = other.o_orderdate();
            o_totalprice = other.o_totalprice();
            quantity += other.quantity();
        }

        @Override
        public String toString() {
            return "Result{" +
                    "c_name='" + c_name + '\'' +
                    ", c_custkey=" + o_custkey +
                    ", o_orderkey=" + o_orderkey +
                    ", o_orderdate=" + o_orderdate.toString() +
                    ", o_totalprice=" + o_totalprice +
                    ", quantity=" + quantity +
                    '}';
        }
    }


    @Override
    public List<?> execute(TPCHStreamDB db) {
        //filtered LineItem
        var filteredLineItem = db.lineitem()
                .collect(Collectors.groupingBy(LineItemRow::l_orderkey, Collector.of(
                        InnerSelect::new, InnerSelect::aggregate, (a, b) -> null
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
                (c, o) -> new CustomerOrder(c.c_name(), c.c_custkey(), o.o_orderkey(), o.o_orderdate(), o.o_totalprice())
        );

        Comparator<Accumulator> comparator = SQLStreamUtils.comparator(row -> -row.o_totalprice, row -> row.o_orderdate);


        // join lineitem and after with custAndJoin
        return SQLStreamUtils.streamJoin(
                        custAndOrdersJoin,
                        CustomerOrder::o_orderkey,
                        db.lineitem(),
                        LineItemRow::l_orderkey,
                        (custAndOrder, lineitem) -> new LastRecord(custAndOrder.c_name(),
                                custAndOrder.c_custkey(),
                                lineitem.l_orderkey(),
                                custAndOrder.o_orderdate(),
                                custAndOrder.o_totalprice(),
                                lineitem.l_quantity()
                        )
                )
                .collect(Collectors.groupingBy(
                        row -> new CustomerOrder(row.c_name, row.c_custkey(), row.o_orderkey(), row.o_orderdate(), row.o_totalprice())
                        ,
                        Collector.of(Accumulator::new, Accumulator::aggregate, (a, b) -> null)
                ))
                .values()
                .stream()
                .sorted(comparator)
                .limit(100)
                .toList();
    }


    public static void main(String[] args) {
        new Query18().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }


}
