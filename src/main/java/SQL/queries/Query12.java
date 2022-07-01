package SQL.queries;

import SQL.dataset.*;
import SQL.datatypes.Date;
import SQL.sql_utils.SQLStreamUtils;
import SQL.sql_utils.tuple_types.StringStringPair;

import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;


public class Query12 implements Query {


    static class Result {
        String l_shipmode;
        double high_line_count;
        double low_line_count;

        void aggregate(StringStringPair other) {
            l_shipmode = other.fst();
            if (other.snd().equals("1-URGENT") || other.snd().equals("2-HIGH")) {
                high_line_count += 1;
            } else {
                low_line_count += 1;
            }
        }

        @Override
        public String toString() {
            return "Result{" +
                    "l_shipmode='" + l_shipmode + '\'' +
                    ", high_line_count=" + high_line_count +
                    ", low_line_count=" + low_line_count +
                    '}';
        }
    }

    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1994-01-01");
        Date max = new Date("1995-01-01");
        Comparator<Result> comparator = Comparator.comparing(row -> row.l_shipmode);
        return SQLStreamUtils
                .streamJoin(
                        db.lineitem()
                                .filter(l -> l.l_shipmode().equals("MAIL") || l.l_shipmode().equals("SHIP"))
                                .filter(l -> l.l_commitdate().compareTo(l.l_receiptdate()) < 0)
                                .filter(l -> l.l_shipdate().compareTo(l.l_commitdate()) < 0)
                                .filter(l -> l.l_receiptdate().fromTo(min, max))
                        ,
                        LineItemRow::l_orderkey,
                        db.orders(),
                        OrdersRow::o_orderkey,
                        (l, o) -> new StringStringPair(l.l_shipmode(), o.o_orderpriority())
                ).collect(Collectors.groupingBy(StringStringPair::fst,
                        Collector.of(
                                Result::new, Result::aggregate, (a, b) -> null)
                ))
                .values()
                .stream()
                .sorted(comparator)
                .toList();

    }


    public static void main(String[] args) {
        new Query12().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
