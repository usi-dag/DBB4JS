package SQL.queries;

import SQL.dataset.LineItemRow;
import SQL.dataset.PartRow;
import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;
import SQL.sql_utils.SQLStreamUtils;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Query17 implements Query {

    static class LineItemAvg {
        long l_partkey;
        double avg;
        double count;

        public void aggregate(LineItemRow l) {
            l_partkey = l.l_partkey();
            avg += l.l_quantity();
            count += 1;
        }

        public LineItemAvg finalizer() {
            avg = 0.2D * (avg / count);
            return this;
        }
    }

    record Result(double avg_yearly) {
        @Override
        public String toString() {
            return "Result{" +
                    "avg_yearly=" + avg_yearly +
                    '}';
        }
    }

    @Override
    public List<?> execute(TPCHStreamDB db) {

        var lineItemGroupBy = db.lineitem()
                .collect(Collectors.groupingBy(LineItemRow::l_partkey, Collector.of(
                        LineItemAvg::new, LineItemAvg::aggregate, (a, b) -> null, LineItemAvg::finalizer
                ))).values().stream();


        var lineItemPartJoin = SQLStreamUtils.streamJoin(
                db.part()
                        .filter(p -> p.p_brand().equals("Brand#23"))
                        .filter(p -> p.p_container().equals("MED BOX")),
                PartRow::p_partkey,
                lineItemGroupBy,
                l -> l.l_partkey,
                (p, l) -> l
        );


        var LineItemAndLineItemJoin = SQLStreamUtils.streamJoinFilter(
                        lineItemPartJoin,
                        l -> l.l_partkey,
                        db.lineitem(),
                        LineItemRow::l_partkey,
                        (l1, l2) -> l2.l_extendedprice(),
                        (l1, l2) -> l2.l_quantity() < l1.avg
                ).mapToDouble(t -> t)
                .sum() / 7.0D;

        return Collections.singletonList(new Result((LineItemAndLineItemJoin)));


    }

    public static void main(String[] args) {
        new Query17().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
