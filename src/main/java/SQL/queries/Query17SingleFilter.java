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

public class Query17SingleFilter implements Query {
    @Override
    public List<?> execute(TPCHStreamDB db) {

        var lineItemGroupBy = db.lineitem()
                .collect(Collectors.groupingBy(LineItemRow::l_partkey, Collector.of(
                        Query17.LineItemAvg::new, Query17.LineItemAvg::aggregate, (a, b) -> null, Query17.LineItemAvg::finalizer
                ))).values().stream();


        var lineItemPartJoin = SQLStreamUtils.streamJoin(
                db.part()
                        .filter(p -> p.p_brand().equals("Brand#23") && p.p_container().equals("MED BOX")),
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

        return Collections.singletonList(new Query17.Result((LineItemAndLineItemJoin)));

    }
}
