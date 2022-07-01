package SQL.queries;

import SQL.dataset.LineItemRow;
import SQL.dataset.PartRow;
import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;
import SQL.sql_utils.GroupByCollector;
import SQL.sql_utils.SQLCollectors;
import SQL.sql_utils.SQLStreamUtils;

import java.util.Collections;
import java.util.List;

public class Query17SqlGroupBy implements Query {



    @Override
    public List<?> execute(TPCHStreamDB db) {
        var lineItemGroupBy = db.lineitem()
                .collect(SQLCollectors.groupingBy(LineItemRow::l_partkey, GroupByCollector.of(
                        (LineItemRow l) -> new Query17Imperative.Avg(l.l_partkey()),
                        (a,b) -> a.aggregate(b.l_quantity()),
                        (a, b) -> null,
                        Query17Imperative.Avg::finalizer
                )))
                .values()
                .stream();



        var lineItemPartJoin = SQLStreamUtils.streamJoin(
                db.part()
                        .filter(p -> p.p_brand().equals("Brand#23"))
                        .filter(p -> p.p_container().equals("MED BOX")),
                PartRow::p_partkey,
                lineItemGroupBy,
                i -> i.l_partkey,
                (p, l) -> l
        );


        var LineItemAndLineItemJoin = SQLStreamUtils.streamJoinFilter(
                        lineItemPartJoin,
                        l1 -> l1.l_partkey,
                        db.lineitem(),
                        LineItemRow::l_partkey,
                        (l1, l2) -> l2.l_extendedprice(),
                        (l1, l2) -> l2.l_quantity() < l1.avg
                ).mapToDouble(t-> t)
                .sum() / 7.0D;

        return Collections.singletonList(new Query17.Result((LineItemAndLineItemJoin)));
    }
}
