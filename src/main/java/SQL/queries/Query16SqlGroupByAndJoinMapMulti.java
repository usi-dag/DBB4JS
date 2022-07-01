package SQL.queries;

import SQL.dataset.*;
import SQL.sql_utils.GroupByCollector;
import SQL.sql_utils.SQLCollectors;
import SQL.sql_utils.SQLStreamUtils;
import SQL.sql_utils.StringMultiContains;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class Query16SqlGroupByAndJoinMapMulti implements Query {
    @Override
    public List<?> execute(TPCHStreamDB db) {
        Set<Integer> sizes = Set.of(49, 14, 23, 45, 19, 3, 36, 9);

        // filter Part
        var filteredPart = db.part()
                .filter(p -> !p.p_brand().equals("Brand#45"))
                .filter(p -> !p.p_type().startsWith("MEDIUM POLISHED"))
                .filter(p -> sizes.contains(p.p_size()));


        //join filtered part and partSupplier
        var partAndPartSupp = SQLStreamUtils.streamJoinMapMulti(
                filteredPart,
                PartRow::p_partkey,
                db.partsupp(),
                PartSuppRow::ps_partkey,
                (p, ps) ->
                        new Query16.PartSuppPart(p.p_brand(), p.p_type(), p.p_size(), ps.ps_suppkey())
        );

        Comparator<Query16Imperative.Result> c1 = SQLStreamUtils.comparator(r -> -r.supplier_cnt, p -> p.p_brand);
        Comparator<Query16Imperative.Result> c2 = SQLStreamUtils.comparator(r -> r.p_type, r -> r.p_size);


        StringMultiContains stringMultiContains = new StringMultiContains("Customer", "Complaints");

        return SQLStreamUtils.streamRightAntiJoin(
                        db.supplier().filter(s -> stringMultiContains.match(s.s_comment())),
                        SupplierRow::s_suppkey,
                        partAndPartSupp,
                        Query16.PartSuppPart::ps_suppkey
                ).collect(SQLCollectors.groupingBy(
                        p -> new Query16.GroupBy(p.p_brand(), p.p_type(), p.p_size()),
                        GroupByCollector.of(
                                (Query16.PartSuppPart psp) -> new Query16Imperative.Result(psp.p_brand(), psp.p_type(), psp.p_size()),
                                (a, b) -> a.aggregate(b.ps_suppkey()),
                                (a, b) -> null)
                ))
                .values()
                .stream()
                .sorted(c1.thenComparing(c2))
                .toList();
    }
}
