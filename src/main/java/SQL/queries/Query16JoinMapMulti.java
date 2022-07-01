package SQL.queries;

import SQL.dataset.*;
import SQL.sql_utils.SQLStreamUtils;
import SQL.sql_utils.StringMultiContains;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Query16JoinMapMulti implements Query {
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

        Comparator<Query16.Result> c1 = SQLStreamUtils.comparator(r -> -r.supplier_cnt, p -> p.p_brand);
        Comparator<Query16.Result> c2 = SQLStreamUtils.comparator(r -> r.p_type, r -> r.p_size);


        StringMultiContains stringMultiContains = new StringMultiContains("Customer", "Complaints");

        return SQLStreamUtils.streamRightAntiJoin(
                        db.supplier().filter(s -> stringMultiContains.match(s.s_comment())),
                        SupplierRow::s_suppkey,
                        partAndPartSupp,
                        Query16.PartSuppPart::ps_suppkey
                ).collect(Collectors.groupingBy(
                        p -> new Query16.GroupBy(p.p_brand(), p.p_type(), p.p_size()),
                        Collector.of(Query16.Result::new, Query16.Result::aggregate, (a, b) -> null, Query16.Result::finalizer)
                ))
                .values()
                .stream()
                .sorted(c1.thenComparing(c2))
                .toList();
    }
}
