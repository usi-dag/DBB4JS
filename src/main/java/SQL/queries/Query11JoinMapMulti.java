package SQL.queries;

import SQL.dataset.*;
import SQL.sql_utils.SQLStreamUtils;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Query11JoinMapMulti implements Query {
    @Override
    public List<?> execute(TPCHStreamDB db) {
        var innerNationSupplierJoin = SQLStreamUtils.streamJoinMapMulti(
                db.nation().filter(nationRow -> nationRow.n_name().equals("GERMANY")),
                NationRow::n_nationkey,
                db.supplier(),
                SupplierRow::s_nationkey,
                (n, s) -> s
        );

        var innerSupplierPartSuppJoin = SQLStreamUtils.streamJoinMapMulti(
                        innerNationSupplierJoin,
                        SupplierRow::s_suppkey,
                        db.partsupp(),
                        PartSuppRow::ps_suppkey,
                        (s, ps) -> ps)
                .collect(
                        Collector.of(Query11.InnerSelect::new, Query11.InnerSelect::aggregate, (a, b) -> null, Query11.InnerSelect::finalizer));


        var outerNationSupplierJoin = SQLStreamUtils.streamJoinMapMulti(
                db.nation().filter(nationRow -> nationRow.n_name().equals("GERMANY")),
                NationRow::n_nationkey,
                db.supplier(),
                SupplierRow::s_nationkey,
                (n, s) -> s
        );

        var outerSupplierPartSuppJoin = SQLStreamUtils.streamJoinMapMulti(
                outerNationSupplierJoin,
                SupplierRow::s_suppkey,
                db.partsupp(),
                PartSuppRow::ps_suppkey,
                (s, ps) -> ps
        ).collect(Collectors.groupingBy(PartSuppRow::ps_partkey, Collector.of(
                Query11.OuterSelect::new, Query11.OuterSelect::aggregate, (a, b) -> null)
        ));

        Comparator<Query11.OuterSelect> comparator = Comparator.comparingDouble(o -> -o.value);


        return outerSupplierPartSuppJoin.values()
                .stream()
                .filter(outerSelect -> outerSelect.value > innerSupplierPartSuppJoin.sum)
                .sorted(comparator)
                .toList();
    }
}
