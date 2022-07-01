package SQL.queries;

import SQL.dataset.*;
import SQL.sql_utils.GroupByCollector;
import SQL.sql_utils.SQLCollectors;
import SQL.sql_utils.SQLStreamUtils;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collector;

public class Query11SqlGroupBy implements Query {
    @Override
    public List<?> execute(TPCHStreamDB db) {

        var innerNationSupplierJoin = SQLStreamUtils.streamJoin(
                db.nation().filter(nationRow -> nationRow.n_name().equals("GERMANY")),
                NationRow::n_nationkey,
                db.supplier(),
                SupplierRow::s_nationkey,
                (n, s) -> s
        );


        var innerSupplierPartSuppJoin = SQLStreamUtils.streamJoin(
                        innerNationSupplierJoin,
                        SupplierRow::s_suppkey,
                        db.partsupp(),
                        PartSuppRow::ps_suppkey,
                        (s, ps) -> ps)
                .collect(
                        Collector.of(Query11.InnerSelect::new, Query11.InnerSelect::aggregate, (a, b) -> null, Query11.InnerSelect::finalizer));


        var outerNationSupplierJoin = SQLStreamUtils.streamJoin(
                db.nation().filter(nationRow -> nationRow.n_name().equals("GERMANY")),
                NationRow::n_nationkey,
                db.supplier(),
                SupplierRow::s_nationkey,
                (n, s) -> s
        );

        var outerSupplierPartSuppJoin = SQLStreamUtils.streamJoin(
                outerNationSupplierJoin,
                SupplierRow::s_suppkey,
                db.partsupp(),
                PartSuppRow::ps_suppkey,
                (s, ps) -> ps
        ).collect(SQLCollectors.groupingBy(PartSuppRow::ps_partkey, GroupByCollector.of(
                (PartSuppRow p) -> new Query11Imperative.Result(p.ps_partkey()),
                (a, b) -> a.aggregate(b.ps_supplycost(), b.ps_availqty()),
                (a, b) -> null)
        ));

        Comparator<Query11Imperative.Result> comparator = Comparator.comparingDouble(o -> -o.value);


        return outerSupplierPartSuppJoin.values()
                .stream()
                .filter(outerSelect -> outerSelect.value > innerSupplierPartSuppJoin.sum)
                .sorted(comparator)
                .toList();
    }
}
