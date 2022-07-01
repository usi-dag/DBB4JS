package SQL.queries;

import SQL.dataset.*;
import SQL.sql_utils.SQLStreamUtils;
import SQL.sql_utils.tuple_types.DoubleLongPair;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Query2JoinMapMulti implements Query {

    @Override
    public List<?> execute(TPCHStreamDB db) {
        var innerRegionNationJoin = SQLStreamUtils.streamJoinMapMulti(
                db.region().filter(r -> r.r_name().equals("EUROPE")),
                RegionRow::r_regionkey,
                db.nation(),
                NationRow::n_regionkey,
                (r, n) -> n
        );

        var innerNationSupplierJoin = SQLStreamUtils.streamJoinMapMulti(
                innerRegionNationJoin,
                NationRow::n_nationkey,
                db.supplier(),
                SupplierRow::s_nationkey,
                (n, s) -> s
        );

        var innerSupplierPartSupplierJoin = SQLStreamUtils.streamJoinMapMulti(
                        innerNationSupplierJoin,
                        SupplierRow::s_suppkey,
                        db.partsupp(),
                        PartSuppRow::ps_suppkey,
                        (s, ps) -> ps
                ).collect(Collectors.groupingBy(PartSuppRow::ps_partkey, Collector.of(
                        Query2.InnerSelect::new, Query2.InnerSelect::aggregate, (a, b) -> null
                )))
                .values()
                .stream();

        var partAndPsJoin = SQLStreamUtils.streamJoinMapMulti(
                db.part().filter(p -> p.p_size() == 15)
                        .filter(p -> p.p_type().endsWith("BRASS")),
                PartRow::p_partkey,
                innerSupplierPartSupplierJoin,
                (i) -> i.ps_partkey,
                (p, i) -> new Query2.InnerPart(p.p_partkey(), p.p_mfgr(), i.min)
        );

        var innerSelectPartSuppJoin = SQLStreamUtils.streamJoinMapMulti(
                partAndPsJoin,
                (i) -> new DoubleLongPair(i.min(), i.p_partkey()),
                db.partsupp(),
                (ps) -> new DoubleLongPair(ps.ps_supplycost(), ps.ps_partkey()),
                (i, ps) -> new Query2.InnerPartSupp(ps.ps_partkey(), i.p_mfgr(), i.min(), ps.ps_suppkey())

        );

        var regionNationJoin = SQLStreamUtils.streamJoinMapMulti(
                db.region().filter(r -> r.r_name().equals("EUROPE")),
                RegionRow::r_regionkey,
                db.nation(),
                NationRow::n_regionkey,
                (r, n) -> n
        );

        var nationSupplierJoin = SQLStreamUtils.streamJoinMapMulti(
                regionNationJoin,
                NationRow::n_nationkey,
                db.supplier(),
                SupplierRow::s_nationkey,
                (n, s) -> new Query2.NationSupplier(s.s_suppkey(), s.s_acctbal(), s.s_name(), n.n_name(), s.s_address(), s.s_phone(), s.s_comment())
        );

        Comparator<Query2.Result> c1 = Comparator.comparing(r -> -r.s_acctbal());
        Comparator<Query2.Result> c2 = Comparator.comparing(Query2.Result::n_name);
        Comparator<Query2.Result> c3 = Comparator.comparing(Query2.Result::s_name);
        Comparator<Query2.Result> c4 = Comparator.comparing(Query2.Result::p_partkey);
        Comparator<Query2.Result> comparator = c1.thenComparing(c2).thenComparing(c3).thenComparing(c4);

        return SQLStreamUtils.streamJoinMapMulti(
                        nationSupplierJoin,
                        Query2.NationSupplier::s_suppkey,
                        innerSelectPartSuppJoin,
                        Query2.InnerPartSupp::ps_suppkey,
                        (s, ps) -> new Query2.Result(s.s_acctbal(), s.s_name(), s.n_name(), ps.p_partkey(), ps.p_mfgr(), s.s_address(), s.s_phone(), s.s_comment())
                )
                .sorted(comparator)
                .limit(100)
                .toList();

    }
}
