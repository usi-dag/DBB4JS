package SQL.queries;

import SQL.dataset.*;
import SQL.sql_utils.SQLStreamUtils;
import SQL.sql_utils.tuple_types.DoubleLongPair;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;


public class Query2 implements Query {

    public static class InnerSelect {
        double min = Integer.MAX_VALUE;
        public long ps_partkey;

        public void aggregate(PartSuppRow ps) {
            if (min > ps.ps_supplycost()) {
                ps_partkey = ps.ps_partkey();
                min = ps.ps_supplycost();
            }
        }
    }

    record NationSupplier(long s_suppkey, double s_acctbal, String s_name, String n_name, String s_address,
                          String s_phone, String s_comment) {
    }

    record InnerPart(long p_partkey,
                     String p_mfgr, double min) {
    }

    record InnerPartSupp(long p_partkey,
                         String p_mfgr, double min, long ps_suppkey) {
    }


    record Result(double s_acctbal, String s_name, String n_name, long p_partkey,
                  String p_mfgr, String s_address,
                  String s_phone, String s_comment) {
    }

    @Override
    public List<?> execute(TPCHStreamDB db) {
        var innerRegionNationJoin = SQLStreamUtils.streamJoin(
                db.region().filter(r -> r.r_name().equals("EUROPE")),
                RegionRow::r_regionkey,
                db.nation(),
                NationRow::n_regionkey,
                (r, n) -> n
        );

        var innerNationSupplierJoin = SQLStreamUtils.streamJoin(
                innerRegionNationJoin,
                NationRow::n_nationkey,
                db.supplier(),
                SupplierRow::s_nationkey,
                (n, s) -> s
        );

        var innerSupplierPartSupplierJoin = SQLStreamUtils.streamJoin(
                        innerNationSupplierJoin,
                        SupplierRow::s_suppkey,
                        db.partsupp(),
                        PartSuppRow::ps_suppkey,
                        (s, ps) -> ps
                ).collect(Collectors.groupingBy(PartSuppRow::ps_partkey, Collector.of(
                        InnerSelect::new, InnerSelect::aggregate, (a, b) -> null
                )))
                .values()
                .stream();

        var partAndPsJoin = SQLStreamUtils.streamJoin(
                db.part().filter(p -> p.p_size() == 15)
                        .filter(p -> p.p_type().endsWith("BRASS")),
                PartRow::p_partkey,
                innerSupplierPartSupplierJoin,
                (i) -> i.ps_partkey,
                (p, i) -> new InnerPart(p.p_partkey(), p.p_mfgr(), i.min)
        );

        // chiedere questo !!!! QQQ
        var innerSelectPartSuppJoin = SQLStreamUtils.streamJoin(
                partAndPsJoin,
                (i) -> new DoubleLongPair(i.min, i.p_partkey()),
                db.partsupp(),
                (ps) -> new DoubleLongPair(ps.ps_supplycost(), ps.ps_partkey()),
                (i, ps) -> new InnerPartSupp(ps.ps_partkey(), i.p_mfgr(), i.min, ps.ps_suppkey())

        );

        var regionNationJoin = SQLStreamUtils.streamJoin(
                db.region().filter(r -> r.r_name().equals("EUROPE")),
                RegionRow::r_regionkey,
                db.nation(),
                NationRow::n_regionkey,
                (r, n) -> n
        );

        var nationSupplierJoin = SQLStreamUtils.streamJoin(
                regionNationJoin,
                NationRow::n_nationkey,
                db.supplier(),
                SupplierRow::s_nationkey,
                (n, s) -> new NationSupplier(s.s_suppkey(), s.s_acctbal(), s.s_name(), n.n_name(), s.s_address(), s.s_phone(), s.s_comment())
        );

        Comparator<Result> c1 = Comparator.comparing(r -> -r.s_acctbal());
        Comparator<Result> c2 = Comparator.comparing(Result::n_name);
        Comparator<Result> c3 = Comparator.comparing(Result::s_name);
        Comparator<Result> c4 = Comparator.comparing(Result::p_partkey);
        Comparator<Result> comparator = c1.thenComparing(c2).thenComparing(c3).thenComparing(c4);

        return SQLStreamUtils.streamJoin(
                        nationSupplierJoin,
                        NationSupplier::s_suppkey,
                        innerSelectPartSuppJoin,
                        InnerPartSupp::ps_suppkey,
                        (s, ps) -> new Result(s.s_acctbal(), s.s_name(), s.n_name(), ps.p_partkey(), ps.p_mfgr(), s.s_address(), s.s_phone(), s.s_comment())
                )
                .sorted(comparator)
                .limit(100)
                .toList();

    }

    public static void main(String[] args) {
        new Query2().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
