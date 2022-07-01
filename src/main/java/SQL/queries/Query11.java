package SQL.queries;

import SQL.dataset.*;
import SQL.sql_utils.SQLStreamUtils;

import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Query11 implements Query {

    static class InnerSelect {
        double sum;

        void aggregate(PartSuppRow ps) {
            sum += (ps.ps_supplycost() * ps.ps_availqty());
        }

        InnerSelect finalizer() {
            sum = sum * 0.0001;
            return this;
        }
    }

    static class OuterSelect {
        long ps_partkey;
        double value;


        void aggregate(PartSuppRow ps) {
            ps_partkey = ps.ps_partkey();
            value += (ps.ps_supplycost() * ps.ps_availqty());
        }

        @Override
        public String toString() {
            return "Result{" +
                    "ps_partkey=" + ps_partkey +
                    ", value=" + value +
                    '}';
        }
    }


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
                        Collector.of(InnerSelect::new, InnerSelect::aggregate, (a, b) -> null, InnerSelect::finalizer));


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
        ).collect(Collectors.groupingBy(PartSuppRow::ps_partkey, Collector.of(
                OuterSelect::new, OuterSelect::aggregate, (a, b) -> null)
        ));

        Comparator<OuterSelect> comparator = Comparator.comparingDouble(o -> -o.value);


        return outerSupplierPartSuppJoin.values()
                .stream()
                .filter(outerSelect -> outerSelect.value > innerSupplierPartSuppJoin.sum)
                .sorted(comparator)
                .toList();
    }


    public static void main(String[] args) {
        new Query11().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
