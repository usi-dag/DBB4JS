package SQL.queries;

import SQL.dataset.*;
import SQL.sql_utils.SQLStreamUtils;
import SQL.sql_utils.StringMultiContains;

import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;



public class Query16 implements Query {

    record PartSuppPart(String p_brand, String p_type, int p_size, long ps_suppkey) {
    }

    record GroupBy(String p_brand, String p_type, int p_size) {
    }

    static class Result {
        String p_brand;
        String p_type;
        int p_size;
        int supplier_cnt;

        final Set<Long> supplierCntSet = new HashSet<>();

        void aggregate(PartSuppPart p) {
            this.p_brand = p.p_brand;
            this.p_type = p.p_type;
            this.p_size = p.p_size;
            supplierCntSet.add(p.ps_suppkey());
        }

        @Override
        public String toString() {
            return "Result{" +
                    "p_brand='" + p_brand + '\'' +
                    ", p_type='" + p_type + '\'' +
                    ", p_size=" + p_size +
                    ", supplier_cnt=" + supplier_cnt +
                    '}';
        }

        public Result finalizer(){
            supplier_cnt = supplierCntSet.size();
            return this;
        }
    }

    @Override
    public List<?> execute(TPCHStreamDB db) {
        Set<Integer> sizes = Set.of(49, 14, 23, 45, 19, 3, 36, 9);

        // filter Part
        var filteredPart = db.part()
                .filter(p -> !p.p_brand().equals("Brand#45"))
                .filter(p -> !p.p_type().startsWith("MEDIUM POLISHED"))
                .filter(p -> sizes.contains(p.p_size()));


        //join filtered part and partSupplier
        var partAndPartSupp = SQLStreamUtils.streamJoin(
                filteredPart,
                PartRow::p_partkey,
                db.partsupp(),
                PartSuppRow::ps_partkey,
                (p, ps) ->
                        new PartSuppPart(p.p_brand(), p.p_type(), p.p_size(), ps.ps_suppkey())
        );

        Comparator<Result> c1 = SQLStreamUtils.comparator(r -> -r.supplier_cnt, p -> p.p_brand);
        Comparator<Result> c2 = SQLStreamUtils.comparator(r -> r.p_type, r -> r.p_size);


        StringMultiContains stringMultiContains = new StringMultiContains("Customer", "Complaints");

        return SQLStreamUtils.streamRightAntiJoin(
                        db.supplier().filter(s -> stringMultiContains.match(s.s_comment())),
                        SupplierRow::s_suppkey,
                        partAndPartSupp,
                        PartSuppPart::ps_suppkey
                ).collect(Collectors.groupingBy(
                        p -> new GroupBy(p.p_brand(), p.p_type(), p.p_size()),
                        Collector.of(Result::new, Result::aggregate, (a, b) -> null, Result::finalizer)
                ))
                .values()
                .stream()
                .sorted(c1.thenComparing(c2))
                .toList();

    }

    public static void main(String[] args) {
        new Query16().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
