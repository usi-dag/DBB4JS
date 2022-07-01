package SQL.queries;

import SQL.dataset.*;
import SQL.sql_utils.SQLStreamUtils;
import SQL.sql_utils.StringMultiContains;

import java.util.*;

public class Query16Imperative implements Query {

    record GroupBy(String p_brand, String p_type, int p_size) {
    }

    static class Result {
        final String p_brand;
        final String p_type;
        final int p_size;
        int supplier_cnt = 0;

        final Set<Long> supplierCntSet = new HashSet<>();

        public Result(String p_brand, String p_type, int p_size) {
            this.p_brand = p_brand;
            this.p_type = p_type;
            this.p_size = p_size;
        }

        public void aggregate(long ps_suppkey) {
            if (!supplierCntSet.contains(ps_suppkey)) {
                supplierCntSet.add(ps_suppkey);
                supplier_cnt += 1;
            }
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
    }


    @Override
    public List<?> execute(TPCHStreamDB db) {
        Set<Integer> sizes = Set.of(49, 14, 23, 45, 19, 3, 36, 9);
        var partArr = db.part_arr();
        var partSuppArr = db.partsupp_arr();

        Map<Long, List<PartRow>> partMap = new HashMap<>();
        for (PartRow p : partArr) {
            if (!p.p_brand().equals("Brand#45") && !p.p_type().startsWith("MEDIUM POLISHED") && sizes.contains(p.p_size())) {
                var res = partMap.computeIfAbsent(p.p_partkey(), k -> new ArrayList<>());
                res.add(p);
            }
        }

        StringMultiContains stringMultiContains = new StringMultiContains("Customer", "Complaints");

        Set<Long> supplierSet = new HashSet<>();
        for (SupplierRow s : db.supplier_arr()) {
            if (stringMultiContains.match(s.s_comment())) {
                supplierSet.add(s.s_suppkey());
            }
        }

        Map<GroupBy, Result> finalMap = new HashMap<>();

        for (PartSuppRow ps : partSuppArr) {
            var part = partMap.get(ps.ps_partkey());
            if (part != null) {
                Long ps_suppkey = ps.ps_suppkey();
                for (PartRow p : part) {
                    if (!supplierSet.contains(ps_suppkey)) {
                        var result = finalMap.computeIfAbsent(new GroupBy(p.p_brand(), p.p_type(), p.p_size()),
                                k -> new Result(p.p_brand(), p.p_type(), p.p_size()));
                        if (!result.supplierCntSet.contains(ps_suppkey)) {
                            result.supplierCntSet.add(ps_suppkey);
                            result.supplier_cnt += 1;
                        }
                    }

                }
            }
        }


        Comparator<Result> c1 = SQLStreamUtils.comparator(r -> -r.supplier_cnt, p -> p.p_brand);
        Comparator<Result> c2 = SQLStreamUtils.comparator(r -> r.p_type, r -> r.p_size);

        var arr = finalMap.values().toArray(Result[]::new);
        Arrays.sort(arr, c1.thenComparing(c2));
        return Arrays.asList(arr);


    }

    public static void main(String[] args) {
        new Query16Imperative().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
