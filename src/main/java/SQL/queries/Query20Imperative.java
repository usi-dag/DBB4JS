package SQL.queries;

import SQL.dataset.*;
import SQL.datatypes.Date;
import SQL.sql_utils.MarkedArrayList;
import SQL.sql_utils.tuple_types.LongLongPair;

import java.util.*;

public class Query20Imperative implements Query {

    static class LineItemJoin {
        final LongLongPair key;
        double quantity;

        public LineItemJoin(LongLongPair key) {
            this.key = key;
        }

        public void aggregate(LineItemRow l) {
            quantity += l.l_quantity();
        }


    }


    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1994-01-01");
        Date max = new Date("1995-01-01");
        Set<Long> partKeyValues = new HashSet<>();
        for (PartRow p : db.part_arr()) {
            if (p.p_name().startsWith("forest")) {
                partKeyValues.add(p.p_partkey());
            }
        }

        Map<LongLongPair, List<PartSuppRow>> partSuppMap = new HashMap<>();
        for (PartSuppRow ps : db.partsupp_arr()) {
            if (partKeyValues.contains(ps.ps_partkey())) {
                partSuppMap.computeIfAbsent(new LongLongPair(ps.ps_partkey(), ps.ps_suppkey()), k -> new ArrayList<>())
                        .add(ps);
            }
        }

        Map<LongLongPair, LineItemJoin> lineItemMap = new HashMap<>();
        for (LineItemRow l : db.lineitem_arr()) {
            if (l.l_shipdate().fromTo(min, max)) {
                LongLongPair key = new LongLongPair(l.l_partkey(), l.l_suppkey());
                lineItemMap.computeIfAbsent(key, k -> new LineItemJoin(key))
                        .aggregate(l);
            }
        }

        Map<Integer, List<NationRow>> nationMap = new HashMap<>();
        for (NationRow n : db.nation_arr()) {
            if (n.n_name().equals("CANADA")) {
                nationMap.computeIfAbsent(n.n_nationkey(), k -> new ArrayList<>()).add(n);
            }
        }

        Map<Long, MarkedArrayList<SupplierRow>> supplierMap = new HashMap<>();
        for (SupplierRow s : db.supplier_arr()) {
            var list = nationMap.get(s.s_nationkey());
            if (list != null) {
                supplierMap.computeIfAbsent(s.s_suppkey(), k -> new MarkedArrayList<>()).add(s);
            }
        }

        List<Query20SqlGroupBy.Result> resultList = new ArrayList<>();
        for (LineItemJoin l : lineItemMap.values()) {
            var list = partSuppMap.get(l.key);
            if (list != null) {
                for (PartSuppRow ps : list) {
                    if (ps.ps_availqty() > 0.5 * l.quantity) {
                        var list2 = supplierMap.get(ps.ps_suppkey());
                        if (list2 != null && !list2.isMarked()) {
                            list2.mark();
                            for (SupplierRow s : list2) {
                                resultList.add(new Query20SqlGroupBy.Result(s.s_name(), s.s_address()));
                            }
                        }
                    }
                }
            }
        }


        Comparator<Query20SqlGroupBy.Result> comparator = Comparator.comparing(Query20SqlGroupBy.Result::s_name);
        resultList.sort(comparator);
        return resultList;
    }

    public static void main(String[] args) {
        new Query20Imperative().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
