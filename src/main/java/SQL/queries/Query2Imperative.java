package SQL.queries;

import SQL.dataset.*;
import SQL.sql_utils.tuple_types.DoubleLongPair;
import SQL.sql_utils.tuple_types.LongStringPair;

import java.util.*;


public class Query2Imperative implements Query {

    record C(double s_acctbal,
             String s_name,
             String n_name,
             String s_address,
             String s_phone,
             String s_comment) {
    }

    @Override
    public List<?> execute(TPCHStreamDB db) {
        HashMap<Integer, List<RegionRow>> regionMap = new HashMap<>();
        for (RegionRow r : db.region_arr()) {
            if (r.r_name().equals("EUROPE")) {
                var list = regionMap.computeIfAbsent(r.r_regionkey(), k -> new ArrayList<>());
                list.add(r);
            }
        }

        HashMap<Integer, List<NationRow>> nationMap = new HashMap<>();
        for (NationRow n : db.nation_arr()) {
            var list = regionMap.get(n.n_regionkey());
            if (list != null) {
                nationMap.computeIfAbsent(n.n_nationkey(), k -> new ArrayList<>()).add(n);

            }
        }

        Map<Long, List<SupplierRow>> supplierMap = new HashMap<>();
        for (SupplierRow s : db.supplier_arr()) {
            var list = nationMap.get(s.s_nationkey());
            if (list != null) {
                supplierMap.computeIfAbsent(s.s_suppkey(), k -> new ArrayList<>()).add(s);

            }
        }
        Map<Long, PartSuppRow> partSupplierMap = new HashMap<>();
        for (PartSuppRow ps : db.partsupp_arr()) {
            var list = supplierMap.get(ps.ps_suppkey());
            if (list != null) {
                Long partKey = ps.ps_partkey();
                var supplyCost = partSupplierMap.get(partKey);
                if (supplyCost == null || supplyCost.ps_supplycost() > ps.ps_supplycost()) {
                    partSupplierMap.put(partKey, ps);
                }
            }
        }

        Map<Long, List<PartRow>> partMap = new HashMap<>();
        for (PartRow p : db.part_arr()) {
            if (p.p_size() == 15 && p.p_type().endsWith("BRASS")) {
                partMap.computeIfAbsent(p.p_partkey(), k -> new ArrayList<>()).add(p);
            }
        }


        Map<DoubleLongPair, List<LongStringPair>> innerMap = new HashMap<>();
        for (PartSuppRow ps : partSupplierMap.values()) {
            var list = partMap.get(ps.ps_partkey());
            if (list != null) {
                var supply = ps.ps_supplycost();
                for (PartRow p : list) {
                    innerMap.computeIfAbsent(new DoubleLongPair(supply, p.p_partkey()), k -> new ArrayList<>())
                            .add(new LongStringPair(p.p_partkey(), p.p_mfgr()));
                }
            }
        }

        // left side
        HashMap<Integer, List<RegionRow>> regionMapLeft = new HashMap<>();
        for (RegionRow r : db.region_arr()) {
            if (r.r_name().equals("EUROPE")) {
                var list = regionMapLeft.computeIfAbsent(r.r_regionkey(), k -> new ArrayList<>());
                list.add(r);
            }
        }


        HashMap<Integer, List<NationRow>> nationMapLeft = new HashMap<>();
        for (NationRow n : db.nation_arr()) {

            var list = regionMapLeft.get(n.n_regionkey());
            if (list != null) {
                nationMapLeft.computeIfAbsent(n.n_nationkey(), k -> new ArrayList<>()).add(n);

            }
        }


        Map<Long, List<Query2Imperative.C>> supplierMapLeft = new HashMap<>();
        for (SupplierRow s : db.supplier_arr()) {
            var list = nationMapLeft.get(s.s_nationkey());
            if (list != null) {
                var put = supplierMapLeft.computeIfAbsent(s.s_suppkey(), k -> new ArrayList<>());
                for (var n : list) {
                    put.add(new Query2Imperative.C(s.s_acctbal(), s.s_name(), n.n_name(), s.s_address(), s.s_phone(), s.s_comment()));
                }

            }
        }
        // end of lef side
        List<Query2.Result> listResult = new ArrayList<>();


        for (PartSuppRow ps : db.partsupp_arr()) {
            var list = innerMap.get(new DoubleLongPair(ps.ps_supplycost(), ps.ps_partkey()));
            if (list != null) {
                for (var l : list) {
                    var list2 = supplierMapLeft.get(ps.ps_suppkey());
                    if (list2 != null) {
                        for (var s : list2) {
                            listResult.add(new Query2.Result(s.s_acctbal(), s.s_name(), s.n_name(), l.fst(), l.snd(), s.s_address(), s.s_phone(), s.s_comment()
                            ));
                        }
                    }
                }
            }
        }

        Comparator<Query2.Result> c1 = Comparator.comparing(r -> -r.s_acctbal());
        Comparator<Query2.Result> c2 = Comparator.comparing(Query2.Result::n_name);
        Comparator<Query2.Result> c3 = Comparator.comparing(Query2.Result::s_name);
        Comparator<Query2.Result> c4 = Comparator.comparing(Query2.Result::p_partkey);
        Comparator<Query2.Result> comparator = c1.thenComparing(c2).thenComparing(c3).thenComparing(c4);

        listResult.sort(comparator);
        if (listResult.size() < 100) {
            return listResult;
        } else {
            return listResult.subList(0, 100);
        }
    }
}
