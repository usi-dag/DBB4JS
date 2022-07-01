package SQL.queries;

import SQL.dataset.LineItemRow;
import SQL.dataset.Query;
import SQL.dataset.SupplierRow;
import SQL.dataset.TPCHStreamDB;
import SQL.datatypes.Date;
import SQL.sql_utils.SQLStreamUtils;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Query15JoinMapMulti implements Query {
    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1996-01-01");
        Date max = new Date("1996-04-01");


        var temScanList = db.lineitem().filter(l -> l.l_shipdate().fromTo(min, max))
                .collect(Collectors.groupingBy(LineItemRow::l_suppkey, Collector.of(
                        Query15.TempScan::new,
                        Query15.TempScan::aggregate,
                        (a, b) -> null
                ))).values().stream().toList();

        Comparator<Query15.TempScan> cMax = Comparator.comparing(t -> t.total_revenue);
        var firstJoin = SQLStreamUtils.streamJoinMapMulti(
                temScanList.stream().max(cMax).stream(),
                t -> t.total_revenue,
                temScanList.stream(),
                t -> t.total_revenue,
                (t1, t2) -> t2
        );

        Comparator<Query15.Result> comparator = Comparator.comparing(Query15.Result::s_suppkey);

        return SQLStreamUtils.streamJoinMapMulti(
                        firstJoin,
                        t -> t.supplier_no,
                        db.supplier(),
                        SupplierRow::s_suppkey,
                        (t, s) -> new Query15.Result(s.s_suppkey(), s.s_name(), s.s_address(), s.s_phone(), t.total_revenue)
                )
                .sorted(comparator)
                .toList();
    }
}
