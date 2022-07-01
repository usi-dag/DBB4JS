package SQL.queries;

import SQL.dataset.LineItemRow;
import SQL.dataset.Query;
import SQL.dataset.SupplierRow;
import SQL.dataset.TPCHStreamDB;
import SQL.datatypes.Date;
import SQL.sql_utils.GroupByCollector;
import SQL.sql_utils.SQLCollectors;
import SQL.sql_utils.SQLStreamUtils;

import java.util.Comparator;
import java.util.List;

public class Query15SqlGroupBy implements Query {
    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1996-01-01");
        Date max = new Date("1996-04-01");

        var temScanList = db.lineitem().filter(l -> l.l_shipdate().fromTo(min, max))
                .collect(SQLCollectors.groupingBy(LineItemRow::l_suppkey, GroupByCollector.of(
                        (LineItemRow l) -> new Query15Imperative.TempScan(l.l_suppkey()),
                        (a, b) -> a.aggregate(b.l_extendedprice(), b.l_discount()),
                        (a, b) -> null
                ))).values().stream().toList();

        Comparator<Query15Imperative.TempScan> cMax = Comparator.comparing(t -> t.total_revenue);
        var firstJoin = SQLStreamUtils.streamJoin(
                temScanList.stream().max(cMax).stream(),
                t -> t.total_revenue,
                temScanList.stream(),
                t -> t.total_revenue,
                (t1, t2) -> t2
        );

        Comparator<Query15.Result> comparator = Comparator.comparing(Query15.Result::s_suppkey);

        return SQLStreamUtils.streamJoin(
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
