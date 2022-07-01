package SQL.queries;

import SQL.dataset.*;
import SQL.datatypes.Date;
import SQL.sql_utils.SQLStreamUtils;

import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Query10JoinMapMulti implements Query {
    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1993-10-01");
        Date max = new Date("1994-01-01");

        // Cust and orders join
        var custOrderJoin = SQLStreamUtils.streamJoinMapMulti(
                db.orders().filter(o -> o.o_orderdate().fromTo(min, max)),
                OrdersRow::o_custkey,
                db.customer(),
                CustomerRow::c_custkey,
                (o, c) -> new Query10.CustOrder(c.c_custkey(), c.c_name(), c.c_acctbal(), c.c_address(), c.c_phone(), c.c_comment(), c.c_nationkey(), o.o_orderkey())
        );

        // cust order nation join
        var custOrderNationJoin = SQLStreamUtils.streamJoinMapMulti(
                db.nation(),
                NationRow::n_nationkey,
                custOrderJoin,
                Query10.CustOrder::c_nationkey,
                (n, c) -> new Query10.NationCustOrder(c.c_custkey(), c.c_name(), c.c_acctbal(), n.n_name(), c.c_address(), c.c_phone(), c.c_comment(), c.o_orderkey())
        );

        Comparator<Query10.Result> comparator = Comparator.comparing(result -> -result.revenue);

        // custOrderNation and LineItem join
        return SQLStreamUtils.streamJoinMapMulti(
                        custOrderNationJoin,
                        Query10.NationCustOrder::o_orderkey,
                        db.lineitem().filter(lineItemRow -> lineItemRow.l_returnflag().equals("R")),
                        LineItemRow::l_orderkey,
                        (con, l) -> new Query10.LineItemNationCustOrder(con.c_custkey(), con.c_name(), con.c_acctbal(), con.n_name(), con.c_address(), con.c_phone(),
                                con.c_comment(), l.l_extendedprice(), l.l_discount())
                )
                .collect(Collectors.groupingBy(c -> new Query10.Q10Key(c.c_custkey(), c.c_name(), c.c_acctbal(), c.c_phone(), c.n_name(), c.c_address(), c.c_comment()),
                        Collector.of(
                                Query10.Result::new, Query10.Result::aggregate, (a, b) -> null
                        )
                ))
                .values()
                .stream()
                .sorted(comparator)
                .limit(20)
                .toList();


    }
}
