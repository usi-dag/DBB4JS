package SQL.queries;

import SQL.dataset.CustomerRow;
import SQL.dataset.OrdersRow;
import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;
import SQL.sql_utils.SQLStreamUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Query22 implements Query {

    static class Acctbal {
        double c_acctbal;
        double count;
        double avg;

        public void aggregate(CustomerRow c) {
            c_acctbal += c.c_acctbal();
            count += 1;
        }

        public Acctbal finalizer() {
            this.avg = this.c_acctbal / this.count;
            return this;
        }
    }

    record InnerSelect(String cntrycode, double c_acctbal, long c_custkey) {
    }


    static class Result {
        String cntrycode;
        int numcust;
        double totacctbal;

        public void aggregate(InnerSelect i) {
            cntrycode = i.cntrycode();
            numcust += 1;
            totacctbal += i.c_acctbal();
        }

        @Override
        public String toString() {
            return "Result{" +
                    "cntrycode='" + cntrycode + '\'' +
                    ", numcust=" + numcust +
                    ", totacctbal=" + String.format("%.2f", totacctbal) +
                    '}';
        }
    }

    @Override
    public List<?> execute(TPCHStreamDB db) {
        Set<String> c_phone = Set.of("13", "31", "23", "29", "30", "18", "17");
        var customerGroupBy =
                db.customer().filter(c -> c.c_acctbal() > 0.D)
                        .filter(c -> c_phone.contains(c.c_phone().substring(0, 2)))
                        .collect(Collector.of(Acctbal::new, Acctbal::aggregate, (a, b) -> null, Acctbal::finalizer));


        var customersJoin =
                db.customer()
                        .filter(c -> c_phone.contains(c.c_phone().substring(0, 2)))
                        .filter(c -> c.c_acctbal() > customerGroupBy.avg)
                        .map(c -> new InnerSelect(c.c_phone().substring(0, 2), c.c_acctbal(), c.c_custkey()));


        Comparator<Result> comparator = Comparator.comparing(r -> r.cntrycode);

        return SQLStreamUtils.streamLeftAntiJoin(
                        customersJoin,
                        InnerSelect::c_custkey,
                        db.orders(),
                        OrdersRow::o_custkey
                ).collect(
                        Collectors.groupingBy(InnerSelect::cntrycode, Collector.of(
                                Result::new, Result::aggregate, (a, b) -> null
                        ))
                )
                .values()
                .stream()
                .sorted(comparator)
                .toList();


    }


    public static void main(String[] args) {
        new Query22().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
