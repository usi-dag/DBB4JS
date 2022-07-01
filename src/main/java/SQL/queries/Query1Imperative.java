package SQL.queries;

import SQL.dataset.LineItemRow;
import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;
import SQL.datatypes.Date;
import SQL.sql_utils.tuple_types.StringStringPair;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class Query1Imperative implements Query {

    static class Result {
        final String l_returnflag;
        final String l_linestatus;
        double sum_qty;
        double sum_base_price;
        double sum_disc_price;
        double sum_charge;
        double avg_qty;
        double avg_price;
        double avg_disc;
        double count_order;

        public Result(String l_returnflag, String l_linestatus) {
            this.l_returnflag = l_returnflag;
            this.l_linestatus = l_linestatus;
        }

        public void aggregate(LineItemRow l) {
            sum_qty += l.l_quantity();
            sum_base_price += l.l_extendedprice();
            double disc_price = l.l_extendedprice() * (1.d - l.l_discount());
            sum_disc_price += disc_price;
            sum_charge += disc_price * (1.d + l.l_tax());
            count_order += 1;
            avg_qty += l.l_quantity();
            avg_price += l.l_extendedprice();
            avg_disc += l.l_discount();
        }

        public Result combine(Result other) {
            sum_qty += other.sum_qty;
            sum_base_price += other.sum_base_price;
            sum_disc_price += other.sum_disc_price;
            sum_charge += other.sum_charge;
            count_order += other.count_order;
            avg_qty += other.avg_qty;
            avg_price += other.avg_price;
            avg_disc += other.avg_disc;
            return this;
        }

        public Query1Imperative.Result finalizer() {
            avg_qty = avg_qty / count_order;
            avg_price = avg_price / count_order;
            avg_disc = avg_disc / count_order;
            return this;
        }


        @Override
        public String toString() {
            return "Result{" +
                    "l_returnflag='" + l_returnflag + '\'' +
                    ", l_linestatus='" + l_linestatus + '\'' +
                    ", sum_qty=" + sum_qty +
                    ", sum_base_price=" + sum_base_price +
                    ", sum_disc_price=" + sum_disc_price +
                    ", sum_charge=" + sum_charge +
                    ", avg_qty=" + avg_qty +
                    ", avg_price=" + avg_price +
                    ", avg_disc=" + avg_disc +
                    ", count_order=" + count_order +
                    '}';
        }
    }

    @Override
    public List<?> execute(TPCHStreamDB db) {
        final LocalDate date = LocalDate.parse("1998-12-01", DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        final Date min = new Date(date.minusDays(90).toString());

        HashMap<StringStringPair, Result> map = new HashMap<>();
        for (LineItemRow l : db.lineitem_arr()) {
            if (l.l_shipdate().compareTo(min) <= 0) {
                var result = map.computeIfAbsent(new StringStringPair(l.l_returnflag(), l.l_linestatus()), k -> new Query1Imperative.Result(l.l_returnflag(), l.l_linestatus()));
                result.aggregate(l);
            }
        }
        var mapValues = map.values();
        mapValues.forEach(Result::finalizer);


        Result[] toRet = mapValues.toArray(Result[]::new);
        Comparator<Result> c1 = Comparator.comparing(r -> r.l_returnflag);
        Comparator<Result> c2 = Comparator.comparing(r -> r.l_linestatus);
        Comparator<Result> comparator = c1.thenComparing(c2);
        Arrays.sort(toRet, comparator);
        return Arrays.asList(toRet);
    }

    public static void main(String[] args) {
        new Query1Imperative().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
