package SQL.queries;


import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQ1 implements TestQ{

    @Test
    @Override
    public void testCorrectness() {
        List<?> stream = new Query1().execute(tpchStreamDB);
        List<?> sqlGroupBy = new Query1SqlGroupBy().execute(tpchStreamDB);
        List<?> imperative = new Query1Imperative().execute(tpchStreamDB);
        List<?> parallel = new Query1Parallel().execute(tpchStreamDB);

        int size = stream.size();
        // check size
        assertEquals(size, sqlGroupBy.size());
        assertEquals(size, imperative.size());
        assertEquals(size, parallel.size());



        // check entries
        for (int i = 0; i < size; i++) {
            var a = (Query1.Result) stream.get(i);
            var b = (Query1Imperative.Result) sqlGroupBy.get(i);
            var c = (Query1Imperative.Result) imperative.get(i);
            var d = (Query1Imperative.Result) parallel.get(i);

            try {
                assertEquals(a.toString(), b.toString());
            } catch (AssertionFailedError e) {
                if(checkRoundingError(a.sum_qty, b.sum_qty, a.sum_base_price, b.sum_base_price, a.sum_disc_price,
                        b.sum_disc_price, a.sum_charge, b.sum_charge, a.avg_qty, b.avg_qty,
                        a.avg_price, b.avg_price, a.avg_disc, b.avg_disc, a.count_order, b.count_order)){
                    throw e;
                }
                if(!a.l_linestatus.equals(b.l_linestatus) || !a.l_returnflag.equals(b.l_returnflag)){
                    throw e;
                }
            }

            try {
                assertEquals(a.toString(), c.toString());
            } catch (AssertionFailedError e){
                if(checkRoundingError(a.sum_qty, c.sum_qty, a.sum_base_price, c.sum_base_price, a.sum_disc_price,
                        c.sum_disc_price, a.sum_charge, c.sum_charge, a.avg_qty, c.avg_qty,
                        a.avg_price, c.avg_price, a.avg_disc, c.avg_disc, a.count_order, c.count_order)){
                    throw e;
                }
                if(!a.l_linestatus.equals(c.l_linestatus) || !a.l_returnflag.equals(c.l_returnflag)){
                    throw e;
                }
            }

            try {
                assertEquals(a.toString(), d.toString());
            } catch (AssertionFailedError e){
                if(checkRoundingError(a.sum_qty, d.sum_qty, a.sum_base_price, d.sum_base_price, a.sum_disc_price,
                        d.sum_disc_price, a.sum_charge, d.sum_charge, a.avg_qty, d.avg_qty,
                        a.avg_price, d.avg_price, a.avg_disc, d.avg_disc, a.count_order, d.count_order)){
                    throw e;
                }
                if(!a.l_linestatus.equals(d.l_linestatus) || !a.l_returnflag.equals(d.l_returnflag)){
                    throw e;
                }
            }


        }

    }


    boolean checkRoundingError(double sum_qty_a, double sum_qty_b, double sum_base_price_a, double sum_base_price_b,
                               double sum_disc_price_a, double sum_disc_price_b,
                               double sum_charge_a, double sum_charge_b, double avg_qty_a, double avg_qty_b,
                               double avg_price_a, double avg_price_b, double avg_disc_a, double avg_disc_b,
                               double conut_order_a, double count_order_b){

        return !isARoundingError(sum_qty_a, sum_qty_b) || !isARoundingError(sum_base_price_a, sum_base_price_b)
                || !isARoundingError(sum_charge_a, sum_charge_b) || !isARoundingError(avg_qty_a, avg_qty_b)
                || !isARoundingError(avg_price_a, avg_price_b) || !isARoundingError(avg_disc_a, avg_disc_b)
                || !isARoundingError(conut_order_a, count_order_b) || !isARoundingError(sum_disc_price_a, sum_disc_price_b);
    }

}
