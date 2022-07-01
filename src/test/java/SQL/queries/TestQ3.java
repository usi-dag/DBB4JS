package SQL.queries;

import SQL.datatypes.Date;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQ3 implements TestQ {


    @Test
    @Override
    public void testCorrectness() {
        var stream = new Query3().execute(tpchStreamDB);
        var imperative = new Query3Imperative().execute(tpchStreamDB);
        var joinMapMulti = new Query3JoinMapMulti().execute(tpchStreamDB);
        var partialResult = new Query3SqlGroupBy().execute(tpchStreamDB);
        var joinMapMultiPartialResult = new Query3SqlGroupByAndJoinMapMulti().execute(tpchStreamDB);
        var parallel = new Query3Parallel().execute(tpchStreamDB);

        int size = stream.size();

        // check size
        assertEquals(size, imperative.size());
        assertEquals(size, joinMapMulti.size());
        assertEquals(size, partialResult.size());
        assertEquals(size, joinMapMultiPartialResult.size());
        assertEquals(size, parallel.size());

        var s = stream.toString();


        try {
            assertEquals(s, imperative.toString());
        } catch (AssertionFailedError ef) {
            for (int i = 0; i < size; i++) {
                var a = (Query3.Result) stream.get(i);
                var c = (Query3Imperative.Result) imperative.get(i);

                if (checkRoundingError(a.l_orderkey, c.l_orderkey, a.revenue, c.revenue, a.o_orderdate, c.o_orderdate, a.o_shippriority, c.o_shippriority)) {
                    throw ef;
                }

            }
        }

        assertEquals(s, joinMapMulti.toString());
        assertEquals(s, partialResult.toString());
        assertEquals(s, joinMapMultiPartialResult.toString());
        assertEquals(s, parallel.toString());

    }

    boolean checkRoundingError(long l_orderkey_a, long l_orderkey_b, double revenue_a, double revenue_b, Date o_orderdate_a, Date o_orderdate_b,
                               int o_shippriority_a, int o_shippriority_b) {
        return l_orderkey_a != l_orderkey_b || !isARoundingError(revenue_a, revenue_b) || o_orderdate_a.compareTo(o_orderdate_b) != 0 || o_shippriority_a != o_shippriority_b;
    }
}
