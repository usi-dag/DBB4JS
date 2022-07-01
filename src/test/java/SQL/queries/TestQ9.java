package SQL.queries;

import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQ9 implements TestQ {

    @Test
    @Override
    public void testCorrectness() {
        var stream = new Query9().execute(tpchStreamDB);
        var imperative = new Query9Imperative().execute(tpchStreamDB);
        var joinMapMulti = new Query9JoinMapMulti().execute(tpchStreamDB);
        var sqlGroupBy = new Query9SqlGroupBy().execute(tpchStreamDB);
        var sqlGroupByAndJoinMapMulti = new Query9SqlGroupByAndJoinMapMulti().execute(tpchStreamDB);
        var parallel = new Query9Parallel().execute(tpchStreamDB);

        int size = stream.size();

        // check size
        assertEquals(size, imperative.size());
        assertEquals(size, joinMapMulti.size());
        assertEquals(size, sqlGroupBy.size());
        assertEquals(size, sqlGroupByAndJoinMapMulti.size());
        assertEquals(size, parallel.size());

        var s = stream.toString();
        assertEquals(s, joinMapMulti.toString());
        assertEquals(s, sqlGroupBy.toString());
        assertEquals(s, sqlGroupByAndJoinMapMulti.toString());

        // check entries
        for (int i = 0; i < size; i++) {
            var a = (Query9.Result) stream.get(i);
            var b = (Query9Imperative.Result) imperative.get(i);
            var c = (Query9Imperative.Result) parallel.get(i);

            try {
                assertEquals(a.toString(), b.toString());
            } catch (AssertionFailedError e) {
                if (!a.nation.equals(b.nation) || a.o_year != b.o_year || !isARoundingError(a.amount, b.amount)) {
                    throw e;
                }
            }

            try {
                assertEquals(a.toString(), c.toString());
            } catch (AssertionFailedError e) {
                if (!a.nation.equals(c.nation) || a.o_year != c.o_year || !isARoundingError(a.amount, c.amount)) {
                    throw e;
                }
            }


        }

    }

}
