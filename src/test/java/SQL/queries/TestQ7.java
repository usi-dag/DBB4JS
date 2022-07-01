package SQL.queries;

import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQ7 implements TestQ{

    @Test
    @Override
    public void testCorrectness() {
        var stream = new Query7().execute(tpchStreamDB);
        var imperative = new Query7Imperative().execute(tpchStreamDB);
        var joinMapMulti = new Query7JoinMapMulti().execute(tpchStreamDB);
        var partialResult = new Query7SqlGroupBy().execute(tpchStreamDB);
        var partialResultJoinMapMulti = new Query7SqlGroupByAndJoinMapMulti().execute(tpchStreamDB);

        int size = stream.size();
        assertEquals(size, imperative.size());

        var s =stream.toString();

        try {
            assertEquals(s, imperative.toString());
        } catch (AssertionFailedError e){
            for (int i = 0; i < size; i++) {
                var a = (Query7.Result) stream.get(i);
                var b = (Query7Imperative.Result) imperative.get(i);
                assertEquals(a.cust_nation, b.cust_nation);
                assertEquals(a.supp_nation, b.supp_nation);
                assertEquals(a.l_year,b.l_year);
                assertEquals(a.revenue, b.revenue, 0.0001f);
            }
        }
        assertEquals(s, joinMapMulti.toString());
        assertEquals(s, partialResult.toString());
        assertEquals(s, partialResultJoinMapMulti.toString());
    }
}
