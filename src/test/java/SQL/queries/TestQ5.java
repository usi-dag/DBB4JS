package SQL.queries;

import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQ5 implements TestQ {


    @Test
    @Override
    public void testCorrectness() {
        var stream = new Query5().execute(tpchStreamDB);
        var imperative = new Query5Imperative().execute(tpchStreamDB);
        var joinMapMulti = new Query5JoinMapMulti().execute(tpchStreamDB);
        var partialResult = new Query5SqlGroupBy().execute(tpchStreamDB);
        var partialResultJoinMapMulti = new Query5SqlGroupByAndJoinMapMulti().execute(tpchStreamDB);


        // check size
        int size = stream.size();
        assertEquals(size, imperative.size());
        assertEquals(size, joinMapMulti.size());
        assertEquals(size, partialResult.size());
        assertEquals(size, partialResultJoinMapMulti.size());

        var s = stream.toString();
        assertEquals(s, joinMapMulti.toString());
        assertEquals(s, partialResult.toString());
        assertEquals(s, partialResultJoinMapMulti.toString());

        try {
            assertEquals(s, imperative.toString());
        } catch (AssertionFailedError e){
            for (int i = 0; i < size; i++) {
                var a = (Query5.Result) stream.get(i);
                var b = (Query5Imperative.Result) imperative.get(i);

                assertEquals(a.revenue, b.revenue, 0.0001f);
            }
        }

    }
}
