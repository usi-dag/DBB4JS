package SQL.queries;

import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQ8 implements TestQ {

    @Test
    @Override
    public void testCorrectness() {
        var stream = new Query8().execute(tpchStreamDB);
        var joinMapMulti = new Query8JoinMapMulti().execute(tpchStreamDB);
        var partialResult = new Query8SqlGroupBy().execute(tpchStreamDB);
        var partialResultJoinMapMulti = new Query8SqlGroupByAndJoinMapMulti().execute(tpchStreamDB);
        var imperative = new Query8Imperative().execute(tpchStreamDB);


        // check size
        int size = stream.size();
        assertEquals(size, imperative.size());
        assertEquals(size, joinMapMulti.size());
        assertEquals(size, partialResult.size());
        assertEquals(size, partialResultJoinMapMulti.size());

        var s = stream.toString();
        // check entries
        assertEquals(s, joinMapMulti.toString());
        assertEquals(s, partialResult.toString());
        assertEquals(s, partialResultJoinMapMulti.toString());

        try {
            assertEquals(s, imperative.toString());
        } catch (AssertionFailedError e) {

            for (int i = 0; i < size; i++) {
                var streamValue = ((Query8.Result) stream.get(i));
                var imperativeValue = ((Query8Imperative.Result) imperative.get(i));
                assertEquals(streamValue.mkt_share, imperativeValue.mkt_share, 0.0001f);
                assertEquals(streamValue.o_years, imperativeValue.o_years);
            }
        }

    }
}
