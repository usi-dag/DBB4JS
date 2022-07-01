package SQL.queries;


import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQ6 implements TestQ{

    @Test
    @Override
    public void testCorrectness() {
        var stream = new Query6().execute(tpchStreamDB);
        var imperative = new Query6Imperative().execute(tpchStreamDB);
        var singleFilter = new Query6SingleFilter().execute(tpchStreamDB);
        var parallel = new Query6Parallel().execute(tpchStreamDB);

        // check size
        int size = stream.size();
        assertEquals(size, imperative.size());
        assertEquals(size, singleFilter.size());
        assertEquals(size, parallel.size());

        var s = stream.toString();
        assertEquals(s, singleFilter.toString());
        assertEquals(s, parallel.toString());
        try {
            assertEquals(s, imperative.toString());
        } catch (AssertionFailedError e){
            for (int i = 0; i < size; i++) {
                var streamValue = (Query6.Result) stream.get(i);
                var imperativeValue = (Query6.Result) imperative.get(i);

                assertEquals(streamValue.revenue(), imperativeValue.revenue(), 0.0001f);
            }
        }

    }

}
