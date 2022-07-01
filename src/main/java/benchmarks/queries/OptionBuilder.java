package benchmarks.queries;

import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.TimeUnit;

public class OptionBuilder {

    static Runner getRunner(String className) {

        var opt = new OptionsBuilder()
                .include(className)
                .verbosity(VerboseMode.EXTRA)
                .forks(1)
                .timeUnit(TimeUnit.MILLISECONDS)
                .mode(Mode.AverageTime)
                .measurementIterations(5)
                .warmupIterations(5)
                .build();
        return new Runner(opt);
    }

    static Runner getRunnerForMultipleBenchmark(String[] classNames){
        var optBuilder = new OptionsBuilder();
        for (String className : classNames) {
            optBuilder.include(className);
        }
        optBuilder
                .verbosity(VerboseMode.EXTRA)
                .forks(1)
                .timeUnit(TimeUnit.MILLISECONDS)
                .mode(Mode.AverageTime)
                .measurementIterations(5)
                .warmupIterations(5);
        return new Runner(optBuilder.build());
    }


    static Runner getRunnerForMultipleBenchmark(Class<?>[] classNames){
        var optBuilder = new OptionsBuilder();
        for (var className : classNames) {
            optBuilder.include(className.getSimpleName());
        }
        optBuilder
                .verbosity(VerboseMode.EXTRA)
                .forks(1)
                .timeUnit(TimeUnit.MILLISECONDS)
                .mode(Mode.AverageTime)
                .measurementIterations(5)
                .warmupIterations(5);
        return new Runner(optBuilder.build());
    }
}
