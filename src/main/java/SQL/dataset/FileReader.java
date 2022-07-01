package SQL.dataset;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class FileReader<E> {

    public List<E> readTBLFile(String path, Function<String[], E> function) {
        List<E> list = new ArrayList<>();
        try {
            final BufferedReader in = new BufferedReader(
                    new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8));
            String line;
            while ((line = in.readLine()) != null)  {
                list.add(function.apply(line.split("\\|")));
            }
            in.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;

    }


}
