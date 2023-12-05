package learn.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class CustomDataSource implements SourceFunction<Map<String, String>> {
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Map<String, String>> sourceContext) throws Exception {
        while (isRunning) {
            Map<String, String> map = new HashMap<>();
            map.put("id", String.valueOf(System.currentTimeMillis() % 10));
            map.put("date", new SimpleDateFormat().format(new Date()));
            map.put("time", String.valueOf(System.currentTimeMillis()));
            sourceContext.collect(map);
            Thread.sleep(3000*1);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
