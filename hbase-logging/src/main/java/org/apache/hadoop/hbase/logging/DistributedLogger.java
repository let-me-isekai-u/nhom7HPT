package org.apache.hadoop.hbase.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DistributedLogger {
    private static final Logger LOG = LoggerFactory.getLogger(DistributedLogger.class);
    private static final String LOG_SERVER_URL = "http://logserver.example.com/collect"; // Cần thay thế
    private static final ExecutorService executor = Executors.newFixedThreadPool(2);

    public static void log(String nodeId, String message) {
        String logEntry = "{" +
                "\"nodeId\":\"" + nodeId + "\"," +
                "\"message\":\"" + message + "\"," +
                "\"timestamp\":\"" + System.currentTimeMillis() + "\"}";
        
        executor.submit(() -> sendLog(logEntry));
    }

    private static void sendLog(String logEntry) {
        try {
            URL url = new URL(LOG_SERVER_URL);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            
            try (OutputStream os = conn.getOutputStream()) {
                os.write(logEntry.getBytes(StandardCharsets.UTF_8));
                os.flush();
            }
            
            int responseCode = conn.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                LOG.warn("Failed to send log: " + responseCode);
            }
        } catch (Exception e) {
            LOG.error("Error sending log: ", e);
        }
    }
}
