package org.example.telegram_connector.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.telegram_connector.container.TelegramIntermediateRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class TelegramApiHelper {
    private static TelegramApiHelper instance;

    private static final Logger LOG = LoggerFactory.getLogger(TelegramApiHelper.class);

    private String apiUrl = "https://api.telegram.org/bot%s/getUpdates";
    private String apiUrlWithOffset = "https://api.telegram.org/bot%s/getUpdates?offset=%d";
    private String token;

    private TelegramApiHelper() {

    }

    public static synchronized TelegramApiHelper getInstance() {
        if (instance == null) {
            instance = new TelegramApiHelper();
        }
        return instance;
    }

    public JsonNode getUpdates() throws IOException {
        return getUpdatesApiCall(apiUrl);
    }

    public TelegramIntermediateRecord getUpdates(long offset) throws IOException {
        JsonNode result = getUpdatesApiCall(apiUrl);
        return offsetResult(result, offset);
    }

    public void setBotToken(String token) {
        this.token = token;
        this.apiUrl = String.format("https://api.telegram.org/bot%s/getUpdates", token);
    }

    private TelegramIntermediateRecord offsetResult(JsonNode result, long offset) throws IOException {
        for (JsonNode node : result) {
            JsonNode message = node.get("message");
            if (!message.isMissingNode()) {
                String text = message.get("text").asText();
                if (!text.isEmpty()) {
                    long updateId = node.get("update_id").asLong();
                    if (updateId == offset) {
                        return new TelegramIntermediateRecord(
                                        updateId,
                                        "chat1",
                                        text
                                );
                    }
                }
            }
        }
        return null;
    }

    private JsonNode getUpdatesApiCall(String apiUrl) throws IOException {
        URL url = new URL(apiUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");

        BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String inputLine;
        StringBuilder content = new StringBuilder();

        while ((inputLine = in.readLine()) != null) {
            content.append(inputLine);
        }

        in.close();
        connection.disconnect();

        JsonNode result = new ObjectMapper().readTree(content.toString()).path("result");

        return result;
    }
}
