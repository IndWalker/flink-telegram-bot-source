package org.example.processing;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.telegram_connector.container.TelegramMessage;
import org.example.telegram_connector.TelegramSource;


public class Processing {
    public static void main(String[] args) throws Exception {
        String botToken = System.getenv("TELEGRAM_BOT_TOKEN");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TelegramMessage> messages = env.fromSource(new TelegramSource(botToken), WatermarkStrategy.noWatermarks(), "Telegram Source");

        DataStream<String> messages2 = messages.map(new MapFunction<TelegramMessage, String>() {

            @Override
            public String map(TelegramMessage value) throws Exception {
                return value.getChatId() + ":" + value.getText();
            }
        });

        messages2.print();

        env.execute("Telegram Reader");

//        for (int i = 0; i < 10; i++) {
//            String botToken = System.getenv("TELEGRAM_BOT_TOKEN");
//            String urlString = String.format("https://api.telegram.org/bot%s/getUpdates", botToken);
//            URL url = new URL(urlString);
//            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
//            connection.setRequestMethod("GET");
//
//            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
//            String inputLine;
//            StringBuilder content = new StringBuilder();
//
//            while ((inputLine = in.readLine()) != null) {
//                content.append(inputLine);
//            }
//            JsonNode jsonNode = new ObjectMapper().readTree(content.toString());
//
//            in.close();
//            connection.disconnect();
//
//            JsonNode result = jsonNode.get("result");
//            System.out.println(result);
//        }
    }
}
