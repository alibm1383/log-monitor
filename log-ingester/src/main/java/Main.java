import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


public class Main {
    public static void main(String[] args) {

        if(args.length != 1)
        {
            System.out.println("Enter the path of file");
            return;
        }
        String filePath = args[0];
        Reader input ;

        try {
            input = new FileReader(filePath);
        }
        catch (Exception e)
        {
            System.out.println("Error finding file" + e.getMessage());
            return;
        }

        String component = extractComponent(filePath);
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        String topic = "raw-logs";

        try (BufferedReader reader = new BufferedReader(input)) {

            String line;
            int lines = 0;
            int logs = 0;
            int malformed = 0;

            String thread,level,logger,message;
            LocalDateTime dateTime;
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");


            while ((line = reader.readLine()) != null) {
                lines++;
                try
                {
                    String[] parts = line.split(" – ", 2);
                    String[] details = parts[0].split(" ");
                    message = parts[1].trim();
                    String date = details[0];
                    String time = details[1];
                    dateTime = LocalDateTime.parse(date+" "+time,formatter);
                    thread = details[2].substring(1,details[2].length()-1);
                    level = details[3];
                    logger = details[4];
                    logs++;
                    LogEntry log = new LogEntry(component,dateTime,thread,level,logger,message);
                    String json = mapper.writeValueAsString(log);
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, component, json);
                    producer.send(record).get();
                    System.out.println("JSON: " + json);

                }
                catch (Exception e)
                {
                    malformed++;
                    System.out.println("Error on line:");
                    System.out.println(line);
                    e.printStackTrace();
                }
            }

        } catch (IOException e) {
            System.out.println("Error reading file: " + e.getMessage());
        }
        finally {
            producer.close();
        }
    }

    private static String extractComponent(String filePath) {
        String fileName = new File(filePath).getName();
        int dotIndex = fileName.lastIndexOf('.');
        return dotIndex > 0 ? fileName.substring(0, dotIndex) : fileName;
    }

}