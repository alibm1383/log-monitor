import java.io.*;
import java.nio.file.*;
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
    public static void main(String[] args) throws IOException,InterruptedException {
        if (args.length != 1)
        {
            System.out.println("enter the path of directory");
            return;
        }

        Path directory = Path.of(args[0]);

        processExistingFiles(directory);
        watchDirectory(directory);
    }

    private static void processFile(Path filePath)
    {
        String fileName  = extractFileName(filePath);
        String component = fileName.split("_")[0];
        Path malformedFile = filePath.toAbsolutePath().getParent()
                .resolve("malformedLogs").resolve(fileName+"-malformed.log");


        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        String topic = "raw-logs";

        BufferedWriter writer = null ;
        try (BufferedReader reader =  Files.newBufferedReader(filePath)) {

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
                //ArrayIndex , DateTimeParse , kafkaError
                catch (Exception e)
                {
                    if (writer == null)
                    {
                        Files.createDirectories(malformedFile.getParent());
                        writer = Files.newBufferedWriter(malformedFile);
                    }
                    malformed++;
                    writer.write(line + "\n" + "Exception : " + e.getMessage() + "\n\n");
                    System.out.println(line);
                    e.printStackTrace();
                }
            }

        } catch (IOException e) {
            System.out.println("Error reading file: " + e.getMessage());
        }
        finally {
            producer.close();
            try
            {
                if (writer!=null) {
                    writer.close();
                }
                Files.delete(filePath);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }

        }
    }

    private static void processExistingFiles(Path directory)  {
        //lazy-load
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            //delete is atomic
            for (Path file : stream) {
                if (Files.isRegularFile(file)) {
                    processFile(file);
                }
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private static void watchDirectory(Path directory) throws IOException, InterruptedException {

        WatchService watchService = FileSystems.getDefault().newWatchService();
        directory.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

        while (true) {
            WatchKey key = watchService.take();

            //ham-zamani
            for (WatchEvent<?> event : key.pollEvents()) {
                Path fileName = (Path) event.context();
                Path fullPath = directory.resolve(fileName);

                if (Files.isRegularFile(fullPath)) {
                    processFile(fullPath);
                }
            }

            boolean valid = key.reset();
            if (!valid) {
                System.out.println("directory is not available");
                break;
            }
        }
    }

    private static String extractFileName(Path filePath) {
        String fileName = filePath.getFileName().toString();
        int dotIndex = fileName.lastIndexOf('.');
        return dotIndex > 0 ? fileName.substring(0, dotIndex) : fileName;
    }
}