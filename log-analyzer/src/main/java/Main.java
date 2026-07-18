import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


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

        try (BufferedReader reader = new BufferedReader(input)) {

            String line;
            int lines = 0;
            int logs = 0;
            int malformed = 0;

            String thread,level,logger,message;
            LocalDateTime dateTime;
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");


            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                lines++;
                try
                {
                    String[] parts = line.split(" – ", 2);
                    String[] details = parts[0].split(" ");
                    message = parts[1].trim();
                    String date = details[0];
                    String time = details[1];
                    dateTime = LocalDateTime.parse(date+" "+time,formatter);
                    thread = details[2];
                    level = details[3];
                    logger = details[4];
                    String component = extractComponent(filePath);
                    logs++;
                    LogEntry log = new LogEntry(component,dateTime,thread,level,logger,message);


                }
                catch (Exception e)
                {
                    malformed++;
                }
            }

        } catch (IOException e) {
            System.out.println("Error reading file: " + e.getMessage());
        }
    }

    private static String extractComponent(String filePath) {
        String fileName = new File(filePath).getName();
        int dotIndex = fileName.lastIndexOf('.');
        return dotIndex > 0 ? fileName.substring(0, dotIndex) : fileName;
    }

}