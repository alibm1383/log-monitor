import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MainTest {

    private MockProducer<String, String> mockProducer;
    private ObjectMapper mapper;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }


    @Test
    void parsesValidLogLine() {
        String line = "2021-07-12 01:22:42,114 [ThreadName] INFO package.name.ClassName – some message";
        LogEntry entry = Main.parseLine(line, "myComponent");

        assertEquals("myComponent", entry.getComponent());
        assertEquals("ThreadName", entry.getThread());
        assertEquals("INFO", entry.getLevel());
        assertEquals("package.name.ClassName", entry.getLogger());
        assertEquals("some message", entry.getMessage());
        assertEquals(2021, entry.getTimestamp().getYear());
    }

    @Test
    void throwsOnMalformedLine_missingDash() {
        String line = "this is not a valid log line at all";
        assertThrows(Exception.class, () -> Main.parseLine(line, "comp"));
    }

    @Test
    void throwsOnInvalidDate() {
        String line = "not-a-date 01:22:42,114 [ThreadName] INFO package.name.ClassName – msg";
        assertThrows(Exception.class, () -> Main.parseLine(line, "comp"));
    }

    @Test
    void throwsOnMissingLevel() {
        String line = "2021-07-12 01:22:42,114 [ThreadName] – msg";
        assertThrows(Exception.class, () -> Main.parseLine(line, "comp"));
    }

    @Test
    void throwsOnEmptyLine() {
        assertThrows(Exception.class, () -> Main.parseLine("", "comp"));
    }


    @Test
    void handlesMultipleDashesInMessage() {
        String line = "2021-07-12 01:22:42,114 [ThreadName] INFO package.name.ClassName – msg – with – dashes";
        LogEntry entry = Main.parseLine(line, "comp");
        assertEquals("msg – with – dashes", entry.getMessage());
    }



    @Test
    void extractFileName_removesExtension() throws Exception {
        Path file = tempDir.resolve("serviceA_2025-07-01.log");
        Files.createFile(file);
        var method = Main.class.getDeclaredMethod("extractFileName", Path.class);
        method.setAccessible(true);
        String result = (String) method.invoke(null, file);
        assertEquals("serviceA_2025-07-01", result);
    }

    @Test
    void extractFileName_noExtension() throws Exception {
        Path file = tempDir.resolve("serviceA_2025-07-01");
        Files.createFile(file);
        var method = Main.class.getDeclaredMethod("extractFileName", Path.class);
        method.setAccessible(true);
        String result = (String) method.invoke(null, file);
        assertEquals("serviceA_2025-07-01", result);
    }


    @Test
    void processFile_sendsValidLinesToKafka() throws IOException {
        Path logFile = tempDir.resolve("serviceA_2025-07-01.log");
        Files.writeString(logFile,
                "2021-07-12 01:22:42,114 [ThreadName] INFO package.name.ClassName – valid message\n");

        Main.processFile(logFile, mockProducer, mapper);

        List<org.apache.kafka.clients.producer.ProducerRecord<String, String>> history =
                mockProducer.history();
        assertEquals(1, history.size());
        assertTrue(history.get(0).value().contains("valid message"));
        assertEquals("raw-logs", history.get(0).topic());
    }

    @Test
    void processFile_writesMalformedLinesToDlqFile() throws IOException {
        Path logFile = tempDir.resolve("serviceA_2025-07-01.log");
        Files.writeString(logFile, "this is not valid at all\n");

        Main.processFile(logFile, mockProducer, mapper);

        Path malformedFile = tempDir.resolve("malformedLogs")
                .resolve("serviceA_2025-07-01-malformed.log");
        assertTrue(Files.exists(malformedFile));
        String content = Files.readString(malformedFile);
        assertTrue(content.contains("this is not valid at all"));
    }

    @Test
    void processFile_deletesFileAfterProcessing() throws IOException {
        Path logFile = tempDir.resolve("serviceA_2025-07-01.log");
        Files.writeString(logFile,
                "2021-07-12 01:22:42,114 [ThreadName] INFO package.name.ClassName – msg\n");

        Main.processFile(logFile, mockProducer, mapper);

        assertFalse(Files.exists(logFile));
    }

    @Test
    void processFile_deletesFileEvenWhenAllLinesAreMalformed() throws IOException {
        Path logFile = tempDir.resolve("serviceA_2025-07-01.log");
        Files.writeString(logFile, "garbage line\n");

        Main.processFile(logFile, mockProducer, mapper);

        assertFalse(Files.exists(logFile));
    }

    @Test
    void processFile_handlesMixOfValidAndInvalidLines() throws IOException {
        Path logFile = tempDir.resolve("serviceA_2025-07-01.log");
        Files.writeString(logFile,
                "2021-07-12 01:22:42,114 [ThreadName] INFO package.name.ClassName – valid one\n" +
                        "garbage line\n" +
                        "2021-07-12 01:23:00,000 [ThreadName] ERROR package.name.ClassName – valid two\n");

        Main.processFile(logFile, mockProducer, mapper);

        assertEquals(2, mockProducer.history().size());

        Path malformedFile = tempDir.resolve("malformedLogs")
                .resolve("serviceA_2025-07-01-malformed.log");
        assertTrue(Files.exists(malformedFile));
        assertTrue(Files.readString(malformedFile).contains("garbage line"));
    }

    @Test
    void processFile_extractsComponentNameFromFileName() throws IOException {
        Path logFile = tempDir.resolve("myComponent_2025-07-01.log");
        Files.writeString(logFile,
                "2021-07-12 01:22:42,114 [ThreadName] INFO package.name.ClassName – msg\n");

        Main.processFile(logFile, mockProducer, mapper);

        String json = mockProducer.history().get(0).value();
        assertTrue(json.contains("\"component\":\"myComponent\""));
    }

    @Test
    void processFile_doesNotCreateMalformedFileWhenAllValid() throws IOException {
        Path logFile = tempDir.resolve("serviceA_2025-07-01.log");
        Files.writeString(logFile,
                "2021-07-12 01:22:42,114 [ThreadName] INFO package.name.ClassName – msg\n");

        Main.processFile(logFile, mockProducer, mapper);

        Path malformedFile = tempDir.resolve("malformedLogs")
                .resolve("serviceA_2025-07-01-malformed.log");
        assertFalse(Files.exists(malformedFile));
    }
}