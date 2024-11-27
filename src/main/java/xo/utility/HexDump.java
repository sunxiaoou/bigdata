package xo.utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;

public class HexDump {
    private static final Logger LOG = LoggerFactory.getLogger(HexDump.class);

    public static void hexDump(byte[] bytes) {
        final int bytesPerLine = 16; // Number of bytes to display per line

        LOG.info(String.format("Buffer length: %d", bytes.length));
        for (int offset = 0; offset < bytes.length; offset += bytesPerLine) {
            StringBuilder logLine = new StringBuilder();
            // Print memory address
            logLine.append(String.format("%08X | ", offset));

            // Print hexadecimal representation
            for (int i = 0; i < bytesPerLine; i ++) {
                int index = offset + i;
                if (index < bytes.length) {
                    logLine.append(String.format("%02X", bytes[index]));
                } else {
                    logLine.append("  "); // Padding for incomplete lines
                }
                if (i % 4 == 3) logLine.append(" "); // Group by 4 bytes
            }

            // Print ASCII representation
            logLine.append("| ");
            for (int i = 0; i < bytesPerLine; i++) {
                int index = offset + i;
                if (index < bytes.length) {
                    char c = (char) bytes[index];
                    if (c >= 32 && c <= 126) { // Printable ASCII range
                        logLine.append(c);
                    } else {
                        logLine.append('.');
                    }
                } else {
                    logLine.append(" ");
                }
            }
            LOG.info(logLine.toString());   // Output the log line
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            LOG.error("Usage: java BinaryFilePrinter <binary file path>");
            return;
        }


        byte[] fileBytes;
        try (FileInputStream fis = new FileInputStream(args[0]);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            // Read file content into a byte array
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                baos.write(buffer, 0, bytesRead);
            }
            fileBytes = baos.toByteArray();
        }
        hexDump(fileBytes);
    }
}