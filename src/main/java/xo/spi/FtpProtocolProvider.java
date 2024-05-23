package xo.spi;

public class FtpProtocolProvider implements ProtocolProvider {
    public void connect() {
        System.out.println("Connecting via FTP");
    }
}