import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class TCPClient implements AutoCloseable{
    private BufferedReader input;
    private PrintWriter output;
    private Socket socket;
    final private int tcpPort;
    final private String ipAddress = "localhost";

    public TCPClient(int tcpPort) throws IOException {
        System.out.println("1PORT:  sending to port: " + tcpPort + " request:::");
        this.tcpPort = tcpPort;
        this.socket = new Socket(ipAddress, tcpPort);
        System.out.println("2PORT:  sending to port: " + socket.getLocalPort() + " request:::");
        this.input = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));
        this.output = new PrintWriter(this.socket.getOutputStream(), true);
        System.out.println("3PORT:  sending to port: " + tcpPort + " request:::");
    }

    public void send(String request){
        System.out.println("PORT: " + socket.getPort() + " sending to port: " + tcpPort + " request:::" + request);
        output.write("get-value 2");
        output.flush();
    }
    public static String sendTCPMessage(int tcpPort, String request){
        String result;
        try(Socket tcpClient = new Socket("localhost", tcpPort);
         BufferedReader clientInput = new BufferedReader(new InputStreamReader(tcpClient.getInputStream()));
         PrintWriter clientOutput = new PrintWriter(tcpClient.getOutputStream(), true)) {
                 System.out.println("PORT: "+ tcpClient.getLocalPort() +" sending tcp request to ::: " + tcpPort);
                 clientOutput.write(request);
                 result = "OK";
        }
        catch (Exception e){
            e.printStackTrace();
            result = "Error";
        }
        return result;
    }


    @Override
    public void close() throws Exception {
        socket.close();
        input.close();
        output.close();
    }
}
