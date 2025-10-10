import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args){

        DatabaseNode secondNode = new DatabaseNode(2802, "2", "2");
        DatabaseNode mainNode = new DatabaseNode(2801, "1", "1", new InetSocketAddress(secondNode.getTcpPort()));

        mainNode.start();
        secondNode.start();
    }
}
