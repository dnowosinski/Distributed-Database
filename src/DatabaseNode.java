import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.KeyPair;
import java.sql.SQLOutput;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

public class DatabaseNode {
    private int tcpPort;
    private Map<String, String> data;
    private String key;
    private String value;
    private Set<InetSocketAddress> nodes = new HashSet<>();

    public int getTcpPort(){ return this.tcpPort; }

    public DatabaseNode(int tcpPort, Map<String, String> data, InetSocketAddress node) {
        this.tcpPort = tcpPort;
        this.data = new HashMap<>(data);
        this.nodes.add(node);
    }
    public DatabaseNode(int tcpPort, Map<String, String> data) {
        this.tcpPort = tcpPort;
        this.data = new HashMap<>(data);
    }

    public void start(){
        new Thread(()-> listenTcp()).start();

        //connectToNodes();
    }

    public void connectToNodes(){
//        Socket
//        nodes.forEach(node -> new Thread().start());
    }

    public void listenTcp(){
        try {
            ///
            System.out.println("Server listening on port: " + getTcpPort() + " ---- ");
            ////
            ServerSocket server = new ServerSocket(tcpPort);
            while (true){
                Socket clientSocket = server.accept();
                new Thread(()-> handleTcpRequest(clientSocket)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void handleTcpRequest(Socket clientSocket){

    }


}





///import java.io.*;
//import java.net.*;
//import java.util.*;
//
//public class DatabaseNode {
//  private int tcpPort;
//  private Map<Integer, Integer> data;
//  private Set<InetSocketAddress> nodes;
//
//  public DatabaseNode(int tcpPort, Map<Integer, Integer> data, Set<InetSocketAddress> nodes) {
//    this.tcpPort = tcpPort;
//    this.data = data;
//    this.nodes = nodes;
//  }
//
//  public void start() {
//    // uruchomienie nasłuchiwania na porcie TCP
//    new Thread(() -> listenTcp()).start();
//
//    // podłączenie się do węzłów w sieci
//    for (InetSocketAddress addr : nodes) {
//      connectToNode(addr);
//    }
//  }
//
//  private void listenTcp() {
//    try (ServerSocket serverSocket = new ServerSocket(tcpPort)) {
//      while (true) {
//        // oczekiwanie na połączenie od klienta
//        Socket socket = serverSocket.accept();
//        // obsługa połączenia w osobnym wątku
//        new Thread(() -> handleTcpRequest(socket)).start();
//      }
//    } catch (IOException e) {
//      // obsługa wyjątku
//    }
//  }
//
//  private void handleTcpRequest(Socket socket) {
//    try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//         PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
//      String request = in.readLine();
//      String[] parts = request.split(":");
//      String command = parts[0];
//      int key = Integer.parseInt(parts[1]);
//      switch (command) {
//        case "GET":
//          int value = data.getOrDefault(key, -1);
//          out.println(value);
//          break;
//        case "PUT":
//          int newValue = Integer.parseInt(parts[2]);
//          data.put(key, newValue);
//          out.println("OK");
//          break;
//        case "DELETE":
//          data.remove(key);
//          out.println("OK");
//          break;
//      }
//    } catch (IOException e) {
//      // obsługa wyjątku
//    }
//  }
//
//  private void connectToNode(InetSocketAddress addr) {
//    try (Socket socket = new Socket()) {
//      socket.connect(addr);
//      // utworzenie nowego wątku do obsługi połączenia z węzłem
//      new Thread(() -> handleNodeConnection(socket)).start();
//    } catch (IOException e) {
//      // obsługa wyjątku
//    }
//  }
//
//  private void handleNodeConnection(Socket socket






//try(BufferedReader fromClient = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
//            PrintWriter toClient = new PrintWriter(clientSocket.getOutputStream(), true)) {
//            String incomingData = fromClient.readLine();
//            if (!incomingData.isEmpty())
//                System.out.println("incoming request on port::: " + tcpPort + ", data::: "  + incomingData);
//
//            if (!incomingData.isEmpty() && data.get(incomingData) == null){
//                for (InetSocketAddress nodeToConnect : nodes){
//                    new Thread(()->{
//                        try(Socket socket = new Socket("localhost", nodeToConnect.getPort());
//                            BufferedReader serverInput = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//                            PrintWriter serverOutput = new PrintWriter(socket.getOutputStream(), true)){
//                            String toSend = incomingData;
//                            serverOutput.write(toSend);
//                            System.out.println("request sent to::: " + nodeToConnect.getPort());
////                            String response = serverInput.readLine();
////                            System.out.println("response from port " + nodeToConnect.getPort() + "::: " + response);
//                        }
//                        catch (Exception e){
//                            e.printStackTrace();
//                        }
//                    }).start();
//                }
//            }
//            else{
//                if (!incomingData.isEmpty())
//                    System.out.println("Data found on port: " + tcpPort + "::: " + data.get(incomingData));
//            }
//
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }