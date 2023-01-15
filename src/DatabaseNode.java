import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.KeyPair;
import java.sql.SQLOutput;
import java.util.*;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class DatabaseNode {
    private int TCP_PORT;
    final private String IP_ADDRESS = "localhost";
    //private Map<String, String> data;
    private String key;
    private String value;
    private Set<InetSocketAddress> nodes = new HashSet<>();
    private Map<String, Transaction> transactionMap = new HashMap<>();

    public int getTcpPort(){ return this.TCP_PORT; }
    public String getValue(){ return this.value;}
    private void setKeyValue(String key, String value){
        this.key = key;
        this.value = value;
    }
    private void setValue(String value){ this.value = value; }

    private void createTransaction(int clientPort, int toReceive, boolean isNodeRequest){
        this.transactionMap.put(Transaction.getNextId(), new Transaction(clientPort, toReceive, isNodeRequest));
    }

    public DatabaseNode(int tcpPort, String key, String value, InetSocketAddress node) {
        this.TCP_PORT = tcpPort;
        //this.data = new HashMap<>(data);
        this.key = key;
        this.value = value;
        this.nodes.add(node);
    }
    public DatabaseNode(int tcpPort, String key, String value) {
        this.TCP_PORT = tcpPort;
        //this.data = new HashMap<>(data);
        this.key = key;
        this.value = value;
    }

    public DatabaseNode(int tcpPort) {
        this.TCP_PORT = tcpPort;
    }

    public DatabaseNode() {

    }

    public void main(String[] args){
        DatabaseNode node = new DatabaseNode();
        for (int i = 0; i < args.length; i++){
            switch (args[i]){
                case "-tcpport":{
                    node = new DatabaseNode(Integer.valueOf(args[++i]));
                }
                break;
                case "-record":{
                    String[] arguments = args[++i].split(":");
                    node.setKeyValue(arguments[0], arguments[1]);
                    //validation??
                }
                break;
                case "connect":{

                }
                break;
                default:{

                }
            }
        }
    }

    public void start(){
        new Thread(()-> listenTcp()).start();

        //connectToNodes();
    }

    public void connectToNodes(){
//        Socket
//        nodes.forEach(node -> new Thread().start());
    }

    private void listenTcp(){
        try {
            ///
            System.out.println("Server listening on port: " + getTcpPort() + " ---- ");
            ////
            ServerSocket server = new ServerSocket(TCP_PORT);
            while (true){
                Socket clientSocket = server.accept();
                new Thread(()-> handleTcpRequest(clientSocket)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private List<String> populateReqeust(String request) throws InterruptedException {
        System.out.println("PORT: "+ getTcpPort() +" populating request to children::: " + nodes.size());
        List<String> responses = new ArrayList<>(nodes.size());
        Executor executor = Executors.newFixedThreadPool(nodes.size());
        CountDownLatch latch = new CountDownLatch(nodes.size());
        for (InetSocketAddress node : nodes){
            executor.execute(()->{
                try(Socket tcpClient = new Socket("localhost", node.getPort());
                BufferedReader clientInput = new BufferedReader(new InputStreamReader(tcpClient.getInputStream()));
                PrintWriter clientOutput = new PrintWriter(tcpClient.getOutputStream(), true)) {
                    clientOutput.println(request);
                    String response;
                    while ((response = clientInput.readLine()) != null) {
                        responses.add(response);
                    }
                }
               catch(Exception e){
                    e.printStackTrace();
               }
               finally {
                    latch.countDown();
               }
            });
        }
        latch.await();
        return responses;
    }

    private void handleReply(){

    }

    private void handleTcpRequest(Socket clientSocket){
        try(BufferedReader serverInput = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter serverOutput = new PrintWriter(clientSocket.getOutputStream(), true)) {

            String request = serverInput.readLine();
            System.out.println("PORT: "+ getTcpPort() +" incoming request::: " + request);
            String[] splittedRequest = request.split(" ");
            String operation = splittedRequest[0];
            String argument = "";
            if(splittedRequest.length != 1){
                argument = splittedRequest[1];
            }
            switch (operation){
                case "get-value":{
                    System.out.println("PORT: "+ getTcpPort() +" performing get-value for key::: " + argument);
                        String returnValue = "";
                        if (this.key.equals(argument)){
                            returnValue = getValue();
                        }
                        else if (this.nodes.isEmpty()){
                            returnValue = "ERROR";
                        }
                        else{
                            try {
                                List<String> responses = populateReqeust(request);
                                for (String response : responses){
                                    if (response != "Error"){
                                        returnValue = response;
                                        break;
                                    }
                                }

                            }
                            catch (Exception e){
                                e.printStackTrace();
                            }
                        }
                        if (returnValue != ""){
                            serverOutput.println(returnValue);
                        }
                }
                break;
                case "set-value":{
                    String[] arguments = argument.split(":");
                    System.out.println("PORT: "+ getTcpPort() +" performing set-value for key::: " + arguments[0]);
                    if(arguments.length == 2){
                        String returnValue = "";
                        if (this.key.equals(arguments[0])){
                            setValue(arguments[1]);
                            returnValue = "OK";
                        }
                        else if (this.nodes.isEmpty()){
                            returnValue = "ERROR";
                        }
                        else{
                            try {
                                List<String> responses = populateReqeust(request);
                                for (String response : responses){
                                    if (response != "Error"){
                                        returnValue = response;
                                        break;
                                    }
                                }

                            }
                            catch (Exception e){
                                e.printStackTrace();
                            }
                        }
                        if (returnValue != ""){
                            serverOutput.println(returnValue);
                        }
                    }
                }
                break;
                case "find-key":{
                    System.out.println("PORT: "+ getTcpPort() +" performing find-key for key::: " + argument);
                    String returnValue = "";
                    if (this.key.equals(argument)){
                        returnValue = IP_ADDRESS + ":" + TCP_PORT;
                    }
                    else if (this.nodes.isEmpty()){
                        returnValue = "ERROR";
                    }
                    else{
                        try {
                            List<String> responses = populateReqeust(request);
                            for (String response : responses){
                                if (response != "Error"){
                                    returnValue = response;
                                    break;
                                }
                            }

                        }
                        catch (Exception e){
                            e.printStackTrace();
                        }
                    }
                    if (returnValue != ""){
                        serverOutput.println(returnValue);
                    }
                }
                break;
                case "get-max":{
                    System.out.println("PORT: "+ getTcpPort() +" performing get-max");
                    String returnValue = "";
                    System.out.println("PORT: "+ getTcpPort() + " this.nodes.isEmpty()::: " + this.nodes.isEmpty());
                    if (this.nodes.isEmpty()){
                        returnValue = getValue();
                    }
                    else {
                        try {
                            List<String> responses = populateReqeust(request);
                            int max = Integer.valueOf(getValue());
                            System.out.println(responses + " : " + max);
                            for (String response : responses) {
                                if (Integer.valueOf(response) > max) {
                                    max = Integer.valueOf(response);
                                }
                            }
                            returnValue = String.valueOf(max);

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    if (returnValue != "") {
                        System.out.println("Returning::: " + returnValue);
                        serverOutput.println(returnValue);
                    }
                }
                break;
                case "get-min":{
                    System.out.println("PORT: "+ getTcpPort() +" performing get-min");
                    String returnValue = "";
                    if (this.nodes.isEmpty()){
                        returnValue = getValue();
                    }
                    else {
                        try {
                            List<String> responses = populateReqeust(request);
                            int min = Integer.valueOf(getValue());
                            for (String response : responses) {
                                if (Integer.valueOf(response) < min) {
                                    min = Integer.valueOf(response);
                                }
                            }
                            returnValue = String.valueOf(min);

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    if (returnValue != "") {
                        System.out.println("Returning::: " + returnValue);
                        serverOutput.println(returnValue);
                    }
                }
                break;
                case "new-record":{
                    String returnValue = "";
                    String[] arguments = argument.split(":");
                    if(arguments.length == 2){
                        this.setKeyValue(arguments[0], arguments[1]);
                        returnValue = "OK";
                    }

                    if (returnValue != "") {
                        System.out.println("Returning::: " + returnValue);
                        serverOutput.println(returnValue);
                    }
                }
                break;
                case "terminate":{
                   //opTerminate();
                }
                break;
                case "connect":{
                    //connect node
                }
                break;
                default:{
                    //wrong request
                }
            }


        }
        catch (Exception e){
            e.printStackTrace();
        }
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