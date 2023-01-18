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
import java.util.stream.Collectors;

public class DatabaseNode {
    private int TCP_PORT;
    final private String IP_ADDRESS = "localhost";
    private String key;
    private String value;
    private Set<InetSocketAddress> nodes = new HashSet<>();
    private Map<String, Transaction> transactionMap = new HashMap<>();
    private ServerSocket server;
    private List<String> transactionIds = new ArrayList<>();
    static private int idCounter = 0;

    public int getTcpPort(){ return this.TCP_PORT; }
    public String getValue(){ return this.value;}
    public void setKeyValue(String key, String value){
        this.key = key;
        this.value = value;
    }
    private void setValue(String value){ this.value = value; }

//    private void createTransaction(int clientPort, int toReceive, boolean isNodeRequest){
//        this.transactionMap.put(Transaction.getNextId(), new Transaction(clientPort, toReceive, isNodeRequest));
//    }

    private String createTransaction(){
        String newId = getTcpPort()+":"+String.valueOf(++idCounter);
        this.transactionIds.add(newId);
        return String.valueOf(newId);
    }

    private void addToTransactions(String id){
        this.transactionIds.add(id);
    }

    private boolean holdsTransaction(String id){
        return this.transactionIds.contains(id)? true : false;
    }

    public DatabaseNode(int tcpPort, String key, String value, InetSocketAddress node) {
        this.TCP_PORT = tcpPort;
        this.key = key;
        this.value = value;
        this.nodes.add(node);
    }
    public DatabaseNode(int tcpPort, String key, String value) {
        this.TCP_PORT = tcpPort;
        this.key = key;
        this.value = value;
    }

    public DatabaseNode(int tcpPort) {
        this.TCP_PORT = tcpPort;
        start();
    }

    public DatabaseNode() {
    }

    public static void main(String[] args){
        DatabaseNode node = new DatabaseNode();
        for (int i = 0; i < args.length; i++){
            switch (args[i]){
                case "-tcpport":{
                    node = new DatabaseNode(Integer.valueOf(args[++i]));  //222
                }
                break;
                case "-record":{
                    String[] arguments = args[++i].split(":");
                    node.setKeyValue(arguments[0], arguments[1]);
                    //validation??
                }
                break;
                case "-connect":{  //
                    String[] arguments = args[++i].split(":"); //  -connect 3131:111
                    node.nodes.add(new InetSocketAddress(arguments[0], Integer.valueOf(arguments[1])));   //adds parent to the nodes!
                    final String nodeAdr = node.IP_ADDRESS;
                    final int nodePort = node.getTcpPort();
                    new Thread(()->{
                        try(Socket tcpClient = new Socket(arguments[0], Integer.valueOf(arguments[1]));
                            BufferedReader clientInput = new BufferedReader(new InputStreamReader(tcpClient.getInputStream()));
                            PrintWriter clientOutput = new PrintWriter(tcpClient.getOutputStream(), true)) {
                                clientOutput.println("connect " + nodeAdr + ":" + nodePort);
                        }
                        catch(Exception e){
                            e.printStackTrace();
                        }
                    }).start();

                }
                break;
                default:{
                }
            }
        }
        return;
    }

    public void start(){
        new Thread(()-> listenTcp()).start();
    }

    private void listenTcp(){
        try {
            ///
            System.out.println("Server listening on port: " + getTcpPort() + " ---- ");
            ////
            server = new ServerSocket(TCP_PORT);
            while (true){
                Socket clientSocket = server.accept();
                new Thread(()-> handleTcpRequest(clientSocket)).start();
            }
        } catch (IOException e) {}
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

    private void handleTcpRequest(Socket clientSocket){
        try(BufferedReader serverInput = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter serverOutput = new PrintWriter(clientSocket.getOutputStream(), true)) {

            String request = serverInput.readLine();
            //System.out.println("PORT: "+ getTcpPort() +" incoming request::: " + request);
            String[] splittedRequest = request.split(" ");  //[conn][lh:2]
            if (splittedRequest.length > 1 && splittedRequest[splittedRequest.length - 2].equals("id")){
                if (holdsTransaction(splittedRequest[splittedRequest.length - 1])){
                    serverOutput.println("ERROR");
                    return;
                }
                else {
                    addToTransactions(splittedRequest[splittedRequest.length - 1]);
                }
                splittedRequest = Arrays.copyOfRange(splittedRequest, 0 , splittedRequest.length - 2);
            }
            else {
                System.out.println("PORT: "+ getTcpPort() +" incoming request::: " + request);
                String transactionId = createTransaction();
                request += " id " + transactionId;
            }
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
                            returnValue = this.key + ":" + getValue();
                        }
                        else if (this.nodes.size() <= 1){
                            returnValue = "ERROR";
                        }
                        else{

                            try {
                                List<String> responses = populateReqeust(request);
                                System.out.println("response get-value::: " + responses);
                                returnValue = "ERROR";
                                for (String response : responses){
                                    if (!response.equals("ERROR")){
                                        returnValue = response;
                                        break;
                                    }
                                }
                            }
                            catch (Exception e){
                                e.printStackTrace();
                            }
                        }
                        System.out.println("returning get-value::: " + returnValue);
                        if (!returnValue.equals("")){
                            System.out.println("Returning::: " + returnValue);
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
                        else if (this.nodes.size() <= 1){
                            returnValue = "ERROR";
                        }
                        else{
                            try {
                                List<String> responses = populateReqeust(request);
                                for (String response : responses){
                                    if (!response.equals("ERROR")){
                                        returnValue = response;
                                        break;
                                    }
                                }

                            }
                            catch (Exception e){
                                e.printStackTrace();
                            }
                        }
                        if (!returnValue.equals("")){
                            System.out.println("Returning::: " + returnValue);
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
                    else if (this.nodes.size() <= 1){
                        returnValue = "ERROR";
                    }
                    else{
                        try {
                            List<String> responses = populateReqeust(request);
                            returnValue = "ERROR";
                            for (String response : responses){
                                if (!response.equals("ERROR")){
                                    returnValue = response;
                                    break;
                                }
                            }
                        }
                        catch (Exception e){
                            e.printStackTrace();
                        }
                    }
                    if (!returnValue.equals("")){
                        System.out.println("Returning::: " + returnValue);
                        serverOutput.println(returnValue);
                    }
                }
                break;
                case "get-max":{
                    System.out.println("PORT: "+ getTcpPort() +" performing get-max");
                    String returnValue = "";
                    if (this.nodes.size() <= 1){
                        returnValue = this.key + ":" + getValue();;
                    }
                    else {
                        try {
                            List<String> responses = populateReqeust(request);
                            Map<String, String> responsesKeyVal = responses.stream()
                                    .filter(s -> !s.equals("ERROR"))
                                    .map(s -> s.split(":"))
                                    .collect(Collectors.toMap(e -> e[1], e -> e[0], (oldValue, newValue) -> newValue));
                            responses = responsesKeyVal.keySet().stream().toList();
                            int max = Integer.valueOf(getValue());
                            responsesKeyVal.put(getValue(), this.key);
                            System.out.println(responses + " : " + max);
                            for (String response : responses) {
                                if (response.equals("ERROR")){
                                    continue;
                                }
                                if (Integer.valueOf(response) > max) {
                                    max = Integer.valueOf(response);
                                }
                            }
                            returnValue = responsesKeyVal.get(String.valueOf(max)) + ":" + max;

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    if (!returnValue.equals("")) {
                        System.out.println("Returning::: " + returnValue);
                        serverOutput.println(returnValue);
                    }
                }
                break;
                case "get-min":{
                    System.out.println("PORT: "+ getTcpPort() +" performing get-min");
                    String returnValue = "";
                    if (this.nodes.size() <= 1){
                        returnValue = this.key + ":" + getValue();
                    }
                    else {
                        try {
                            List<String> responses = populateReqeust(request);
                            Map<String, String> responsesKeyVal = responses.stream()
                                    .filter(s -> !s.equals("ERROR"))
                                    .map(s -> s.split(":"))
                                    .collect(Collectors.toMap(e -> e[1], e -> e[0], (oldValue, newValue) -> newValue));
                            responses = responsesKeyVal.keySet().stream().toList();
                            int min = Integer.valueOf(getValue());
                            responsesKeyVal.put(getValue(), this.key);
                            for (String response : responses) {
                                if (response.equals("ERROR")){
                                    continue;
                                }

                                if (Integer.valueOf(response) < min) {
                                    min = Integer.valueOf(response);
                                }
                            }
                            returnValue = responsesKeyVal.get(String.valueOf(min)) + ":" + min;

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    if (!returnValue.equals("")) {
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

                    if (!returnValue.equals("")) {
                        System.out.println("Returning::: " + returnValue);
                        serverOutput.println(returnValue);
                    }
                }
                break;
                case "terminate":{
                    String returnValue = "";
                    String eraseRequest = "erase " + IP_ADDRESS + ":" + getTcpPort();
                    if(this.nodes.isEmpty()){
                        returnValue = "OK";
                    }
                    else{
                        try{
                            List<String> responses = populateReqeust(eraseRequest);
                            for(String response : responses){
                                System.out.println("terminated::: " + response);
                                if (!response.equals("OK")){
                                    returnValue = "ERROR";
                                    break;
                                }
                                else {
                                    returnValue = "OK";
                                }
                            }

                        }
                        catch (Exception e){
                            e.printStackTrace();
                        }
                    }
                    if (!returnValue.equals("")) {
                        System.out.println("Returning::: " + returnValue);
                        serverOutput.println(returnValue);
                        System.out.println("Listening on PORT: " + getTcpPort() + " closed");
                        server.close();
                    }
                }
                break;
                case "erase":{
                    String returnValue = "";
                    String[] arguments = argument.split(":");
                    if(arguments.length == 2){
                        List<InetSocketAddress> toErase = new ArrayList<>();
                        for (InetSocketAddress node : nodes){
                            if(node.getAddress().toString().contains(arguments[0]) && node.getPort() == Integer.valueOf(arguments[1])){
                                if (nodes.contains(node)){
                                    toErase.add(node);
                                }
                                else
                                    returnValue = "ERROR";
                            }
                        }
                        if(nodes.removeAll(toErase)){
                            returnValue = "OK";
                        }
                        if (!returnValue.equals("")) {
                            System.out.println("Returning::: " + returnValue);
                            serverOutput.println(returnValue);
                        }
                    }
                }
                break;
                case "connect":{
                    String[] arguments = argument.split(":");
                    System.out.println("CONNECT TO " + getTcpPort() + " port::: " + arguments[1]);
                    if(arguments.length == 2){
                        nodes.add(new InetSocketAddress(arguments[0], Integer.valueOf(arguments[1])));
                    }
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
