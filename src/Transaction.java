public class Transaction {
    public Transaction(int clientPort, int toReceive, boolean isNode){
        this.id = String.valueOf(++idCounter);
        this.clientPort = clientPort;
        this.toReceive = toReceive;
        this.isNode = isNode;
    }
    static private int idCounter = 0;
    final private String id;
    final private int clientPort;
    final private int toReceive;
    private int received;
    private String value;
    private boolean isNode;

    public int getClientPort() {
        return clientPort;
    }

    public int getReceived() {
        return received;
    }

    public int getToReceive() {
        return toReceive;
    }

    public void setValue(String value){
        this.value = value;
    }

    public void incrReceived(){
        this.received++;
    }
    static public String getNextId(){
        return String.valueOf(idCounter + 1);
    }

    static public boolean isNodeRequest(String request){

        return true;
    }
}
