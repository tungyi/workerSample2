import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.rabbitmq.client.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.persistence.EntityManagerFactory;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;

public class SampleCode {
    private static EntityManagerFactory entityManagerFactory;
    private static MqttClient mqttClient;

    public static void main(String[] args) {

        String brokerIP = "172.22.12.195", brokerUser = "admin", brokerPwd = "admin", brokerVhost = "/";
        int amqpport = 5672, mqttport = 1883;
        boolean brokerSSL = false;

        //<editor-fold desc="Subscribe Topic for AMQP">
        List<String> subscribeTopics = new ArrayList<>();
        //-----------Assign Topic : /wisepaas/sample/+/devinfoack-----------
        subscribeTopics.add(".wisepaas.sample.*.devinfoack");
        //-----------Connect to Broker and Receive Message---------------
        ConnectAMQP(brokerIP, amqpport, brokerUser, brokerPwd, brokerVhost, brokerSSL, subscribeTopics);
        //</editor-fold>

        //<editor-fold desc="Subscribe Topic for MQTT">
        subscribeTopics.clear();
        subscribeTopics.add("/wisepaas/sample/+/devinfoack");
        ConnectMQTT(brokerIP, mqttport, brokerUser, brokerPwd, brokerSSL, subscribeTopics);
        //</editor-fold>

        //<editor-fold desc="Assign Specific Topic (/wisepaas/sample/agent/devinfoack) and Publish Message to Broker">
        publish("/wisepaas/sample/agent/devinfoack", "This is a testing message");
        //</editor-fold>

        //<editor-fold desc="PostgreSQL Control">
        String postgresqlIP = "127.0.0.1", postgresqlDB = "wisepaas", postgresqUsr = "admin", postgresqlPwd = "admin";
        int postgresqlPort = 5432;
        //------------------Set Url, User, Password of PostgreSQL-----------------//
        try {
            Class.forName("org.postgresql.Driver").newInstance();
            String url = String.format("jdbc:postgresql://%1$s:%2$d/%3$s", postgresqlIP, postgresqlPort, postgresqlDB);
            java.sql.Connection con = DriverManager.getConnection(url, postgresqUsr, postgresqlPwd);
            Statement st = con.createStatement();
            System.out.println("initSQLDB Success");
            String sql = " select * from device"; //SQL Command
            ResultSet rs = st.executeQuery(sql);

            while (rs.next()) {
                System.out.println(rs.getString(1)); //print results
            }
            rs.close();
            st.close();
            con.close();
        } catch (Throwable ex) {
            System.out.println("initSQLDB Exception: " + ex.getMessage());
        }

        //</editor-fold>
        //<editor-fold desc="MongoDB Query/Add/Update/Delete">
        String mongoDB_host = "127.0.0.1", mongoDB_usr = "admin", mongoDB_pwd = "admin", mongoDB_DBName = "WISE-PaaS";
        String collectionName = "SampleCollection";
        int mongoDB_port = 27017;
        MongoCredential credential = MongoCredential.createCredential(mongoDB_usr, mongoDB_DBName, mongoDB_pwd.toCharArray());
        List credentials = new ArrayList();
        credentials.add(credential);
        //-------------------Connect to MongoDB-----------------//
        MongoClient mongoClient = new MongoClient(new ServerAddress(mongoDB_host, mongoDB_port), credentials);
        //-------------------Create Collection--------------------//
        MongoDatabase mongoDatabase = mongoClient.getDatabase(mongoDB_DBName);
        if (!mongoClient.getDB(mongoDB_DBName).collectionExists(collectionName)) {
            mongoDatabase.createCollection(collectionName);
        }

        //-------------------Insert Data---------------------//
        MongoCollection<Document> sampleCollection = mongoDatabase.getCollection(collectionName);
        Document mongoDoc = new Document();
        mongoDoc.append("AGENTID", "0000123456789").append("AGENTNAME", "Sample Agent").append("ts", new Date());
        sampleCollection.insertOne(mongoDoc);

        //--------------------Query Data--------------------//
        org.bson.Document match = new org.bson.Document("$match",
                new org.bson.Document("AGENTID", "0000123456789"));
        List<Bson> pipeline = new ArrayList<>();
        pipeline.add(match);
        AggregateIterable<Document> aggregated = sampleCollection.aggregate(pipeline);
        MongoCursor<org.bson.Document> cursor = aggregated.iterator();
        while (cursor.hasNext()) {
            Document mongoResDoc = cursor.next();
            System.out.println("AGENTID:  " + mongoResDoc.get("AGENTID"));
            System.out.println("AGENTNAME:  " + mongoResDoc.get("AGENTNAME"));
        }
        //-------------------Update Data---------------------//
        Document fromDoc = new Document();
        fromDoc.append("AGENTID", "0000123456789");
        Document targetDoc = new Document();
        targetDoc.append("AGENTNAME", "Sample Modified Agent");
        sampleCollection.updateOne(fromDoc, new Document("$set", targetDoc));
        //-------------------Remove Data---------------------//
        sampleCollection.deleteOne(fromDoc);
        //</editor-fold>
    }

    private final static String EXCHANGE_NAME = "amq.topic";
    private final static String EXCHANGE_TYPE = "topic";
    private final static String QUEUE_NAME = "sample-BrokerQueue";
    private static Connection _connection;
    private static Channel _channel;

    private static void ConnectAMQP(String host, int port, String usr, String pwd, String vhost, boolean ssl, List<String> topics) {
        boolean ret = false;

        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUsername(usr);
            factory.setPassword(pwd);
            factory.setVirtualHost(vhost);
            factory.setHost(host);
            factory.setPort(port);

            if (ssl) {
                factory.useSslProtocol();
            }
            _connection = factory.newConnection();
            _channel = _connection.createChannel();
            _channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE, true, false, null);  //  durable, non-autodelete
            bindQueue(topics);
            ret = true;
        } catch (IOException | NoSuchAlgorithmException | KeyManagementException | TimeoutException ex) {
            System.out.println(ex.getMessage());
            ret = false;
        }

        if (ret) {
            System.out.println("AMQP Connection Success");
        } else {
            System.out.println("AMQP Connection Exception");
        }
    }

    private static void bindQueue(List<String> topics) {
        try {
            _channel.queueDeclare(QUEUE_NAME, true, false, false, null); // durable, no exclusive connection, non-autodelete, argument
            for (String topic : topics) {
                _channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, topic);
            }
            subscribe();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    private static void subscribe() {
        try {
            Consumer consumer = new DefaultConsumer(_channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                        throws IOException {
                    String content = new String(body, "UTF-8");
                    System.out.println("AMQP Message Arrived: " + content);
                }
            };

            boolean autoAck = true;  // if false, need call basicAck like above
            _channel.basicConsume(QUEUE_NAME, autoAck, consumer);
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    private final static int CONNECTION_TIMEOUT = 10;
    private final static int KEEPALIVE_INTERVAL = 5;
    private final static int MQTT_QOS = 0;

    public static void ConnectMQTT(String host, int port, String usr, String pwd, boolean ssl, List<String> topics) {
        MqttConnectOptions options = new MqttConnectOptions();
        MQTTMessage mqttMessage = new MQTTMessage();
        options.setCleanSession(true);
        options.setUserName(usr);
        options.setPassword(pwd.toCharArray());
        options.setConnectionTimeout(CONNECTION_TIMEOUT);
        options.setKeepAliveInterval(KEEPALIVE_INTERVAL);

        String url = String.format("tcp://%1$s:%2$d", host, port);
        if (ssl) {
            url = String.format("ssl://%1$s:%2$d", host, port);
            try {
                options.setSocketFactory(getScketFactory());
            } catch (Exception e) {
            }
        }

        try {
            Random rand = new Random();
            String _clientid = String.format("Common-%04x%04x", rand.nextInt(0x10001), rand.nextInt(0x10001));
            mqttClient = new MqttClient(url, _clientid, new MemoryPersistence());
            mqttClient.connect(options);
            mqttClient.setCallback(mqttMessage);
            subscribeToTopic(topics);
            System.out.println("MQTT Connection Success");
        } catch (MqttException ex) {
            System.out.println("MQTT Connection Exception - " + ex.getMessage());
        }
    }

    protected static void subscribeToTopic(List<String> topics) {
        try {
            for (String topic : topics) {
                mqttClient.subscribe(topic, MQTT_QOS);
            }
        } catch (MqttException ex) {
        }
    }

    private static SSLSocketFactory getScketFactory() throws Exception {
        SSLContext sslContext = null;
        sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, new TrustManager[]{new TrustAllX509TrustManager()}, new SecureRandom());

        return sslContext.getSocketFactory();
    }

    private static class TrustAllX509TrustManager implements X509TrustManager {
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }

        public void checkClientTrusted(java.security.cert.X509Certificate[] certs,
                                       String authType) {
        }

        public void checkServerTrusted(java.security.cert.X509Certificate[] certs,
                                       String authType) {
        }
    }

    public static void publish(String topic, String message) {
        try {
            MqttTopic mqttTopic = mqttClient.getTopic(topic);
            MqttMessage mqttMessage = new MqttMessage();
            mqttMessage.setPayload(message.getBytes());
            mqttMessage.setQos(MQTT_QOS);
            mqttMessage.setRetained(false);
            MqttDeliveryToken token = mqttTopic.publish(mqttMessage);
            token.waitForCompletion();
        } catch (MqttException ex) {
        }
    }

    private static class MQTTMessage implements MqttCallback {

        @Override
        public void connectionLost(Throwable cause) {

        }

        @Override
        public void messageArrived(String topic, MqttMessage message) throws Exception {
            String content = "";
            try {
                content = new String(message.getPayload(), "UTF-8");
                System.out.println("MQTT Message Arrived: " + content);
            } catch (UnsupportedEncodingException ex) {
            }
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {

        }
    }
}
