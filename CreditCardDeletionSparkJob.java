import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import java.util.List;


public class CreditCardDeletionSparkJob {


   public static void main(String[] args) {
       // Create a SparkConf and set up Spark configuration
       SparkConf conf = new SparkConf().setAppName("CreditCardDeletion");
       JavaSparkContext sc = new JavaSparkContext(conf);


       // Create a connection to SQS
       AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
       String queueUrl = "YOUR_QUEUE_URL";


       // Receive credit card IDs from SQS queue and delete from Redshift
       while (true) {
           // Receive messages from SQS queue
           ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest(queueUrl).withMaxNumberOfMessages(10);
           List<com.amazonaws.services.sqs.model.Message> messages = sqsClient.receiveMessage(receiveRequest).getMessages();
           if (messages.isEmpty()) {
               break;
           }


           // Extract credit card IDs from messages and delete from Redshift
           JavaRDD<String> creditCardIdsRDD = sc.parallelize(messages).map(com.amazonaws.services.sqs.model.Message::getBody);
           creditCardIdsRDD.foreach(CreditCardDeletionSparkJob::deleteFromRedshift);


           // Delete processed messages from SQS queue
           for (com.amazonaws.services.sqs.model.Message message : messages) {
               sqsClient.deleteMessage(new DeleteMessageRequest(queueUrl, message.getReceiptHandle()));
           }
       }


       // Stop the Spark context
       sc.stop();
   }


   private static void deleteFromRedshift(String creditCardId) {
   // Delete credit card from Redshift
   // Set up Redshift connection details
   String jdbcUrl = "jdbc:redshift://YOUR_REDSHIFT_ENDPOINT:5439/YOUR_DATABASE";
   String username = "YOUR_USERNAME";
   String password = "YOUR_PASSWORD";


   Connection conn = null;
   PreparedStatement pstmt = null;


   try {
       // Establish connection to Redshift
       conn = DriverManager.getConnection(jdbcUrl, username, password);


       // Prepare the DELETE statement
       String sql = "DELETE FROM credit_cards WHERE id = ?";
       pstmt = conn.prepareStatement(sql);
       pstmt.setString(1, creditCardId);
       int rowsAffected = pstmt.executeUpdate();
       System.out.println("Deleted " + rowsAffected + " rows for credit card ID: " + creditCardId);
   } catch (SQLException e) {
       e.printStackTrace();
   } finally {
       try {
           if (pstmt != null) {
               pstmt.close();
           }
           if (conn != null) {
               conn.close();
           }
       } catch (SQLException e) {
           e.printStackTrace();
       }
   }
}
}
