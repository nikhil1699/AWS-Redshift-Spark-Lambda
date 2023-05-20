import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import java.util.ArrayList;
import java.util.List;


public class CreditCardDeletionLambda implements RequestHandler<Object, String> {


   private final String queueUrl = "YOUR_QUEUE_URL";
   private final int batchSize = 100;


   public String handleRequest(Object input, Context context) {
       List<Integer> creditCardIds = getCreditCardIdsFromRedshift();
       AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();


       // Send credit card IDs in batches to SQS queue
       List<SendMessageBatchRequestEntry> entries = new ArrayList<>();
       for (int i = 0; i < creditCardIds.size(); i++) {
           SendMessageBatchRequestEntry entry = new SendMessageBatchRequestEntry()
                   .withId(String.valueOf(i))
                   .withMessageBody(String.valueOf(creditCardIds.get(i)));
           entries.add(entry);


           // Send batch of messages when reaching the batch size or end of credit card IDs
           if (entries.size() == batchSize || i == creditCardIds.size() - 1) {
               SendMessageBatchRequest batchRequest = new SendMessageBatchRequest()
                       .withQueueUrl(queueUrl)
                       .withEntries(entries);


               SendMessageBatchResult result = sqsClient.sendMessageBatch(batchRequest);
               List<SendMessageBatchResult.Entry> failedEntries = result.getFailed();
               if (!failedEntries.isEmpty()) {
                   // Retry failed messages individually
                   for (SendMessageBatchResult.Entry failedEntry : failedEntries) {
                       sqsClient.sendMessage(queueUrl, failedEntry.getMessageBody());
                   }
               }


               // Clear batch entries
               entries.clear();
           }
       }


       return "Credit card deletion process started";
   }


   private List<Integer> getCreditCardIdsFromRedshift() {
       List<Integer> creditCardIds = new ArrayList<>();
       // retrieve credit card IDs from Redshift ...


       return creditCardIds;
   }
}
