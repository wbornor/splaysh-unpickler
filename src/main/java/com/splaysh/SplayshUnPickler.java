package com.splaysh;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;

/**
 * Created by wesbornor on 4/14/16.
 */
public class SplayshUnPickler {
	private static final Logger logger = LoggerFactory.getLogger(SplayshUnPickler.class);
	private static final boolean ISTEST = true;

	private AmazonDynamoDBClient dynamo = null;
	private String accessKey, secretKey;
	private String line;
	private String itemTableName;
	private Table itemTable = null;

	public String readSource() {
		StringBuilder source = new StringBuilder();

		try {
			FileReader fileReader = new FileReader(
					"/Users/wesbornor/git/splaysh-unpickler/src/main/resources/splayshdb-all.json");

			BufferedReader bufferedReader = new BufferedReader(fileReader);

			while ((line = bufferedReader.readLine()) != null) {
				source.append(line);
			}

			// Always close files.
			bufferedReader.close();
		} catch (FileNotFoundException ex) {
			logger.error("Unable to open file");
		} catch (IOException ex) {
			logger.error("Error reading file");
		}
		logger.info(source.toString().substring(1, 10));
		return source.toString();
	}

	public List unpickle(String source) {
		List<String> records = null;

		try {
			records = JSONUtils.toObject(List.class, source);
		} catch (JSONException e) {
			e.printStackTrace();
		}

		return records;
	}

	public void putRecords() {

		if (this.accessKey == null) {
			dynamo = new AmazonDynamoDBClient();
		} else {
			dynamo = new AmazonDynamoDBClient(new BasicAWSCredentials(this.accessKey, this.secretKey));
		}

		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		df.setTimeZone(TimeZone.getTimeZone("UTC"));

		/*DynamoDB dynamodb = new DynamoDB(dynamo);
		itemTable = dynamodb.getTable("splayshdb.items");

		Item item = new Item()
				.withPrimaryKey("Id", 206)
				.withString("Title", "20-Bicycle 206")
				.withString("Description", "206 description")
				.withString("BicycleType", "Hybrid")
				.withString("Brand", "Brand-Company C")
				.withNumber("Price", 500)
				.withStringSet("Color",  new HashSet<String>(Arrays.asList("Red", "Black")))
				.withString("ProductCategory", "Bike")
				.withBoolean("InStock", true)
				.withNull("QuantityOnHand")
				.withList("RelatedItems", relatedItems)
				.withMap("Pictures", pictures)
				.withMap("Reviews", reviews);

		// Put the item
		PutItemSpec update = new PutItemSpec()
				.withItem(item);

		logger.info("updating dynamo record: id:" + id + ", nut_id:" + nut_id);
		try {
			if (!ISTEST) {
				itemTable.putItem(update);
			}
		} catch (Exception e) {
			logger.error("Error updating items record", e);
		}*/
	}

	public static void main(String[] args) {
		SplayshUnPickler unPickler = new SplayshUnPickler();
		List jsonArray = unPickler.unpickle(unPickler.readSource());

	}
}
