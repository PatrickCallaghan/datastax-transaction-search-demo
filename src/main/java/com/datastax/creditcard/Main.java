package com.datastax.creditcard;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.creditcard.dao.CreditCardDao;
import com.datastax.creditcard.model.Transaction;
import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;

public class Main {

	private static Logger logger = LoggerFactory.getLogger(Main.class);
	private DateTime date;
	private static int BATCH = 10000;
	private boolean insert = false;

	public Main() {

		// Create yesterdays date at midnight
		this.date = new DateTime().minusDays(30).withTimeAtStartOfDay();

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		String noOfCreditCardsStr = PropertyHelper.getProperty("noOfCreditCards", "10000");
		String noOfTransactionsStr = PropertyHelper.getProperty("noOfTransactions", "100000");

		CreditCardDao dao = new CreditCardDao(contactPointsStr.split(","));

		int noOfTransactions = Integer.parseInt(noOfTransactionsStr);
		int noOfCreditCards = Integer.parseInt(noOfCreditCardsStr);

		logger.info("Writing " + noOfTransactions + " transactions for " + noOfCreditCards + " credit cards.");

		Timer timer = new Timer();
		int total = 0;
		for (int i = 0; i < noOfTransactions; i++) {
			dao.insertTransaction(createRandomTransaction(noOfCreditCards));

			if (i > 0 && i % BATCH == 0) {
				total += BATCH;
				logger.info("Wrote " + total + " records");
			}
		}
		timer.end();
		logger.info("Credit Cards Load took " + timer.getTimeTakenSeconds() + " secs.");
	}

	private Transaction createRandomTransaction(int noOfCreditCards) {
		
		int creditCardNo = new Double(Math.ceil(Math.random() * noOfCreditCards)).intValue();
		
		//Allow for some skew
		if (Math.random() < .05){
			creditCardNo = creditCardNo % 1000;
		}
		
		int noOfItems = new Double(Math.ceil(Math.random() * 5)).intValue();

		String location = locations.get(new Double(Math.random() * locations.size()).intValue());

		int randomLocation = new Double(Math.random() * issuers.size()).intValue();
		String issuer = issuers.get(randomLocation);
		String note = notes.get(randomLocation);

		// create time by adding a random no of seconds to the midnight of
		// yesterday.
		date = date.plusSeconds(new Double(Math.random() * 100).intValue());

		Transaction transaction = new Transaction();
		createItemsAndAmount(noOfItems, transaction);
		transaction.setCreditCardNo(new Integer(creditCardNo).toString());
		transaction.setMerchant(issuer);
		transaction.setTransactionId(UUID.randomUUID().toString());
		transaction.setTransactionTime(date.toDate());
		transaction.setLocation(location);
		transaction.setNotes(note);
		return transaction;
	}

	private void createItemsAndAmount(int noOfItems, Transaction transaction) {
		Map<String, Double> items = new HashMap<String, Double>();
		double totalAmount = 0;

		for (int i = 0; i < noOfItems; i++) {

			double amount = new Double(Math.random() * 1000);
			items.put("item" + i, amount);

			totalAmount += amount;
		}
		transaction.setAmount(totalAmount);
		transaction.setItems(items);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Main();

		System.exit(0);
	}

	private List<String> locations = Arrays.asList("London", "Manchester", "Liverpool", "Glasgow", "Dundee",
			"Birmingham");

	private List<String> issuers = Arrays.asList("Tesco", "Sainsbury", "Asda Wal-Mart Stores", "Morrisons",
			"Marks & Spencer", "Boots", "John Lewis", "Waitrose", "Argos", "Co-op", "Currys", "PC World", "B&Q",
			"Somerfield", "Next", "Spar", "Amazon", "Costa", "Starbucks", "BestBuy", "Wickes", "TFL", "National Rail",
			"Pizza Hut", "Local Pub");

	private List<String> notes = Arrays.asList("Shopping", "Shopping", "Shopping", "Shopping", "Shopping", "Pharmacy",
			"HouseHold", "Shopping", "Household", "Shopping", "Tech", "Tech", "Diy", "Shopping", "Clothes", "Shopping",
			"Amazon", "Coffee", "Coffee", "Tech", "Diy", "Travel", "Travel", "Eating out", "Eating out");
}
