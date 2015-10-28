package com.datastax.creditcard;

import java.util.Arrays;
import java.util.List;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.creditcard.dao.CreditCardDao;
import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;

public class RunRequests {

	private static Logger logger = LoggerFactory.getLogger(RunRequests.class);

	public RunRequests() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		String noOfCreditCardsStr = PropertyHelper.getProperty("noOfCreditCards", "1000000");
		String noOfRequestsStr = PropertyHelper.getProperty("noOfRequests", "1000");
		
		CreditCardDao dao = new CreditCardDao(contactPointsStr.split(","));

		int noOfCreditCards = Integer.parseInt(noOfCreditCardsStr);
		int noOfRequests = Integer.parseInt(noOfRequestsStr);

		logger.info("Start Querying for " + noOfRequestsStr + " random requests");
		SearchService solrJ = new SolrJService();

		Timer timer = new Timer();
		for (int i = 0; i < noOfRequests; i++) {			
			String search = notes.get(new Double(Math.random() * notes.size()).intValue());
			solrJ.getTransactions(new Double(Math.ceil(Math.random() * noOfCreditCards)).intValue() + "", search);
		}
		timer.end();
		logger.info("SolrJ took " + timer.getTimeTakenMillis() + " ms.");
		
		SearchService cqlSolr = new Cql3SolrService(dao);

		timer = new Timer();
		for (int i = 0; i < noOfRequests; i++) {
			String search1 = notes.get(new Double(Math.random() * notes.size()).intValue());
			cqlSolr.getTransactions(new Double(Math.ceil(Math.random() * noOfCreditCards)).intValue() + "", search1);
		}
		timer.end();
		logger.info("CQL Solr Query  took " + timer.getTimeTakenMillis() + " ms.");
		
		SearchService cql = new Cql3Service(dao);
		
		timer = new Timer();
		for (int i = 0; i < noOfRequests; i++) {
			String search2 = notes.get(new Double(Math.random() * notes.size()).intValue());
			cql.getTransactions(new Double(Math.ceil(Math.random() * noOfCreditCards)).intValue() + "", search2);
		}
		timer.end();
		logger.info("CQL Query took " + timer.getTimeTakenMillis() + " ms.");		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new RunRequests();

		System.exit(0);
	}

	private List<String> notes = Arrays.asList("Shopping", "Shopping", "Shopping", "Shopping", "Shopping", "Pharmacy",
			"HouseHold", "Shopping", "Household", "Shopping", "Tech", "Tech", "Diy", "Shopping", "Clothes", "Shopping",
			"Amazon", "Coffee", "Coffee", "Tech", "Diy", "Travel", "Travel", "Eating out", "Eating out");
}
