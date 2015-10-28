package com.datastax.creditcard.dao;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.creditcard.model.Transaction;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CreditCardDao {

	private static Logger logger = LoggerFactory.getLogger(CreditCardDao.class);
	private static final int DEFAULT_LIMIT = 10000;
	private Session session;

	private DateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");
	private static String keyspaceName = "datastax_transaction_search_demo";

	private static String transactionTable = keyspaceName + ".transactions";
	private static String latestTransactionTable = keyspaceName + ".latest_transactions";

	private static final String INSERT_INTO_TRANSACTION = "Insert into " + transactionTable
			+ " (cc_no, transaction_time, transaction_id, location, merchant, amount, user_id, status, notes) values (?,?,?,?,?,?,?,?,?);";
	private static final String INSERT_INTO_LATEST_TRANSACTION = "Insert into " + latestTransactionTable
			+ " (cc_no, transaction_time, transaction_id, location, merchant, amount, user_id, status, notes) values (?,?,?,?,?,?,?,?,?) using ttl 864000";	

	private static final String GET_TRANSACTIONS_BY_ID = "select * from "
			+ transactionTable + " where transaction_id = ?";
	private static final String GET_TRANSACTIONS_BY_CCNO = "select * from "
			+ latestTransactionTable + " where cc_no = ? order by transaction_time desc";
	
	
	private PreparedStatement insertTransactionStmt;
	private PreparedStatement insertLatestTransactionStmt;
	private PreparedStatement getTransactionById;
	private PreparedStatement getTransactionByCCno;
	private PreparedStatement getTransactionByCCnoSolrQuery;
	
	public CreditCardDao(String[] contactPoints) {

		Cluster cluster = Cluster.builder()				
				.addContactPoints(contactPoints)
				.build();

		this.session = cluster.connect();
		
		try {
			this.insertTransactionStmt = session.prepare(INSERT_INTO_TRANSACTION);
			this.insertLatestTransactionStmt = session.prepare(INSERT_INTO_LATEST_TRANSACTION);

			this.getTransactionById = session.prepare(GET_TRANSACTIONS_BY_ID);
			this.getTransactionByCCno = session.prepare(GET_TRANSACTIONS_BY_CCNO);

			this.insertLatestTransactionStmt.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
			this.insertTransactionStmt.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
			
		} catch (Exception e) {
			e.printStackTrace();
			session.close();
			cluster.close();
		}
	}

	public void saveTransaction(Transaction transaction) {
		insertTransaction(transaction);
	}
	
	public void insertTransaction(Transaction transaction) {

		session.execute(this.insertTransactionStmt.bind(transaction.getCreditCardNo(), transaction.getTransactionTime(),
				transaction.getTransactionId(), transaction.getLocation(),
				transaction.getMerchant(), transaction.getAmount(), transaction.getUserId(), transaction.getStatus(), transaction.getNotes()));
		session.execute(this.insertLatestTransactionStmt.bind(transaction.getCreditCardNo(), transaction.getTransactionTime(),
				transaction.getTransactionId(), transaction.getLocation(),
				transaction.getMerchant(), transaction.getAmount(), transaction.getUserId(), transaction.getStatus(), transaction.getNotes()));
	}

	public Transaction getTransaction(String transactionId) {

		ResultSet rs = this.session.execute(this.getTransactionById.bind(transactionId));
		
		Row row = rs.one();
		if (row == null){
			throw new RuntimeException("Error - no transaction for id:" + transactionId);			
		}

		return rowToTransaction(row);		
	}

	private Transaction rowToTransaction(Row row) {

		Transaction t = new Transaction();
		
		t.setAmount(row.getDouble("amount"));
		t.setCreditCardNo(row.getString("cc_no"));
		t.setMerchant(row.getString("merchant"));
		t.setLocation(row.getString("location"));
		t.setTransactionId(row.getString("transaction_id"));
		t.setTransactionTime(row.getDate("transaction_time"));
		t.setUserId(row.getString("user_id"));
		t.setNotes(row.getString("notes"));
		t.setStatus(row.getString("status"));

		return t;
	}
	
	public Transaction getTransactions(String transactionId) {

		logger.info("Getting transaction :" + transactionId);
		
		ResultSet resultSet = this.session.execute(getTransactionById.bind(transactionId));		
		Row row = resultSet.one();

		if (row == null){
			throw new RuntimeException("Error - no issuer for id:" + transactionId);			
		}
		return rowToTransaction(row);
	}
	
	public List<Transaction> getTransactionsSolrQuery(String ccNo, String note) {
		
		String query = "select * from " + latestTransactionTable + " where cc_no = '" + ccNo + "' and  solr_query = '{\"q\":\"notes:"
				+ note.replace(" ", "+") + "\", \"fq\":\"cc_no:" + ccNo +"\", \"fq\":\"notes:"
				+ note.replace(" ", "+") + "\", \"distrib.singlePass\": true}';";
			
		ResultSet resultSet = this.session.execute(query);		
		List<Transaction> transactions = new ArrayList<Transaction>();
		Iterator<Row> rows = resultSet.iterator();

		try{
		while(rows.hasNext()) {
			Transaction transaction = rowToTransaction(rows.next());
		
			if (transaction.getNotes().contains(note)){
				transactions.add(transaction);
			}
		}
		}catch (Exception e){
			e.printStackTrace();
		}
		return transactions;
	}
	
	public List<Transaction> getLatestTransactionsForCCNoSearchNotes(String ccNo, String note) {		
		ResultSet resultSet = this.session.execute(getTransactionByCCno.bind(ccNo));		
		List<Row> rows = resultSet.all();
		
		List<Transaction> transactions = new ArrayList<Transaction>();

		for (Row row : rows){
			
			Transaction transaction = rowToTransaction(row);
			
			if (transaction.getNotes().contains(note)){
				transactions.add(transaction);
			}
		}
		return transactions;
	}

}
