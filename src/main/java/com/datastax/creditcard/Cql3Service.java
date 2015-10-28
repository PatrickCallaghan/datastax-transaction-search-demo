package com.datastax.creditcard;

import com.datastax.creditcard.dao.CreditCardDao;

public class Cql3Service implements SearchService {
	
	private CreditCardDao dao;

	public Cql3Service (CreditCardDao dao){
		this.dao = dao;
	}

	@Override
	public void getTransactions(String ccNo, String note) {
		dao.getLatestTransactionsForCCNoSearchNotes(ccNo, note);
	}

}
