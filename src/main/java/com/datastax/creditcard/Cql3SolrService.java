package com.datastax.creditcard;

import com.datastax.creditcard.dao.CreditCardDao;

public class Cql3SolrService implements SearchService {

	
	private CreditCardDao dao;

	public Cql3SolrService (CreditCardDao dao){
		this.dao = dao;
	}
	
	@Override
	public void getTransactions(String ccNo, String note) {
		dao.getTransactionsSolrQuery(ccNo, note);
	}

}
