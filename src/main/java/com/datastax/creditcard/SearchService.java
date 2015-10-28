package com.datastax.creditcard;

public interface SearchService {

	public void getTransactions(String ccNo, String search);
}
