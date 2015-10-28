package com.datastax.creditcard;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrJService implements SearchService {

	private static Logger logger = LoggerFactory.getLogger(SolrJService.class);

	private SolrServer solr;

	public SolrJService() {
		String urlString = "http://localhost:8983/solr/datastax_transaction_search_demo.latest_transactions";
		this.solr = new HttpSolrServer(urlString);
	}

	@Override
	public void getTransactions(String ccNo, String search) {

		SolrQuery query = new SolrQuery();
		query.set("q", "cc_no:" + ccNo);
		query.add("fq", "notes:\"*" + search + "*\"");
		query.add("distrib.singlePass", "true");

		QueryResponse response;
		try {
			response = solr.query(query);
			SolrDocumentList list = response.getResults();

			for (SolrDocument document : list) {
				document.getFieldValue("cc_no");
			}
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
	}
}
