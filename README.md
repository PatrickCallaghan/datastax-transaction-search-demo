Transaction Search Demo
========================

This requires DataStax Enterprise running in Solr mode.

To create the schema, run the following

	mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetup"
	
To create some transactions, run the following 
	
	mvn clean compile exec:java -Dexec.mainClass="com.datastax.creditcard.Main" 

You can the following to change the default no of transactions and credit cards 
	
	-DnoOfTransactions=10000000 -DnoOfCreditCards=1000000
	
To create the solr core, run 

	bin/dsetool create_core datastax_transaction_search_demo.latest_transactions generateResources=true reindex=true coreOptions=rt.yaml
	
To run the requests run the following 
	
	mvn clean compile exec:java -Dexec.mainClass="com.datastax.creditcard.RunRequests"
	
To change the no of requests add the following

	-DnoOfRequests=100000 -DnoOfCreditCards=1000000	
	
To remove the tables and the schema, run the following.

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaTeardown"
    
    