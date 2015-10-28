create keyspace if not exists datastax_transaction_search_demo WITH replication = {'class': 'NetworkTopologyStrategy', 'Solr': 1};

use datastax_transaction_search_demo; 


create table if not exists transactions(
	cc_no text,
	transaction_time timestamp,
 	transaction_id text,
 	user_id text,
	location text,
	items map<text, double>,
	merchant text,
	amount double,
	status text,
	notes text,
	PRIMARY KEY (transaction_id)
) WITH compaction = {'class': 'DateTieredCompactionStrategy'};

create table if not exists latest_transactions(
	cc_no text,
	transaction_time timestamp,
 	transaction_id text,
 	user_id text,
	location text,
	items map<text, double>,
	merchant text,
	amount double,
	status text,
	notes text,
	PRIMARY KEY (cc_no, transaction_time)
) WITH compaction = {'class': 'DateTieredCompactionStrategy'};

