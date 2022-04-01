package org.lappsgrid.askme.elastic;


import java.io.BufferedReader;


class ElasticQueryResponse {
	
	String response;
	
	public ElasticQueryResponse(String res) {
		
		this.response = new String(res);
	}

	
	public String getResults() {
		
		return(this.response);
	}
	
}