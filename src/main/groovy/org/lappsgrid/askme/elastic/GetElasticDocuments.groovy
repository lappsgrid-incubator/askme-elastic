package org.lappsgrid.askme.elastic


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import java.util.StringTokenizer;

import java.io.InputStreamReader;
import java.net.MalformedURLException;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.BasicCookieStore
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.ssl.SSLContextBuilder;
import javax.net.ssl.SSLContext;
import org.apache.http.impl.client.HttpClientBuilder;

import groovy.util.logging.Slf4j
import org.lappsgrid.askme.core.api.Packet
import org.lappsgrid.askme.core.api.Query
import org.lappsgrid.askme.core.api.Status
import org.lappsgrid.askme.core.model.Document
import org.lappsgrid.askme.core.model.Section
import org.lappsgrid.askme.core.Configuration;


/**
 * Interacts with the ElasticSearch database. It is handed a Packet from the message,
 * prepares a post request to the ElasticSearch REST API, collects the data in an
 * instance of ElasticDocumentList, and adds the documents from that list to the
 * Packet.
 */
@Slf4j("logger")
class GetElasticDocuments {

    String elastic_address;
    String elastic_host;
    String elastic_port;
    String index;

    static final String limitPrefix = new String("{\"from\": 0, \"size\":");
    static final String queryPrefix = new String("\"query\": { \"multi_match\" : { \"query\" :");
    //static final String fields = new String("\"fields\" : [ \"abstract\", \"authKeywords\", \"authors\", \"body\", \"content_url\", \"contents\", \"cover_date\", \"doi\", \"eissn\", \"endingPage\"," +
    //        "\"fetched\", \"file_urls\", \"filepath\", \"id\", \"issn\", \"issue\", \"metadata_update\", \"online_pubdate\", \"openaccess\", \"path\", \"pmid\", \"pmc\", \"preprint\"," +
    //        "\"priority\", \"publication_date\", \"publisher\", \"pubname\", \"sha1\", \"source\", \"startingPage\", \"tags\", \"text\", \"time\", \"title\", \"url\", \"year\", \"vol\"],");
    static final String fields = new String("\"fields\" : [\"title\", \"abstract\", \"body\"],");
    static final String tiebreaker = new String("\"tie_breaker\": 0.5 } } }");


    GetElasticDocuments() {

        logger.info("Creating Elastic client")
        Configuration config = new Configuration();
        this.elastic_host = config.ELASTICHOST;
        this.elastic_port = config.ELASTICPORT;
        this.elastic_address = new String("http://" + elastic_host + ":" + elastic_port + "/")
    }


    int changeCollection(String name) {

        this.index = new String(name);
        return nDocs
    }


    Packet answer(Packet packet, String id) {

        logger.info("Generating answer for Message {}", id)

        Query query = packet.query

        logger.info("Sending query to Elastic: {}", query.query)
        //System.out.println(">>> GetElasticDocuments.answer() - Sending query to Elastic: " + query.query)
        ElasticQueryResponse response = null;
        try {
			System.out.println('>>> GetElasticDocuments.answer() - calling query()')
            response = this.query(packet.core, packet.query);
			//System.out.println('>>> GetElasticDocuments.answer() - calling query() DONE')
        }
        catch (Exception e) {
            logger.info("An exception occured initializing QueryResponse");
            System.out.println(">>> GetElasticDocuments.answer() - An exception occured initializing QueryResponse");
            packet.status = Status.ERROR
            packet.message = e.message
            packet.documents = []
            return packet
        }
        if (response == null) {
            logger.info("Got null response from database");
            System.out.println(">>> GetElasticDocuments.answer() - Got null response from database");
        	packet.status = Status.ERROR
        	packet.message = "Got null response from database"
	        packet.documents = []
            return packet
        }
        System.out.println('>>> GetElasticDocuments.answer() - Building ElasticDocumentList')
        System.out.println(response)
        System.out.println(response.getResults())
        ElasticDocumentList documents = new ElasticDocumentList(response.getResults());
        System.out.println('>>> GetElasticDocuments.answer() - Building ElasticDocumentList DONE')

        logger.info("Received {} documents", documents.size())
        packet.documents = documents.getDocs();
        return packet
    }


    ElasticQueryResponse query(String core, Query query) {

    	System.out.println('>>> GetElasticDocuments.query() - initializing reponse to null')
        ElasticQueryResponse queryResponse = null;

        try {
        	//System.out.println('>>> GetElasticDocuments.query() - creating httpClient')
            HttpClient httpClient = HttpClientBuilder.create().build();
	    	//System.out.println('>>> GetElasticDocuments.query() - getting resuest')
            HttpPost getRequest = new HttpPost(this.elastic_address + core + "/_search");

            logger.info("GetElasticDocuments.query() elastic address:  " + this.elastic_address + core + "/_search");
            //System.out.println(">>> GetElasticDocuments.query() - address : " + this.elastic_address + core + "/_search");
            String queryline = new String(this.limitPrefix + query.count + "," + queryPrefix + "\"" + query.question + "\", " + this.fields + this.tiebreaker);
            StringEntity requestEntity = new StringEntity(queryline, ContentType.APPLICATION_JSON);
            logger.info("GetElasticDocuments.query() query:  " + queryline);
            //System.out.println(">>> GetElasticDocuments.query() - query   : " + queryline);

            getRequest.setEntity(requestEntity);

            //System.out.println(">>> GetElasticDocuments.query() - executing request");
            HttpResponse response = httpClient.execute(getRequest);
            //System.out.println(">>> GetElasticDocuments.query() - executing request DONE");

            BufferedReader br = new BufferedReader(
                    new InputStreamReader((response.getEntity().getContent())));

            String output = new String();
            output = br.readLine();

            httpClient.getConnectionManager().shutdown();

            queryResponse = new ElasticQueryResponse(output);
            br.close();

        } catch (IOException io) {
            System.out.println(">>> GetElasticDocuments.query() - Exception:");
            System.out.println(io.getStackTrace());
        }

		logger.info("GetElasticDocuments.query(): returning response");
        System.out.println(">>> GetElasticDocuments.query(): returning response");

        return (queryResponse);
    }

}
