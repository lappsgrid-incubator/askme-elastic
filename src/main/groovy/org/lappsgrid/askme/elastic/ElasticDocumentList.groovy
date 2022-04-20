package org.lappsgrid.askme.elastic;


import java.text.NumberFormat;
import groovy.util.logging.Slf4j;
import org.lappsgrid.askme.core.model.Document;
import org.lappsgrid.askme.core.model.Section;
import org.lappsgrid.askme.core.CSVParser;


@Slf4j("logger")
class ElasticDocumentList{
	
	List<String> tv = new ArrayList<String>();
	List<Object> fields = new ArrayList<Object>();
	List<Document> doclist = new ArrayList<Document>();;
	Stanford nlp;
	
	
	public ElasticDocumentList(String responsedata) {
		
		this.parseDocument(responsedata);
	}
	
	
	private void parseDocument(String data){
		
		Document doc = new Document();
		CSVParser parser = new CSVParser();
		String delim = new String(":");
		boolean firstDoc=true, firstID=true;
		
		
		logger.info("ElasticDocumentList parseDocument invoked()");
		logger.info("ElasticDocumentList parseDocument data is: " + data);
		
		
		nlp = new Stanford();
		this.fields =  parser.parseLine(data);
		for(int i=0;i<fields.size();i++){
			
			String field = (String) fields.get(i);
			System.out.println("parsed field> " + field);
			
			
			//trim off leading underscore character
			if(field.startsWith("_")) {
				field = field.substring(1, field.length());
			}
			
			this.tv = field.split(delim);
			
			//populate document object with fields present
			//keying on score field as beginning of new document
			if(this.tv.get(0).equals("id")) {
				if(firstID) {
					firstID=false;	
				} else{
					doc.id = this.tv.get(1);
					firstID=true;
				}
			}
			else if(this.tv.get(0).equals("abstract")) {
				String articleAbstract = new String(this.tv.get(1));
				doc.articleAbstract = nlp.process(articleAbstract);
			}
			else if(this.tv.get(0).equals("authKeywords")) {
				doc.authKeyWords = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("authors")) {
				doc.authors = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("body")) {
				String body = new String(this.tv.get(1));
				doc.body = nlp.process(body);
				doc.articleAbstract = nlp.process(body);
			}
			else if(this.tv.get(0).equals("contents_url")) {
				doc.contents_url = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("contents")) {
				doc.contents = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("coverDate")) {
				doc.coverDate = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("doi")) {
				doc.doi = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("eissn")) {
				doc.eissn = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("endingPage")) {
				doc.endingPage = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("fetched")) {
				doc.fetched = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("file_urls")) {
				doc.file_urls = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("filepath")) {
				doc.filepath = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("issn")) {
				doc.issn = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("issue")) {
				doc.issue = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("metadata_update")) {
				doc.metadata_update = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("online_pubdate")) {
				doc.online_pubdate = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("openaccess")) {
				doc.openaccess = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("path")) {
				doc.path = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("pmid")) {
				doc.pmid = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("pmc")) {
				doc.pmc = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("preprint")) {
				doc.preprint = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("priority")) {
				doc.priority = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("publisher")) {
				doc.publisher = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("pubname")) {
				doc.pubname = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("sha1")) {
				doc.sha1 = new String(this.tv.get(1));
			}
			else if (this.tv.get(0).equals("score")) {
				String tmp = new String(this.tv.get(1));
				String sScore = new String(tmp.substring(0, tmp.length()-1));
								
				NumberFormat nf = NumberFormat.getInstance();
				if(firstDoc) {
					firstDoc = false;
					doc.nscore = nf.parse(sScore).floatValue();
				}
				else {
					doclist.add(doc);
					doc = new Document();
					doc.nscore = nf.parse(sScore).floatValue();
				}
			}
			else if(this.tv.get(0).equals("source")) {
				doc.source = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("startingPage")) {
				doc.startingPage = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("tags")) {
				doc.tags = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("time")) {
				doc.time = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("title")) {
				String title = new String(this.tv.get(1));
				doc.title = nlp.process(title);
			}
			else if(this.tv.get(0).equals("UserLicense")) {
				doc.UserLicense = new String(this.tv.get(1));
			}
			else if (this.tv.get(0).equals("url") || this.tv.get(0).equals("URL")) {
				doc.url = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("vol")) {
				doc.publication_date = new String(this.tv.get(1));
			}
			else if(this.tv.get(0).equals("year")) {
				String year = new String(this.tv.get(1));
				if(year.endsWith("}")){
					year = year.substring(0,year.length()-1);
				}
				doc.publication_date = year;
			}
		}
		
		//add last document
		doclist.add(doc);
	}
	
	
	public int size() {
		return(doclist.size());
	}
	
	public List<Document> getDocs() {
		
		return(this.doclist);
	}	
}
