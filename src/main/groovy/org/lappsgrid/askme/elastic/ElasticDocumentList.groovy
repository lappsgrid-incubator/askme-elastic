package org.lappsgrid.askme.elastic;


import groovy.util.logging.Slf4j;
import groovy.json.JsonSlurper;
import org.lappsgrid.askme.core.model.Document;


/**
 * Takes the string that was returned from the database query, retrieves all hits
 * from the parsed string, and for each hit create a Document instance. Pulls out
 * _source properties for each hit and adds them to the Document, calling StanfordNLP
 * on some of the fields.
 */
@Slf4j("logger")
class ElasticDocumentList {

    List<Document> doclist = new ArrayList<Document>();
    Stanford nlp;


    public ElasticDocumentList(String responsedata) {

        this.parseElasticResponse(responsedata);
    }


    private void parseElasticResponse(String data) {

        Document doc;

        logger.info("ElasticDocumentList parseElasticResponse invoked");

        def slurped = new JsonSlurper().parseText(data);
        System.out.println(">>> Read Elastic response");
        for (int i = 0; i < slurped.hits.hits.size(); i++) {
            def hit = slurped.hits.hits[i];
            doc = createDocument(i, hit);
            printDocument(doc);
            doclist.add(doc);
        }
    }


    private Document createDocument(int i, Object hit) {

        nlp = new Stanford();
        Document doc = new Document();
        //System.out.println('\n>>> ' + i + ' ' + hit._id); }

        // defaults that can be overruled by the properties in _source of the hit
        doc.id = hit._id;
        doc.nscore = hit._score;

        hit._source.each { k, v -> 

            //println "${k} ==> ${v}" }
        
            if (k == "id") {
                doc.id = v;

            } else if (k == "abstract") {
                String articleAbstract = new String(v);
                doc.articleAbstract = nlp.process(articleAbstract);

            } else if (k == "authKeywords") {
                doc.authKeyWords = new String(v);

            } else if (k == "authors") {
                doc.authors = new String(v);

            // TODO: this seems wrong because it assumes that if there is a body then
            // there is no abstract, but it could potentially overwrite an already added
            // abstract.
            } else if (k == "body" || k == "text") {
                String body = new String(v);
                doc.body = nlp.process(body);
                doc.articleAbstract = doc.body;

            } else if (k == "contents_url") {
                doc.contents_url = new String(v);

            } else if (k == "contents") {
                doc.contents = new String(v);

            } else if (k == "coverDate") {
                doc.coverDate = new String(v);

            } else if (k == "doi") {
                doc.doi = new String(v);

            } else if (k == "eissn") {
                doc.eissn = new String(v);

            } else if (k == "endingPage") {
                doc.endingPage = new String(v);

            } else if (k == "fetched") {
                doc.fetched = new String(v);

            } else if (k == "file_urls") {
                doc.file_urls = new String(v);

            } else if (k == "filepath") {
                doc.filepath = new String(v);

            } else if (k == "issn") {
                doc.issn = new String(v);

            } else if (k == "issue") {
                doc.issue = new String(v);

            } else if (k == "metadata_update") {
                doc.metadata_update = new String(v);

            } else if (k == "online_pubdate") {
                doc.online_pubdate = new String(v);

            } else if (k == "openaccess") {
                doc.openaccess = new String(v);

            } else if (k == "path") {
                doc.path = new String(v);

            } else if (k == "pmid") {
                doc.pmid = new String(v);

            } else if (k == "pmc") {
                doc.pmc = new String(v);

            } else if (k == "preprint") {
                doc.preprint = new String(v);

            } else if (k == "priority") {
                doc.priority = new String(v);

            } else if (k == "publisher") {
                doc.publisher = new String(v);

            } else if (k == "pubname") {
                doc.pubname = new String(v);

            } else if (k == "sha1") {
                doc.sha1 = new String(v);

            } else if (k == "score") {
                // TODO: why is the last digit stripped from the score?
                String tmp = new String(v);
                String sScore = new String(tmp.substring(0, tmp.length() - 1));
                doc.nscore = nf.parse(sScore).floatValue();

            } else if (k == "source") {
                doc.source = new String(v);

            } else if (k == "startingPage") {
                doc.startingPage = new String(v);

            } else if (k == "tags") {
                doc.tags = new String(v);

            } else if (k == "time") {
                doc.time = new String(v);

            } else if (k == "title") {
                String title = new String(v);
                doc.title = nlp.process(title);

            } else if (k == "UserLicense") {
                doc.UserLicense = new String(v);

            } else if (k == "url" || k == "URL") {
                doc.url = new String(v);

            } else if (k == "vol") {
                doc.publication_date = new String(v);

            } else if (k == "year") {
                String year = new String(v);
                doc.publication_date = year;
            }
        };

        return doc;
    }


    // to be removed when the toString method is available on Document
    private void printDocument(Document doc) {
        println(sprintf("<Document %s score=%s url=%s>", doc.id, doc.nscore, doc.url));
    }


    public int size() {
        return (doclist.size());
    }


    public List<Document> getDocs() {

        return (this.doclist);
    }
}
