package index;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;

public class index {
	/*Normalize the terms*/
	public ArrayList<String> TokenNormalize(String token) {
		ArrayList<String> tok = new ArrayList<String>();
		if(token.startsWith("http") || token.startsWith("www") || token.startsWith("@")) {
			token = "";
		}
		
		/*remove all no alpha/numeric characters*/
		if(!token.matches("[[0-9]+[.]*]+")) {
			token = token.replaceAll("[^a-zA-Z0-9]", " ");
		}
		
		String[] split = token.split(" ");
		for(int i=0; i<split.length; i++) {
			String temp = split[i];
			if(temp.matches("[^a-zA-Z0-9]+")) {
				temp = "";
			}
			
			if(temp.length() == 1) {
				temp = "";
			}
			if(!temp.equals("")) {
				/*convert to lowercase*/
				temp = temp.toLowerCase();
			}
			tok.add(temp);
		}
		
		return tok;
	}
	
	public String jaccard(String term, Set<String> table) {
		Iterator<String> list = table.iterator();
		while(list.hasNext()) {
			int a=0, b=0, c=0;
			String tempTerm = list.next();
			HashMap<String,Integer> union = new HashMap<String,Integer>();
			
			/*calculating a and b*/
			for(int n=1; n<=term.length(); n++) {
				union.put(term.substring(n-1, n), 1);
			}
			a = union.size();
			union.clear();
			for(int n=1; n<=tempTerm.length(); n++) {
				union.put(tempTerm.substring(n-1, n), 1);
			}
			b = union.size();
			union.clear();
			
			/*calculating value of c*/
			for(int n=1; n<=term.length(); n++) {
				union.put(term.substring(n-1, n), 1);
			}
			for(int n=1; n<=tempTerm.length(); n++) {
				union.put(tempTerm.substring(n-1, n), 1);
			}
			c = union.size();
			double jacc = (double)c/(double)(a+b-c);
			/*error tolerance: 0.005*/
			if(jacc > 0.995 && jacc < 1.005) {
				term = tempTerm;
				break;
			}
		}
		
		
		
		return term;
	}
	
	public boolean checkDateTime(String strDate) {
		boolean present = false;
		String pattern = "yy-MM-dd HH:mm:ss";
		SimpleDateFormat format = new SimpleDateFormat();
		try {
			format.applyPattern(pattern);
			format.parse(strDate);
			present = true;
		} catch (ParseException e) {
			present = false;
		}
		return present;
	}
	
	public int searchIndex(String searchString, ArrayList<String> userlist, String dir){
		IndexSearcher searcher = null;
		org.apache.lucene.search.Query query = null;
		TopDocs hits = null;
		
		int UserTF = 0;
		String UserIndexDirectory = dir+"IndexDirectory";

		try	{
			IndexReader reader = IndexReader.open(FSDirectory.open(new File(UserIndexDirectory)), true);
			searcher = new IndexSearcher(reader);
		
			Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_31 );
		
			QueryParser qp = new QueryParser(Version.LUCENE_31 , "contents", analyzer);
			query = qp.parse(searchString);
		
			hits = searcher.search(query, 1000);
			
			if (hits.totalHits == 0) {
//				System.out.println("No data found.");
			} else {
				for (int i = 0; i < hits.scoreDocs.length; i++) { 
					Document doc = searcher.doc(hits.scoreDocs[i].doc);
					String user = doc.get("user");
					if(userlist.contains(user)) {
						int termid = reader.getTermFreqVector(hits.scoreDocs[i].doc, "contents").indexOf(searchString);
						int[] termList = reader.getTermFreqVector(hits.scoreDocs[i].doc, "contents").getTermFrequencies();
						
						if(termid > -1 && termid < termList.length) {
							UserTF += termList[termid];
						}
					}
				} 
			}
			reader.close();
		} catch (Exception e) {
			System.out.println(searchString);
			e.printStackTrace(); 
		} 
		return UserTF;
	} 
	
	public ArrayList<String> searchIndexUser(String searchString){
		IndexSearcher searcher = null;
		org.apache.lucene.search.Query query = null;
		TopDocs hits = null;

		String UserIndexDirectory = "usersIndexDirectory";
		ArrayList<String> userlist = new ArrayList<String>();

		try	{
			IndexReader reader = IndexReader.open(FSDirectory.open(new File(UserIndexDirectory)), true);
			searcher = new IndexSearcher(reader);
		
			Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_31 );
			
			/*parse the query and construct the Query object*/
			QueryParser qp = new QueryParser(Version.LUCENE_31 , "contents", analyzer);
			query = qp.parse(searchString); 
		
			hits = searcher.search(query, 1000);
			
			if (hits.totalHits == 0) {
//				System.out.println("No data found.");
			} else {
				for (int i = 0; i < hits.scoreDocs.length; i++) { 
					Document doc = searcher.doc(hits.scoreDocs[i].doc);
					String user = doc.get("user");
					userlist.add(user);
				} 
			}
			reader.close();
		} catch (Exception e) {
			System.out.println(searchString);
			e.printStackTrace(); 
		} 
		return userlist;
	} 
	
	@SuppressWarnings("deprecation")
	public void createIndex(String dirname)	{
		Logger logger = Logger.getLogger("indexer");
		String allFiles = dirname;
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_30);  
		String UserIndexDirectory = dirname+"IndexDirectory";
		try  { 
			/* Store the index in file */
			Directory directory = new SimpleFSDirectory(new File(UserIndexDirectory));     
			IndexWriter iwriter = new IndexWriter(directory, analyzer, true,MaxFieldLength.UNLIMITED); 
			File dir = new File(allFiles); 
			File[] files = dir.listFiles();  
   
			for (File file : files) {   
				Document doc = new Document();
				doc.add((Fieldable) new Field("user", file.getName(), Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.YES )); 
				
				Reader reader = new FileReader(file.getCanonicalPath()); 

				doc.add(new Field("contents", reader, Field.TermVector.YES)); iwriter.addDocument(doc); 
			}

			iwriter.optimize(); iwriter.close();
			logger.log(Level.INFO,"Document indexes formed");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
} 

