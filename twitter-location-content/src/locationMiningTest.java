import index.index;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;


public class locationMiningTest {
	public static void main(String args[]) {
		
		Logger logger = Logger.getLogger("indexer");
		
		Statement s = null;
        try {
            String userName = "root";
            String password = "admin";
            String url = "jdbc:mysql://localhost/twitter_geolocation";
            Class.forName ("com.mysql.jdbc.Driver").newInstance ();
            Connection conn = DriverManager.getConnection (url, userName, password);
            logger.log(Level.INFO,"Database connection established");
            s = conn.createStatement();
        }
        catch (Exception e) {
            System.err.println ("Cannot connect to database");
            e.printStackTrace();
        }
        
        try {
        	s.execute("TRUNCATE test_user_info");
        } catch (SQLException e) {
        	e.printStackTrace();
        }
		
		
		ArrayList<String> stopwords = new ArrayList<String>();
		try {
			FileInputStream fstream = new FileInputStream("stopwords.txt");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String word;
			while((word = br.readLine()) != null) {
				stopwords.add(word);
			}
			logger.log(Level.INFO, "Stop words added");
			in.close();
			fstream.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}		
		
		HashMap<String,Integer> dictionary = new HashMap<String,Integer>();
		HashMap<String,String> jaccardEqui = new HashMap<String,String>();
		HashMap<String,Integer> userMap = new HashMap<String,Integer>();
		index tweet_eval = new index();
		
		/*SESSION 2 : read from file*/
		try {
			FileInputStream fstream = new FileInputStream("dictionary");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String word;
			while((word = br.readLine()) != null) {
				dictionary.put(word,0);
			}
			logger.log(Level.INFO, "Dictionary added");
			in.close();
			fstream.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}		
		
		
		/*Obtaining Word List*/
		ArrayList<String> termlist = new ArrayList<String>();
		Set<String> termset = dictionary.keySet();
		Iterator<String> termiterate = termset.iterator();
		while(termiterate.hasNext()) {
			termlist.add(termiterate.next());
		}
		
		/*SESSION 2 - Read city list*/
		ArrayList<String> citylist = new ArrayList<String>();
		try {
			FileInputStream fstream = new FileInputStream("city");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String word;
			while((word = br.readLine()) != null) {
				citylist.add(word);
			}
			logger.log(Level.INFO, "City List created");
			in.close();
			fstream.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}		
		
		/*SESSION 2 : Read distribution list*/
		double[][] distribution = new double[termlist.size()][citylist.size()];
		try {
			FileInputStream fstream = new FileInputStream("distribution");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String dist;
			int i=0;
			while((dist = br.readLine()) != null) {
				String[] split = dist.split("\t");
				for(int j=0; j<split.length; j++) {
					if(!split[j].equals("")) {
						distribution[i][j] = Double.parseDouble(split[j]);
					}
				}
				++i;
			}
			logger.log(Level.INFO, "Distribution read");
			in.close();
			fstream.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}		
		
		
		/*Testing Data*/
		int TotalTerm = 0;
		logger.log(Level.INFO, "Obtaining test user tweet");
		try {
			FileInputStream fstream = new FileInputStream("test_set_tweets.txt");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String str, concat_str = "";
			String[] tweet = new String[4];
			boolean done = true;
			int count = 0;
			while((str = br.readLine()) != null) {
				if(count == 10000) {
					break;
				}
				if(count%500 == 0) {
					logger.log(Level.INFO,"Reading line "+count);
				}
				
				if(done) {
					done = false;
					StringTokenizer tok = new StringTokenizer(str,"\t");
					tweet[0] = tok.nextToken();
					tweet[1] = tok.nextToken();
					while(tok.hasMoreTokens()) {
						String temptok = tok.nextToken();
						if(!temptok.equals("") || temptok != null) {
							if(tok.hasMoreTokens()) {
								concat_str = concat_str+temptok;
							} else {
								if(tweet_eval.checkDateTime(temptok)) {
									done = true;
									tweet[3] = temptok;
								}
							}
						}
					}
					if(done) {
						tweet[2] = concat_str.replaceAll("\t", " ");
					}
				} else {
					StringTokenizer tok = new StringTokenizer(str,"\t");
					while(tok.hasMoreTokens()) {
						String temptok = tok.nextToken();
						if(!temptok.equals("") || temptok != null) {
							if(tok.hasMoreTokens()) {
								concat_str = concat_str+temptok;
							} else {
								if(tweet_eval.checkDateTime(temptok)) {
									done = true;
									tweet[3] = temptok;
								}
							}
						}
					}
					if(done) {
						tweet[2] = concat_str.replaceAll("\t", " ");
					}
				}
				
				if(done) {
					StringTokenizer tweetTok = new StringTokenizer(tweet[2]);
					while(tweetTok.hasMoreTokens()) {
						String tweetTerm = tweetTok.nextToken().trim();
						ArrayList<String> term = tweet_eval.TokenNormalize(tweetTerm);
						for(int i=0; i<term.size(); i++) {
							String tempTerm = term.get(i);
							if(!tempTerm.equals("") && !stopwords.contains(tempTerm)) {
								String jaccardTerm = "";
								if(!dictionary.containsKey(tempTerm)) {
									if(!jaccardEqui.containsKey(tempTerm)) {
										Set<String> dict_term = dictionary.keySet();
										jaccardTerm = tweet_eval.jaccard(tempTerm,dict_term);
										if(!jaccardTerm.equals(tempTerm)) {
											jaccardEqui.put(tempTerm, jaccardTerm);
										}
									} else {
										jaccardTerm = jaccardEqui.get(tempTerm);
									}
								} else {
									jaccardTerm = tempTerm;
								}
//								System.out.println(jaccardTerm);
								if(!jaccardTerm.equals("")) {
									userMap.put(tweet[0], 0);
									BufferedWriter dout = new BufferedWriter(new FileWriter("testusers/"+tweet[0],true));
									
									dout.write(jaccardTerm+"\n");
									dout.close();
									++TotalTerm;
								}
								
							}
						}
					}
					concat_str = "";
				}
				++count;
			}
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		logger.log(Level.INFO, "Tweet message from test user read");
		
		
		try {
			logger.log(Level.INFO, "Obtaining test user info");
			
			ArrayList<String> users = new ArrayList<String>();
			Iterator<String> userIterate = userMap.keySet().iterator();
			while(userIterate.hasNext()) {
				users.add(userIterate.next());
			}
			
			FileInputStream fstream = new FileInputStream("test_set_users.txt");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String user_info = "", query = "";
			int count = 0;
			while((user_info = br.readLine()) != null) {
				if(count%500 == 0) {
					if(!query.equals("")) {
						s.executeUpdate("INSERT INTO test_user_info(user_name,user_city,user_state) VALUES "+query);
					}
					query = "";
				}
				StringTokenizer tok = new StringTokenizer(user_info,"\t");
				if(tok.hasMoreTokens()) {
					String user = tok.nextToken();
					if(users.contains(user)) {
						StringTokenizer next_tok = new StringTokenizer(tok.nextToken(), ",");
						if(next_tok.hasMoreTokens()) {
							String city_name = next_tok.nextToken();
							String state_name = "";
							if(next_tok.hasMoreTokens()) {
								state_name = next_tok.nextToken();
							}
							if(query.equals("")) {
								query = "('"+user+"','"+city_name+"','"+state_name+"')";
							} else {
								query = query+",('"+user+"','"+city_name+"','"+state_name+"')";
							}
							++count;
						}
					}
				}
				
			}
			
			if(!query.equals("")) {
				s.executeUpdate("INSERT INTO test_user_info(user_name,user_city,user_state) VALUES "+query);
			}
			logger.log(Level.INFO, count+" test user's info obtained");
			in.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
		
		
		/*Estimating location probability*/
		ArrayList<String> test_user = new ArrayList<String>();
		try {
			ResultSet result = s.executeQuery("SELECT DISTINCT user_name FROM test_user_info");
			while(result.next()) {
				test_user.add(result.getString(1));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		logger.log(Level.INFO, "Calculating Test user Probability");
		
		double[][] prob = new double[test_user.size()][citylist.size()];
		try {
			tweet_eval.createIndex("testusers");
			for(int u=0; u<test_user.size(); u++) {
				logger.log(Level.INFO, "For user "+u);
				ArrayList<String> testuserlist = new ArrayList<String>();
				testuserlist.add(test_user.get(u));
				for(int i=0; i<citylist.size(); i++) {
					prob[u][i] = 0.0;
					System.out.print(".");
					for(int j=0; j<termlist.size(); j++) {
						int count = tweet_eval.searchIndex(termlist.get(j), testuserlist, "testusers");
						if(count > 0) {
							prob[u][i] += (distribution[j][i]*((double)count/(double)TotalTerm));
						}
					}
				}
				System.out.println();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			BufferedWriter dout = new BufferedWriter(new FileWriter("probability_output"));
			for(int i=0; i<test_user.size(); i++) {
				dout.write(test_user.get(i));
				for(int j=0; j<citylist.size(); j++) {
					dout.write(" "+prob[i][j]);
				}
				dout.write("\n");
			}
			dout.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
		logger.log(Level.INFO, "Test user Probability written to disk");
		
		String[] highest = new String[test_user.size()];
		for(int i=0; i<test_user.size(); i++) {
			double high = 0;
			for(int j=0; j<citylist.size(); j++) {
				if(high < prob[i][j]) {
					high = prob[i][j];
					highest[i] = citylist.get(j);
				}
			}
		}
		
		try {
			BufferedWriter dout = new BufferedWriter(new FileWriter("output"));
			for(int i=0; i<test_user.size(); i++) {
				dout.write("For user "+test_user.get(i)+": "+highest[i]);
				dout.write("\n");
			}
			dout.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
		logger.log(Level.INFO, "Highest Location Probability written to disk");		
		
	}
}

