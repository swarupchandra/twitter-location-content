import index.index;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.logging.*;



public class locationMiningTrain {
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
        	s.execute("TRUNCATE users");
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
		
		logger.log(Level.INFO,"Reading tweets to extract terms");
		HashMap<String,Integer> dictionary = new HashMap<String,Integer>();
		HashMap<String,String> jaccardEqui = new HashMap<String,String>();
		HashMap<String,Integer> userMap = new HashMap<String,Integer>();
		index tweet_eval = new index();
		
		
		try {
			FileInputStream fstream = new FileInputStream("training_set_tweets.txt");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String str, concat_str = "";
			String[] tweet = new String[4];
			boolean done = true;
			int count = 0;
			while((str = br.readLine()) != null) {
				if(count == 5000) {
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
								if(!jaccardTerm.equals("")) {
									userMap.put(tweet[0], 0);
									BufferedWriter dout = new BufferedWriter(new FileWriter("users/"+tweet[0],true));
									
									dout.write(jaccardTerm+"\n");
									dout.close();
									if(!dictionary.containsKey(jaccardTerm)) {
										dictionary.put(jaccardTerm, 0);
									}
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
		
		logger.log(Level.INFO, "Dictionary obtained");
		
		
		/*Obtaining Word List*/
		ArrayList<String> termlist = new ArrayList<String>();
		Set<String> termset = dictionary.keySet();
		Iterator<String> termiterate = termset.iterator();
		while(termiterate.hasNext()) {
			termlist.add(termiterate.next());
		}
		
		try {
			BufferedWriter dout = new BufferedWriter(new FileWriter("dictionary"));
			for(int i=0; i<termlist.size(); i++) {
				dout.write(termlist.get(i));
				dout.write("\n");
			}
			dout.close();	
		} catch(Exception e) {
			e.printStackTrace();
		}
		logger.log(Level.INFO, "Dictionary written to disk");
		
		/*Training - user information*/
		
		/*initialize*/
		try {
			logger.log(Level.INFO, "Obtaining user info");
			
			ArrayList<String> users = new ArrayList<String>();
			Iterator<String> userIterate = userMap.keySet().iterator();
			while(userIterate.hasNext()) {
				users.add(userIterate.next());
			}
			
			FileInputStream fstream = new FileInputStream("training_set_users.txt");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String user_info = "", query = "";
			int count = 0;
			while((user_info = br.readLine()) != null) {
				if(count%500 == 0) {
					if(!query.equals("")) {
						s.executeUpdate("INSERT INTO users(user_name,user_city,user_state) VALUES "+query);
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
				s.executeUpdate("INSERT INTO users(user_name,user_city,user_state) VALUES "+query);
			}
			logger.log(Level.INFO, count+" user's info obtained");
			in.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
		
		logger.log(Level.INFO, "Obtaining user city list");
		/*Obtaining City List*/
		ArrayList<String> citylist = new ArrayList<String>();
		try {
			ResultSet cityset = s.executeQuery("SELECT DISTINCT user_city FROM users");
			while(cityset.next()) {
				citylist.add(cityset.getString(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		/*writing city list to memory*/
		try {
			BufferedWriter dout = new BufferedWriter(new FileWriter("city"));
			for(int i=0; i<citylist.size(); i++) {
				dout.write(citylist.get(i));
				dout.write("\n");
			}
			dout.close();	
		} catch(Exception e) {
			e.printStackTrace();
		}
		logger.log(Level.INFO, "City list written to disk");
		
				
		/*Calculating Probability*/
		logger.log(Level.INFO, "Calculating Probability Distribution");
		
		double[][] distribution = new double[termlist.size()][citylist.size()];
		index index = new index();
		logger.log(Level.INFO, "Creating document indexes");
		index.createIndex("users");
		
		for(int i=0; i<termlist.size(); i++) {
			if(i%100 == 0) {
				System.out.println();
				logger.log(Level.INFO, "PD : checking for word count "+i);
			}
			System.out.print(".");
			for(int j=0; j<citylist.size(); j++) {
				try {
					ArrayList<String> userlist = new ArrayList<String>();
					ResultSet userresult = s.executeQuery("SELECT DISTINCT user_name FROM users WHERE user_city = '"+citylist.get(j)+"'");
					while(userresult.next()) {
						userlist.add(userresult.getString(1));
					}
					distribution[i][j] = (double)index.searchIndex(termlist.get(i),userlist,"users");
					
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
		for(int i=0; i<citylist.size(); i++) {
			double total = 0.0;
			for(int j=0; j<termlist.size(); j++) {
				total += distribution[j][i];
			}
			for(int j=0; j<termlist.size(); j++) {
				distribution[j][i] = distribution[j][i]/total;
			}
			
		}
		
		logger.log(Level.INFO, "Distribution Probability List created");
		
		/*write distribution to disk*/
		try {
			BufferedWriter dout = new BufferedWriter(new FileWriter("distribution"));
			for(int i=0; i<termlist.size(); i++) {
				for(int j=0; j<citylist.size(); j++) {
					dout.write(""+distribution[i][j]+"\t");
				}
				dout.write("\n");
			}
			dout.close();	
		} catch(Exception e) {
			e.printStackTrace();
		}
		logger.log(Level.INFO, "distribution written to disk");
	}
}



