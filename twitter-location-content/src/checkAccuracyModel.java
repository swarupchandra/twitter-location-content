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
import java.util.logging.Level;
import java.util.logging.Logger;

import model.geolocator;


public class checkAccuracyModel {
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
		
		/*read dictionary*/
        logger.log(Level.INFO, "Reading Dictionary");
        ArrayList<String> dictionary = new ArrayList<String>();
		try {
			FileInputStream fstream = new FileInputStream("dictionary");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String word;
			while((word = br.readLine()) != null) {
				dictionary.add(word);
			}
			logger.log(Level.INFO, "Dictionary obtained");
			in.close();
			fstream.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
		
		/*read city list*/
		ArrayList<String> citylist = new ArrayList<String>();
		try {
			FileInputStream fstream = new FileInputStream("city");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String word;
			while((word = br.readLine()) != null) {
				citylist.add(word);
			}
			logger.log(Level.INFO, "city obtained");
			in.close();
			fstream.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
		
		/*read probability distribution*/
		double[][] distribution = new double[dictionary.size()][citylist.size()];
		try {
			FileInputStream fstream = new FileInputStream("distribution");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String word;
			int count = 0;
			while((word = br.readLine()) != null) {
				String[] split = word.split("\t");
				for(int i=0; i<split.length; i++) {
					if(!split[i].equals("")) {
						distribution[count][i] = Double.parseDouble(split[i]);
					}
				}
				++count;
			}
			logger.log(Level.INFO, "distribution obtained");
			in.close();
			fstream.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
		
		HashMap<String,ArrayList<Double>> modelParam = new HashMap<String,ArrayList<Double>>();
		try {
			FileInputStream fstream = new FileInputStream("model_parameters");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String word;
			int count = 0;
			while((word = br.readLine()) != null) {
				String[] split = word.split(" ");
				ArrayList<Double> temp = new ArrayList<Double>();
				for(int i=0; i<split.length; i++) {
					temp.add(Double.parseDouble(split[i]));
				}
				modelParam.put(dictionary.get(count), temp);
				++count;
			}
			logger.log(Level.INFO, "model parameters obtained");
			in.close();
			fstream.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
		
		/*use model parameter value with output*/
		index index = new index();
		for(int i=0; i<dictionary.size(); i++) {
			
			logger.log(Level.INFO,i+" "+dictionary.get(i));
			for(int j=0; j<citylist.size(); j++) {
				ArrayList<String> users = index.searchIndexUser(dictionary.get(i));
				ArrayList<String> cityusers = new ArrayList<String>();
				try {
					ResultSet res = s.executeQuery("SELECT user_name from users where user_city = '"+citylist.get(j)+"'");
					while(res.next()) {
						cityusers.add(res.getString(1));
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
				int count = 0;
				for(int n=0; n<cityusers.size(); n++) {
					if(!users.contains(cityusers.get(n))) {
						++count;
					}
				}
				double latitude = 0;
				double longitude = 0;
				try {
					ResultSet res = s.executeQuery("SELECT DISTINCT latitude,longitude from users where user_city = '"+cityusers.get(0)+"'");
					if(res.next()) {
						latitude = Double.parseDouble(res.getString(1));
						longitude = Double.parseDouble(res.getString(2));
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
				if(count == 0) {
					int tf = index.searchIndex(dictionary.get(i), cityusers, "users");
					ArrayList<Double> param = modelParam.get(dictionary.get(i));
					/*get distance d*/
					
					geolocator dist = new geolocator();
					double d = dist.distance(param.get(2), param.get(3), latitude, longitude);
					double a = param.get(0)*Math.pow(d,-param.get(1));
					distribution[i][j] *= Math.pow(a, tf);
				} else if(count == cityusers.size()) {
					ArrayList<Double> param = modelParam.get(dictionary.get(i));
					/*get distance d*/
					geolocator dist = new geolocator();
					double d = dist.distance(param.get(2), param.get(3), latitude, longitude);
					double a = param.get(0)*Math.pow(d,-param.get(1));
					distribution[i][j] *= 1.0-a;
				}
			}
		}
		
		try {
			BufferedWriter dout = new BufferedWriter(new FileWriter("model_distribution"));
			for(int i=0; i<distribution.length; i++) {
				for(int j=0; j<distribution[i].length; j++) {
					dout.write(distribution[i][j]+" ");
				}
				dout.write("\n");
			}
			dout.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.log(Level.INFO,"Model distribution written to disk");
		
		ArrayList<String> test_user = new ArrayList<String>();
		try {
			ResultSet result = s.executeQuery("SELECT DISTINCT user_name FROM test_user_info");
			while(result.next()) {
				test_user.add(result.getString(1));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		logger.log(Level.INFO, "Calculating Test user model Probability");
		
		int TotalTerm = 0;
		try {
//			index.createIndex("testusers");
			for(int u=0; u<test_user.size(); u++) {
				ArrayList<String> testuserlist = new ArrayList<String>();
				testuserlist.add(test_user.get(u));
				for(int j=0; j<dictionary.size(); j++) {
					TotalTerm += index.searchIndex(dictionary.get(j), testuserlist, "testusers");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		double[][] prob = new double[test_user.size()][citylist.size()];
		try {
			for(int u=0; u<test_user.size(); u++) {
				logger.log(Level.INFO, "For user "+u);
				ArrayList<String> testuserlist = new ArrayList<String>();
				testuserlist.add(test_user.get(u));
				for(int i=0; i<citylist.size(); i++) {
					prob[u][i] = 0.0;
					System.out.print(".");
					for(int j=0; j<dictionary.size(); j++) {
						int count = index.searchIndex(dictionary.get(j), testuserlist, "testusers");
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
			BufferedWriter dout = new BufferedWriter(new FileWriter("model_probability_output"));
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
		logger.log(Level.INFO, "Test user model probability written to disk");
		
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
			BufferedWriter dout = new BufferedWriter(new FileWriter("model_output"));
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
