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
import java.util.logging.Level;
import java.util.logging.Logger;

import model.findParameter;


public class LocationMiningModel {
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
		
		index index = new index();
		double minlongitude=-124.77;
		double maxlongitude=-66.95;
		double maxlatitude=49.38;
		double minlatitude=24.52;
		ArrayList<Double> param = new ArrayList<Double>();
		try {
			logger.log(Level.INFO, "Calculating parameter value <C alpha>");
			
			for(int i=0; i<dictionary.size(); i++) {
				logger.log(Level.INFO, "For "+i+" - "+dictionary.get(i));
				/*get user list*/
				ArrayList<String> users = index.searchIndexUser(dictionary.get(i));
				HashMap<String,Integer> cities = new HashMap<String,Integer>();
				for(int u=0; u<users.size(); u++) {
					try {
						ResultSet res = s.executeQuery("SELECT user_city from users where user_name='"+users.get(u)+"'");
						while(res.next()) {
							cities.put(res.getString(1), 0);
						}
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
				
				Iterator<String> iterate = cities.keySet().iterator();
				ArrayList<String> wordCities = new ArrayList<String>();
				while(iterate.hasNext()) {
					wordCities.add(iterate.next());
				}
				
				findParameter paramProcess = new findParameter();
				
				ArrayList<Double> param1 = paramProcess.doProcessing(wordCities, s, minlongitude, maxlongitude, maxlatitude, minlatitude);
				/*write parameter to file <C alpha>*/
				param.add(param1.get(0));
				param.add(param1.get(1));
				param.add(param1.get(6));
				param.add(param1.get(7));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			BufferedWriter dout = new BufferedWriter(new FileWriter("model_parameters"));
			for(int i=0; i<param.size(); i+=4) {
				dout.write(param.get(i)+" "+param.get(i+1)+" "+param.get(i+2)+" "+param.get(i+3));
				dout.write("\n");
			}
			dout.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
