import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;


import model.geolocator;


public class checkAccuracy {
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
        
        ArrayList<String> probOutput = new ArrayList<String>();
		try {
			FileInputStream fstream = new FileInputStream("model_output");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String word;
			while((word = br.readLine()) != null) {
				probOutput.add(word);
			}
			logger.log(Level.INFO, "Output obtained");
			in.close();
			fstream.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
		
		ArrayList<Double> calculateLat = new ArrayList<Double>();
		for(int i=0; i<probOutput.size(); i++){
			String[] split = probOutput.get(i).split(":");
			try {
				ResultSet res = s.executeQuery("SELECT latitude,longitude from users where user_city = '"+split[1].trim()+"'");
				if(res.next()) {
					calculateLat.add(res.getDouble(1));
					calculateLat.add(res.getDouble(2));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		
		ArrayList<Double> testLatLon = new ArrayList<Double>();
		try {
			ResultSet res = s.executeQuery("SELECT user_city, user_state, id FROM test_user_info");
			while(res.next()) {
				double lat = Double.parseDouble(res.getString(1).substring(3));
				double lon = Double.parseDouble(res.getString(2));
				testLatLon.add(lat);
				testLatLon.add(lon);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		if(testLatLon.size() != calculateLat.size()) {
			System.out.println(testLatLon.size()+", "+calculateLat.size());
			System.out.println("error");
		}
		
		
		geolocator dist = new geolocator();
		System.out.println("Calculating accuracy");
		int total = 0, truetotal = 0;
		
		for(int i=0; i<calculateLat.size(); i++) {
			double errordistance = dist.distance(testLatLon.get(i), testLatLon.get(i+1), calculateLat.get(i) , calculateLat.get(i+1));
			++i;
			if(errordistance <= 900) {
				++truetotal;
			}
			++total;
		}
		
		
		System.out.println("Accuracy = "+((double)truetotal*100/(double)total)+"%");
	}
}
