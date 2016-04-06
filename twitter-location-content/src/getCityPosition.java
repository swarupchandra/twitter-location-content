import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;


public class getCityPosition {
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
        
        ArrayList<String> citylist = new ArrayList<String>();
		try {
			ResultSet cityset = s.executeQuery("SELECT DISTINCT user_city FROM users");
			while(cityset.next()) {
				citylist.add(cityset.getString(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		ArrayList<String> position = new ArrayList<String>();
		for(int i=0; i<citylist.size(); i++) {
			String[] split = citylist.get(i).split(" ");
			String param = split[0];
			if(split.length > 1) {
				for(int j=1; j<split.length; j++) {
					param = param+"%20"+split[j];
				}
			}
			String urlStr = "http://tinygeocoder.com/create-api.php?q="+param;
		
			// Send a GET request to the servlet
			try	{
				
				// Send data
				URL url = new URL(urlStr);
				HttpURLConnection conn = (HttpURLConnection) url.openConnection ();
				
				BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
				StringBuffer sb = new StringBuffer();

				String line;
				
				while ((line = rd.readLine()) != null) {
					sb.append(line);
				}
				rd.close();
				String result = sb.toString();
//				System.out.println(result);
				System.out.println(citylist.get(i)+" : "+result);
				position.add(result);
				Thread.sleep(500);

				// Get the response
				
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
		
		if(citylist.size() != position.size()) {
			System.out.println("error in size match");
		} else {
			for(int i=0; i<position.size(); i++) {
				if(!position.get(i).equals("")) {
					String[] split = position.get(i).split(",");
					
					System.out.println(i+" : "+split[0]+", "+split[1]);
					try {
						s.executeUpdate("Update users SET latitude='"+split[0]+"', longitude='"+split[1]+"' WHERE user_city='"+citylist.get(i)+"'");
					} catch (SQLException e) {
						e.printStackTrace();
					}
				} else {
					System.out.println("No position for "+citylist.get(i));
				}
				
			}
		}
		
	}
}
