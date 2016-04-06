import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
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


public class topAccuracy {
	public static void main(String args[]) {
		
		Logger logger = Logger.getLogger("indexer");
		int errdist = 500;
		
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
			FileInputStream fstream = new FileInputStream("city");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String word;
			while((word = br.readLine()) != null) {
				citylist.add(word);
			}
			in.close();
			fstream.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
		
		ArrayList<String> userlist = new ArrayList<String>();
		try {
			ResultSet res = s.executeQuery("select user_name from test_user_info");
			while(res.next()) {
				userlist.add(res.getString(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		ArrayList<String> baseprob = new ArrayList<String>();
		try {
			FileInputStream fstream = new FileInputStream("probability_output");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String word;
			while((word = br.readLine()) != null) {
				baseprob.add(word);
			}
			in.close();
			fstream.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
		
		ArrayList<String> modelprob = new ArrayList<String>();
		try {
			FileInputStream fstream = new FileInputStream("model_probability_output");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String word;
			while((word = br.readLine()) != null) {
				modelprob.add(word);
			}
			in.close();
			fstream.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
		
		System.out.println("For baseline probability");
		ArrayList<ArrayList<String>> topbp = new ArrayList<ArrayList<String>>();
		sort sort = new sort();
		for(int i=0; i<baseprob.size(); i++) {
			ArrayList<Double> bp = new ArrayList<Double>();
			
			String[] split = baseprob.get(i).split("\t");
			double[] sortbp = new double[split.length-1];
			int count = 0;
			for(int j=1; j<split.length; j++) {
				if(!split[j].equals("")) {
					bp.add(Double.parseDouble(split[j]));
					sortbp[count] = Double.parseDouble(split[j]);
					++count;
				}
			}
			if(bp.size() > 0) {
				sort.mergeSort(sortbp, 1, count);
				ArrayList<String> top10bp = new ArrayList<String>();
				for(int j=0; j<10; j++) {
					int index = bp.indexOf(sortbp[j]);
					top10bp.add(citylist.get(index));
				}
				topbp.add(top10bp);
			}
		}
		
		System.out.println("For model probability");
		ArrayList<ArrayList<String>> topmodel = new ArrayList<ArrayList<String>>();
		for(int i=0; i<modelprob.size(); i++) {
			ArrayList<Double> model = new ArrayList<Double>();
			
			String[] split = modelprob.get(i).split(" ");
			double[] sortmodel = new double[split.length-1];
			int count = 0;
			for(int j=1; j<split.length; j++) {
				if(!split[j].equals("")) {
					model.add(Double.parseDouble(split[j]));
					sortmodel[count] = Double.parseDouble(split[j]);
					++count;
				}
				
			}
			if(model.size() > 0) {
				sort.mergeSort(sortmodel, 1, count);
				ArrayList<String> top10model = new ArrayList<String>();
				for(int j=0; j<10; j++) {
					int index = model.indexOf(sortmodel[j]);
					top10model.add(citylist.get(index));
				}
				topmodel.add(top10model);
			}
			
		}
		
		/*Check accuracy*/
		
		try {
			BufferedWriter dout = new BufferedWriter(new FileWriter("top10_base_output"));
			for(int i=0; i<topbp.size(); i++) {
				dout.write(userlist.get(i));
				for(int j=0; j<topbp.get(i).size(); j++) {
					dout.write(" "+topbp.get(i).get(j));
				}
				dout.write("\n");
			}
			dout.close();	
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		try {
			BufferedWriter dout = new BufferedWriter(new FileWriter("top10_model_output"));
			for(int i=0; i<topmodel.size(); i++) {
				dout.write(userlist.get(i));
				for(int j=0; j<topmodel.get(i).size(); j++) {
					dout.write(" "+topmodel.get(i).get(j));
				}
				dout.write("\n");
			}
			dout.close();	
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		/*get test users lat and long*/
		
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
		
		/*baseline estimation*/
		int total = 0, truetotal = 0;
		geolocator dist = new geolocator();
		for(int i=0; i<topbp.size(); i++) {
			ArrayList<Double> calculateLat = new ArrayList<Double>();
			for(int j=0; j<topbp.get(i).size(); j++) {
				try {
					ResultSet res = s.executeQuery("SELECT latitude,longitude from users where user_city = '"+topbp.get(i).get(j)+"'");
					if(res.next()) {
						calculateLat.add(res.getDouble(1));
						calculateLat.add(res.getDouble(2));
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			
			for(int j=0; j<calculateLat.size(); j++) {
				double errordistance = dist.distance(testLatLon.get(j), testLatLon.get(j+1), calculateLat.get(j) , calculateLat.get(j+1));
				++j;
				if(errordistance <= errdist) {
					++truetotal;
					break;
				}
				
			}
			++total;
		}
		
		System.out.println("Baseline Accuracy = "+((double)truetotal*100/(double)total)+"%");
		
		total = 0;
		truetotal = 0;
		
		for(int i=0; i<topmodel.size(); i++) {
			ArrayList<Double> calculateLat = new ArrayList<Double>();
			for(int j=0; j<topmodel.get(i).size(); j++) {
				try {
					ResultSet res = s.executeQuery("SELECT latitude,longitude from users where user_city = '"+topmodel.get(i).get(j)+"'");
					if(res.next()) {
						calculateLat.add(res.getDouble(1));
						calculateLat.add(res.getDouble(2));
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			
			for(int j=0; j<calculateLat.size(); j++) {
				double errordistance = dist.distance(testLatLon.get(j), testLatLon.get(j+1), calculateLat.get(j) , calculateLat.get(j+1));
				++j;
				if(errordistance <= errdist) {
					++truetotal;
					break;
				}
				
			}
			++total;
		}
		
		System.out.println("Model Accuracy = "+((double)truetotal*100/(double)total)+"%");
		
	}
}

class sort {
	void mergeSort(double[] A, int p, int r) {
		if(p < r) {
			int q = (p+r)/2;
			mergeSort(A,p,q);
			mergeSort(A,q+1,r);
			merge(A,p,q,r);
		}
	}
	
	void merge(double[] A, int p, int q, int r) {
		int n1 = q - p + 1;
		int n2 = r - q;
		double[] L = new double[n1+1];
		double[] R = new double[n2+1];
		for(int i=0; i<n1; i++) {
			L[i] = A[p + i - 1];
		}
		L[n1] = 99999;
		for(int i=0; i<n2; i++) {
			R[i] = A[q + i];
		}
		R[n2] = 99999;
		int i=0, j=0;
		for(int k=p-1; k<r; k++) {
			if(L[i] <= R[j]) {
				A[k] = L[i];
				++i;
			} else {
				A[k] = R[j];
				++j;
			}
		}
		
	}
}



