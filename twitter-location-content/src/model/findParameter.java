package model;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;


public class findParameter {
	public ArrayList<Double> doProcessing(ArrayList<String> wordCities, Statement s, double minlongitude, double maxlongitude, double maxlatitude, double minlatitude) {
		
		double alpha = -100;
		double C = -100;
		double thismaxLat = -1;
		double thisminLat = -1;
		double thismaxLog = -1;
		double thisminLog = -1;
		double centerlat = -1;
		double centerlon = -1;
		
		/*divide the map into lattice*/
		double longdiv = Math.abs((maxlongitude - minlongitude)/28);
		double latdiv = Math.abs((maxlatitude - minlatitude)/12);
		
		double localminLattitude=minlatitude;
		double localmaxLattitude=localminLattitude+latdiv;
		while(localmaxLattitude<maxlatitude ) {
			double localminLongitude=minlongitude;
			double localmaxLongitude=localminLongitude+longdiv;
			while(localmaxLongitude<maxlongitude) {
				
				/*get city list within the present lattice*/
				ArrayList<String> latticeCities = getCities(localminLattitude, localmaxLattitude, localminLongitude, localmaxLongitude,s);
				
				if(latticeCities.size() > 0) {
					/*get cities from the given list in present lattice*/
					ArrayList<String> cities = new ArrayList<String>();
					for(int n=0; n<wordCities.size(); n++) {
						if(latticeCities.contains(wordCities.get(n))) {
							cities.add(wordCities.get(n));
						}
					}
					
					/*get lattice center coordinates*/
					double centerLatitude = (localmaxLattitude + localminLattitude)/2;
					double centerLongitude = (localmaxLongitude + localminLongitude)/2;
					
					/*get distance from lattice center to each city*/
					HashMap<String,Double> distance = getDistance(centerLatitude, centerLongitude, s);
					
					/*form S and SDash array to get parameter value*/
					HashMap<String,Double> S = new HashMap<String,Double>();
					HashMap<String,Double> SDash = new HashMap<String,Double>();
					
					Iterator<String> distIterate = distance.keySet().iterator();
					while(distIterate.hasNext()) {
						String city = distIterate.next();
						if(cities.contains(city)) {
							S.put(city, distance.get(city));
						} else {
							SDash.put(city, distance.get(city));
						}
					}
					
					/*get parameters*/
					goldenSection search = new goldenSection();
					double[] param = search.goldenSectionSearch(0.0, 1.0, 0.0, 2.0, S, SDash);
					
					/*store the highest values*/
					if((C <= param[0] && alpha < param[1]) || (C < param[0] && alpha <= param[1])) {
						C = param[0];
						alpha = param[1];
						thismaxLat = localmaxLattitude;
						thisminLat = localminLattitude;
						thismaxLog = localmaxLongitude;
						thisminLog = localminLongitude;
						centerlat = centerLatitude;
						centerlon = centerLongitude;
					}
				}
				
				localminLongitude +=longdiv;
				localmaxLongitude +=longdiv;
			}
			localminLattitude += latdiv;
			localmaxLattitude += latdiv;
		}
		
		/*C,alpha,minlongitude,maxlongitude,maxlatitude,minlatitude, centerlatitude,centerlongitude*/
		ArrayList<Double> info = new ArrayList<Double>();
		info.add(C);
		info.add(alpha);
		
		info.add(thisminLog);
		info.add(thismaxLog);
		
		info.add(thismaxLat);
		info.add(thisminLat);
		info.add(centerlat);
		info.add(centerlon);
		
		
		return info;		
	}
	
	ArrayList<String> getCities(double minLatitude, double maxLatitude, double minLongitude, double maxLongitude, Statement s) {
		ArrayList<String> refineCities = new ArrayList<String>();
		String query = "Select DISTINCT user_city FROM users where latitude >= "+minLatitude+" && latitude < "+maxLatitude+" && longitude >= "+minLongitude+" && longitude < "+maxLongitude;
		ResultSet res;
		try {
			res = s.executeQuery(query);
			while(res.next()) {
				refineCities.add(res.getString(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return refineCities;
		
	}
	
	HashMap<String,Double> getDistance (double centerlatitude, double centerlongitude, Statement s) {
		HashMap<String, Double> cityDistance = new HashMap<String, Double>();
		/*read city list file*/
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
		
		/*get distance for each city*/
		for(int i=0; i<citylist.size(); i++) {
			double latitude = 0, longitude = 0;
			String query = "Select DISTINCT latitude, longitude FROM users WHERE user_city = '"+citylist.get(i)+"'";
			try {
				ResultSet res = s.executeQuery(query);
				if(res.next()) {
					latitude = Double.parseDouble(res.getString(1));
					longitude = Double.parseDouble(res.getString(2));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			geolocator dist = new geolocator();
			cityDistance.put(citylist.get(i), dist.distance(latitude, longitude, centerlatitude, centerlongitude));
		}
		
		return cityDistance;
	}

}


