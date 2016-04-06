package model;

import java.util.HashMap;
import java.util.Iterator;

public class goldenSection {
	
	public double[] goldenSectionSearch(double C1, double C2, double alpha1, double alpha2, HashMap<String,Double> S, HashMap<String,Double> SDash) {
		double[] parameter = new double[2];
		parameter[0] = -1;
		parameter[1] = -1;
		boolean done = false;
		double  r = 0.6180;
		while(!done) {
			double x1 = C1 + (1 - r)*(C2 - C1);
			double x2 = C1 + r*(C2 - C1);
			double y1 = alpha1 + (1 - r)*(alpha2 - alpha1);
			double y2 = alpha1 + r*(alpha2 - alpha1);
			
			if(Math.abs((x2 - x1)) < 0.001 || Math.abs((y2 - y1)) < 0.001) {
				parameter[0] = x2;
				parameter[1] = y2;
				done = true;
			}		
			
			if(!done) {
				/*evaluate for the 4 combination*/
				double[] res = new double[4];
				res[0] = objective_function(x1,y1,S,SDash);
				res[1] = objective_function(x1,y2,S,SDash);
				res[2] = objective_function(x2,y1,S,SDash);
				res[3] = objective_function(x2,y2,S,SDash);
				
				/*find maximum*/
				double max = res[0];
				int index = 0;
				for(int i=0; i<res.length; i++) {
					if(max < res[i]) {
						max = res[i];
						index = i;
					}
				}
				if(index == 0) {
					C2 = x2;
					alpha2 = y2;
				} else if (index == 1) {
					C2 = x2;
					alpha1 = y1;
				} else if (index == 2) {
					C1 = x1;
					alpha2 = y2;
				} else {
					C1 = x1;
					alpha1 = y1;
				}
			}
		}
		/*parameter[0] = C, parameter[1] = alpha*/
		return parameter;
	}
	
	public double objective_function(double c,double alpha,HashMap<String,Double> S, HashMap<String,Double> SDash) {
		double value = 0.0;
		
		/*word belongs to S (word,citydistance)*/
		Iterator<String> SIterate = S.keySet().iterator();
		while(SIterate.hasNext()) {
			double d = S.get(SIterate.next());
			value += Math.log(c * Math.pow(d,-(alpha)));
		}		
			
		/*word belongs to SDash (word,citydistance)*/
		Iterator<String> SDashIterate = SDash.keySet().iterator();
		while(SDashIterate.hasNext()) {
			double d = SDash.get(SDashIterate.next());
			value += Math.log(1-(c * Math.pow(d,-(alpha))));
		}
		return value;
	}
	
}