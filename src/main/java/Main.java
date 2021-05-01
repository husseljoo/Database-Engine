import java.util.Date;
import java.text.SimpleDateFormat;

public class Main {

	
	
	public static void main(String[] args) {
		String a="1.4";
		try {
			Double d=Double.parseDouble(a);
			System.out.println(d);
		
		}catch(Exception e) {e.printStackTrace();}
//		Object o="2.2";
//		String type="java.lang.Double";
//		boolean b=sameValueHusseljo(a, o, type);
//		System.out.println(b);
		String s1="Ahmed";
		String s2="AAussein";
//		System.out.println(s1.compareTo(s2));
		
	}
	
	public static boolean sameValueHusseljo(String a,Object o,String type) {
		String s=o.toString();
		
//		String type="java.lang.Date";
		boolean b=false;
		try {
		switch(type) {
		  case "java.lang.Integer":
		    Integer w=Integer.parseInt(a);
		    Integer w2=Integer.parseInt(s);
		    b=w.equals(w2); 
		    break;
		  case "java.lang.Double":
		    Double d=Double.parseDouble(a);
		    Double d2=Double.parseDouble(s);
		    b=d.equals(d2);
		    break;
		  case "java.lang.Date":
			 Date date1=new SimpleDateFormat("yyyy/MM/dd").parse(a);
			 Date date2=new SimpleDateFormat("yyyy/MM/dd").parse(s);
			 b=date1.equals(date2);
			 break;
		  default:
		    b=a.equals(s);
		}
		}
		catch(Exception e) {}
		
		return b;
		
	}

}
