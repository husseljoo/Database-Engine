import java.text.SimpleDateFormat;
import java.util.Date;

public class Main {

	
	
	public static void main(String[] args) {
		
		 
		
		 	String input = "Fri Apr 01 00:00:00 EET 2011";
	       String minDate="1990-01-01";
	        
	        try {
	        SimpleDateFormat Parser = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz yyyy");
	        Date date = Parser.parse(input);
	        System.out.println(date.getClass());
	        }
	        catch(Exception e) {
	        	System.out.println("couldnt parse it!"); }
	        
	        try {
		        SimpleDateFormat Parser = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz yyyy");
		        int year = Integer.parseInt(minDate.trim().substring(0, 4));
                int month = Integer.parseInt(minDate.trim().substring(5, 7));
                int day = Integer.parseInt(minDate.trim().substring(8));
                Date dob = new Date(year - 1900, month - 1, day);
		        
                System.out.println(dob.getDate());
//		        Date date = Parser.parse(dob);
		        System.out.println("e7na fel minDate");
		        System.out.println(dob.getClass());
		        
		        }
		        catch(Exception e) {
		        	System.out.println("couldnt parse it!"); }
		
		
		
//		Date date_passed = new Date(2011 - 1900, 4 - 1, 1);
//		 
//		 int n=0;
//		 date_passed.getDate();
//		 String dateString=date_passed+"";
//		 System.out.println(dateString);
//		
//		 
//		try {
//		System.out.println("aa");
//		Date dateParsedBack=new Date(dateString);
////		dateParsedBack.parse(dateString);
//		System.out.println("bb");
//			System.out.println("dateParsedBack: "+dateParsedBack);
//			System.out.println("dateParsedBack class is: "+dateParsedBack);
//		
//		
//		}
//		catch(Exception e) {
//			System.out.println("couldnt pasrse it!");}
		
		
		
		
		
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
