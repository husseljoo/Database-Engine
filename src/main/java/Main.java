import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;


public class Main {

	
	
	public static void main(String[] args) {
		
		String strTableName = "Student";
    	DBApp dbApp = new DBApp( );
    	dbApp.init();
    	Hashtable htblColNameType = new Hashtable( );
    	htblColNameType.put("id", "java.lang.Integer");
    	htblColNameType.put("name", "java.lang.String");
    	htblColNameType.put("gpa", "java.lang.double");
    	
    	Hashtable htblColNameMin = new Hashtable( );
    	htblColNameMin.put("id", "0");
    	htblColNameMin.put("name", "A");
    	htblColNameMin.put("gpa", "0");
    	
    	Hashtable htblColNameMax = new Hashtable( );
    	htblColNameMax.put("id", "10000");
    	htblColNameMax.put("name", "ZZZZZZZZZZZ");
    	htblColNameMax.put("gpa", "4");
    	
    	try {
    	dbApp.createTable( strTableName, "id", htblColNameType,htblColNameMin,htblColNameMax);
    	
    	//INSERTION INTO TABLES
    	Hashtable htbl_values = new Hashtable( );
    	htbl_values.put("id", 1);
    	htbl_values.put("name", "ZZZZZZZZZZZ");
    	htbl_values.put("gpa", 3);
    	
    	
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	
    	htbl_values.put("id", 13);
    	dbApp.insertIntoTable("Student",htbl_values);

    	htbl_values.put("id", 100);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	htbl_values.put("id", 22);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	
    	htbl_values.put("id", 3);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	htbl_values.put("id", 43);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	
    	
    	htbl_values.put("id", 232);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	htbl_values.put("id", 4);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	
    	htbl_values.put("id", 17);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	htbl_values.put("id",2);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	}
    	catch(Exception e) {}
    	
    	
    	TableInfo StudentTableInfo=(TableInfo)dbApp.deserialize("src/main/resources/data/Student/tableInfo.class");
    	String studentPath="src/main/resources/data/Student/";
    	
    	//Print all pages with their contents
    	
    	for (int i = 0; i < StudentTableInfo.pages.size(); i++) {
    		String s=(String)(StudentTableInfo.pages.get(i)[0]);
    		System.out.println("Tuples in "+s+":"+"  MAX VALUE IS "+((Integer)StudentTableInfo.pages.get(i)[1]));
    		Page page=(Page)deserialize(studentPath+"/"+s+".class");
    		for (int j = 0; j < page.tuples.size(); j++) {
    			System.out.println("tuple"+j+": "+page.tuples.get(j).record.toString());}
    		System.out.println();
    	}		 
		
//		 	String input = "Fri Apr 01 00:00:00 EET 2011";
//	       String minDate="1990-01-01";
//	        
//	        try {
//	        SimpleDateFormat Parser = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz yyyy");
//	        Date date = Parser.parse(input);
//	        System.out.println(date.getClass());
//	        }
//	        catch(Exception e) {
//	        	System.out.println("couldnt parse it!"); }
//	        
//	        try {
//		        SimpleDateFormat Parser = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz yyyy");
//		        int year = Integer.parseInt(minDate.trim().substring(0, 4));
//                int month = Integer.parseInt(minDate.trim().substring(5, 7));
//                int day = Integer.parseInt(minDate.trim().substring(8));
//                Date dob = new Date(year - 1900, month - 1, day);
//		        
//                System.out.println(dob.getDate());
////		        Date date = Parser.parse(dob);
//		        System.out.println("e7na fel minDate");
//		        System.out.println(dob.getClass());
//		        
//		        }
//		        catch(Exception e) {
//		        	System.out.println("couldnt parse it!"); }
//		
		
		
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
	public static Object deserialize(String path) {
    	Object o=null;
    	try
        {   
            // Reading the object from a file
    		FileInputStream file = new FileInputStream(path);
            ObjectInputStream in = new ObjectInputStream(file);
              
            // Method for deserialization of object
            try{o= in.readObject();}catch(Exception e) {}
              
            in.close();
            file.close();
            
            
        }
          
        catch(Exception e)
        {
        	e.printStackTrace();
        	System.out.println("IOException is caught");
        	
        }
    	return o;

    }
}
