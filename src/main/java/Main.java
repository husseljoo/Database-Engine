import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

public class Main {

	
	public static void main(String[] args) {
		
		System.out.println("Wassup");
		Vector<String> vect=new Vector<>();
		
		vect.add("gpa");
		vect.add("last_name");
		vect.add("first_name");
		vect.add("dob");
		
		
		Object[][] obj=getMinMax("students",vect);
		
		for(int i=0;i<obj.length;i++) {
			System.out.println();
			System.out.println("Min: "+obj[i][0]);
			System.out.println("Max: "+obj[i][1]);
			System.out.println();
		}
		
	}
	
	public static Object[][] getMinMax(String str_TableName,Vector<String> vect_arrColName){

	BufferedReader objReader = null;
	String strCurrentLine="";
	//skip the first line
	try {
		objReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		strCurrentLine = objReader.readLine();
	} catch (IOException e) {
		e.printStackTrace();
	}
	
	
	Object[][] returnList=new Object[vect_arrColName.size()][2];
	
	
	//actually start looking for column values
	try {
		
		String[] splitted=null;
		boolean bool=false;
		for (int i = 0; (strCurrentLine = objReader.readLine()) != null; i++) {
			splitted=strCurrentLine.split(",");
			if(splitted[0].equals(str_TableName)) {
				bool=true;
				break;}}
		
		int counter=vect_arrColName.size();
		while(counter>0) {
			for(int i=0;i<vect_arrColName.size();i++) {
			   if(splitted[1].equals(vect_arrColName.get(i))) {    	
				   	
				    Object min=splitted[5];
				    Object max=splitted[6];
				   	returnList[i][0]=min; //min
				   	returnList[i][1]=max; //max
				   	strCurrentLine = objReader.readLine();
				   	System.out.println(strCurrentLine);
				   	splitted=strCurrentLine.split(",");
			   		counter--;
//			   		System.out.println();
//			   		System.out.println("Column name is:"+vect_arrColName.get(i));
//			   		System.out.println("minimum is:"+min.toString());
//				   	System.out.println("maximum is:"+max.toString());
//				   	System.out.println();
				   	break;
			   }
				
				}
			}
		
		} catch (IOException e) {
		e.printStackTrace();
	}  
	
	return returnList;
	}
}







