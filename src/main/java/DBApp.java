import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;
public class DBApp implements DBAppInterface{
	FileWriter csvWriter;
	
	public void init()
	{
		String currLine ="Table Name, Column Name, Column Type, ClusteringKey, Indexed, min, max\n";
		try 
		{
			csvWriter = new FileWriter("/home/husseljo/Desktop/DB2Project/src/main/resources/metadata.csv");
			csvWriter.append(currLine);
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
	}


    public void createTable(String str_TableName,
    		String str_ClusteringKeyColumn,
    		Hashtable<String,String> htbl_ColNameType,
    		Hashtable<String,String> htbl_ColNameMin,
    		Hashtable<String,String> htbl_ColNameMax )
    		throws DBAppException{
 
    	Enumeration<String> enumeration = htbl_ColNameType.keys();
    	String currLine = "";
    	String columnFormat=""; //FORMAT TO BE INSERTED IN CSV FILE OF TABLE FOR CONSISTENCY
    	Vector<String> columnFormatArr=new Vector();
    	while(enumeration.hasMoreElements()) 
        {
        	String columnName= enumeration.nextElement();
        	String columnType=htbl_ColNameType.get(columnName);
        	String clusterKey=Boolean.toString(columnName.equals(str_ClusteringKeyColumn)); 
        	String columnMin=htbl_ColNameMin.get(columnName);
        	String columnMax=htbl_ColNameMax.get(columnName);
        	
        	currLine+= str_TableName+","+columnName+","+columnType+","+clusterKey+","+"False,"+columnMin+","+columnMax+"\n";
        	columnFormatArr.add(columnName);
        	try 
        	{
				csvWriter.append(currLine);
			} 
        	catch (IOException e) 
        	{
				e.printStackTrace();
			}
        	currLine = "";
        }
        try 
        {
			csvWriter.flush();
		} catch (IOException e) 
        {
			e.printStackTrace();
		}
        /*********************************************************************************/
        //Once we create a Table, we create a folder/directory with its name inside the tables folder
        //it will contain all the pages of the table as well as a table_name.csv that contains info about
        // its page files (.class or .ser) i.e key range of page,bit indicating whether its full or not etc.
        Path path =Paths.get("/home/husseljo/Desktop/DB2Project/src/main/resources/data/"+str_TableName);
        try 
        {

            Files.createDirectories(path);    
            
            
         } catch (Exception e) {
        	  e.printStackTrace();
            System.err.println("Failed to create directory!" + e.getMessage());

          }
        
        System.out.println(columnFormatArr.toString());
        
        TableInfo tableInfo=new TableInfo();
        tableInfo.colOrder=columnFormatArr;
        tableInfo.clusteringKey=str_ClusteringKeyColumn;
        serialize(tableInfo,path+"/"+"tableInfo.class");
    }

    public static void serialize(Object o,String path) {
//    	String filename = "/home/husseljo/Desktop/DB2Project/src/main/resources/testTuple.ser"; //testTuple.class also works?
        
        try
	        {   
        	//Saving of object in a file
	        FileOutputStream file = new FileOutputStream(path);
	        ObjectOutputStream out = new ObjectOutputStream(file);
	        out.writeObject(o);
	        out.close();
	        file.close();
	        System.out.println("Object has been serialized");

	        }
	          
	    catch(IOException ex)
	        {
	        ex.printStackTrace();
	        System.out.println("IOException is caught");
	        }

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
            System.out.println("Object has been deserialized ");
            
        }
          
        catch(Exception e)
        {
        	e.printStackTrace();
        	System.out.println("IOException is caught");
        	
        }
    	return o;

    } 
    
    public void insertIntoTable(String str_TableName,
    		Hashtable<String,Object> htbl_ColNameValue)
    		throws DBAppException{
    
    		boolean b=checkValidity(str_TableName,htbl_ColNameValue);
    		if(b) 
    		{
    			String path="/home/husseljo/Desktop/DB2Project/src/main/resources/data/"+str_TableName+"/tableInfo.class";
    			TableInfo tableInfo=(TableInfo)deserialize(path);
    			Tuple tuple=new Tuple();
    			String clusteringKey=tableInfo.clusteringKey;
    			
    			for(int i=0;i<tableInfo.colOrder.size();i++) 
    			{
    				tuple.record.add(htbl_ColNameValue.get(tableInfo.colOrder.get(i)));
    			}
    			if(tableInfo.pages.size()==0)
    			{
    				Page page = new Page();
    				page.tuples.add(tuple);
    				Object[] pageInfo = {"page0",htbl_ColNameValue.get(clusteringKey)};
    				tableInfo.pages.add(pageInfo);
    				
    				String pathOfPage = "/home/husseljo/Desktop/DB2Project/src/main/resources/data/"+str_TableName+"/page0.class";
    				serialize(page,pathOfPage);
    				serialize(tableInfo,path);
    			}
    			else
    			{
    				
    			}
    		}
    	}    	
    
    public static boolean checkValidity(String str_TableName,Hashtable<String,Object> htbl_ColNameValue) throws DBAppException{
    	
    	Path path =Paths.get("/home/husseljo/Desktop/DB2Project/src/main/resources/data/"+str_TableName);
    	if (!Files.exists(path)) {
    		throw new DBAppException("Specified Table does not exist at all!");
    		}
    	
    	BufferedReader objReader = null;
		String strCurrentLine;
		try {
			objReader = new BufferedReader(new FileReader("/home/husseljo/Desktop/DB2Project/src/main/resources/metadata.csv"));
			strCurrentLine = objReader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
			}

		String[] arrOfStr = new String[0];
		int positionOfTable = 0;
		boolean found = false;
		ArrayList<String> fieldNames = new ArrayList();
		ArrayList<String> dataTypes = new ArrayList();
		ArrayList<String> min = new ArrayList();
		ArrayList<String> max = new ArrayList();
		Tuple tuple = new Tuple();
		try {
			for (int i = 0; (strCurrentLine = objReader.readLine()) != null; i++) {
				arrOfStr = strCurrentLine.split(",");
//				for(int j=0;j<arrOfStr.length;j++) System.out.println( j+" :"+arrOfStr[j]);//DEBUGGER
				
				if (arrOfStr[0].equals(str_TableName)) {
					positionOfTable = i;
					found = true;
				} else
					found=false;
				if (found) {
					fieldNames.add(arrOfStr[1]);  //Retrieve data info
					dataTypes.add(arrOfStr[2]);
					min.add(arrOfStr[5]);
					max.add(arrOfStr[6]);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
//		for(int j=0;j<arrOfStr.length;j++) System.out.println( j+" :"+arrOfStr[j]);
//		
//		System.out.println(fieldNames.size());
//		System.out.println(dataTypes); 
//		System.out.println(min);       
//		System.out.println(max);       
		
		
		
		
		
		Enumeration<String> enumeration = htbl_ColNameValue.keys();
		Object rawData;
		boolean b=false;
		boolean columnExists=false;
		
		
		while (enumeration.hasMoreElements()) {
			String columnName = enumeration.nextElement();
			for (int i = 0; i < fieldNames.size(); i++) {
				if (columnName.equals(fieldNames.get(i))) {
					rawData = htbl_ColNameValue.get(columnName); // (String) htbl_ColNameValue.get(columnName);
					columnExists=true;}
				else 
					continue;
				if (dataTypes.get(i).equals("java.lang.Integer")) {
						int data;
						try {
							data =(Integer)rawData;
						} catch (Exception e) {
							throw new DBAppException("Wrong input, please enter a value for " + columnName + " of type integer");
						}
						if (data > Integer.parseInt(min.get(i)) && data < Integer.parseInt(max.get(i)))
								;//tuple.record.add(i, rawData.toString());
						else {
							throw new DBAppException("Wrong input, please enter a value for " + columnName + " between "
									+ min.get(i) + " and " + max.get(i));
						}
					} else if (dataTypes.get(i).equals("java.lang.Double")) {
						double data;
						try {
							data = (Double)rawData;
						} catch (Exception e) {
							System.out.println("Wrong input, please enter a value for " + columnName + " of type double");
							throw new DBAppException();
						}
						if (data > Double.parseDouble(min.get(i)) && data < Double.parseDouble(max.get(i)))
								;//tuple.record.add(i, rawData.toString());
						else {
							System.out.println("Wrong input, please enter a value for " + columnName + " between "
									+ min.get(i) + " and " + max.get(i));
							throw new DBAppException();
						}
					} else if (dataTypes.get(i).equals("java.util.Date")) {
						Date data;
						SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
						try {
							data = (Date)rawData;
						} catch (Exception e) {
							throw new DBAppException("Wrong input, please enter a value for " + columnName
									+ " of type date in the format " + '"' + "yyy-MM-dd" + '"');
						}
						try {
							if (data.after(formatter.parse(min.get(i))) && data.before(formatter.parse(max.get(i))))
									;//tuple.record.add(i, rawData.toString());
							else {
								throw new DBAppException("Wrong input, please enter a value between " + columnName + " between "
										+ min.get(i) + " and " + max.get(i));
							}
						} catch (Exception e) {
							e.printStackTrace();
							throw new DBAppException();
						}
					} 
					//CHECKING BOUNDARY MIN &MAX STILL NOT IMPLEMENTED CORRECTLY
//					else if ( rawData.valueOf(max.get(i)) >0 && ((String)rawData).compareTo(max.get(i)) < 0) //rawData.equals(min.get(i))
//						tuple.record.add(i, rawData.toString());
//					else {
//						throw new DBAppException("Wrong input, please enter a value for " + columnName + " between "
//								+ min.get(i) + " and " + max.get(i));
//					}
				}
//				if(!columnExists)
//					throw new DBAppException("Input Data invalid!"+columnName+" is not a field!");
//				columnExists=false;
			
				}  
			b=true;
			return b;
    	}

    public void updateTable(String str_TableName,
    		String str_ClusteringKeyValue,
    		Hashtable<String,Object> htbl_ColNameValue
    		)
    		throws DBAppException{
    	
    }
    public void deleteFromTable(String str_TableName,
    		Hashtable<String,Object> htbl_ColNameValue)
    		throws DBAppException{
    	
    }
    public void createIndex(String
    		str_TableName,
    		String[] str_arrColName) throws DBAppException{}
    public Iterator selectFromTable(SQLTerm[] arr_SQLTerms,
    		String[] str_arrOperators)
    		throws DBAppException{
    	//queries have to be more than operators
    	if(arr_SQLTerms.length<=str_arrOperators.length) {
    		System.out.println("queries have to be more than operators!");
    		throw new DBAppException();
    	}
    	
    	Hashtable<Integer,String> ht=new Hashtable<Integer,String>(); 
    	return (Iterator)ht;
    	
    }
    
    public static void main(String[] args) throws IOException, DBAppException 
    {
  	String strTableName = "Student";
    	DBApp dbApp = new DBApp( );
    	dbApp.init();
//    	Hashtable htblColNameType = new Hashtable( );
//    	htblColNameType.put("id", "java.lang.Integer");
//    	htblColNameType.put("name", "java.lang.String");
//    	htblColNameType.put("gpa", "java.lang.double");
//    	
//    	Hashtable htblColNameMin = new Hashtable( );
//    	htblColNameMin.put("id", "0");
//    	htblColNameMin.put("name", "A");
//    	htblColNameMin.put("gpa", "6.0");
//    	
//    	Hashtable htblColNameMax = new Hashtable( );
//    	htblColNameMax.put("id", "10000");
//    	htblColNameMax.put("name", "ZZZZZZZZZZZ");
//    	htblColNameMax.put("gpa", "0.7");
//    	
//    	dbApp.createTable( strTableName, "id", htblColNameType,htblColNameMin,htblColNameMax);
//    	
//    	Hashtable htbl_values = new Hashtable( );
//    	htbl_values.put("id", 3);
//    	htbl_values.put("name", "Samir");
//    	htbl_values.put("gpa", 4);
    	
//   	dbApp.insertIntoTable("Student",htbl_values);
    	Object obj=dbApp.deserialize("/home/husseljo/Desktop/DB2Project/src/main/resources/data/Student/page0.class");
    	System.out.println(obj);
    	Page arr=(Page)obj;
    	for (int i = 0; i < arr.tuples.size(); i++) {
			System.out.println(arr.tuples.get(i).record.toString());
		}
    	
    	
    }
    
}
