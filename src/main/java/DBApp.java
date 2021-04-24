import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.io.File;

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
        while(enumeration.hasMoreElements()) 
        {
        	String columnName= enumeration.nextElement();
        	String columnType=htbl_ColNameType.get(columnName);
        	String clusterKey=Boolean.toString(columnName.equals(str_ClusteringKeyColumn)); 
        	String columnMin=htbl_ColNameMin.get(columnName);
        	String columnMax=htbl_ColNameMax.get(columnName);
        	
        	currLine+= str_TableName+","+columnName+","+columnType+","+clusterKey+","+"False,"+columnMin+","+columnMax+"\n";
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
        try {

            Path path =Paths.get("/home/husseljo/Desktop/DB2Project/src/main/resources/tables/"+str_TableName);
            Files.createDirectories(path);    
            
          } catch (Exception e) {
        	  e.printStackTrace();
            System.err.println("Failed to create directory!" + e.getMessage());

          }
        
        
        
        
        
        
        
    }


    public void insertIntoTable(String str_TableName,
    		Hashtable<String,Object> htbl_ColNameValue)
    		throws DBAppException{
    
    	Path path =Paths.get("/home/husseljo/Desktop/DB2Project/src/main/resources/tables/"+str_TableName);
    	if (!Files.exists(path))
    		throw new DBAppException(); 
    	else {
    		
    		String s=stringifyHT(str_TableName,htbl_ColNameValue);
    		
    	}    	
    }

    
    //returns string (comma seperated)representing record to be inserted in Page 
    // simply extracts values from HashTable 
    //however we have to check whether HT input matches table (same column names and number of columns)
    // if error exists return empty string and throw error in insertIntoTable method
    //also we have to check the type of every value to check whether values are correct
    // and maybe cast the Object to it (does not really matter as we return a string at the end of the day)
    
    //STILL NOT IMPLEMENTED!!
    public static String stringifyHT(String str_TableName,Hashtable<String,Object> htbl_ColNameValue) {
    	
    	Enumeration<String> enumeration = htbl_ColNameValue.keys();
    	String currLine = "";
        while(enumeration.hasMoreElements()) 
        {
        	String columnName= enumeration.nextElement();
        	Object columnValue=htbl_ColNameValue.get(columnName);
        	
        	
        	currLine+= columnValue+",";
        	 }

    	
    	return "";
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
    	Hashtable htblColNameType = new Hashtable( );
    	htblColNameType.put("id", "java.lang.Integer");
    	htblColNameType.put("name", "java.lang.String");
    	htblColNameType.put("gpa", "java.lang.double");
    	
    	Hashtable htblColNameMin = new Hashtable( );
    	htblColNameMin.put("id", "0");
    	htblColNameMin.put("name", "A");
    	htblColNameMin.put("gpa", "6.0");
    	
    	Hashtable htblColNameMax = new Hashtable( );
    	htblColNameMax.put("id", "10000");
    	htblColNameMax.put("name", "ZZZZZZZZZZZ");
    	htblColNameMax.put("gpa", "0.7");
    	
    	dbApp.createTable( strTableName, "id", htblColNameType,htblColNameMin,htblColNameMax);
    	
    	
    	
    }
    
}
