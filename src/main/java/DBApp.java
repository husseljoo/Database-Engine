import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;

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
        
        
        
        
        
        
        
        
        
    }

    public void createIndex(String
    		str__TableName,
    		String[] str__arrColName) throws DBAppException{}
    public void insertIntoTable(String str__TableName,
    		Hashtable<String,Object> htbl_ColNameValue)
    		throws DBAppException{}
    public void updateTable(String str__TableName,
    		String str__ClusteringKeyValue,
    		Hashtable<String,Object> htbl_ColNameValue
    		)
    		throws DBAppException{
    	
    }
    public void deleteFromTable(String str__TableName,
    		Hashtable<String,Object> htbl_ColNameValue)
    		throws DBAppException{
    	
    }
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
