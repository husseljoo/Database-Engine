import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

public class DBApp implements DBAppInterface{
	static String pathOfFile="src/main/resources/data/";
	public void init()
	{
		FileWriter csvWriter;
		BufferedReader objReader = null;
		String strCurrentLine="";
		try {
			objReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			strCurrentLine = objReader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
			}
		
		if(strCurrentLine != null)
			return;
		String currLine ="Table Name, Column Name, Column Type, ClusteringKey, Indexed, min, max\n";
		try 
		{
			
			csvWriter = new FileWriter("src/main/resources/metadata.csv");
			csvWriter.append(currLine);
			csvWriter.flush();
			csvWriter.close();
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
    	
    	FileWriter fileWrite;
		try {
			fileWrite = new FileWriter("src/main/resources/metadata.csv",true);
		
    	BufferedWriter csvWrite=new BufferedWriter(fileWrite);
    	
    	if(str_ClusteringKeyColumn==null||str_TableName==null)
    		throw new DBAppException("Cannot leave a field empty");
    	validateExistence(htbl_ColNameType,str_ClusteringKeyColumn);
    	Enumeration<String> enumeration = htbl_ColNameType.keys();
    	String currLine = "";
    	String columnFormat=""; //FORMAT TO BE INSERTED IN CSV FILE OF TABLE FOR CONSISTENCY
    	Vector<String> columnFormatArr=new Vector();
    	int pos = 0;
    	int counter = 0;
    	String clusterKeyType="";
    	while(enumeration.hasMoreElements()) 
        {
        	String columnName= enumeration.nextElement();
        	String columnType=htbl_ColNameType.get(columnName);
        	String clusterKey=Boolean.toString(columnName.equals(str_ClusteringKeyColumn)); 
        	if(columnName.equals(str_ClusteringKeyColumn))
        		{
        		pos = counter;
        		clusterKeyType = columnType;
        		}
        	String columnMin=htbl_ColNameMin.get(columnName);
        	String columnMax=htbl_ColNameMax.get(columnName);
        	String min=columnMin;
        	String max=columnMax;
        	
        	if(columnName==null||columnType==null||columnMin==null||columnMin==null)
        		throw new DBAppException("Cannot leave a field empty");
        	
        	String typeLowerCase=columnType.toLowerCase();
        	if(!(typeLowerCase.equals("java.lang.integer")||typeLowerCase.equals("java.lang.double")||typeLowerCase.equals("java.util.date")||typeLowerCase.equals("java.lang.string")))
        		throw new DBAppException("Incorrect data type");
        	
        	if(columnType.toLowerCase().equals("java.lang.integer")) {
        		try {
        			Integer.parseInt(min);
        			Integer.parseInt(max);
        		}
        		catch(Exception e) {
        			throw new DBAppException("Incorrect minimum or maximum data type");
        		}
        	}
        	else if(columnType.toLowerCase().equals("java.lang.double")) {
        		try {
        			Double.parseDouble(min);
        			Double.parseDouble(max);
        		}
        		catch(Exception e) {
        			throw new DBAppException("Incorrect minimum or maximum data type");
        		}
        	}
        	else if(columnType.toLowerCase().equals("java.util.date")) {
            	try {
            		Date date1=new SimpleDateFormat("yyyy-MM-dd").parse(min);
            		Date date2=new SimpleDateFormat("yyyy-MM-dd").parse(max);
            	}
            	catch(Exception e) {
            		throw new DBAppException("Incorrect minimum or maximum data type");
            	}
        	}
        	
        	if(compareTo(columnMin,columnMax)>0)
        		throw new DBAppException("the minimum should be less than the maximum");
        	
        	//validateExistence(htbl_ColNameType,columnName);
        	
        	if(columnType.equals("java.lang.String")) 
        	{        		
        		min='"'+columnMin+'"';
        		max='"'+columnMax+'"';
        	}
        	currLine+= str_TableName+","+columnName+","+columnType+","+clusterKey+","+"False,"+min+","+max+"\n";
        	columnFormatArr.add(columnName);
        	try 
        	{
				csvWrite.append(currLine);
			} 
        	catch (IOException e) 
        	{
				e.printStackTrace();
			}
        	currLine = "";
        	counter++;
        }
        
        /*********/
        //Once we create a Table, we create a folder/directory with its name inside the tables folder
        //it will contain all the pages of the table as well as a table_name.csv that contains info about
        // its page files (.class or .ser) i.e key range of page,bit indicating whether its full or not etc.
        Path path =Paths.get(pathOfFile+str_TableName);
        try 
        {
            Files.createDirectories(path);
            
         } catch (Exception e) {
        	  e.printStackTrace();
         //   System.err.println("Failed to create directory!" + e.getMessage());

          }
        
        
        TableInfo tableInfo=new TableInfo();
        tableInfo.colOrder=columnFormatArr;
        tableInfo.clusteringKey=str_ClusteringKeyColumn;
        tableInfo.clusterKeyIndex = pos;
        tableInfo.clusterKeyType=clusterKeyType;
        serialize(tableInfo,path+"/"+"tableInfo.class");
        csvWrite.close();
		}
		catch(IOException e) {
			;
		}
    }
    public static void validateExistence(Hashtable<String,String> htbl, String columnName) throws DBAppException
    {
    	Enumeration<String> enumeration = htbl.keys();
    	int c=0;
    	while (enumeration.hasMoreElements()) {
    		String data=enumeration.nextElement();
    		if (data.equals(columnName))
    			c++;
    	}
    	if(c>1)
			throw new DBAppException("Column names need to be unique");
    	else if(c==0)
    		throw new DBAppException("Clustering key doesn't exist");
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
	        
	        }
	          
	    catch(IOException ex)
	        {
	        ex.printStackTrace();
	        //System.out.println("IOException is caught");
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
            
            
        }
          
        catch(Exception e)
        {
        	e.printStackTrace();
        //	System.out.println("IOException is caught");
        	
        }
    	return o;

    } 
    public void insertIntoTable(String str_TableName,
    		Hashtable<String,Object> htbl_ColNameValue)
    		throws DBAppException{
    
    			checkValidity(str_TableName,htbl_ColNameValue);
    			String path=pathOfFile+str_TableName+"/tableInfo.class";
    			TableInfo tableInfo=(TableInfo)deserialize(path);
    			Tuple tuple=new Tuple();
    			String clusteringKey=tableInfo.clusteringKey;
    			
    			for(int i=0;i<tableInfo.colOrder.size();i++) 
    				tuple.record.add(htbl_ColNameValue.get(tableInfo.colOrder.get(i)));
    			
    			Object keyValue=htbl_ColNameValue.get(clusteringKey);
    			Object[] tupleRef = new Object[2];
    			
    			boolean indexOnPrimary = isIndexOnPrimaryExists(tableInfo);
    			DDVector index = (DDVector)deserialize("src/main/resources/data/Student/id/index.class");
    			boolean hasIndex = false;
    			if(index==null)
    				hasIndex = false;
    			if(!hasIndex)
    			{
    				if(tableInfo.pages.size()==0)
        			{
        				Page page = new Page();
        				page.insert(tuple);
        				Object[] pageInfo = {"1",keyValue};
        				tupleRef[0]="1";
        				tupleRef[1]=0;
        				tableInfo.pages.add(pageInfo);
        				tableInfo.nonOverflowPageNum++;
        				tableInfo.numOfPages++;
        				String pathOfPage = pathOfFile+str_TableName+"/1.class";
        				serialize(page,pathOfPage);
        				serialize(tableInfo,path);
        			}
        			else
        			{
        				int position=0;
        				for(int i=0;i<tableInfo.pages.size();i++) 
        				{
        					if((compareTo(keyValue,tableInfo.pages.get(i)[1])<0)||(i==tableInfo.pages.size()-1 && compareTo(keyValue,tableInfo.pages.get(i)[1])>0))
        						{
        						position=i;
        						break;
        						}
        				}
        				
        				String pathOfPage = pathOfFile+str_TableName+"/"+tableInfo.pages.get(position)[0].toString() +".class";
        				Page page=(Page) deserialize(pathOfPage);
        				Page pageToUpdate = null;
        				if (!page.isFull()) 
        					{
        						tupleRef[0] = tableInfo.pages.get(position)[0].toString();
        						tupleRef[1]= add(tuple, page,false,tableInfo.clusterKeyIndex,tableInfo,position);
        						serialize(page,pathOfPage);
        						serialize(tableInfo,path);
        					}
        					else 
        					{
        						tupleRef = createNewPage(tuple,page,tableInfo,tableInfo.pages.get(position)[0].toString(),keyValue,str_TableName,position);
        						
        						serialize(page,pathOfPage);
        						serialize(tableInfo,path);
        					}
        				}
        			
        			
        			//get best index this table has, and best for this tuple
    				
        			//temp
    				
        			if(true)
        			{
        				Vector<String> indexedCol = tableInfo.indices.get(0).indexedCol;
            			
            			
            			Object[][] minmax=getMinMax(str_TableName, indexedCol);
            			int[] colNum = new int[indexedCol.size()]; 
            	    	for(int i = 0;i<indexedCol.size();i++)
            	    		colNum[i]=tableInfo.colOrder.indexOf(indexedCol.get(i));
            	    	
            	    	int[] cell = new int[colNum.length];
            	    	for(int k = 0;k<colNum.length;k++)
            	    		cell[k] = findDiv(minmax[k][0],minmax[k][1],minmax[k][2],tuple.record.get(colNum[k]));
            	    	
            	    	
            	    	Vector bucketInCell = index.getFromDimensions(cell);
            	   		if(bucketInCell.size()==0)
            	    	{
            	    		Bucket bucket = new Bucket();
            	    		bucket.insert(tupleRef);
            	    		String pathOfF = pathOfFile+str_TableName+"/id/"+index.bucketNumber+".class";
            	    		serialize(bucket, pathOfF);
            	    		Object[] bucketInfo = new Object[2];
            	    		bucketInfo[0] = index.bucketNumber+"";
            	    		bucketInfo[1] = false;
            	    		index.insertAtDimensions(cell, bucketInfo);
            	    	}
            	    	else
            	    	{
            	    		boolean noFoundBucket = true;
            	    		for(int k =0;k<bucketInCell.size();k++)
            	    		{
            	    			Object[] bucketInfo = (Object[]) bucketInCell.get(k);
            	    			if(bucketInfo[1].equals(false))
            	    			{
            	    				String pathOfBucket = pathOfFile+str_TableName+"/id/"+"/"+bucketInfo[0]+".class";
            	    				Bucket bucket = (Bucket)deserialize(pathOfBucket);
            	    				bucket.insert(tupleRef);
            	    				boolean isFull = bucket.isFull();
            	    				serialize(bucket, pathOfBucket);
            	    				bucketInfo[1] = isFull;
            	    				noFoundBucket = false;
            	    			}
            	    		}
            	    		if(noFoundBucket)
            	    		{
            	    			Bucket bucket = new Bucket();
            	    			bucket.insert(tupleRef);
            	    			String pathOfF = pathOfFile+str_TableName+"/id/"+index.bucketNumber+".class";
                	    		serialize(bucket, pathOfF);
            	    			Object[] bucketInfo = new Object[2];
    	        	        	bucketInfo[0] = index.bucketNumber+"";
    	        	        	bucketInfo[1] = false;
    	        	        	index.insertAtDimensions(cell, bucketInfo);
            	    		}
            	    	}
            	   		serialize(index,"src/main/resources/data/Student/id/index.class");
        			}
    			}
    			else //index on primary exists
    			{
    				
    			}
   }
    public static boolean isIndexOnPrimaryExists(TableInfo tableInfo)
    {
    	for(int i =0;i<tableInfo.indices.size();i++)
    	{
    		for(int j =0;j<tableInfo.indices.get(i).indexedCol.size();j++)
    		{
    			if(tableInfo.clusteringKey.equals(tableInfo.indices.get(i).indexedCol.get(j)))
    				return true;
    		}
    	}
    	return false;
    }
    public static IndexInfo getBestIndex()
    {
    	return null;
    }
    public static Object[] createNewPage(Tuple tuple, Page page, TableInfo tableInfo, String pageName, Object keyValue, String str_TableName, int posOfCurrentPage)
    {
    	//no next page
    	Object[] res = new Object[2];
     	if(tableInfo.pages.lastElement()[0].toString().equals(pageName))
    	{
    		if(compareTo(tableInfo.pages.lastElement()[1].toString(),keyValue.toString())>0)
    		{
    			
    			//last elem akbar, put in new page
    			Page newPage = new Page();
    			newPage.insert(page.tuples.lastElement());
    			res[0] = tableInfo.numOfPages+"";
    			tableInfo.numOfPages++;
    			Object[] pageInfo = {tableInfo.numOfPages+"",tableInfo.pages.get(tableInfo.pages.size()-1)[1]};//esmaha maynfa3sh yeb2a .size, what if feeh 0,0_A el mfrood 1 msh 2
    			tableInfo.pages.add(pageInfo);
    			tableInfo.nonOverflowPageNum++;
    			
    			String pathOfPage = pathOfFile+str_TableName+"/"+pageInfo[0].toString() +".class";
    			serialize(newPage,pathOfPage);

    			res[1]=add(tuple,page,true,tableInfo.clusterKeyIndex,tableInfo,posOfCurrentPage);
    			
    		}
    		else
    		{
    			//tuple akbar
    			Page newPage = new Page();
    			newPage.insert(tuple);
    			tableInfo.numOfPages++;
    			Object[] pageInfo = {tableInfo.numOfPages+"",keyValue};
    			res[0] = pageInfo[0];
    			res[1]= 0;
    			tableInfo.pages.add(pageInfo);
    			tableInfo.nonOverflowPageNum++;
    			
    			String pathOfPage = pathOfFile+str_TableName+"/"+pageInfo[0].toString() +".class";
    			serialize(newPage,pathOfPage);
    		}
    			
    	}
    	else
    	{
    		int positionOfNextPage = posOfCurrentPage+1;
    		String pathOfPage = pathOfFile+str_TableName+"/"+tableInfo.pages.get(positionOfNextPage)[0].toString() +".class";
			Page nextPage=(Page) deserialize(pathOfPage);
    		//next page is full
    		if(nextPage.isFull())
    		{
    			Page overFlow = new Page();
    			overFlow.insert(page.tuples.lastElement());
    			tableInfo.numOfPages++;
    			Object[] pageInfo = {tableInfo.numOfPages+"",tableInfo.pages.get(posOfCurrentPage)[1]};
    			String path = pathOfFile+str_TableName+"/"+tableInfo.numOfPages +".class";
    			res[0]=tableInfo.numOfPages+"";
    			tableInfo.pages.insertElementAt(pageInfo,positionOfNextPage);
    			serialize(overFlow,path);
    			res[1]=add(tuple,page,true,tableInfo.clusterKeyIndex,tableInfo,posOfCurrentPage);
    		}
    		//next page is not full
    		else
    		{
    			res[0] = tableInfo.pages.get(posOfCurrentPage)[0].toString();
    			add(page.tuples.lastElement(),nextPage,false,tableInfo.clusterKeyIndex,tableInfo,posOfCurrentPage+1);
    			res[1]=add(tuple,page,true,tableInfo.clusterKeyIndex,tableInfo,posOfCurrentPage);
    			
    		}
    		serialize(nextPage,pathOfPage);
    		
    	}
     	return res;
    }
    public static int compareTo(Object s1, Object s2)
    {
    	if(s1 instanceof Date && s2 instanceof Date)
    	{
    		Date d1 = (Date)s1;
    		Date d2 = (Date)s2;
    		return d1.compareTo(d2);
    	}
    	try
    	{
    		return Integer.compare(Integer.parseInt(s1.toString()), Integer.parseInt(s2.toString()));
    	}
    	catch(Exception e)
    	{
    		try
    		{
    			return Double.compare(Double.parseDouble(s1.toString()), Double.parseDouble(s2.toString()));
    		}
    		catch(Exception e1)
    		{
    			try
    			{
    			SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd");
    			Date date1 = (Date)s1;
    		    Date date2 = (Date)s2;
    		    format.format(date1);
    		    format.format(date2);
    			return date1.compareTo(date2);
    			}
    			catch(Exception e2)
    			{
    				return s1.toString().compareTo(s2.toString());
    			}
    		}
    	}
    }
    public static int add(Tuple tuple,Page page, boolean isTruncate,int pos,TableInfo tableInfo,int posOfCurrentPage) 
    {
    	Comparator<Tuple> c = new Comparator<Tuple>() {
            public int compareReal(Tuple t1,Tuple t2, int pos)
            {
                return compareTo(t1.record.get(pos),t2.record.get(pos));
            }

			public int compare(Tuple t1, Tuple t2) {
				return compareReal(t1,t2,pos);
			}
        };
        if(isTruncate)
   		 page.tuples.remove(page.tuples.size()-1);
    	int position=Math.abs(-(Collections.binarySearch(page.tuples, tuple,c))-1);
    	page.tuples.insertElementAt(tuple, position);
    	tableInfo.pages.get(posOfCurrentPage)[1] = page.tuples.lastElement().record.get(pos);
    	return position;
    }
    public static void checkValidity(String str_TableName,Hashtable<String,Object> htbl_ColNameValue) throws DBAppException{
    	
    	Path path =Paths.get(pathOfFile+str_TableName);
    	if (!Files.exists(path)) {
    		throw new DBAppException("Specified Table does not exist");
    		}
    	
    	BufferedReader objReader = null;
		String strCurrentLine="";
		try {
			objReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			strCurrentLine = objReader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
			}

		String[] arrOfStr = new String[0];
		boolean found = false;
		ArrayList<String> fieldNames = new ArrayList();
		ArrayList<String> dataTypes = new ArrayList();
		ArrayList<String> min = new ArrayList();
		ArrayList<String> max = new ArrayList();
		try {
			for (int i = 0; (strCurrentLine = objReader.readLine()) != null; i++) {
				arrOfStr = strCurrentLine.split(",");
				
				if (arrOfStr[0].equals(str_TableName)) {
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
		
		Enumeration<String> enumeration = htbl_ColNameValue.keys();
		Object rawData;
		boolean columnExists=false;
		
		while (enumeration.hasMoreElements()) {
			String columnName = enumeration.nextElement();
			for (int i = 0; i < fieldNames.size(); i++) {
				if (columnName.equals(fieldNames.get(i))) {
					rawData = htbl_ColNameValue.get(columnName); // (String) htbl_ColNameValue.get(columnName);
					columnExists=true;
					}
				else 
					continue;
				if (dataTypes.get(i).toLowerCase().equals("java.lang.integer")) {
						int data;
						try {
							data =Integer.parseInt(rawData.toString());
						} catch (Exception e) {
							throw new DBAppException("Wrong input, please enter a value for " + columnName + " of type integer");
						}
						if (data >= Integer.parseInt(min.get(i)) && data <= Integer.parseInt(max.get(i)))
								;
						else {
							throw new DBAppException("Wrong input, please enter a value for " + columnName + " between "
									+ min.get(i) + " and " + max.get(i));
						}
					} else if (dataTypes.get(i).toLowerCase().equals("java.lang.double")) {
						double data;
						try {
							data = Double.parseDouble(rawData.toString());
						} catch (Exception e) {
							throw new DBAppException("Wrong input, please enter a value for " + columnName + " of type double");
						}
						if (data >= Double.parseDouble(min.get(i)) && data <= Double.parseDouble(max.get(i)))
								;
						else {
							throw new DBAppException("Wrong input, please enter a value for " + columnName + " between "
									+ min.get(i) + " and " + max.get(i));
						}
					} else if (dataTypes.get(i).toLowerCase().equals("java.util.date")) {
						
						try { 
							SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd");
							format.format(rawData);
							
						} catch (Exception e) {
							throw new DBAppException("Wrong input, please enter a value for " + columnName
									+ " of type date in the format " + '"' + "yyyy-MM-dd" + '"');
						}
						try {
							Date minLimit = new SimpleDateFormat("yyyy-MM-dd").parse(min.get(i));
							Date maxLimit = new SimpleDateFormat("yyyy-MM-dd").parse(max.get(i));
							
							if ((((Date) rawData).after(minLimit)) || rawData.equals(minLimit) && (((Date) rawData).before(maxLimit) || rawData.equals(maxLimit)))
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
					else 
					{	
						
						String minLimit = min.get(i).substring(1, min.get(i).length()-1).toLowerCase();
						String maxLimit = max.get(i).substring(1, max.get(i).length()-1).toLowerCase();
						if ( (rawData.toString().toLowerCase().compareTo(minLimit) < 0 || rawData.toString().toLowerCase().compareTo(maxLimit) > 0)) //rawData.equals(min.get(i)
							throw new DBAppException("Wrong input, please enter a value for " + columnName + " between "
								+ min.get(i) + " and " + max.get(i));
					}
				}
				if(!columnExists)
					throw new DBAppException("Input Data invalid! "+columnName+" is not a field!");
				columnExists=false;
				
			
				}  
			
    	}
    public void updateTable(String str_TableName,
    		String str_ClusteringKeyValue,
    		Hashtable<String,Object> htbl_ColNameValue
    		)
    		throws DBAppException{
    	
    	Path pathTable =Paths.get(pathOfFile+str_TableName);
    	if (!Files.exists(pathTable))
    		throw new DBAppException("Specified Table does not exist at all!");
    		
    	
    	String path=pathOfFile+str_TableName+"/tableInfo.class";
		TableInfo tableInfo=(TableInfo)deserialize(path);;
		String clusteringKey=tableInfo.clusteringKey;
		
		if( htbl_ColNameValue.size() != (tableInfo.colOrder.size()-1))
			throw new DBAppException("More Hashtable Keys than expected!");
		
		for(int i=0;i<tableInfo.colOrder.size();i++) 
		{
			if(!tableInfo.colOrder.get(i).equals(clusteringKey)){
				if(!htbl_ColNameValue.containsKey(tableInfo.colOrder.get(i)))
					throw new DBAppException("Wrong Hashtable Keys!");
				}
		}
		//if this code is reached then Hashtable contains correct column name values ---> check validity(type and range)
		
		BufferedReader objReader = null;
		String strCurrentLine;
		try {
			objReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			strCurrentLine = objReader.readLine(); //read first Header line (skip it to read data later)
		} catch (IOException e) {e.printStackTrace();}
		
		String[] arrOfStr=new String[7];
		String str_CurrentLine;
		int positionOfTable=0;
		
		String clusteringColumnType="";
		
		try {
			for (int i = 0; (str_CurrentLine = objReader.readLine()) != null; i++) {
				arrOfStr = str_CurrentLine.split(",");
				
				if (arrOfStr[0].equals(str_TableName)) {
					break; //start of Table found in metadata file(prevent linearly searching for every column)
					}
				}
			//System.out.println(str_CurrentLine);//DEBUGGER
			
			 
			
			String currentColumnValue;
			String columnType;
			String columnMinimum;
			String columnMaximum;
			
			//check whether each (by now determined to be valid)key column 
			
			if(arrOfStr[1].equals(clusteringKey)) {//or arrOfStr[3]==true and parse it to Boolean clustering key in metadata
				currentColumnValue=str_ClusteringKeyValue;
				clusteringColumnType=arrOfStr[2];
			}
			else 
				currentColumnValue=htbl_ColNameValue.get(arrOfStr[1])+""; //gets value of current column
			
			columnType=arrOfStr[2];
			columnMinimum=arrOfStr[5];
			columnMaximum=arrOfStr[6];
			checkValidUpdated(currentColumnValue,columnType,columnMinimum,columnMaximum);
			
			
			
			while((str_CurrentLine = objReader.readLine()) != null ) {
					if(str_CurrentLine==null)
						break;
					
					arrOfStr = str_CurrentLine.split(",");
					if (arrOfStr[0].equals(str_TableName)) {
					//	System.out.println(str_CurrentLine);//DEBUGGER
						if(arrOfStr[1].equals(clusteringKey)) {
							currentColumnValue=str_ClusteringKeyValue;
							clusteringColumnType=arrOfStr[2];}
						else 
							currentColumnValue=htbl_ColNameValue.get(arrOfStr[1])+""; //gets value of current column
						
						columnType=arrOfStr[2];
						columnMinimum=arrOfStr[5];
						columnMaximum=arrOfStr[6];
						checkValidUpdated(currentColumnValue,columnType,columnMinimum,columnMaximum);
						}
					else
						break;
				
			}
		}
		catch (IOException e) {
			e.printStackTrace();
			}
		
//		Now check whether str_ClusteringKeyValue exists at all in any page
		
		
		

		Tuple tuple=new Tuple();
		
		//create tuple to be inserted instead of existing tuple (updating it)
		for(int i=0;i<tableInfo.colOrder.size();i++) 
		{	
			if(tableInfo.colOrder.get(i).equals(clusteringKey) && tableInfo.clusterKeyType.toLowerCase().equals("java.util.date"))
				{
					int year = Integer.parseInt(str_ClusteringKeyValue.substring(0, 4));
					int month = Integer.parseInt(str_ClusteringKeyValue.substring(5, 7));
					int day = Integer.parseInt(str_ClusteringKeyValue.substring(8));
					
					Date date = new Date(year-1900,month-1,day);
					tuple.record.add(date);
				}
			else if(tableInfo.colOrder.get(i).equals(clusteringKey))
				tuple.record.add(str_ClusteringKeyValue);
			else
				tuple.record.add(htbl_ColNameValue.get(tableInfo.colOrder.get(i)));
			}
		
		
		//get position of the page that contains tuple to be updated
		int pagePosition=-1;
		for(int i=0;i<tableInfo.pages.size();i++) 
		{
			if((compareTo(tuple.record.get(tableInfo.clusterKeyIndex),tableInfo.pages.get(i)[1])<=0))
				{ 
				pagePosition=i;
				break;
				}
		}
		if(pagePosition==-1)
			throw new DBAppException("str_ClusteringKeyValue does not exist at all(too big)!"); 
		
		String pageName=(String)tableInfo.pages.get(pagePosition)[0];
		
		if(pagePosition==-1)throw new DBAppException("no place");//should be redundant as we have a checker to be implemented above
		
		
		Comparator<Tuple> c = new Comparator<Tuple>() {
            public int compareReal(Tuple t1,Tuple t2, int pos)
            {
                return compareTo(t1.record.get(pos),t2.record.get(pos));
            }

			public int compare(Tuple t1, Tuple t2) {
				return compareReal(t1,t2,tableInfo.clusterKeyIndex);
			}
        };
        String pagePath=pathOfFile+str_TableName+"/"+pageName+".class";
        Page page=(Page)deserialize(pagePath);
        ;
        //get position of tuple to be updated
        int tuplePosition=Math.abs(-(Collections.binarySearch(page.tuples, tuple,c))-1)-1;
        if(clusteringColumnType.equals("java.util.String"))
        	tuplePosition+=1;
		
      //checks if the tuple we matched is actually the one with the str_ClusteringKeyValue
      //meaning if the to be updated tuple exists at all
        
        Object actualFoundTuple=page.tuples.get(tuplePosition).record.get(tableInfo.clusterKeyIndex);
        boolean clusteringKeyValueDoesExist=sameValue(str_ClusteringKeyValue,actualFoundTuple,clusteringColumnType);
        
        if(!clusteringKeyValueDoesExist){ 
        	
        //	System.out.println(page.tuples.get(tuplePosition).record.get(tableInfo.clusterKeyIndex).getClass());//DEBUGGER
        	//System.out.println("input ClusterinKeyValue -->"+str_ClusteringKeyValue.getClass());//DEBUGGER
        	
        	throw new DBAppException("The ClusteringKeyValue you entered does not exist!"); 
        }
        
        
        
        
	//	System.out.println("Page position is: "+pagePosition); //DEBUGGER
	//	System.out.println("Tuple position is: "+tuplePosition);//DEBUGGER
		
		page.tuples.set(tuplePosition,tuple);
		serialize(page,pagePath);
		
		
		
    }
    
    
    public static void removeFromIndexes(Vector<Tuple> tuplesToDelete,String str_TableName,Vector<IndexInfo> Indexes,int clusterKeyIndex,String ignoreIndexName){
    	
    	//It is for the same table
    	//deleting from indexes only, already has been deleted from table
    	
    	ArrayList<String> deserializedPagesNames=new ArrayList<>();
		ArrayList<Object[]> deserializedPagesObj=new ArrayList<>();
    	
    	for(IndexInfo Index:Indexes) {
    		String currentIndexName=Index.indexName;
    		if(currentIndexName.equals(ignoreIndexName))
    			continue;
    		Vector<Integer> colNum=Index.colNum; //postition of columns index is built on
    		Vector<String> indexedCol=Index.indexedCol;
    		Vector<Object> indexColValues=new Vector<>();
    		Object[][] minmax=Index.minmax;
    		
    		String pathOfGrid =pathOfFile+str_TableName+"/"+currentIndexName;
			String gridFileName=new File(pathOfGrid).listFiles()[0].getName(); //because we do not store the .class file name of the DDVector
			pathOfGrid=pathOfGrid+"/"+gridFileName+".class";
			
			DDVector Grid=(DDVector)deserialize(pathOfGrid);
    		
		
			
			
			
    		for(Tuple tuple: tuplesToDelete) {
    			Object bucketCell=Grid;
    			Vector<Object> tupleRecords=tuple.record;
    			
    			for(int i=0;i<colNum.size();i++) {
        			indexColValues.add(tupleRecords.get(colNum.get(i)));} 
    			
    			List<Integer> bucketCellPosition=new ArrayList<Integer>();
    			
    			for (int i=0;i<indexColValues.size();i++) {
    				int min=(Integer)minmax[i][0];
    				int max=(Integer)minmax[i][1];
    				String type=(String)minmax[i][2];
    				
    				//narrowing down till the last cell
    				int position=findDiv(min, max, type, indexColValues.get(i));
    				bucketCellPosition.add(position);
    				bucketCell=((DDVector)bucketCell).array.get(position);
    				
    				}
    			
    			Vector<Object[]> vectorOfBuckets=(Vector)bucketCell;
    			Vector<Object[]> vectorOfBucketsNew=new Vector<>();
//    			for(Object[] bucketObj : vectorOfBuckets) {
    			for(int i=0;i<vectorOfBuckets.size();i++) {
//    				String bucketName=(String)bucketObj[0];
    				String bucketName=(String)vectorOfBuckets.get(i)[0];
					//deserialize the bucket itself
					String pathOfBucket=pathOfFile+str_TableName+currentIndexName+"/"+bucketName+".class";
					Bucket bucket=(Bucket)deserialize(pathOfBucket); 
					
					Vector<Object[]> vectorOftupleRef=bucket.tupleReferences;
				    
					tupleOuter:for(Object[] tupleRef : vectorOftupleRef) {
				    	String pageName=(String) tupleRef[0]; //maybe its an Integer(due to the naming convention)?
				    	int rowNumber=(Integer) tupleRef[1];
				    	
				    	//in order to not keep serializing and deserializing the same page
				    	//in a short period of time
				    	Page page=null;
				    	
				    	
				    	if(!deserializedPagesNames.contains(pageName)) {
				    		//serialize the page
				    		String pathOfPage=pathOfFile+str_TableName+"/"+pageName+".class";
				    		page=(Page)deserialize(pathOfPage);
				    		//cache the deserialized page info
				    		deserializedPagesObj.add(new Object[]{pathOfPage,page});
				    		deserializedPagesNames.add(pageName);
				    		
				    		
				    	
				    	}
				    	else {
				    		//use already deserialized page
				    		for(Object[] deserializedPage: deserializedPagesObj) {
				    			String pathOfPage=(String)deserializedPage[0];
								if(pathOfPage.endsWith(pageName+".class"))
									page=(Page)deserializedPage[1];
									break;
							}
				    		
				    	}
				    
				    	Tuple tupleInBucket=page.tuples.get(rowNumber);
			    		Vector<Object> tupleRecord=tupleInBucket.record;
			    		
			    		for(int j=0;i<page.tuples.size();j++) {
				    		if(tupleRecord.get(clusterKeyIndex).equals(tuple.record.get(clusterKeyIndex))) {
				    			page.tuples.remove(j);
				    			//GRid has to be updated as well
				    			continue tupleOuter;}
				    		
				    	}
    		
			    		
			    		
    		
    	}}}}
    	
    	//serialize all pages and grids at the end
		//------
    	for(Object[] deserializedPage: deserializedPagesObj) {
			String pathOfPage=(String)deserializedPage[0];
			Page page=(Page)deserializedPage[1];
			serialize(page,pathOfPage);
		}
    	
    }
    
    
    
    
    
    public void deleteFromTable(String str_TableName,
    		Hashtable<String,Object> htbl_ColNameValue)
    		throws DBAppException
    {
    	
    	Path pathTable =Paths.get(pathOfFile+str_TableName);
    	if (!Files.exists(pathTable))
    		throw new DBAppException("Specified Table does not exist");
    	
    	String path=pathOfFile+str_TableName+"/tableInfo.class";
		TableInfo tableInfo=(TableInfo)deserialize(path);
		String[] columnName=new String[htbl_ColNameValue.size()];
		Enumeration<String> enumeration = htbl_ColNameValue.keys();
		Object[] values=new Object[htbl_ColNameValue.size()];
		
		//get values from the Hashtable
		for(int i=0;i<htbl_ColNameValue.size();i++) {
			columnName[i]=enumeration.nextElement();
			values[i]=htbl_ColNameValue.get(columnName[i]);
		}
		for(int i=0;i<columnName.length;i++) {
			if(!(tableInfo.colOrder.contains(columnName[i])))
					throw new DBAppException(columnName[i]+" doesn't exist");
		}
		  
		Hashtable<String,Object> hash = new Hashtable<String, Object>();
		for(int i=0;i<columnName.length;i++) {
			hash.put(columnName[i], values[i]);
		}
		checkValidity(str_TableName,hash);
		
		
		IndexInfo bestIndex=getBestIndex(); //returns null if no good index exists (partial or full)
		
		if(bestIndex!=null) {
			
			String indexName=bestIndex.indexName;
			Vector<String> indexedCol=bestIndex.indexedCol;
		
			Path pathOfGrid =Paths.get(pathOfFile+str_TableName+"/"+indexName);
			String str_pathOfGrid=pathOfGrid.toString();
			String gridFileName=new File(str_pathOfGrid).listFiles()[0].getName(); //because we do not store the .class file name of the DDVector
			str_pathOfGrid=str_pathOfGrid+"/"+gridFileName+".class";
			
			DDVector Grid=(DDVector)deserialize(str_pathOfGrid);
			
			List<String> queryColumns = new ArrayList<>(Arrays.asList(columnName));
			
//			columnName; //name of columns in query
//			values; //their values
			
			List<Integer> coordinates=new ArrayList<>();
			
			//determine which grid cells I will use
			//indexedCol is Vector of positions of columns index is built on
			for (int i=0;i<indexedCol.size();i++) {
				boolean b=queryColumns.contains(indexedCol.get(i));
				if(b)
					coordinates.add(i);
				else
					coordinates.add(-1); //meaning it is a partial query all 10 intervals have to be scanned
					}
			
			
//			class Local{
//				public void deleteTuples(int size,ArrayList<Integer> listOfcoordinatesOfTheSpecificBucket) {
//					if(size==0)
//						return;
//					else {
//						//delete the tuples in the index , get the reference and delete in table and then all other indexes
//						for(int i;i<;i++) {}
//					}}}
			
			boolean partial=false;
			if(coordinates.contains(-1))
				partial=true;
			
				Object[][] minmax=bestIndex.minmax;
				Vector<Integer> colNum=bestIndex.colNum;
			
			if(!partial) {
				//delete the tuples in the cell directly
				//it is going to be one cell only because it is a full query
				// and because delete does not delete with ranges but only AND (arguments stored in a Hashtable)
				Object bucketCell=Grid;
				Vector<Tuple> tuplesToDelete=new Vector<>();
				
				for (int i=0;i<colNum.size();i++) {
					int min=(Integer)minmax[i][0];
					int max=(Integer)minmax[i][1];
					String type=(String)minmax[i][2];
					
					//narrowing down till the last cell
					int position=findDiv(min, max, type, indexedCol.get(i));
					bucketCell=((DDVector)bucketCell).array.get(position);
					
					}
					
					if(!(bucketCell instanceof Vector))
						return;//something is wrong if it returns
					
				
//					String vectorOfBuckets=(String)bucketCell.get(0);
					
						Vector<Object[]> vectorOfBuckets=(Vector)bucketCell;
						ArrayList<String> deserializedPagesNames=new ArrayList<>();
						ArrayList<Object[]> deserializedPagesObj=new ArrayList<>();
						
						for(Object[] bucketObj : vectorOfBuckets) {
							
							//if((Boolean)bucketObj[1]) continue;//True means Bucket is Full so skip it 
								
							
							String bucketName=(String)bucketObj[0];
							//deserialize the bucket itself
							String pathOfBucket=pathOfFile+str_TableName+indexName+"/"+bucketName+".class";
							Bucket bucket=(Bucket)deserialize(pathOfBucket); //the one bu
							
							Vector<Object[]> vectorOftupleRef=bucket.tupleReferences;
						    
							tupleOuter:for(Object[] tupleRef : vectorOftupleRef) {
						    	String pageName=(String) tupleRef[0]; //maybe its an Integer(due to the naming convention)?
						    	int rowNumber=(Integer) tupleRef[1];
						    	
						    	//in order to not keep serializing and deserializing the same page
						    	//in a short period of time
						    	Page page=null;
						    	
						    	
						    	if(!deserializedPagesNames.contains(pageName)) {
						    		//serialize the page
						    		String pathOfPage=pathOfFile+str_TableName+"/"+pageName+".class";
						    		page=(Page)deserialize(pathOfPage);
						    		//cache the deserialized page info
						    		deserializedPagesObj.add(new Object[]{pathOfPage,page});
						    		deserializedPagesNames.add(pageName);
						    		
						    		
						    	
						    	}
						    	else {
						    		//use already deserialized page
						    		for(Object[] deserializedPage: deserializedPagesObj) {
						    			String pathOfPage=(String)deserializedPage[0];
										if(pathOfPage.endsWith(pageName+".class"))
											page=(Page)deserializedPage[1];
											break;
									}
						    		
						    	}
						    
						    	Tuple tuple=page.tuples.get(rowNumber);
					    		Vector<Object> tupleRecord=tuple.record;
						    	//check if tuple is eligibile to be deleted
						    	for(int i=0;i<colNum.size();i++) {
						    		if(!tupleRecord.get(i).equals(indexedCol)) //it should work as colNum and indexedCol are in sync
						    			continue tupleOuter;
						    	}
						    	//if this code space is reached, then the tuple should be deleted
						    	
						    	page.tuples.remove(rowNumber); //actually delete it from the table
					    		tuplesToDelete.add(tuple);
					    		
					    		//HANDLE THIS LATER
					    		// if it becomes empty notify tableinfo & alter it
					    		if(page.tuples.size()==0) {
					    			
					    			//tableInfo.numOfPages--;
									//Files.delete(Paths.get(pathOfPage));
									//tableInfo.pages.remove(thePageItself); alter Page class to have its name/number
					    		}
					    		
					    		
					    		
					    		
//					    		removeFromIndexes(tuple,str_TableName,Indexes,indexName); //2nd argument denotes index not to consider (already removed Tuple from)
					    		//methodToRemoveTuple from all other indexes on the table accordingly
					    		
					    		//Vector<Tuple> tuples
					    		
					    		//serialize it at the end
					    		//serialize(vectorOfBuckets,pathOfPage);
						    
						    
						    
						    }
						}
						//here serialize all pages
						for(Object[] deserializedPage: deserializedPagesObj) {
							String pathOfPage=(String)deserializedPage[0];
							Page page=(Page)deserializedPage[1];
							serialize(page,pathOfPage);
						}
					
					
					Vector<IndexInfo> Indexes=tableInfo.indices;
					
					removeFromIndexes(tuplesToDelete,str_TableName,Indexes,tableInfo.clusterKeyIndex,indexName); //2nd argument denotes index not to consider (already removed Tuple from)
//					vectorDeletedTuples.add(tuple);
					
					return;
					
					
			
			}
			else {
				//HANDLE PARTIAL QUERIES HERE
				//consider them (look at their tuple references)
				//because they might not satisfy the entire query 
				return;
			}
			
		}
		

//		hash //hashtable of both of them
		
		
		/*------
		
		If there is no good index to do amplify finding the tuples,
		proceed using linear scans, but after each successful deletion, 
		 delete the tuple from every Index the table has
		
		//------*/
		
		
		
		for(int i=0;i<tableInfo.pages.size();i++) {
			String pageName=(String)tableInfo.pages.get(i)[0];
			String pagePath=pathOfFile+str_TableName+"/"+pageName+".class";
	        Page page=(Page)deserialize(pagePath);
	        Boolean pageDeleted=false;
	        for(int j=0;j<page.tuples.size();j++) {
	        	boolean found=true;
	        	for(int k=0;k<columnName.length;k++) {
	        		if(!(page.tuples.get(j).record.get(tableInfo.colOrder.indexOf(columnName[k])).toString().equals(values[k].toString()))) {
	        			found=false;
	        			break;
	        		}	
	        	}
	        	if (found) {
	        		page.tuples.remove(j);
	        		j--;
	        		if(page.tuples.size()==0) {
	        			pageDeleted=true;
	        			Path pathOfPage=FileSystems.getDefault().getPath(pagePath);
	        			try {
	        				tableInfo.numOfPages--;
							Files.delete(pathOfPage);
							tableInfo.pages.remove(i);
							i--;
							break;
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
	        				
	        		}
	        	}
	        }
	        if(!pageDeleted) {
	        	tableInfo.pages.get(i)[1] = page.tuples.lastElement().record.get(tableInfo.clusterKeyIndex);
	        	serialize(page,pagePath);
	        }
		}
		serialize(tableInfo,path);
    	
    }
    public static void checkValidUpdated(String currentColumnValue,String columnType,String columnMinimum,String columnMaximum) throws DBAppException{
    	
    	if(columnMaximum.charAt(0)=='"'&&columnMaximum.charAt(columnMaximum.length()-1)=='"')
   		  columnMaximum=columnMaximum.substring(1,columnMaximum.length()-1);
   	  
   	  	if(columnMinimum.charAt(0)=='"'&&columnMinimum.charAt(columnMinimum.length()-1)=='"')
   		  columnMinimum=columnMinimum.substring(1,columnMinimum.length()-1);
   	  
   	  	if(currentColumnValue.charAt(0)=='"'&&currentColumnValue.charAt(currentColumnValue.length()-1)=='"')
   		  currentColumnValue=currentColumnValue.substring(1,currentColumnValue.length()-1);
    	switch(columnType.toLowerCase()) {
    	
		  case ("java.lang.integer"):
			  try {
				//  System.out.println("currentColumnValue"+currentColumnValue);
				//  System.out.println(currentColumnValue.getClass());
				  
				  Integer columnValue=Integer.parseInt(currentColumnValue);
				//  System.out.println("columnValue is: "+columnValue);
				  
				  Integer minValue=Integer.parseInt(columnMinimum);
				//  System.out.println("columnMinimum is: "+columnMinimum);
				  
				  Integer maxValue=Integer.parseInt(columnMaximum);
				//  System.out.println("columnMaximum is: "+columnMaximum);
				  
				  if(columnValue<minValue||columnValue>maxValue)
					  throw new DBAppException("Input Value "+currentColumnValue+" is out of range!");
			  }
			  catch(Exception e){
				  throw new DBAppException("The column named "+currentColumnValue+" should be an Integer");
			  }
			  
			  break;
		  case "java.lang.double":
			  try {
				  Double columnValue=Double.parseDouble(currentColumnValue);
				  Double minValue=Double.parseDouble(columnMinimum);
				  Double maxValue=Double.parseDouble(columnMaximum);
				  if(columnValue<minValue||columnValue>maxValue)
					  throw new DBAppException("Input Value "+currentColumnValue+" is out of range!");
			  }
			  catch(Exception e){
				  e.printStackTrace();
				  throw new DBAppException("The column named "+currentColumnValue+" should be a Double");
			  }
		    break;
		  case "java.util.date":
			  try {
				  	int year = Integer.parseInt(columnMinimum.trim().substring(0, 4));
	                int month = Integer.parseInt(columnMinimum.trim().substring(5, 7));
	                int day = Integer.parseInt(columnMinimum.trim().substring(8));
	                Date minValue = new Date(year - 1900, month - 1, day);
	                
	                year = Integer.parseInt(columnMaximum.trim().substring(0, 4));
	                month = Integer.parseInt(columnMaximum.trim().substring(5, 7));
	                day = Integer.parseInt(columnMaximum.trim().substring(8));
	                Date maxValue = new Date(year - 1900, month - 1, day);
				  	
				  	boolean b=false;
				  	
				  	try {
				  		SimpleDateFormat Parser = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz yyyy");
				  		Date columnValue = Parser.parse(currentColumnValue);
				  		if(columnValue.before(minValue)||columnValue.after(maxValue))
							  throw new DBAppException("Input Value "+currentColumnValue+" is out of range!");
				  	
				  	}
				  	catch(Exception e) {
				  		b=true;
				  	}
				  	
				  	if(b) {
				  		year = Integer.parseInt(currentColumnValue.trim().substring(0, 4));
		                month = Integer.parseInt(currentColumnValue.trim().substring(5, 7));
		                day = Integer.parseInt(currentColumnValue.trim().substring(8));
		                Date columnValue = new Date(year - 1900, month - 1, day);
		                if(columnValue.before(minValue)||columnValue.after(maxValue))
							  throw new DBAppException("Input Value "+currentColumnValue+" is out of range!");
				  	}
			      	
				  
	               

				
			  }
			  catch(Exception e){
				  throw new DBAppException("The column named "+currentColumnValue+" should be a Date");
			  }
			 break;
		  default:
			  
			  if(currentColumnValue.compareTo(columnMinimum)<0 || currentColumnValue.compareTo(columnMaximum)>0)
				  throw new DBAppException("Input Value "+currentColumnValue+" is wrong!");
				
		    
		}
		
    }
    public static boolean sameValue(String a,Object o,String type) {
		String s=o.toString();
		
		boolean b=false;
		try {
		switch(type.toLowerCase()) {
		  case "java.lang.integer":
		    Integer w=Integer.parseInt(a);
		    Integer w2=Integer.parseInt(s);
		    b=w.equals(w2); 
		    break;
		  case "java.lang.double":
		    Double d=Double.parseDouble(a);
		    Double d2=Double.parseDouble(s);
		    b=d.equals(d2);
		    break;
		  case ("java.util.date"):
			  if(o instanceof Date)
			  {
			  	int year = Integer.parseInt(a.substring(0, 4));
				int month = Integer.parseInt(a.substring(5, 7));
				int day = Integer.parseInt(a.substring(8));	
				Date date = new Date(year-1900,month-1,day);
				b=date.equals((Date)o);
			  }
		  break;
		  default:
		    b=a.equals(s);
		}
		}
		catch(Exception e) {}
		
		return b;
		
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
    	
    	
    	Object[][] returnList=new Object[vect_arrColName.size()][3];
    	
    	
    	//actually start looking for column values
    	try {
    		
    		String[] splitted=null;
    		boolean bool=false;
    		int i =0;
    		for (i= 0; (strCurrentLine = objReader.readLine()) != null; i++) {
    			splitted=strCurrentLine.split(",");
    			if(splitted[0].equals(str_TableName)) {
    				bool=true;
    				break;}}
    		
    		
    		for(int j =0;j<vect_arrColName.size();j++)
    		{
    				splitted=strCurrentLine.split(",");
    				if(vect_arrColName.get(j).equals(splitted[1]))
    				{
    					Object min=splitted[5];
    				    Object max=splitted[6];
    				    returnList[j][2] = splitted[2].toLowerCase();
    				   	returnList[j][0]=min; //min
    				   	returnList[j][1]=max; //max
    				   	strCurrentLine = objReader.readLine();
    				   	splitted=strCurrentLine.split(",");
    				   	break;
    				}
    				
    		}
    		for(int j =0;j<vect_arrColName.size();j++)
    		{
    			for(int k = i;(strCurrentLine = objReader.readLine()) != null;k++)
    			{
    				splitted=strCurrentLine.split(",");
    				if(vect_arrColName.get(j).equals(splitted[1]))
    				{
    					Object min=splitted[5];
    				    Object max=splitted[6];
    				    returnList[j][2] = splitted[2].toLowerCase();
    				   	returnList[j][0]=min; //min
    				   	returnList[j][1]=max; //max
    				   	strCurrentLine = objReader.readLine();
    				   	splitted=strCurrentLine.split(",");
    				   	break;
    				}
    				
    			}
    		}
    		
    		} catch (IOException e) {
    		e.printStackTrace();
    	}  
    	
    	return returnList;
    	}
    public void createIndex(String
    		str_TableName,
    		String[] str_arrColName) throws DBAppException
    {
    	
    	//Create new index
    	int sizeGrid=str_arrColName.length+1;
    	DDVector index=new DDVector(sizeGrid);
    	
    	//Init it's info
    	String indexName ="";
    	for(int i =0;i<str_arrColName.length;i++)
    		indexName+=str_arrColName[i];
    	
    	IndexInfo indexinfo = new IndexInfo(indexName,str_arrColName);
    	
    	//Create directory for this index
    	Path path =Paths.get(pathOfFile+str_TableName+"/"+indexName);
        try 
        {
            Files.createDirectories(path);
            
        } 
        catch (Exception e) 
        {
        	  throw new DBAppException("Already has index on these columns");
        }
        
        
    	//get min and max of every col in input array from metadata
    	Object[][] minmax=getMinMax(str_TableName, indexinfo.indexedCol);
    	
    	//convert Object[][] to Vector<Object[][]>
//    	Vector<Object[][]> vectr_minmax=new Vector<Object[][]>();
//    	for(int i=0;i<minmax.length;i++) {
//    		vectr_minmax.add(minmax[i]);
//    	}
    	
    	indexinfo.minmax=minmax;
    	
    	//Get tableinfo of this table
    	String tablePath="src/main/resources/data/"+str_TableName+"/";
    	TableInfo tableInfo=(TableInfo)deserialize("src/main/resources/data/Student/tableInfo.class");
    	//Put the info of index in this tableInfo
    	
    	
//    	colNum holds position of each column that index is built on
    	int[] colNum = new int[str_arrColName.length]; 
    	for(int i = 0;i<str_arrColName.length;i++)
    		colNum[i]=tableInfo.colOrder.indexOf(str_arrColName[i]);
    	
    	//Hussein added this part for deleteFromTable (did not delete the above paragraph yet because below code depends on colNum being a String[])
    	Vector<Integer> colNumVector = new Vector<>();
    	for(int i = 0;i<str_arrColName.length;i++)
    		colNumVector.add(tableInfo.colOrder.indexOf(str_arrColName[i]));
    	
    	indexinfo.colNum=colNumVector;
    	tableInfo.indices.add(indexinfo);
    	
    	//Cell holds the exact coordinate of cell of index we are going to insert in
    	int[] cell = new int[str_arrColName.length];
    	
    	
    	//for over each page, over each row/tuple
    	for (int i = 0; i < tableInfo.pages.size(); i++) 
    	{
    		String s=(tableInfo.pages.get(i)[0].toString());
    		Page page=(Page)deserialize(tablePath+s+".class");
    		for (int j = 0; j < page.tuples.size(); j++) 
    		{
    			Tuple tuple= page.tuples.get(j);
    			//get each value from tuple of input columns
    			for(int k = 0;k<colNum.length;k++)
    				cell[k] = findDiv(minmax[k][0],minmax[k][1],minmax[k][2],tuple.record.get(colNum[k]));
    			
    			
    			Vector bucketInCell = index.getFromDimensions(cell);
    			if(bucketInCell.size()==0)
    			{
    				Bucket bucket = new Bucket();
    				Object[] tupleRef = new Object[2];
    				tupleRef[0] = s;
    				tupleRef[1] = j;
    				bucket.insert(tupleRef);
    				String pathOfFile = path+"/"+index.bucketNumber+".class";
    				serialize(bucket, pathOfFile);
    				Object[] bucketInfo = new Object[2];
    				bucketInfo[0] = index.bucketNumber+"";
    				bucketInfo[1] = false;
    				index.insertAtDimensions(cell, bucketInfo);
    			}
    			else
    			{
    				boolean noFoundBucket = true;
    				for(int k =0;k<bucketInCell.size();k++)
    				{
    					Object[] bucketInfo = (Object[]) bucketInCell.get(k);
    					if(bucketInfo[1].equals(false))
    					{
    						String pathOfBucket = path+"/"+bucketInfo[0]+".class";
    						Bucket bucket = (Bucket)deserialize(pathOfBucket);
    						Object[] tupleRef = new Object[2];
    	    				tupleRef[0] = s;
    	    				tupleRef[1] = j;
    	    				bucket.insert(tupleRef);
    	    				boolean isFull = bucket.isFull();
    	    				serialize(bucket, pathOfBucket);
    	    				bucketInfo[1] = isFull;
    	    				noFoundBucket = false;
    					}
    				}
    				if(noFoundBucket)
    				{
    					Bucket bucket = new Bucket();
        				Object[] tupleRef = new Object[2];
        				tupleRef[0] = s;
        				tupleRef[1] = j;
        				bucket.insert(tupleRef);
        				String pathOfFile = path+"/" +index.bucketNumber+ ".class";
        				serialize(bucket, pathOfFile);
        				Object[] bucketInfo = new Object[2];
        				bucketInfo[0] = index.bucketNumber+"";
        				bucketInfo[1] = false;
        				index.insertAtDimensions(cell, bucketInfo);
    				}
    			}
    		}
    		serialize(page, tablePath+s+".class");
    	}
    	//serialize index and tableInfo
    	serialize(tableInfo, "src/main/resources/data/Student/tableInfo.class");
    	serialize(index,path+"/index.class");
    }
    public Iterator selectFromTable(SQLTerm[] arr_SQLTerms,
    		String[] str_arrOperators)
    		throws DBAppException{
    	//queries have to be more than operators
    	if(arr_SQLTerms.length<=str_arrOperators.length)
    	{
    		//System.out.println("queries have to be more than operators!");
    		throw new DBAppException();
    	}
    	Hashtable<Integer,String> ht=new Hashtable<Integer,String>(); 
    	return (Iterator)ht;	
    }
    public static int findDiv(Object min, Object max, Object type,Object val)
    {
    
		if(type.toString().equals("java.util.date"))
			return 0;
		else if (type.toString().equals("java.lang.double"))
			return findDivNum(Double.parseDouble(min.toString()),Double.parseDouble(max.toString()),Double.parseDouble(val.toString()));
		else if (type.toString().equals("java.lang.integer"))
			return findDivNum(Integer.parseInt(min.toString()),Integer.parseInt(max.toString()),Integer.parseInt(val.toString()));
		else //string
			return findDivNum(min.toString(),max.toString(),val.toString());
    }
    public static int position(String val) {
		int combBefore=0;
		for(int i=1;i<val.length();i++)
			combBefore+=(int)Math.pow(26, i);
		for(int i=0;i<val.length();i++) {
			if(val.charAt(i)=='a')
				continue;
			combBefore+=(val.charAt(i)-97)*(int)Math.pow(26, val.length()-i-1);
		}
		return ++combBefore;
	}
	public static int findDivNum(int min, int max, int val)
    {
    	int range=max-min+1;
    	int divisionSize=range/10;
    	int nthComb=val-min+1;
    	int division=(int)Math.ceil(nthComb*1.0/divisionSize);
		if(division==11)
			division--;
		return division==11?10:division;
    }
	public static int findDivNum(double min, double max, double val)
    {
    	return findDivNum((int)min, (int)max, (int)val);
    }
	public static int findDivNum(String min, String max, String val) {
		return findDivNum(position(min),position(max),position(val));
	}
    public static int findDivNum(Date min, Date max, Date val)
    {
    	return 1;
    }
    public static void main(String[] args) throws IOException, DBAppException, ParseException 
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
    	htblColNameMin.put("gpa", "0");
    	
    	Hashtable htblColNameMax = new Hashtable( );
    	htblColNameMax.put("id", "10000");
    	htblColNameMax.put("name", "ZZZZZZZZZZZ");
    	htblColNameMax.put("gpa", "4");
    	
    	dbApp.createTable( strTableName, "id", htblColNameType,htblColNameMin,htblColNameMax);
    	
    	TableInfo StudentTableInfo=(TableInfo)dbApp.deserialize("src/main/resources/data/Student/tableInfo.class");
    	//INSERTION INTO TABLES
    	Hashtable htbl_values = new Hashtable( );
    	htbl_values.put("id", 1);
    	htbl_values.put("name", "ZZZZZZZZZZZ");
    	htbl_values.put("gpa", 3);
    	
    	
    	dbApp.insertIntoTable("Student",htbl_values);
    	StudentTableInfo=(TableInfo)dbApp.deserialize("src/main/resources/data/Student/tableInfo.class");
    	
    	htbl_values.put("id", 13);
    	dbApp.insertIntoTable("Student",htbl_values);
    	StudentTableInfo=(TableInfo)dbApp.deserialize("src/main/resources/data/Student/tableInfo.class");
    	htbl_values.put("id", 100);
    	dbApp.insertIntoTable("Student",htbl_values);
    	StudentTableInfo=(TableInfo)dbApp.deserialize("src/main/resources/data/Student/tableInfo.class");
    	htbl_values.put("id", 22);
    	dbApp.insertIntoTable("Student",htbl_values);
    	StudentTableInfo=(TableInfo)dbApp.deserialize("src/main/resources/data/Student/tableInfo.class");
    	
    	htbl_values.put("id", 7);
    	dbApp.insertIntoTable("Student",htbl_values);
    	StudentTableInfo=(TableInfo)dbApp.deserialize("src/main/resources/data/Student/tableInfo.class");
    	htbl_values.put("id", 43);
    	dbApp.insertIntoTable("Student",htbl_values);
    	StudentTableInfo=(TableInfo)dbApp.deserialize("src/main/resources/data/Student/tableInfo.class");
    	
    	
    	htbl_values.put("id", 232);
    	dbApp.insertIntoTable("Student",htbl_values);
    	StudentTableInfo=(TableInfo)dbApp.deserialize("src/main/resources/data/Student/tableInfo.class");
    	htbl_values.put("id", 8);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	
    	htbl_values.put("id", 17);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	htbl_values.put("id",2);
    	dbApp.insertIntoTable("Student",htbl_values);
    	htbl_values.put("id",5);
    	dbApp.insertIntoTable("Student",htbl_values);
    	htbl_values.put("id",4);
    	dbApp.insertIntoTable("Student",htbl_values);
    	htbl_values.put("id",3);
    	dbApp.insertIntoTable("Student",htbl_values);
    	htbl_values.put("id",6);
    	dbApp.insertIntoTable("Student",htbl_values);
    	htbl_values.put("id",10);
    	dbApp.insertIntoTable("Student",htbl_values);
    	htbl_values.put("id",11);
    	dbApp.insertIntoTable("Student",htbl_values);
    	htbl_values.put("id",9);
    	dbApp.insertIntoTable("Student",htbl_values);
    	TableInfo StudentTableInfo1=(TableInfo)dbApp.deserialize("src/main/resources/data/Student/tableInfo.class");
    	String studentPath="src/main/resources/data/Student/";
    	
    	Hashtable<String,Object> delete = new Hashtable<String,Object>( );
    	delete.put("gpa","3"); 
    	delete.put("name","DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD");
    	   	
    	
    	
    	
    	dbApp.createIndex( strTableName, new String[] {"id"} ); 
    	StudentTableInfo1=(TableInfo)dbApp.deserialize("src/main/resources/data/Student/tableInfo.class");
    	//Print all pages with their contents
    	DDVector index = (DDVector)dbApp.deserialize("src/main/resources/data/Student/id/index.class");
    	for (int i = 0; i < index.bucketNumber; i++) {
    		String s=i+"";
    		System.out.println("Tuples in "+s+":"+"  MAX VALUE IS ");
    		Bucket bucket=(Bucket)deserialize("src/main/resources/data/Student/id/"+s+".class");
    		for (int j = 0; j < bucket.tupleReferences.size(); j++) 
    		{
    			System.out.println("tuple"+j+": "+bucket.tupleReferences.get(j)[0].toString()+", "+ bucket.tupleReferences.get(j)[1].toString());
    		}
    		System.out.println();
    	}
    	
    	
    	}
    
    }
