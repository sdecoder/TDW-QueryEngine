/***
 * Excerpted from "The Definitive ANTLR Reference",
 * published by The Pragmatic Bookshelf.
 * Copyrights apply to this code. It may not be used to create training material, 
 * courses, books, articles, and the like. Contact us if you are in doubt.
 * We make no guarantees that this code is fit for any purpose. 
 * Visit http://www.pragmaticprogrammer.com/titles/tpantlr for more book information.
***/
import java.io.File;

import org.antlr.runtime.*;
import org.plsql.*;

public class Test {
	
    public static void main(String[] args) throws Exception {
    	String dir_name="D:\\project\\PLSQL";
    	File myDir=new File(dir_name);
    	//String file_name = "u_isd_kamilzhu.sql";
    	//String file_name_new = file_name.replace("sql", "py");
    	//System.out.print(file_name_new + '\n');
    	File[]contents = myDir.listFiles();
    	int sql_rowcount = 0;
    	int todo_count = 0;
    	int row_count = 0;
    	
 /*   	String testa = "aaaa, vBool".s;
    	String reg = "(?i)" + "vbool";
    	testa = testa.replaceAll(reg, "vbool");
    	System.out.print(testa);
  */
   	
    	for(int i = 0; i < contents.length; i ++ )
    	{
		    // Create an input character stream from standard in
		    ANTLRNoCaseFileStream input = new ANTLRNoCaseFileStream(dir_name + '\\' + contents[i].getName());
		    	
		    // Create an ExprLexer that feeds from that stream
		    PLSQLLexer lexer = new PLSQLLexer(input);
		        
		    // Create a stream of tokens fed by the lexer
		    CommonTokenStream tokens = new CommonTokenStream(lexer);
		    
		    // Create a parser that feeds off the token stream
		    System.out.print(contents[i].getName() + "\n");
		    File Dir = new File("D:\\project\\result" + '\\' + contents[i].getName().split("[.]")[0]);
		    if(!Dir.exists())
		    	Dir.mkdirs();
		    PLSQLParser parser = new PLSQLParser(tokens, "D:\\project\\result" + '\\' + contents[i].getName().split("[.]")[0] + "\\", contents[i].getName().replace("sql", "py"));
		    //PLSQLParser parser = new PLSQLParser(tokens, "result" + '\\' + contents[i].getName());
		    parser.sqlplus_file();	
		    
		    //System.out.print("row_count: " + lexer.row_count + '\n');
		    //System.out.print("todo_count: " + parser.todo_count + '\n');
		    //System.out.print("sql_rowcount: " + parser.sql_count + '\n');
		    row_count += lexer.row_count;
		    todo_count += parser.todo_count;
		    sql_rowcount += parser.sql_count;
    	}
	    System.out.print("over" + '\n');
	    System.out.print("sql_rowcount:" + sql_rowcount + '\n');
	    System.out.print("todo_count:" + todo_count + '\n');
	    System.out.print("row_count:" + row_count + '\n');
	    double tis = (1.0 - Double.parseDouble(Integer.toString(todo_count)) / (row_count - sql_rowcount)) * 100;
	    double tes = (Double.parseDouble(Integer.toString(sql_rowcount)) / (row_count)) * 100; 
	    System.out.println("All the SQL should be revised:");
	    System.out.println("\t1. Static SQL should be fixed for converting the variables in VALUES, WHERE.");
	    System.out.println("\t2. Reference to DATE should be fixed.");
	    System.out.print("SQL should be revised is " + 
	    		tes + "%\n");
	    System.out.print("Transform Ratio (-SQL) is " + 
	    		tis + "%\n");
   	
/*    	while(true)
    	{
	    	Scanner in = new Scanner(System.in);
	    	
	    	System.out.print("please input the sql file path(print e fo exit):\n");	    	
	    	String file_path = in.nextLine();
	    	
	    	if(file_path.equalsIgnoreCase("e"))
	    	{
	    		break;
	    	}
	    		
	    	int pos = file_path.lastIndexOf("\\");
	    	String file_name = file_path.substring(pos + 1, file_path.length());
	    	file_name = file_name.replace("sql", "py");
	    	System.out.print(file_name + "\n");
	    	
	    	ANTLRNoCaseFileStream input = new ANTLRNoCaseFileStream(file_path);
	    	PLSQLLexer lexer = new PLSQLLexer(input);
	    	CommonTokenStream tokens = new CommonTokenStream(lexer);
	    	PLSQLParser parser = new PLSQLParser(tokens, ".\\", file_name);
	    	parser.sqlplus_file();
	    	System.out.print("translate success\n");
    	}*/
    }
}



