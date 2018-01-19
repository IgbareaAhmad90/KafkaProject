package javafxapplication1;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;



public class TopicFile {
	static String PATH = "Topics//";
	public synchronized static void println(String UserName,String Topic, String msg) {
		try {
			FileWriter fw = new FileWriter(PATH +UserName+"__"+Topic+".txt", true);
			
			BufferedWriter bw = new BufferedWriter(fw);
			PrintWriter out = new PrintWriter(bw);
			out.println(msg);
			out.flush(); 
			fw.close();
			bw.close();
			out.close();
		} catch (IOException e) {
			System.out.println("Error File");
			e.printStackTrace();
		}	
	}
	public synchronized static void log(String UserName,String msg) {
		try {
			FileWriter fw = new FileWriter("Log_File__"+UserName+".txt", true);
			BufferedWriter bw = new BufferedWriter(fw);
			PrintWriter out = new PrintWriter(bw);
			out.println(msg);
			out.flush();
			fw.close();
			bw.close();
			out.close();
		} catch (IOException e) {
			System.out.println("Error File");
			e.printStackTrace();
		}	
	}
}
