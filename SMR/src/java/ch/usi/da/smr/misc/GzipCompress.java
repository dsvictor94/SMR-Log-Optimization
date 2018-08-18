package ch.usi.da.smr.misc;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipCompress {

	public GzipCompress(){
		
	}
	
	public byte[] compress(String str) throws Exception {
	    if (str == null || str.length() == 0) {
	        return null;
	    }
	    //System.out.println("String length : " + str.length());
	    ByteArrayOutputStream obj=new ByteArrayOutputStream();
	    GZIPOutputStream gzip = new GZIPOutputStream(obj);
	    gzip.write(str.getBytes("UTF-8"));
	    gzip.close();	    
	    //System.out.println("Output String length : " + outStr.length());	    
	    return obj.toByteArray();
	 }

	 public String decompress(byte[] bytes) throws Exception {
	    if (bytes == null || bytes.length == 0) {
	        return null;
	    }
	    //System.out.println("Input String length : " + bytes.length);	    
	    GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(bytes));
	    BufferedReader bf = new BufferedReader(new InputStreamReader(gis, "UTF-8"));
	    String outStr = "";
	    String line;
	    while ((line=bf.readLine())!=null) {
	      outStr += line;
	    }
	    //System.out.println("Output String length : " + outStr.length());
	    return outStr;	    
	 }
	
	public String generateString(int length, String charactersPattern){
		Random rng = new Random();
	    char[] text = new char[length];
	    for (int i = 0; i < length; i++)
	    {
	        text[i] = charactersPattern.charAt(rng.nextInt(charactersPattern.length()));
	    }
	    return new String(text);
	}	
 
	
	public static void main(String[] args) throws Exception {
	    String string = "I am what I am hhhhhhhhhhhhhhhhhhhhhhhhhhhhh"
	            + "bjggujhhhhhhhhh"
	            + "rggggggggggggggggggggggggg"
	            + "esfffffffffffffffffffffffffffffff"
	            + "esffffffffffffffffffffffffffffffff"
	            + "esfekfgy enter code here`etd`enter code here wdd"
	            + "heljwidgutwdbwdq8d"
	            + "skdfgysrdsdnjsvfyekbdsgcu"
	            +"jbujsbjvugsduddbdj";

	    GzipCompress gzip = new GzipCompress();
	    
	    System.out.println("after compress:");
	    byte[] compressed = gzip.compress(string);
	    System.out.println(compressed);
	    System.out.println("after decompress:");
	    String decomp = gzip.decompress(compressed);
	    System.out.println(decomp);
	}
}





