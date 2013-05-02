
package kestrelinit;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import backtype.storm.spout.KestrelThriftClient;
import java.nio.ByteBuffer;
import com.google.common.io.Files;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.thrift7.TException;

import com.google.common.io.Files;

public class KestrelInit {

	public static void display_help(){
		System.out.println("KestrelInit watches directory for new files to upload to kestrel queue");
		System.out.println("KestrelInit <kesterl server> <kestrel thrift port> <kestrel queue name> <input data dir> <remove-files?>");
		
	}
	
	public static void main(String [] args) throws NumberFormatException, TException, IOException, InterruptedException{
		
		if (args.length != 5) {
			display_help();
			return;
		}
		
		
		
		KestrelThriftClient ktc = new KestrelThriftClient(args[0], Integer.parseInt(args[1]));
		
		String qName = args[2];
		try{
			ktc.flush_queue(qName); 
		}
		catch (Exception e) {
			System.out.println("Queue " + qName + " does not yet exist");
		}
			
		File dir = new File(args[3]);
		for(;;) {
			File[] files = dir.listFiles();
			Arrays.sort(files);
			for(File f : files){
				if(f.isFile()){
					String filename = f.getName();
					String extension = filename.substring(filename.lastIndexOf(".") + 1, filename.length());
					// only queue compressed files
					if(extension.equals("gz")){
						try {
							byte[] fData = Files.toByteArray(f);
							ByteBuffer bb = ByteBuffer.wrap(fData, 0, fData.length );
							List<ByteBuffer> items = new ArrayList<ByteBuffer>();
							items.add(bb);
							int res = ktc.put(qName, items, 180000);
							System.out.println("added " + f.getName() + " success: " + res);
							
							if(args[4].equals("true")){
								f.delete();
							}
						}
						catch(Exception e){
							e.printStackTrace();
						}
					}
				}
			}
			Thread.sleep(200);
		}
	}
}
	
