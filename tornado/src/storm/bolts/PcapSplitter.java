package storm.bolts;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import javax.xml.bind.DatatypeConverter;

import org.apache.jute.compiler.JBuffer;
import org.jnetpcap.Pcap;
import org.jnetpcap.PcapHeader;
import org.jnetpcap.nio.JMemory;
import org.jnetpcap.packet.JRegistry;
import org.jnetpcap.packet.PcapPacket;
import org.jnetpcap.packet.PcapPacketHandler;
import org.jnetpcap.packet.JRegistry;
import org.jnetpcap.protocol.lan.Ethernet;
import org.jnetpcap.protocol.network.Ip4;

import storm.util.TupleHelpers;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Input - compressed buffer
 * Emits - Packets
 */
//public class PcapSplitter extends BaseBasicBolt {

public class PcapSplitter implements IRichBolt{
	private static final long serialVersionUID = 1L;
	private static final StringBuilder errbuf = new StringBuilder();
	private PcapPacketHandler<OutputCollector> handler;
	private OutputCollector collector;
	private Ip4 ip;
	
	public byte[] unpack(int bytes) {
		  return new byte[] {
		    (byte)((bytes >>> 24) & 0xff),
		    (byte)((bytes >>> 16) & 0xff),
		    (byte)((bytes >>>  8) & 0xff),
		    (byte)((bytes       ) & 0xff)
		  };
	}
	
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    	this.collector = collector;
    	ip = new Ip4(); // Preallocat IP version 4 header
    	handler = new PcapPacketHandler<OutputCollector>() {  
  		  public void nextPacket(PcapPacket packet, OutputCollector collector) {
  			  try{
	  			  if (packet.hasHeader(ip)) {
	  				  int srcInt = ip.sourceToInt();
	  				  String src = InetAddress.getByAddress(unpack(srcInt)).getHostAddress();
	  				  byte[] pBuffer = new byte[packet.getTotalSize()];
	    			  if(packet.transferStateAndDataTo(pBuffer) != packet.getTotalSize())
	    			  {
	    				  System.out.println("failed to copy packet data");
	    			  }
	    			  else{
	    				  collector.emit("packets", new Values(pBuffer, src));
	    			  }
	  			  }
  			  }
  			  catch(Exception e){
  				  e.printStackTrace();
  			  }
  		  } 
    	};
    }
    	
	public File loadPcap(byte[] compData) throws IOException{
		File tmpDir = new File("/dev/shm");
		File outFile = File.createTempFile("tmp", "pcap", tmpDir);
		unGzip(compData, outFile);
		//return Pcap.openOffline(outFile.getAbsolutePath(), errbuf);
		return outFile;
	}
	
	public void unGzip(byte[] compressedData, File outFile) throws IOException {
		  GZIPInputStream gin = new GZIPInputStream(new ByteArrayInputStream(compressedData));
		  FileOutputStream fos = new FileOutputStream(outFile);
		  byte[] buf = new byte[100000]; // Buffer size is a matter of taste and application...
		  int len;
		  while ( ( len = gin.read(buf) ) > 0 )
		    fos.write(buf, 0, len);
		  gin.close();
		  fos.close();
	}
	
	@Override
	/**
	 * Input: compressed pcap file
	 * 
	 * Processing:
	 * 	1) Unzip pcap to /dev/shm/pcapX
	 *  2) Instantiate pcap from /dev/shm/pcapX
	 *  3) Iterate through pcap file emitting individual pkts
	 *  
	 * Emits: 
	 * 1) Emits a byte[] PcapPacket structure that
	 *  can be de-serialized back into a PcapPacket object
	 *  
	 * 2) Emits (timestamp, capturelen, totallen) raw pkt stats
	 */
	public void execute(Tuple tuple){
		
		if (TupleHelpers.isTickTuple(tuple)) {
			System.out.println();
			return;
		}
		Pcap pcap = null;
		File data = null;

		try {
			data = loadPcap(tuple.getBinary(0));
			pcap = Pcap.openOffline(data.getAbsolutePath(), errbuf);
			System.out.println("Starting packet loop");
			pcap.loop(Pcap.LOOP_INFINITE, handler, collector);  
		} catch (IOException e) {
			e.printStackTrace();
		}
        finally {  
        	if(pcap != null)pcap.close();
        	if(data != null)data.delete();
        }  
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("rawstats", new Fields("sec", "wirelen", "caplen"));
        declarer.declareStream("packets", new Fields("packet", "srcip"));
    }

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}