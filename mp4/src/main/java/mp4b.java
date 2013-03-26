import java.io.*;
import java.util.Hashtable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class mp4b {
    public static void main(String[] args) throws IOException {

        //if (args.length  == 0)
            //return;

        /* Write a java program that creates a table called “MetaHumans”.
        The program should create 2 column families to hold the data.
        The first column family should be named “background”
            and should contain the qualifiers id, gender, race, side.
        The second column family should be named “powers”
            and contain the qualifiers corresponding to their power name, ex. “powers:supervision” . */

        Configuration config = HBaseConfiguration.create();
        String sTableMetaHumans = "MetaHumans";
        String[] sColumnFamiliesBackgroundPowers = new String[] { "background", "powers"};

        BuildSchema(config, sTableMetaHumans, sColumnFamiliesBackgroundPowers);
        LoadData(config, sTableMetaHumans, sColumnFamiliesBackgroundPowers, args[0]);


    }

    public static void BuildSchema(Configuration config, String sTable, String[] sColumnFamilies)    throws IOException
    {
        HBaseAdmin admin = new HBaseAdmin(config);
        if (admin.tableExists(sTable)) {
            if (!admin.isTableDisabled(sTable))
                admin.disableTable(sTable);

            admin.deleteTable(sTable);
        }

        HTableDescriptor htd = new HTableDescriptor(sTable);
        for (String sColumnFamily : sColumnFamilies)
        {
            htd.addFamily(new HColumnDescriptor(sColumnFamily));
        }

        admin.createTable(htd);
    }

    public static void LoadData(Configuration config, String sTable, String[] sColumnFamilies, String sPath)    throws IOException
    {

        BufferedReader reader = null;

        Hashtable<String, String> hashFamilies = new Hashtable<String, String>();
        HTable table = new HTable(config, sTable);

        try {


            hashFamilies.put("id", "background");
            hashFamilies.put("gender", "background");
            hashFamilies.put("race", "background");
            hashFamilies.put("side", "background");
            hashFamilies.put("superspeed", "powers");
            hashFamilies.put("superhearing", "powers");
            hashFamilies.put("supervision", "powers");
            hashFamilies.put("flying", "powers");
            hashFamilies.put("invisibility", "powers");
            hashFamilies.put("icecontrol", "powers");
            hashFamilies.put("superhealing", "powers");
            hashFamilies.put("magicpowers", "powers");
            hashFamilies.put("spidersense", "powers");
            hashFamilies.put("superbrain", "powers");


            String sLine;
            Integer iColumnCount = 0;
            reader = new BufferedReader(new FileReader(sPath));
            if ((sLine = reader.readLine()) != null)
            {
                // header
                // System.out.println(sLine);
                String[] header = sLine.split(" ");

                // data
                while ((sLine = reader.readLine()) != null) {

                    String[] data = sLine.split(" ");

                    if(data.length > 0)
                    {
                        Put rowAdd = new Put(Bytes.toBytes(data[0]));
                        iColumnCount = 1;
                        while (iColumnCount < data.length)
                        {
                            rowAdd.add(Bytes.toBytes(hashFamilies.get(header[iColumnCount])),
                                    Bytes.toBytes(header[iColumnCount]), Bytes
                                    .toBytes(data[iColumnCount]));

                            iColumnCount++;
                        }
                        if (iColumnCount > 1)
                            table.put(rowAdd);

                    }
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null)
                    reader.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

    }
}
