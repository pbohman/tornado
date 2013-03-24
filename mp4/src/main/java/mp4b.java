/**
 * Created with IntelliJ IDEA.
 * User: Josh Haycraft
 * Date: 3/24/13
 * Time: 4:13 PM
 * To change this template use File | Settings | File Templates.
 */

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class mp4b {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();

        /* Write a java program that creates a table called “MetaHumans”.
        The program should create 2 column families to hold the data.
        The first column family should be named “background”
            and should contain the qualifiers id, gender, race, side.
        The second column family should be named “powers”
            and contain the qualifiers corresponding to their power name, ex. “powers:supervision” . */

        String sTableMetaHumans = "MetaHumans";

        HBaseAdmin admin = new HBaseAdmin(config);
        if (admin.tableExists(sTableMetaHumans)) {
            if (!admin.isTableDisabled(sTableMetaHumans))
                admin.disableTable(sTableMetaHumans);

            admin.deleteTable(sTableMetaHumans);
        }

        HTableDescriptor htd = new HTableDescriptor(sTableMetaHumans);
        htd.addFamily(new HColumnDescriptor("background"));
        htd.addFamily(new HColumnDescriptor("powers"));

        admin.createTable(htd);
    }
}
