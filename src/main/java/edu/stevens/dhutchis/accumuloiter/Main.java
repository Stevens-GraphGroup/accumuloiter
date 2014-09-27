/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.stevens.dhutchis.accumuloiter;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author dhutchis
 */
public class Main {
    private final String tableName = "TestTableIterator";
    private final String columnFamily="";
    private final String columnVisibility="";
    
    
    public void testIter() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
        Instance instance = new MockInstance();
        Connector conn = instance.getConnector("root", new PasswordToken(""));
        
        conn.tableOperations().create(tableName);
        
        /*Text rowID = new Text("row1");
        Text colFam = new Text("myColFam");
        Text colQual = new Text("myColQual");
        ColumnVisibility colVis = new ColumnVisibility();
        long timestamp = System.currentTimeMillis();
        Value value = new Value("myValue".getBytes());
        Mutation mutation = new Mutation(rowID);
        mutation.put(colFam, colQual, colVis, timestamp, value);*/
        
        String iterName = "summingIter";
		
        // Setup IteratorSetting
        IteratorSetting cfg = new IteratorSetting(1, iterName, SummingCombiner.class);
        LongCombiner.setEncodingType(cfg, LongCombiner.Type.STRING);
        // add columns to act on
        List<IteratorSetting.Column> combineColumns = new ArrayList<>();
        combineColumns.add(new IteratorSetting.Column(columnFamily, "leg"));
        //combineColumns.add(new IteratorSetting.Column(columnFamily, "pet"));
        Combiner.setColumns(cfg, combineColumns);

        // Add Iterator to table
        conn.tableOperations().attachIterator(tableName, cfg);
        // Verify successful add
        Map<String,EnumSet<IteratorUtil.IteratorScope>> iterMap = conn.tableOperations().listIterators(tableName);
        EnumSet<IteratorUtil.IteratorScope> iterScope = iterMap.get(iterName);
        Assert.assertNotNull(iterScope);
        Assert.assertTrue(iterScope.containsAll(EnumSet.allOf(IteratorUtil.IteratorScope.class)));
        
        Text row1 = new Text("row1");
        Text cqleg = new Text("leg");
        Value[] vlegs = new Value[] {new Value("3".getBytes()), new Value("4".getBytes()), new Value("5".getBytes())  };
        
        Mutation m1 = new Mutation(row1);
        for (Value vleg : vlegs)
            m1.put(new Text(columnFamily), cqleg, vleg);
        
        BatchWriterConfig config = new BatchWriterConfig();
        BatchWriter writer = conn.createBatchWriter(tableName, config);
        writer.addMutation(m1);
        writer.flush();
        
        // check results
        Scanner scan = conn.createScanner(tableName, Authorizations.EMPTY);
        System.out.println("Scanner range: "+scan.getRange());
        for(Entry<Key,Value> entry : scan) {
//            Text row = entry.getKey().getRow();
//            Value value = entry.getValue();
            System.out.println(entry); // hmm, MockAccumulo does not seem to use iterators...
        }
        
    }
    
    public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
        new Main().testIter();
        
    }
}
