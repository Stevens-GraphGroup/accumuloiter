/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.stevens.dhutchis.accumuloiter;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.util.shell.commands.ImportDirectoryCommand;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.*;

/**
 *
 * @author dhutchis
 */
public class MainTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    
    public MainTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of testIter method, of class Main.
     */
    @Test
    public void testTestIter() throws Exception {
        System.out.println("testIter");
        Main main = new Main();

        File tempDir = tempFolder.newFolder();
        MiniAccumuloCluster accumulo = new MiniAccumuloCluster(tempDir, "password");
        accumulo.start(); // doesn't work on Dylan's computer for some reason.  The OS closes the Zookeeper connection.
        Instance instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
        Connector conn = instance.getConnector("root", new PasswordToken("password"));

        main.testIter(conn);

        accumulo.stop();
        tempDir.delete();
        
    }

    /**
     * Test of main method, of class Main.
     */
//    @Test
//    public void testMain() throws Exception {
//        System.out.println("main");
//        String[] args = null;
//        Main.main(args);
//        // TODO review the generated test code and remove the default call to fail.
//        //fail("The test case is a prototype.");
//    }
    
}
