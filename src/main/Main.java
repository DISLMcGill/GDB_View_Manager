package main;

import gen.ViewLexer;
import gen.ViewParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.neo4j.graphdb.Relationship;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Main {
    //main class used for main.QueryParser.java as the parser.

    // Defining parameters
    protected static Map<String, Set<String>> nodeTable = new ConcurrentHashMap<String, Set<String>>();
    protected static Map<String, Set<Relationship>> pathTable = new ConcurrentHashMap<>();
    protected static Map<String, Set<String>> edgeTable = new ConcurrentHashMap<>();


    protected static Map<String, String> pathRelTable = new ConcurrentHashMap<>();
    protected static Map<String, String> typeTable = new ConcurrentHashMap<>();

    // Added for keeping track of the view queries for non-materialized views
    protected static Map<String, String> viewQueryTable = new ConcurrentHashMap<>();
    protected static Map<String, String> viewReturnVarTable = new ConcurrentHashMap<>();

    public static ParseTreeWalker walker = new ParseTreeWalker();
    public static QueryParser vql = new QueryParser();

    public static Neo4jGraphConnector connector;
    public static long totalTime = 0;

    public static void main(String[] args){

        try {

            // size could be set to any of the ["small", "medium", "large"]
            String size = "small";

            // Getting the Connector object to Neo4j
            connector = new Neo4jGraphConnector(size);
            System.out.println(args.length);

            // Check if any inputs are given [This is to facilitate running long tests]
            
            // Running commands of a file without clearing the cache
            if (args.length == 1) {
                String fileName = args[0];
                ArrayList<String> commands_in_order = getExperimentCommands(fileName);

                // NOTE: Need to set this per experiment: Setting System.out to a file for easier processing
                //PrintStream o = new PrintStream(new File("./test/view_use/baseline/outputs/global/warm_medium_new.txt"));
        
                //PrintStream console = System.out;
        
                //System.setOut(o);
 
                for (String cmd: commands_in_order) {
                    if (cmd.equals("quit")) {
                        break;
                    }
                    else {
                        System.out.println(cmd);
                        processSingleCmd(cmd);
                        System.out.println("*********************************");
                    }
                }
            } else if (args.length == 2 && args[0].equals("cold")) {
                String cmd = args[1];
                // This is to warm up the system with bringing the post index into the memory
                //String simplecmd = "baseline test MATCH (n:Post) WHERE n.postId = '3468801' RETURN n";
                String simplecmd = "CREATE VIEW AS V_test MATCH (n:Post) WHERE n.postId = '3468801' RETURN n";
                processSingleCmd(simplecmd);
                System.out.println(cmd);
                processSingleCmd(cmd);
            } else if (args.length == 2 && (args[0].equals("method1") || args[0].equals("method2"))) {
                String fileName = args[1];
                ArrayList<String> commands_in_order = getExperimentCommands(fileName);
 
                for (String cmd: commands_in_order) {
                    if (cmd.equals("quit")) {
                        break;
                    }
                    else {
                        System.out.println(cmd);
                        processSingleCmd(cmd);
                        System.out.println("*********************************");
                    }
                }
            } else if (args.length == 2 && args[0].equals("cold_use")) {
                PrintStream o = new PrintStream(new File("./test/tmp.txt"));
        
                PrintStream console = System.out;
        
                // Sending create log stuff to a tmp file
                System.setOut(o);

                // Create all the views in the system
                BufferedReader reader;
                try {
                    reader = new BufferedReader(new FileReader("test/views.txt"));
                    String line = reader.readLine();

                    while (line != null) {
                        System.out.println(line);
                        processSingleCmd("CREATE VIEW AS " + line);
                        line = reader.readLine();
                    }

                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                // Send use logs to the console
                System.setOut(console);
                // Run the "View Use" command that is the input
                String cmd = args[1];
                System.out.println(cmd);
                processSingleCmd(cmd);
            } else if (args.length == 2 && args[0].equals("cold_use_old")) {
                PrintStream o = new PrintStream(new File("./test/tmp.txt"));
        
                PrintStream console = System.out;
        
                // Sending create log stuff to a tmp file
                System.setOut(o);

                // Create all the views in the system
                BufferedReader reader;
                try {
                    reader = new BufferedReader(new FileReader("test/views_old.txt"));
                    String line = reader.readLine();

                    while (line != null) {
                        System.out.println(line);
                        processSingleCmd("CREATE VIEW AS " + line);
                        line = reader.readLine();
                    }

                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                // Send use logs to the console
                System.setOut(console);
                // Run the "View Use" command that is the input
                String cmd = args[1];
                System.out.println(cmd);
                processSingleCmd(cmd);
            } else if (args.length == 2 && args[0].equals("test")) {
                // add an argument to an existing node
                String query = "baseline test MATCH (n:Post) WHERE n.postId = '3468801' RETURN n";
                processSingleCmd(query);
            }

            // Big interactive loop to process the input commands
            else { terminal(); }

        }
        catch(Exception e){
            e.printStackTrace();
        }
        finally{
            connector.shutdown();
        }

    }

    public static ArrayList<String> getExperimentCommands(String filePath){
        ArrayList<String> commands = new ArrayList<String>();
        try {
            Scanner sc = new Scanner(new File(filePath));
            while(sc.hasNextLine()){
                String line = sc.nextLine();
                commands.add(line);
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
        return commands;
    }

    // Process a single command given as input
    private static void processSingleCmd(String command) {
        // These parameters should be set manually by the user
        boolean materialized = false;
        boolean debug = false;

        try {
            if (command.startsWith("printOrView")) {
                vql.printOrClauseViews();
            } else if (command.startsWith("printDependencies")) {
                vql.printDependencies();
            } else if (command.startsWith("printNode")) {
                System.out.println(nodeTable.toString());
            } else if (command.startsWith("clear")) {
                vql.clearAll();
            } else if (command.startsWith("wipe")) {
                connector.executeQuery("MATCH (n) REMOVE n.views");
            } else if (command.startsWith("view wipe")) {
                //connector.excuteInternalBaselineQuery("MATCH (n) DETACH DELETE n");
                System.out.println("Should wipe views");
            } else if (command.startsWith("debug switch")) {
                debug = !debug;
            } else if (command.startsWith("count")) {
                Set<String> keys = nodeTable.keySet();
                for (String key : keys) {
                    System.out.println(key + ":" + nodeTable.get(key).size());
                }
            } else if (command.equals("show index")) {
                // Prints out the indexes of the database
                connector.showIndexes();
            } else if (command.startsWith("CREATE INDEX")) {
                // Used for creating a new index in the database 
                connector.createIndex(command);
            } else if (command.startsWith("delete all views")) {
                // Used for dropping an index in the database 
                connector.clearViewnameProperty();
            } else if (command.startsWith("baseline")) {
                String query = command.substring(command.indexOf(" ", command.indexOf(" ") + 1) + 1);
                long start = System.currentTimeMillis();
                int querySize = connector.excuteBaselineQuery(query);
                long duration = System.currentTimeMillis() - start;
                System.out.println("Took " + duration + " ms to execute baseline query");
                System.out.println("Baseline returned " + querySize + " nodes (or edges)");
            } else if (command.startsWith("range")) {
                // For conducting the nodeid range experiments
                int limit  =  Integer.valueOf(command.substring(command.indexOf(" ", command.indexOf(" ") + 1) + 1));
                Set<String> nodeids = new HashSet<String>();
                for (int i = 1; i < limit + 1; i++) {
                    nodeids.add(Integer.toString(i));
                }
                String query = "MATCH (n) WHERE ID(n) in " +  nodeids + " RETURN DISTINCT n";
                long start = System.currentTimeMillis();
                int querySize = connector.excuteBaselineQuery(query);
                long duration = System.currentTimeMillis() - start;
                System.out.println("Took " + duration + " ms to execute baseline query");
                System.out.println("Baseline returned " + querySize + " nodes (or edges)");
            } else if (command.startsWith("WITH NON_MATERIALIZED VIEWS")) {

                // Case for Basic Local Use Query Processing
                if (command.contains("LOCAL BASIC")) {
                    // This is a manual process for processing the non-materialized use queries with only 1 view 
                    String viewname = command.split("\\s+")[5];
                    String type = typeTable.get(viewname);
                    String rewritten_query = "";
                    long start, duration;
                    int querySize;
                    
                    if (type.equals("PATH")) {
                        // Rewrite for path views
                        // For returning both nodes and edges in a path query uncomment the line below
                        //rewritten_query = viewQueryTable.get(viewname) + " WITH nodes(" + viewReturnVarTable.get(viewname) + ") AS no, relationships(" + viewReturnVarTable.get(viewname) + ") AS re WITH [node in no | id(node)] AS nodeids, [rel in re | id(rel)] AS edgeids MATCH (n) WHERE ID(n) IN nodeids MATCH ()-[r]-() WHERE ID(r) IN edgeids RETURN n,r";
                        
                        // Just getting the nodes in a path query
                        
                        rewritten_query = createPathQueryNonMaterializedTerm(viewQueryTable.get(viewname)) + " AS v MATCH (n) WHERE ID(n) IN v RETURN n";
                        System.out.println(rewritten_query);

                        start = System.currentTimeMillis();
                        querySize = connector.excuteBasicNoneMaterializedQuery(rewritten_query);
                        duration = System.currentTimeMillis() - start;
                    } else {
                        // Rewrite for node views 
                        rewritten_query = viewQueryTable.get(viewname) + " WITH COLLECT(DISTINCT ID(" + viewReturnVarTable.get(viewname) + ")) AS v MATCH (n) WHERE ID(n) IN v RETURN n";
                        System.out.println(rewritten_query);

                        start = System.currentTimeMillis();
                        querySize = connector.excuteBaselineQuery(rewritten_query);
                        duration = System.currentTimeMillis() - start;
                    }                    

                    System.out.println("Took " + duration + " ms to execute query");
                    System.out.println("Non-materialized returned " + querySize + " elements");
                } 
                
                // Case for Basic Complex Processing
                else if (command.contains("LOCAL COMPLEX")) {
                    // This is a manual process for non-materialized use queries with only 2 views and a local context 
                    String first_view = command.split("\\s+")[5];
                    String second_view = command.split("\\s+")[6];
                    String first_type = typeTable.get(first_view);
                    String second_type = typeTable.get(second_view);

                    String rewritten_query = "";
                    long start, duration;
                    int querySize = 0;

                    if (first_type.equals("NODE") && second_type.equals("NODE")) {
                        System.out.println("Both NODE");
                        // Rewrite for two node views 
                        rewritten_query = viewQueryTable.get(first_view) + " WITH COLLECT(DISTINCT ID(" + viewReturnVarTable.get(first_view) + ")) AS v1 \n" + viewQueryTable.get(second_view) + " WITH v1, COLLECT(DISTINCT ID(" + viewReturnVarTable.get(second_view) + ")) AS v2 \nMATCH (node) WHERE ID(node) IN v1 AND ID(node) IN v2 RETURN node";
                        System.out.println(rewritten_query);

                        start = System.currentTimeMillis();
                        querySize = connector.excuteBaselineQuery(rewritten_query);
                        duration = System.currentTimeMillis() - start;                   

                        System.out.println("Took " + duration + " ms to execute baseline query");
                        System.out.println("Non-materialized returned " + querySize + " elements");
                    } 
                    
                    else if (first_type.equals("PATH") && second_type.equals("PATH")) {
                        System.out.println("Both PATH");

                        String firstPathQueryTerm = createPathQueryNonMaterializedTerm(viewQueryTable.get(first_view));
                        String secondPathQueryTerm = createPathQueryNonMaterializedTerm(viewQueryTable.get(second_view));

                        rewritten_query = firstPathQueryTerm + " AS v1 \n" + secondPathQueryTerm + " AS v2, v1 \n"+
                            "WITH [node IN v1 WHERE node IN v2] AS commonNodes MATCH (res) WHERE ID(res) IN commonNodes RETURN res";
                        
                        System.out.println(rewritten_query);

                        start = System.currentTimeMillis();
                        querySize = connector.excuteBasicNoneMaterializedQuery(rewritten_query);
                        duration = System.currentTimeMillis() - start;

                        System.out.println("Took " + duration + " ms to execute baseline query");
                        System.out.println("Non-materialized returned " + querySize + " elements");

                    } 
                    
                    else if (first_type.equals("PATH")) {
                        // First one is path but second one is node
                        System.out.println("First PATH - Second NODE");

                        String pathQueryTerm = createPathQueryNonMaterializedTerm(viewQueryTable.get(first_view));

                        rewritten_query =  pathQueryTerm + " AS v1 \n" + 
                            viewQueryTable.get(second_view) + " WITH COLLECT(DISTINCT ID(" + viewReturnVarTable.get(second_view) + ")) AS v2, v1 \n" + 
                            "WITH [node IN v1 WHERE node IN v2] AS commonNodes MATCH (res) WHERE ID(res) IN commonNodes RETURN res";
                        
                        System.out.println(rewritten_query);

                        start = System.currentTimeMillis();
                        querySize = connector.excuteBasicNoneMaterializedQuery(rewritten_query);
                        duration = System.currentTimeMillis() - start;

                        System.out.println("Took " + duration + " ms to execute baseline query");
                        System.out.println("Non-materialized returned " + querySize + " elements");
                    } 
                    
                    else if (second_type.equals("PATH")) {
                        // First one is node but second one is path
                        System.out.println("First NODE - Second PATH");
                        
                        String pathQueryTerm = createPathQueryNonMaterializedTerm(viewQueryTable.get(second_view));

                        rewritten_query = viewQueryTable.get(first_view) + " WITH COLLECT(DISTINCT ID(" + viewReturnVarTable.get(first_view) + ")) AS v1 \n" +
                        pathQueryTerm + " AS v2, v1 \n WITH [node IN v1 WHERE node IN v2] AS commonNodes MATCH (res) WHERE ID(res) IN commonNodes RETURN res"; 
                        
                        System.out.println(rewritten_query);

                        start = System.currentTimeMillis();
                        querySize = connector.excuteBasicNoneMaterializedQuery(rewritten_query);
                        duration = System.currentTimeMillis() - start;

                        System.out.println("Took " + duration + " ms to execute baseline query");
                        System.out.println("Non-materialized returned " + querySize + " elements");
                    }
                }

                // Case for Global Use Queries
                else if (command.contains("GLOBAL")) {

                    String rewritten_query = "";
                    long start, duration;
                    int querySize = 0;

                    // There could be multiple views used.
                    int startIndex = command.indexOf("GLOBAL");
                    int endIndex = command.indexOf("MATCH", startIndex + "GLOBAL".length());
                    String matchQuery =  command.substring(command.indexOf("MATCH") + "WHERE".length(), command.indexOf("WHERE", startIndex + "MATCH".length())).trim();

                    String whereClause = command.substring(command.indexOf("WHERE") + "RETURN".length(), command.indexOf("RETURN", startIndex + "WHERE".length())).trim();
                    String returnClause = command.substring(command.indexOf("RETURN"));

                    String[] views = (command.substring(startIndex + "GLOBAL".length(), endIndex).trim()).split("\\s+");
                    String[] view_sub_queries = new String[views.length];

                    int counter =  0;
                    for(String v : views) {
                        if (typeTable.get(v).equals("NODE")) {
                            if (counter == 0) {
                                view_sub_queries[counter] = viewQueryTable.get(v) + " WITH COLLECT(DISTINCT ID(" + viewReturnVarTable.get(v) + 
                                    ")) AS " + v + "\n";
                            } else {
                                view_sub_queries[counter] = viewQueryTable.get(v) + " WITH " + nonMaterializedIntermediateWithClause(v, views) + 
                                    " COLLECT(DISTINCT ID(" + viewReturnVarTable.get(v) + ")) AS " + v + "\n";
                            }
                        } else if (typeTable.get(v).equals("PATH")) {
                            if (counter == 0) {
                                view_sub_queries[counter] = createPathQueryNonMaterializedTerm(viewQueryTable.get(v)) + " AS " + v + "\n";
                            } else {
                                view_sub_queries[counter] = createPathQueryNonMaterializedTerm(viewQueryTable.get(v)) + 
                                        " AS " + v + "," + nonMaterializedIntermediateWithClause(v, views) +  "\n";
                            }
                        }
                        counter ++;
                    }

                    for (String q : view_sub_queries) {
                        rewritten_query += q;
                    }

                    // MATCH (n)-[:POSTED]-(p:Post) WHERE ID(n) IN v1 AND ID(p) IN v2

                    // MATCH (n:User)-[:POSTED]-(p1:Post)-[:PARENT_OF]-(p2:Post) WHERE p1 IN V5 AND n IN V6_2 RETURN p2
                    rewritten_query += "MATCH " + matchQuery;
                    rewritten_query += " WHERE " + rewriteNonMaterializedWhereClause(whereClause);
                    rewritten_query += " RETURN DISTINCT(" + returnClause.split("\\s+")[1] + ")";
                    System.out.println(rewritten_query);

                    start = System.currentTimeMillis();
                    querySize = connector.excuteBasicNoneMaterializedQuery(rewritten_query);
                    duration = System.currentTimeMillis() - start;

                    System.out.println("Took " + duration + " ms to execute baseline query");
                    System.out.println("Non-materialized returned " + querySize + " elements");
                }

            } else {

                // Break up the input stream of characters into vocabulary symbols for a parser
                ViewLexer VL = new ViewLexer(CharStreams.fromString(command));

                CommonTokenStream tokens = new CommonTokenStream(VL);
                ViewParser parser = new ViewParser(tokens); 
                ParseTree tree = parser.root();
                walker.walk(vql, tree);

                if (vql.isViewInstant()) {
                    long now = System.currentTimeMillis();
                    // Mohanna: Changed for testing for now 
                    processMainView(command, materialized);
                    long total = System.currentTimeMillis() - now;
                    System.out.println("Took " + total + "ms to create views");
                } else if (vql.isViewUse()) {
                    long now = System.currentTimeMillis();
                    processUseView(command);
                    long total = System.currentTimeMillis() - now;
                    System.out.println("Took " + total + "ms to use view");
                } else if (vql.isCg()) {
                    long now = System.currentTimeMillis();
                    changeGraph(command);
                    long total = System.currentTimeMillis() - now;
                    System.out.println("Took " + total + "ms to change graph and update view(s)");
                }

                if (!debug) vql.clearAll();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void terminal(){

        // NOTE These parameters should be set manually by the user
        boolean materialized = false;
        boolean debug = false;

        try {
            // Reading the first input command from the terminal
            InputStreamReader isReader = new InputStreamReader(System.in);
            BufferedReader br = new BufferedReader(isReader);
            System.out.print(">> ");
            String command = br.readLine();

            // Big while loop to process the incoming commands
            while (true) {
                if (command.startsWith("quit")) {
                    break;
                } else if (command.startsWith("printOrView")) {
                    vql.printOrClauseViews();
                } else if (command.startsWith("printDependencies")) {
                    vql.printDependencies();
                } else if (command.startsWith("printNode")) {
                    System.out.println(nodeTable.toString());
                } else if (command.startsWith("clear")) {
                    vql.clearAll();
                } else if (command.startsWith("wipe")) {
                    connector.executeQuery("MATCH (n) REMOVE n.views");
                } else if (command.startsWith("view wipe")) {
                    //connector.excuteInternalBaselineQuery("MATCH (n) DETACH DELETE n");
                    System.out.println("should wipe views");
                } else if (command.startsWith("debug switch")) {
                    debug = !debug;
                } else if (command.startsWith("count")) {
                    Set<String> keys = nodeTable.keySet();
                    for (String key : keys) {
                        System.out.println(key + ":" + nodeTable.get(key).size());
                    }
                } else if (command.equals("show index")) {
                    // Prints out the indexes of the database
                    connector.showIndexes();
                } else if (command.startsWith("CREATE INDEX")) {
                    // Used for creating a new index in the database 
                    // CREATE INDEX post_viewname_index FOR (n:Post) ON (n.viewname)
                    // CREATE INDEX parentof_viewname_index FOR ()-[r:PARENT_OF]-() ON (r.viewname)
                    connector.createIndex(command);
                } else if (command.startsWith("delete all views")) {
                    // Used for deleting view's arguments in the system
                    connector.clearViewnameProperty();
                } else if (command.startsWith("baseline")) {
                    String query = command.substring(command.indexOf(" ", command.indexOf(" ") + 1) + 1);
                    long start = System.currentTimeMillis();
                    int querySize = connector.excuteBaselineQuery(query);
                    long duration = System.currentTimeMillis() - start;
                    System.out.println("Took " + duration + " ms to execute baseline query");
                    System.out.println("Baseline returned " + querySize + " nodes (or edges)");
                } else if (command.startsWith("WITH NON_MATERIALIZED VIEWS")) {

                    // Case for Basic Local Use Query Use Queries
                    if (command.contains("LOCAL BASIC")) {
                        // This is a manual process for processing the non-materialized use queries with only 1 view 
                        String viewname = command.split("\\s+")[5];
                        String type = typeTable.get(viewname);
                        String rewritten_query = "";
                        long start, duration;
                        int querySize;
                        
                        if (type.equals("PATH")) {
                            // Rewrite for path views
                            // For returning both nodes and edges in a path query uncomment the line below
                            //rewritten_query = viewQueryTable.get(viewname) + " WITH nodes(" + viewReturnVarTable.get(viewname) + ") AS no, relationships(" + viewReturnVarTable.get(viewname) + ") AS re WITH [node in no | id(node)] AS nodeids, [rel in re | id(rel)] AS edgeids MATCH (n) WHERE ID(n) IN nodeids MATCH ()-[r]-() WHERE ID(r) IN edgeids RETURN n,r";
                            
                            // Just getting the nodes in a path query
                            
                            rewritten_query = createPathQueryNonMaterializedTerm(viewQueryTable.get(viewname)) + " AS v MATCH (n) WHERE ID(n) IN v RETURN n";
                            System.out.println(rewritten_query);

                            start = System.currentTimeMillis();
                            querySize = connector.excuteBasicNoneMaterializedQuery(rewritten_query);
                            duration = System.currentTimeMillis() - start;
                        } else {
                            // Rewrite for node views 
                            rewritten_query = viewQueryTable.get(viewname) + " WITH COLLECT(DISTINCT ID(" + viewReturnVarTable.get(viewname) + ")) AS v MATCH (n) WHERE ID(n) IN v RETURN n";
                            System.out.println(rewritten_query);

                            start = System.currentTimeMillis();
                            querySize = connector.excuteBaselineQuery(rewritten_query);
                            duration = System.currentTimeMillis() - start;
                        }                    

                        System.out.println("Took " + duration + " ms to execute baseline query");
                        System.out.println("Non-materialized returned " + querySize + " elements");
                    } 
                    
                    // Case for Basic Complex Use Queries
                    else if (command.contains("LOCAL COMPLEX")) {
                        // This is a manual process for non-materialized use queries with only 2 views and a local context 
                        String first_view = command.split("\\s+")[5];
                        String second_view = command.split("\\s+")[6];
                        String first_type = typeTable.get(first_view);
                        String second_type = typeTable.get(second_view);

                        String rewritten_query = "";
                        long start, duration;
                        int querySize = 0;

                        if (first_type.equals("NODE") && second_type.equals("NODE")) {
                            System.out.println("Both NODE");
                            // Rewrite for two node views 
                            rewritten_query = viewQueryTable.get(first_view) + " WITH COLLECT(DISTINCT ID(" + viewReturnVarTable.get(first_view) + ")) AS v1 \n" + viewQueryTable.get(second_view) + " WITH v1, COLLECT(DISTINCT ID(" + viewReturnVarTable.get(second_view) + ")) AS v2 \nMATCH (node) WHERE ID(node) IN v1 AND ID(node) IN v2 RETURN node";
                            System.out.println(rewritten_query);

                            start = System.currentTimeMillis();
                            querySize = connector.excuteBaselineQuery(rewritten_query);
                            duration = System.currentTimeMillis() - start;                   

                            System.out.println("Took " + duration + " ms to execute baseline query");
                            System.out.println("Non-materialized returned " + querySize + " elements");
                        } 
                        
                        else if (first_type.equals("PATH") && second_type.equals("PATH")) {
                            System.out.println("Both PATH");

                            String firstPathQueryTerm = createPathQueryNonMaterializedTerm(viewQueryTable.get(first_view));
                            String secondPathQueryTerm = createPathQueryNonMaterializedTerm(viewQueryTable.get(second_view));

                            rewritten_query = firstPathQueryTerm + " AS v1 \n" + secondPathQueryTerm + " AS v2, v1 \n"+
                                "WITH [node IN v1 WHERE node IN v2] AS commonNodes MATCH (res) WHERE ID(res) IN commonNodes RETURN res";
                            
                            System.out.println(rewritten_query);

                            start = System.currentTimeMillis();
                            querySize = connector.excuteBasicNoneMaterializedQuery(rewritten_query);
                            duration = System.currentTimeMillis() - start;

                            System.out.println("Took " + duration + " ms to execute baseline query");
                            System.out.println("Non-materialized returned " + querySize + " elements");

                        } 
                        
                        else if (first_type.equals("PATH")) {
                            // First one is path but second one is node
                            System.out.println("First PATH - Second NODE");

                            String pathQueryTerm = createPathQueryNonMaterializedTerm(viewQueryTable.get(first_view));

                            rewritten_query =  pathQueryTerm + " AS v1 \n" + 
                                viewQueryTable.get(second_view) + " WITH COLLECT(DISTINCT ID(" + viewReturnVarTable.get(second_view) + ")) AS v2, v1 \n" + 
                                "WITH [node IN v1 WHERE node IN v2] AS commonNodes MATCH (res) WHERE ID(res) IN commonNodes RETURN res";
                            
                            System.out.println(rewritten_query);

                            start = System.currentTimeMillis();
                            querySize = connector.excuteBasicNoneMaterializedQuery(rewritten_query);
                            duration = System.currentTimeMillis() - start;

                            System.out.println("Took " + duration + " ms to execute baseline query");
                            System.out.println("Non-materialized returned " + querySize + " elements");
                        } 
                        
                        else if (second_type.equals("PATH")) {
                            // First one is node but second one is path
                            System.out.println("First NODE - Second PATH");
                            
                            String pathQueryTerm = createPathQueryNonMaterializedTerm(viewQueryTable.get(second_view));

                            rewritten_query = viewQueryTable.get(first_view) + " WITH COLLECT(DISTINCT ID(" + viewReturnVarTable.get(first_view) + ")) AS v1 \n" +
                            pathQueryTerm + " AS v2, v1 \n WITH [node IN v1 WHERE node IN v2] AS commonNodes MATCH (res) WHERE ID(res) IN commonNodes RETURN res"; 
                            
                            System.out.println(rewritten_query);

                            start = System.currentTimeMillis();
                            querySize = connector.excuteBasicNoneMaterializedQuery(rewritten_query);
                            duration = System.currentTimeMillis() - start;

                            System.out.println("Took " + duration + " ms to execute baseline query");
                            System.out.println("Non-materialized returned " + querySize + " elements");
                        }
                    }

                    // Case for Global Use Queries
                    else if (command.contains("GLOBAL")) {

                        String rewritten_query = "";
                        long start, duration;
                        int querySize = 0;

                        // There could be multiple views used.
                        int startIndex = command.indexOf("GLOBAL");
                        int endIndex = command.indexOf("MATCH", startIndex + "GLOBAL".length());
                        String matchQuery =  command.substring(command.indexOf("MATCH") + "WHERE".length(), command.indexOf("WHERE", startIndex + "MATCH".length())).trim();

                        String whereClause = command.substring(command.indexOf("WHERE") + "RETURN".length(), command.indexOf("RETURN", startIndex + "WHERE".length())).trim();
                        String returnClause = command.substring(command.indexOf("RETURN"));
                        
                        String[] views = (command.substring(startIndex + "GLOBAL".length(), endIndex).trim()).split("\\s+");
                        String[] view_sub_queries = new String[views.length];

                        int counter =  0;
                        for(String v : views) {
                            if (typeTable.get(v).equals("NODE")) {
                                if (counter == 0) {
                                    view_sub_queries[counter] = viewQueryTable.get(v) + " WITH COLLECT(DISTINCT ID(" + viewReturnVarTable.get(v) + 
                                        ")) AS " + v + "\n";
                                } else {
                                    view_sub_queries[counter] = viewQueryTable.get(v) + " WITH " + nonMaterializedIntermediateWithClause(v, views) + 
                                        " COLLECT(DISTINCT ID(" + viewReturnVarTable.get(v) + ")) AS " + v + "\n";
                                }
                            } else if (typeTable.get(v).equals("PATH")) {
                                if (counter == 0) {
                                    view_sub_queries[counter] = createPathQueryNonMaterializedTerm(viewQueryTable.get(v)) + " AS " + v + "\n";
                                } else {
                                    view_sub_queries[counter] = createPathQueryNonMaterializedTerm(viewQueryTable.get(v)) + 
                                            " AS " + v + "," + nonMaterializedIntermediateWithClause(v, views) +  "\n";
                                }
                            }
                            counter ++;
                        }

                        for (String q : view_sub_queries) {
                            rewritten_query += q;
                        }

                        // MATCH (n)-[:POSTED]-(p:Post) WHERE ID(n) IN v1 AND ID(p) IN v2

                        // MATCH (n:User)-[:POSTED]-(p1:Post)-[:PARENT_OF]-(p2:Post) WHERE p1 IN V5 AND n IN V6_2 RETURN p2
                        rewritten_query += "MATCH " + matchQuery;
                        rewritten_query += " WHERE " + rewriteNonMaterializedWhereClause(whereClause);
                        rewritten_query += " RETURN DISTINCT(" + returnClause.split("\\s+")[1] + ")";
                        System.out.println(rewritten_query);

                        start = System.currentTimeMillis();
                        querySize = connector.excuteBasicNoneMaterializedQuery(rewritten_query);
                        duration = System.currentTimeMillis() - start;

                        System.out.println("Took " + duration + " ms to execute baseline query");
                        System.out.println("Non-materialized returned " + querySize + " elements");
                    }

                } else {

                    // Break up the input stream of characters into vocabulary symbols for a parser
                    ViewLexer VL = new ViewLexer(CharStreams.fromString(command));
                    CommonTokenStream tokens = new CommonTokenStream(VL);
                    ViewParser parser = new ViewParser(tokens);

                    ParseTree tree = parser.root();
                    walker.walk(vql, tree);

                    if (vql.isViewInstant()) {
                        long now = System.currentTimeMillis();
                        processMainView(command, materialized);
                        long total = System.currentTimeMillis() - now;
                        System.out.println("Took " + total + "ms to create views");
                    } else if (vql.isViewUse()) {
                        long now = System.currentTimeMillis();
                        processUseView(command);
                        long total = System.currentTimeMillis() - now;
                        System.out.println("Took " + total + "ms to use view");
                    } else if (vql.isCg()) {
                        long now = System.currentTimeMillis();
                        changeGraph(command);
                        long total = System.currentTimeMillis() - now;
                        System.out.println("Took " + total + "ms to change graph and update view(s)");
                    }

                    if (!debug) vql.clearAll();

                    System.out.print(">> ");
                    command = br.readLine();
                    continue;
                }
                System.out.print(">> ");
                command = br.readLine();
            }
            isReader.close();
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static String createPathQueryNonMaterializedTerm(String query) {
        String finalRes = query + " WITH ";
        List<String> nodeVars = getSubstringsBetweenCharacters(query, '(', ':');
        String pathNodeCollectTerm = "";
        for (String s: nodeVars) {
            pathNodeCollectTerm += "COLLECT(DISTINCT ID(" + s + ")) + ";
        }
        // Trimming the extra characters
        pathNodeCollectTerm = pathNodeCollectTerm.substring(0, pathNodeCollectTerm.length() - 2);

        finalRes += pathNodeCollectTerm;
        return finalRes;
    }

    public static String nonMaterializedIntermediateWithClause(String viewname, String[] views) {
        int index = 0;
        String res = "";

        for (int i = 0; i < views.length; i++) {
            if (views[i].equals(viewname)) {
                index = i;
                break;
            }
        }

        for (int i = 0; i < index; i++) {
            res += (views[i] + ",");
        }

        return res;
    }

    public static String rewriteNonMaterializedWhereClause(String s) {
        // Given an input like p1 IN V1 AND p2 IN V2 AND ... 
        // Return: ID(p1) IN V1 AND ID(p2) IN V2 AND

        String[] splitted = s.split("\\s+");
        String result = "";

        for (int i = 0; i < splitted.length; i++) {
            if (i < splitted.length - 1 && splitted[i+1].equals("IN")){
                result += "ID(" + splitted[i] + ") ";
            } else {
                result += splitted[i] + " "; 
            }
        }

        return result.trim();
    }

    public static void processMainView(String cmd, boolean materialized){

        String viewname = vql.getViewName();
        String fullQuery = cmd.split(viewname)[1];

        String returnSymbol = vql.getReturnValExpr();
        String mainQuery = fullQuery.split("RETURN")[0];
        String returns = fullQuery.split("RETURN")[1].trim();

        String[] returnArray = returns.split(",");

        String makeMiddlewareView = "";

        // Storing the view with its returnType
        typeTable.put(viewname, vql.getReturnType().toString());
        viewQueryTable.put(viewname, mainQuery.trim());
        viewReturnVarTable.put(viewname, returns.trim());

        // Building the makeMiddlewareView and fullQuery strings
        switch(vql.getReturnType()){

            case NODE:{
                fullQuery = mainQuery + "SET(CASE WHEN NOT EXISTS(" + returnSymbol + ".views) THEN " + returnSymbol +" END).views = []" +
                        " SET " + returnSymbol + ".views = (CASE WHEN \"" +viewname+ "\" IN " + returnSymbol +".views THEN [] ELSE [\"" + viewname + "\"] END) + " + returnSymbol + ".views";


                String[] returnSymbols = returnSymbol.split(",");

                String returnClause = "";
                for(String retSym : returnSymbols) {
                    for(String actualRet : returnArray){
                        if(!returnClause.equals("")) returnClause += ",";
                        if(retSym.equals(actualRet)) returnClause += "ID(" + retSym + ")";
                        else returnClause += actualRet;
                    }
                }
                makeMiddlewareView = mainQuery + "RETURN DISTINCT " + returnClause ;

                break;
            }
            case PATHNODES:{
                break;
            }
            case PATH: {
                fullQuery = mainQuery + "FOREACH(pathnode in nodes(" + returnSymbol + ") | SET(CASE WHEN NOT EXISTS(pathnode.views) THEN pathnode END).views = []" +
                        " SET pathnode.views = (CASE WHEN \"" +viewname+ "\" IN pathnode.views THEN [] ELSE [\"" + viewname + "\"] END) + pathnode.views)"
                        + "\nFOREACH(pathnode in relationships(" + returnSymbol + ") | SET(CASE WHEN NOT EXISTS(pathnode.views) THEN pathnode END).views = []" +
                        " SET pathnode.views = (CASE WHEN \"" +viewname+ "\" IN pathnode.views THEN [] ELSE [\"" + viewname + "\"] END) + pathnode.views)";


                makeMiddlewareView = mainQuery + "RETURN " + returnSymbol;

            }
            case DEFAULT: {
                break;
            }
        }

        System.out.println(fullQuery);

        // If materialized then just execute the query and return
        if(materialized) {
            connector.executeQuery(fullQuery);
            return;
        }

        // NODE VIEW: Call connector.executeQuery() with makeMiddlewareView as input and update nodeTable with the result nodes
        if(vql.getReturnType() == QueryParser.retType.NODE) {

            Set<String> nodes = connector.executeQuery(makeMiddlewareView);
            nodeTable.put(viewname, nodes);

            System.out.println("There are " + nodes.size() + " nodes");

        }

        // PATH VIEW: Call
        if(vql.getReturnType() == QueryParser.retType.PATH ){
            // Getting relationships of the path query
            Object[] processedPath = connector.pathQuery(makeMiddlewareView);

            // Mohanna: The following function call is changed by Mohanna to reduce duplicate work in pathQuery() and getPathQueryRelationships()
            Set<Relationship> relationshipSet = (Set<Relationship>) processedPath[0];
            String rlist = (String)processedPath[1];

            // Adding the reversed string to the actual string
            String result = "";
            String temp = "";
            StringBuilder resultSB = new StringBuilder();

            for (int i = rlist.length() - 1; i >= 0; i--) {

                String now = String.valueOf(rlist.charAt(i));

                if (!now.equals("-") & !now.equals(" ") ){
                    temp = rlist.charAt(i) + temp;
                } else {
                    resultSB.append(temp);
                    temp = "";
                    resultSB.append(rlist.charAt(i));
                }
            }
            resultSB.append(temp);
            result = resultSB.toString();

            rlist = rlist + " " + result;

            pathRelTable.put(viewname, rlist);
            pathTable.put(viewname, relationshipSet);

            Set<String> edgeids = new HashSet<String>();

            for(Relationship r : relationshipSet){
                edgeids.add(String.valueOf(r.getId()));
            }

            // Storing edges
            edgeTable.put(viewname, edgeids);

            Set<String> nodeids = connector.getNodeSet();

            System.out.println("There are " + nodeids.size() + " nodes");

            // Storing nodes
            nodeTable.put(viewname, nodeids);
        }

        // Uncomment for testing purposes
        /* System.out.println("Path Table is + " + pathTable.toString());
        System.out.println("Node Table is + " + nodeTable.toString());
        System.out.println("Edge Table is + " + edgeTable.toString());
        System.out.println("PathNode Table is + " + pathnodeTable.toString()); */

    }

    public static long processUseView(String cmd){

        long now = System.currentTimeMillis();

        String fullQuery = "MATCH " + cmd.split("MATCH")[1];

        System.out.println("FullQuery:: "+ fullQuery);
        System.out.println("cmd:: "+ cmd);

        List<String> edgeidentifiers = vql.relationSymbols();
        List<String> nodeidentifiers = vql.nodeSymbols();

        /*System.out.println("****************");
        for (String e: edgeidentifiers) {
            System.out.println(e);
        }
        System.out.println("****************");
        for (String e: nodeidentifiers) {
            System.out.println(e);
        }
        System.out.println("****************");*/

        // If view scope is LOCAL
        if(vql.getViewScope()) {

            System.out.println("Scope:LOCAL");
            //local, so there are omissions for set membership. if there are omissions then it has to be a single view usage

            String appendedToQuery = "";

            LinkedList<String> usedViews = vql.usedViews();

            if(usedViews.size()==1) {

                //There are definitely omissions

                String singleViewName =  usedViews.getFirst();

                //nodes
                for (String id : nodeidentifiers) { //we look at all node identifiers that reside in the query
                    appendedToQuery = appendedToQuery + " AND ID(" + id + ") IN " + nodeTable.get(singleViewName);
                }

                //edges
                for (String id : edgeidentifiers) {
                    appendedToQuery = appendedToQuery + " AND ID(" + id + ") IN " + edgeTable.get(singleViewName);
                }

                String beforeReturn = fullQuery.split("RETURN")[0];
                String afterReturn = " RETURN " + fullQuery.split("RETURN")[1];


                if (vql.containsWhere()) {
                    fullQuery = beforeReturn + appendedToQuery + afterReturn;
                } else {
                    appendedToQuery = appendedToQuery.replaceFirst("AND", "");
                    fullQuery = beforeReturn + "WHERE " + appendedToQuery + afterReturn;
                }
            }

            else if(usedViews.size()>1){
                //Then there are more than 2 views being used and we treat it as a global, since there are IN clauses
                for (String nodeName : vql.addWhereClause.keySet()) {

                    for (String viewName : vql.addWhereClause.get(nodeName)) {
                        String target = nodeName + " IN " + viewName;
                        String replacement = "ID(" + nodeName + ") IN " + nodeTable.get(viewName);

                        fullQuery = fullQuery.replace(target, replacement);

                    }
                }
            }
        }

        else {
            System.out.println("Scope:GLOBAL");

            for (String nodeName : vql.addWhereClause.keySet()) {
                for (String viewName : vql.addWhereClause.get(nodeName)) {

                    String target = nodeName + " IN " + viewName;
                    String replacement = "ID(" + nodeName + ") IN " + nodeTable.get(viewName);

                    fullQuery = fullQuery.replace(target, replacement);

                }
            }
        }

        if(fullQuery.contains("IN null")){
            System.out.println("Nothing in view");
            return 0l;
        }

        System.out.println(fullQuery.length());

        File logger = new File("./test/log.txt");
        try{
            FileWriter l = new FileWriter(logger);
            l.write(fullQuery);
        }
        catch(Exception e) {e.printStackTrace();}

        // edit start from here
        // uncomment it if not do path view

        LinkedList<String> usedViews = vql.usedViews();
        String singleViewName =  usedViews.getFirst();
        
        // Only checking the path pattern correctness if the USE query itsels has a path pattern in it
        if (typeTable.get(singleViewName).equals("PATH") & cmd.contains("p=")){              // need to have "p=" in order to check for correct path
            System.out.println("In the correctness block");
            String beforeReturn = fullQuery.split("RETURN")[0];
            String getPathNodeListQuery = beforeReturn + " RETURN DISTINCT [n IN relationships(p) | id(n)] as path, p";
            String nlist = connector.getPathQueryRelationships(getPathNodeListQuery);

            System.out.println("Check correctness");
            System.out.println("nodeList: "+nlist);

            String[] sublist = nlist.split(" ");


            String fullpath = pathRelTable.get(singleViewName);
            if ((sublist.length != 1)){
                for (int i = 0 ; i<sublist.length; i++){
                    if (!fullpath.contains(sublist[i])){
                        System.out.println(i + " path is not correct");
                    }else{
                        System.out.println(i + " path is correct");
                    }
                }
            }


        }
        else{
            System.out.println("Before calling execute query");
            connector.executeQuery(fullQuery);
            System.out.println("After the call to execute query");
        }

        // edit end from here

        return System.currentTimeMillis()-now;

    }

    public static void changeGraph(String command){
        //if this is called, then the change-graph has already walked through the parser
        System.out.println(vql.getFinalAffectedViews());

        Set<String> instantiations = new HashSet<>();

        for(String cmd : vql.outdatedViews){ //re-evaluate all necessary instants...

            instantiations.add(cmd);

        }
        vql.clearAll();
        for(String cmd : instantiations){
            vql.viewInstants.remove(cmd);
            ViewLexer VL = new ViewLexer(CharStreams.fromString(cmd));
            CommonTokenStream tokens = new CommonTokenStream(VL);
            ViewParser parser = new ViewParser(tokens);

            ParseTree tree = parser.root();
            walker.walk(vql, tree);

            long now = System.currentTimeMillis();
          //  processMainView(cmd, false); //todo uncomment. this actually re-evals but for correctness I just want to know what views are being re-evald
          //  System.out.println("TIME TO RE-EVAL VIEW: " + (System.currentTimeMillis()-now));

            totalTime += System.currentTimeMillis()-now;
        }

        System.out.println("TOTAL TIME FOR ALL: " + totalTime);
        totalTime = 0;

        vql.resetAfterGraphUpdate();

    }

    public static List<String> getSubstringsBetweenCharacters(String input, char startChar, char endChar) {
        List<String> substrings = new ArrayList<>();
        int startIndex = -1;
        
        for (int i = 0; i < input.length(); i++) {
            if (input.charAt(i) == startChar) {
                startIndex = i;
            } else if (input.charAt(i) == endChar && startIndex != -1) {
                substrings.add(input.substring(startIndex + 1, i));
                startIndex = -1;
            }
        }
        
        return substrings;
    }

    // TODO: not sure if this function is called at all
   public static <T> List<Set<T>> split(Set<T> original, int count) {
        // Create a list of sets to return.
        List<Set<T>> result = new ArrayList<Set<T>>(count);

        // Create an iterator for the original set.
        Iterator<T> it = original.iterator();

        // Calculate the required number of elements for each set.
        int each = original.size() / count;

        // Create each new set.
        for (int i = 0; i < count; i++) {
            HashSet<T> s = new HashSet<T>(original.size() / count + 1);
            result.add(s);
            for (int j = 0; j < each && it.hasNext(); j++) {
                s.add(it.next());
            }
        }
        return result;
    }

}