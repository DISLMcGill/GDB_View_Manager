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
            // TODO: The below two function calls should be removed [if not used]
            //loadTablesFromFiles(size);
            //createMetaInfoFromQueries("./test/initFileExample.txt");

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
                        experiment_terminal(cmd);
                        System.out.println("*********************************");
                    }
                }
            } else if (args.length == 2 && args[0].equals("cold")) {
                String cmd = args[1];
                // This is to warm up the system with bringing the post index into the memory
                //String simplecmd = "baseline test MATCH (n:Post) WHERE n.postId = '3468801' RETURN n";
                String simplecmd = "CREATE VIEW AS V_test MATCH (n:Post) WHERE n.postId = '3468801' RETURN n";
                experiment_terminal(simplecmd);
                System.out.println(cmd);
                experiment_terminal(cmd);
            } else if (args.length == 2 && (args[0].equals("method1") || args[0].equals("method2"))) {
                String fileName = args[1];
                ArrayList<String> commands_in_order = getExperimentCommands(fileName);
 
                for (String cmd: commands_in_order) {
                    if (cmd.equals("quit")) {
                        break;
                    }
                    else {
                        System.out.println(cmd);
                        experiment_terminal(cmd);
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
                        experiment_terminal("CREATE VIEW AS " + line);
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
                experiment_terminal(cmd);
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
                        experiment_terminal("CREATE VIEW AS " + line);
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
                experiment_terminal(cmd);
            } else if (args.length == 2 && args[0].equals("test")) {
                // add an argument to an existing node
                String query = "baseline test MATCH (n:Post) WHERE n.postId = '3468801' RETURN n";
                experiment_terminal(query);
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
    private static void experiment_terminal(String command) {
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
                        // TODO: Should a basic match also considers the edges?                             
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
                    //processMainViewMethod1(command, materialized);
                    long total = System.currentTimeMillis() - now;
                    System.out.println("Took " + total + "ms to create views");
                } else if (vql.isViewUse()) {
                    long now = System.currentTimeMillis();

                    // Changed temporarily
                    processUseView(command);
                    //processUseViewMethod2(command);

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
                            // TODO: Should a basic match also considers the edges?                             
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
                        // Changed temporarily
                        processMainView(command, materialized);
                        //processMainViewMethod1(command, materialized);

                        long total = System.currentTimeMillis() - now;
                        System.out.println("Took " + total + "ms to create views");
                    } else if (vql.isViewUse()) {
                        long now = System.currentTimeMillis();

                        // Changed temporarily
                        processUseView(command);
                        //processUseViewMethod2(command);

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

            // TODO: Should we be worried about the deprecated version
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

    public static void processMainViewMethod1(String cmd, boolean materialized) {
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
    
                // Create a new list property for each node or edge which contains all the view the element is a member of
                makeMiddlewareView = mainQuery + " SET " + returnSymbol + ".viewname = CASE WHEN " + returnSymbol + 
                    ".viewname IS NULL THEN ['" + viewname + "'] WHEN NOT ANY(v IN " +  returnSymbol + 
                    ".viewname WHERE v = '" + viewname + "') THEN " + returnSymbol + ".viewname + ['" + viewname + 
                    "'] ELSE " + returnSymbol + ".viewname" + " END RETURN DISTINCT " + returnClause;

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


                //makeMiddlewareView = mainQuery + "RETURN " + returnSymbol;
                makeMiddlewareView = mainQuery + " FOREACH(pathnode in nodes(" + returnSymbol + 
                ") | SET pathnode.viewname = CASE WHEN pathnode.viewname IS NULL THEN ['" + viewname + 
                "'] WHEN NOT '" + viewname + "' IN pathnode.viewname THEN pathnode.viewname + ['" + 
                viewname + "'] ELSE pathnode.viewname END ) FOREACH(pathrel in relationships(" + returnSymbol + 
                ") | SET pathrel.viewname = CASE WHEN pathrel.viewname IS NULL THEN ['" + viewname + 
                "'] WHEN NOT '" + viewname + "' IN pathrel.viewname THEN pathrel.viewname + ['" + 
                viewname + "'] ELSE pathrel.viewname END ) RETURN " + returnSymbol;

            }
            case DEFAULT: {
                break;
            }
        }

        System.out.println(makeMiddlewareView);

        // If materialized then just execute the query and return
        if(materialized) {
            connector.executeQuery(fullQuery);
            return;
        }

        // No information will be stored in the middleware. Everything is on the graph!
        if(vql.getReturnType() == QueryParser.retType.NODE) {

            int resCount = connector.createViewOnGraph(makeMiddlewareView);
            System.out.println("There are " + resCount + " nodes");

        }

        // TODO: Currently no handling of the path patterns
        if(vql.getReturnType() == QueryParser.retType.PATH ){
            // Getting relationships of the path query
            int resCount  = connector.createViewOnGraph(makeMiddlewareView);
            System.out.println("There are " + resCount + " elements");
        }
    }

    // TODO Support for the first method only for local use queries for now
    public static long processUseViewMethod1(String cmd){

        long now = System.currentTimeMillis();

        String fullQuery = "MATCH " + cmd.split("MATCH")[1];

        System.out.println("FullQuery:: "+ fullQuery);
        System.out.println("cmd:: "+ cmd);

        List<String> edgeidentifiers = vql.relationSymbols();
        List<String> nodeidentifiers = vql.nodeSymbols();

        // If view scope is LOCAL
        if(vql.getViewScope()) {

            System.out.println("Scope:LOCAL");
            //local, so there are omissions for set membership. if there are omissions then it has to be a single view usage

            String appendedToQuery = "";

            LinkedList<String> usedViews = vql.usedViews();

            // LOCAL Use with only 1 view
            if(usedViews.size()==1) {

                String singleViewName =  usedViews.getFirst();

                //nodes
                for (String id : nodeidentifiers) { //we look at all node identifiers that reside in the query
                    appendedToQuery = appendedToQuery + " AND '" + singleViewName + "' IN " + id + ".viewname";
                }

                //edges
                for (String id : edgeidentifiers) {
                    appendedToQuery = appendedToQuery + " AND '" + singleViewName + "' IN " + id + ".viewname";
                }

                String beforeReturn = fullQuery.split("RETURN")[0];
                String afterReturn = " RETURN" + fullQuery.split("RETURN")[1];


                if (vql.containsWhere()) {
                    fullQuery = beforeReturn + appendedToQuery + afterReturn;
                } else {
                    appendedToQuery = appendedToQuery.replaceFirst("AND", "");
                    fullQuery = beforeReturn + "WHERE " + appendedToQuery + afterReturn;
                }

                System.out.println("Full query: " +fullQuery);
            }

            // LOCAL Use with more than 1 views
            else if(usedViews.size()>1) {
                //Then there are more than 2 views being used and we treat it as a global, since there are IN clauses
                for (String nodeName : vql.addWhereClause.keySet()) {

                    for (String viewName : vql.addWhereClause.get(nodeName)) {
                        String target = nodeName + " IN " + viewName;
                        String replacement = "'" + viewName + "' IN " + nodeName + ".viewname";

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

        connector.executeQuery(fullQuery);
        
        // Mohanna: commented for testing
        // Only checking the path pattern correctness if the USE query itsels has a path pattern in it
        /*if (typeTable.get(singleViewName).equals("PATH") & cmd.contains("p=")){              // need to have "p=" in order to check for correct path
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
            connector.executeQuery(fullQuery);
        }*/

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

    /**
     *
     * EVERYTHING BELOW THIS POINT IS THE TESTER CODE. SORRY BAD PRACTICE SHOULD HAVE IT AS ANOTHER CLASS
     *
     *
     * */

    public static long noGuiTest(String command, boolean materialized) {

        long time = 0l;

        if (command.startsWith("printDependencies")) {
            vql.printDependencies();
        } else if (command.startsWith("printNode")) {
            System.out.println(nodeTable.toString());
        } else if (command.startsWith("clear")) {
            vql.clearAll();
        } else if (command.startsWith("wipe")) {
            connector.executeQuery("MATCH (n) REMOVE n.views");
        } else {


            ViewLexer VL = new ViewLexer(CharStreams.fromString(command));
            CommonTokenStream tokens = new CommonTokenStream(VL);
            ViewParser parser = new ViewParser(tokens);



            ParseTree tree = parser.root();
            walker.walk(vql, tree);


            if (vql.isViewInstant()) {
                long now = System.currentTimeMillis();
                processMainView(command, materialized);
                long total = System.currentTimeMillis() - now;
               // System.out.println("Took " + total + "ms to create views");
            } else if (vql.isViewUse()) {
                time = processUseView(command);
               // System.out.println("Took " + total + "ms to use view");
            } else if (vql.isCg()) {
                long now = System.currentTimeMillis();
                changeGraph(command);
                long total = System.currentTimeMillis() - now;
              //  System.out.println("Took " + total + "ms to change graph and update view(s)");
            }

        }

        vql.clearAll();
        return time;
    }

    public static void testTextCopy(){


        String test = "MATCH (n:Post) WHERE n.score > 350 WITH COLLECT(ID(n)) as view1 MATCH (n:User) WHERE ID(n) IN view1 RETURN ID(n)";
        String test2 = "MATCH p = (n:User)-[:POSTED]-(po:Post)-[:PARENT_OF]-(po2:Post) WHERE n.reputation > 90000 AND po2.score>100 UNWIND NODES(p) as vn UNWIND RELATIONSHIPS(p) as vr with distinct vn, vr WITH collect(id(vn)) as viewnode, COLLECT(id(vr)) as viewrel MATCH (n) WHERE ID(n) IN viewnode RETURN ID(n)";
        String test3 = "MATCH (n:User)-[:POSTED]-(po:Post)-[:PARENT_OF]-(po2:Post) WHERE n.upvotes > 800 AND po2.score < 0 WITH COLLECT(ID(po2)) as view1 MATCH (n:User) WHERE ID(n) IN view1 RETURN ID(n)";
        String test4 = "MATCH (n:Post) WHERE n.score > 350 WITH COLLECT(ID(n)) as view1 MATCH p = (n:User)-[:POSTED]-(po:Post)-[:PARENT_OF]-(po2:Post) WHERE n.reputation > 90000 AND po2.score>100 WITH view1,p UNWIND NODES(p) as vn UNWIND RELATIONSHIPS(p) as vr with distinct vn, vr,view1, collect(id(vn)) as viewnode, COLLECT(id(vr)) as viewrel MATCH (n) WHERE ID(n) IN viewnode AND ID(n) IN view1 RETURN ID(n)";
        String test5 = "MATCH (n:User)-[:POSTED]-(po:Post)-[:PARENT_OF]-(po2:Post) WHERE n.upvotes > 800 AND po2.score < 0 WITH COLLECT(ID(po2)) as view1 MATCH p = (n:User)-[:POSTED]-(po:Post)-[:PARENT_OF]-(po2:Post) WHERE n.reputation > 90000 AND po2.score>100 WITH view1,p UNWIND NODES(p) as vn UNWIND RELATIONSHIPS(p) as vr with distinct vn, vr,view1, collect(id(vn)) as viewnode, COLLECT(id(vr)) as viewrel MATCH (n) WHERE ID(n) IN viewnode AND ID(n) IN view1 RETURN ID(n)";


        connector.executeQuery(test);

        long ret1time = 0;
        for (int i = 0; i < 5; i++) {
            long now = System.currentTimeMillis();
            connector.executeQuery(test);
            ret1time += System.currentTimeMillis() - now;

        }
        ret1time /= 5;

        System.out.println(ret1time + "LOOK HERE");

        connector.executeQuery(test2);

        long ret2time = 0;
        for (int i = 0; i < 5; i++) {
            long now = System.currentTimeMillis();
            connector.executeQuery(test2);
            ret2time += System.currentTimeMillis() - now;

        }
        ret2time /= 5;
        System.out.println(ret2time + "LOOK HERE");

        connector.executeQuery(test3);
        long ret3time = 0;
        for (int i = 0; i < 5; i++) {
            long now = System.currentTimeMillis();
            connector.executeQuery(test3);

            ret3time += System.currentTimeMillis() - now;

        }
        ret3time /= 5;
        System.out.println(ret3time + "LOOK HERE");

        connector.executeQuery(test4);

        long ret4time = 0;
        for (int i = 0; i < 5; i++) {
            long now = System.currentTimeMillis();
            connector.executeQuery(test4);
            ret4time += System.currentTimeMillis() - now;

        }
        ret4time /= 5;
        System.out.println(ret4time + "LOOK HERE");


        connector.executeQuery(test5);
        long ret5time = 0;
        for (int i = 0; i < 5; i++) {
            long now = System.currentTimeMillis();
            connector.executeQuery(test5);
            ret5time += System.currentTimeMillis() - now;

        }
        ret5time /= 5;


        System.out.println("Times: " + ret1time + ", " + ret2time + ", " + ret3time + ", " + ret4time + ", " + ret5time);



    }

    public static void toFile(String size, String viewName){ //todo rename test3 to test as test3 and test2 are no longer used
        File tableNodes = new File("./test3/" + size + "/" +viewName + "Node.txt");
        File tableRels = new File("./test3/"  + size + "/" +viewName + "Rel.txt");

        try {
            FileWriter nodes = new FileWriter(tableNodes);
            FileWriter rels = new FileWriter(tableRels);


            nodes.write(viewName + ": " + nodeTable.get(viewName) + "\n");
            rels.write(viewName + ": " + edgeTable.get(viewName) + "\n");

            nodes.flush();
            rels.flush();

            nodes.close();
            rels.close();

        }
        catch(Exception e){
            e.printStackTrace();
        }


    }

    // TODO: Is it even getting called in this version?
    public static void loadTablesFromFiles(String size){

        File directory = new File("./test/"+size);

        try {
            for (File fileEntry : directory.listFiles()) {


                String name = fileEntry.getName();
                System.out.println(name);
                String viewName = "";
                if (name.endsWith("Node.txt")) {
                    viewName = name.split("Node\\.txt")[0];

                    Scanner myReader = new Scanner(fileEntry);
                    if(!myReader.hasNextLine()) continue;
                    String line = myReader.nextLine();
                    if(line.contains("null")) continue;
                    if(line.contains("[]")) {
                        nodeTable.put(viewName,new HashSet<>());
                        continue;
                    }

                    String entries = line.split("\\[")[1].split("\\]")[0];
                    entries = entries.trim();

                    Set<String> allEntries =  new HashSet<>(Arrays.asList(entries.split(",")));


                    nodeTable.put(viewName, allEntries);


                }
                if (name.endsWith("Rel.txt")) {
                    viewName = name.split("Rel\\.txt")[0];

                    Scanner myReader = new Scanner(fileEntry);
                    if(!myReader.hasNextLine()) continue;

                    String line = myReader.nextLine();
                    if(line.contains("null")) continue;

                    if(line.contains("[]")) {
                        edgeTable.put(viewName,new HashSet<>());
                        continue;
                    }

                    String entries = line.split("\\[")[1].split("\\]")[0];

                    Set<String> allEntries =  new HashSet<>(Arrays.asList(entries.split(",")));

                    edgeTable.put(viewName, allEntries);

                }

            }
            //todo
        }
        catch(Exception e){
            e.printStackTrace();
        }


    }

    // TODO: Is it even getting called in this version?
    public static void createMetaInfoFromQueries(String viewPath){

        File file = new File(viewPath);
        try {
            Scanner myReader = new Scanner(file);

            while (myReader.hasNextLine()) {

                String line = myReader.nextLine();
                if (line.startsWith("*") || line.startsWith(" ") || line.startsWith("\n")) continue;
                String query = "CREATE VIEW AS " + line.split(";")[0] + " " + line.split(";")[1];

                String viewName = line.split(";")[0];

                System.out.println(query);

                if (query.contains("IN null")) continue;

                ViewLexer VL = new ViewLexer(CharStreams.fromString(query));
                CommonTokenStream tokens = new CommonTokenStream(VL);
                ViewParser parser = new ViewParser(tokens);


                ParseTree tree = parser.root();
                walker.walk(vql, tree);
                vql.clearAll();

            }

            myReader.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }

    }

    public static void testUses(String size){

        loadTablesFromFiles(size); //load first
        //second, fill the meta data tables in vql, needed for the changegraph tests
        try {
            createMetaInfoFromQueries("./test/ViewInits.txt");
            createMetaInfoFromQueries("./test/initFileExample.txt");

            System.out.println("Done populating all tables - moving on to each use");

            File uses = new File("./test/ViewUses.txt");
            Scanner useReader = new Scanner(uses);


            Map<String, Long> durations = new HashMap<>();




            while(useReader.hasNextLine()){
                String line = useReader.nextLine();
                if(line.startsWith("*") || line.startsWith(" ") || line.startsWith("\n")) continue;
                durations.put(line, 0l);
            }

            Set<String> queries = durations.keySet();
            for(int i=0; i<1; i++) {
                for (String q : queries) {

                    durations.put(q, noGuiTest(q, false) + durations.get(q));
                    System.out.println("Query:\n"+q+"\nAvg:\t"+durations.get(q));

                }
            }

        }
        catch(Exception e){
            e.printStackTrace();
        }

    }

    // TODO: Remove these unused functions
    //To test and validate initialization times for views, where view definition queries are stored in fileName under the example format.
    public static void initFile2(String fileName, String size){

        File file = new File(fileName);

        Map<String, Long> durations = new HashMap<>();

        try {
            Scanner myReader = new Scanner(file);

            while(myReader.hasNextLine()){

                String line = myReader.nextLine();
                if(line.startsWith("*") || line.startsWith(" ") || line.startsWith("\n")) continue;
                String query = "CREATE VIEW AS " + line.split(";")[0] + " " + line.split(";")[1];

                String viewName = line.split(";")[0];

                System.out.println(query);

                durations.put(query, 0l);

            }

            myReader.close();

            Set<String> queries = durations.keySet();

            for(int i=0; i<10; i++) {
                for (String q : queries) {
                    long now = System.currentTimeMillis();

                    noGuiTest(q, false);

                    if(i==0) toFile(size, q.split("AS")[1].trim().split(" ")[0].trim());

                    long after = System.currentTimeMillis();

                    durations.put(q, (after - now) + durations.get(q));

                }
            }


            String finalLine = "";
            for(String q : queries){
                durations.put(q, (durations.get(q))/10);
                finalLine += "Query:\n"+q+"\nAvg:\t"+durations.get(q)+"\n";
            }

            FileWriter toInit = new FileWriter(new File("./test/inits"+size));
            toInit.write(finalLine);
            toInit.flush();
            toInit.close();



        }
        catch(Exception e){
            e.printStackTrace();
        }

    }

    public static void textBasedSmart(String queryName){

        long now = System.currentTimeMillis();
        connector.executeQuery(queryName);
        System.out.println(System.currentTimeMillis()-now);

    }

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

    public static void validateReturn(String size){

        testUses(size);
        System.out.println("next;");
//        countQuery("MATCH (n:User) WHERE n.upvotes > 1000 RETURN COUNT(DISTINCT n) as c"); //u 10
//        System.out.println("next;");




//        countQuery("MATCH (n:Post) WHERE n.score > 350 RETURN COUNT(n) as c"); //1
//        countQuery("MATCH (n:Post) WHERE n.score > 350 WITH COLLECT(ID(n)) AS v MATCH (n) WHERE ID(n) IN v RETURN COUNT(n) as c"); // U1

//        countQuery("MATCH (n:Post) WHERE n.score < 800 AND n.score > 350 RETURN COUNT(n) as c"); //2
//        countQuery("MATCH (n:Post) WHERE n.score < 800 AND n.score > 350 WITH COLLECT(ID(n)) AS v MATCH (n) WHERE ID(n) IN v RETURN COUNT(n) as c"); //U2

//        countQuery("MATCH (n:User) WHERE n.upvotes > 1000 RETURN COUNT(n) as c"); //3
//        countQuery("MATCH (n:User) WHERE n.upvotes > 1000 WITH COLLECT(ID(n)) AS v MATCH (n) WHERE ID(n) IN v RETURN COUNT(n) as c"); //U3

//        countQuery("MATCH (n:User) WHERE n.reputation > 90000 RETURN COUNT(n) as c"); //4
//        countQuery("MATCH (n:User) WHERE n.reputation > 90000 WITH COLLECT(ID(n)) AS v MATCH (n) WHERE ID(n) IN v RETURN COUNT(n) as c"); //U4


//        countQuery("MATCH (n:User)-[:POSTED]-(po:Post)-[:PARENT_OF]-(po2:Post) WHERE n.upvotes > 800 AND po.comments > 10  RETURN COUNT(po2) as c"); //6
//        countQuery("MATCH (n:User)-[:POSTED]-(po:Post)-[:PARENT_OF]-(po2:Post) WHERE n.upvotes > 800 AND po.comments > 10 WITH COLLECT(ID(po2)) AS v MATCH (n) WHERE ID(n) IN v RETURN COUNT(n) as c"); //6

//        countQuery("MATCH (n:Post)-[:PARENT_OF]-(m:Post) WHERE m.score > 100 AND m.score < 600 RETURN COUNT(n) as c"); //7
//        countQuery("MATCH (n:Post)-[:PARENT_OF]-(m:Post) WHERE m.score > 100 AND m.score < 600 WITH COLLECT(ID(n)) AS v MATCH (n) WHERE ID(n) IN v RETURN COUNT(n) as c"); //7

//        countQuery("MATCH (n:Tag) WHERE n.tagId = 'java' OR n.tagId = 'html' RETURN COUNT(n) as c"); //8
//        countQuery("MATCH (n:Tag) WHERE n.tagId = 'java' OR n.tagId = 'html'  WITH COLLECT(ID(n)) AS v MATCH (n) WHERE ID(n) IN v RETURN COUNT(n) as c"); // u8

//        countQuery("MATCH (n:User)-[:POSTED]-(po:Post)-[:PARENT_OF]-(po2:Post) WHERE n.upvotes > 800 AND po.comments > 10 AND po2.score > 350 RETURN COUNT(n) as c"); // use 9
        //not smart, actually stupid
//        countQuery("MATCH (n:Post) \n" +
//                "    WHERE n.score > 350 \n" +
//                "    WITH COLLECT(ID(n)) as V1\n" +
//                "    \n" +
//                "    MATCH (n:User)-[:POSTED]-(po:Post)-[:PARENT_OF]-(po2:Post) \n" +
//                "    WHERE n.upvotes > 800 AND po.comments > 10  \n" +
//                "    WITH V1, COLLECT(ID(po2)) as V8\n" +
//                "    \n" +
//                "    MATCH (n) WHERE ID(n) IN V1 AND ID(n) IN V8\n" +
//                "    RETURN COUNT(n) as c"); //U9

//        countQuery("MATCH (n:User)-[:POSTED]-(p:Post) WHERE n.upvotes > 1000 RETURN COUNT(DISTINCT p) as c"); //u 10
//        countQuery("MATCH (n:User) WHERE n.upvotes > 1000 WITH COLLECT(ID(n)) AS v MATCH (n)-[:POSTED]-(p:Post) WHERE ID(n) IN v RETURN COUNT(DISTINCT p) as c"); //u10

//        countQuery("MATCH (n:User)-[:POSTED]-(p:Post)-[:PARENT_OF]-(m:Post) WHERE n.upvotes > 1000 AND m.score > 100 AND m.score < 600 AND n.reputation > 2*p.score OR n.downvotes < p.score RETURN COUNT(n) as c"); //12
//        countQuery("MATCH (n:User) WHERE n.upvotes > 1000 WITH COLLECT(ID(n)) AS v1 " +
//                    "MATCH (n:Post)-[:PARENT_OF]-(m:Post) WHERE m.score > 100 AND m.score < 600 " +
//                            " WITH v1, COLLECT(ID(n)) AS v2 " +
//                            " MATCH (n)-[:POSTED]-(p:Post) WHERE ID(n) IN v1 AND ID(p) IN v2 " +
//                            "AND n.reputation > 2*p.score OR n.downvotes<p.score RETURN COUNT(n) as c"); //u12



//        countQuery("MATCH (n:Post) WHERE n.score > 350 RETURN COUNT(n) as c"); // v1
//        countQuery("MATCH (n:Post) WHERE n.score < 800 AND n.score > 350 RETURN COUNT(n) as c"); //v2
//        countQuery("MATCH (n:User) WHERE n.upvotes>1000 RETURN COUNT(n) as c"); //v3
//        countQuery("MATCH (n:User) WHERE n.reputation > 90000 RETURN COUNT(n) as c"); //U4
//        countQuery("MATCH p = (n:User)-[:POSTED]-(po:Post) WHERE n.reputation < 500 RETURN COUNT(n) as c"); //5 nodes
//        countQuery("MATCH p = (n:User)-[:POSTED]-(po:Post) WHERE n.reputation < 500 RETURN COUNT(p) as c"); //5 paths
//        countQuery("MATCH (n:User)-[:POSTED]-(po:Post)-[:PARENT_OF]-(po2:Post) WHERE n.upvotes > 800 AND po.comments > 10  RETURN COUNT(po2) as c"); //6
//        countQuery("MATCH (n:Post)-[:PARENT_OF]-(m:Post) WHERE m.score > 100 AND m.score < 600 RETURN COUNT(n) as c"); //7
//        countQuery("MATCH (n:User)-[:POSTED]-(p:Post) WHERE n.userId = 19 RETURN COUNT(p) as c"); // u8
//        countQuery("MATCH (n:Tag) WHERE n.tagId = 'java' OR n.tagId = 'html' RETURN COUNT(n) as c"); //U9
//        countQuery("MATCH (n:User)-[:POSTED]-(p:Post) WHERE n.upvotes > 1000 OR p.score > 350 RETURN COUNT(p) as c"); //u10
//        countQuery("MATCH (betterPost:Post)-[:PARENT_OF]-(worstPost:Post) WHERE worstPost.score < 10 AND betterPost.score > worstPost.score * 10 RETURN COUNT(betterPost) as c"); //u11
//        System.out.print("12 result:");
//        countQuery("MATCH (p1:Post)-[:HAS_TAG]-(t:Tag) WITH p1, COUNT(t) as numberOfTags WHERE numberOfTags > 20 RETURN numberOfTags as c"); //12 result
//        countQuery("MATCH (p1:Post)-[:HAS_TAG]-(t:Tag) RETURN COUNT(p1) as c"); //12 paths
//        countQuery("MATCH (p1:Post)-[:HAS_TAG]-(t:Tag) WITH p1, COUNT(t) as numberOfTags WHERE numberOfTags > 20 RETURN COUNT(p1) as c");

//        countQuery("MATCH (n:User)-[:POSTED]-(p:Post)-[:HAS_TAG]-(t:Tag) RETURN COUNT(n) as c"); //13 paths
//        countQuery("MATCH (n:User)-[:POSTED]-(p:Post)-[:HAS_TAG]-(t:Tag) WITH n,t, COUNT(*) as numberOfPosts WITH n,COLLECT(t) as tags, COLLECT(numberOfPosts) as counts, MAX(numberOfPosts) as highestTagCount WITH n,highestTagCount, [i IN range(0, size(counts)-1) | CASE WHEN counts[i] = highestTagCount THEN tags[i] ELSE NULL END] AS finalVal RETURN COUNT(n) as c");



    }

    public static void countQuery(String q){

        connector.counts(q);

    }
}