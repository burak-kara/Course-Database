/********************************************************************
* Authors;
*	Onur KÝRMAN 	S009958 - Electrical & Electronics Engineering
*	Burak KARA		S009893 - Computer Science
*********************************************************************
* PLEASE DO NOT FORGET TO READ THE README.TXT FILE PROVIDED IN ZIP
* IN CASE OF ERRORS WE ADDED ALL POSSIBLE CASES TO SOLVE IT,
* WITH ADDITION TO PROBLEMS WE FACED THROUGH OUT PROJECT DEVELOPMENT.
* ANY OTHER EXPLANATION & CONTACT INFORMATION IS ALSO ADDED.
********************************************************************/

package client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;

import connection.DBConnection;

public class Main {
    private static DBConnection database_connection;

    public static void main(String[] args) {
        instantiateJDBC();
        checkArgs(args);
        try {
            connectDataBase(args);
            main_loop();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (database_connection != null) {
                database_connection.close();
            }
        }
    }

    private static void instantiateJDBC() {
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (InstantiationException | ClassNotFoundException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    private static void checkArgs(String[] args) {
        if (args.length > 3) return;
        System.err.println("Wrong number of arguments!");
        System.err.println("Usage: Main hostname database username password");
        System.exit(0);
    }

    private static void connectDataBase(String[] args) throws Exception {
        String url = String.format("jdbc:mysql://%s/%s?user=%s&password=%s&useSSL=false", args[0], args[1], args[2], args[3]);
        database_connection = new DBConnection(url);
    }

    private static void readData() {
        database_connection.readFiles("sources/farmers.csv");
        database_connection.readFiles("sources/products.csv");
        database_connection.readFiles("sources/markets.csv");
        database_connection.readFiles("sources/produces.csv");
        database_connection.readFiles("sources/buys.csv");
        database_connection.readFiles("sources/registers.csv");
    }

    private static void main_loop() {
        System.out.println("Command line interface is initiated");
        System.out.println("Do not forget to create tables");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String buffer;
        try {
            while (true) {
            	printUsage();
                buffer = reader.readLine();
                if (buffer.equalsIgnoreCase("exit") || buffer.equalsIgnoreCase("quit")) {
                    // we are done
                    System.out.println("Command line interface is closed.");
                    return;
                } else {
                    parse_commands(buffer);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void parse_commands(String buffer) {
        if (buffer == null) {
            System.err.println("There is nothing to parse! Something is wrong in the commands.");
            return;
        }

        // First split by empty space
        String[] subCommands = buffer.split(" ");

        if (subCommands.length < 2) {
            System.err.println("Wrong command!");
            printUsage();
            return;
        }

        String first_part = subCommands[0];
        // Handle any possible space between ADD|REGISTER FARMER (...)
        // TODO original handler
        //String second_part = subCommands.length != 2 ? subCommands[1] + subCommands[2] : subCommands[1];

        // We have to handle the case where there are space in parentheses
        StringBuilder second_part = new StringBuilder(subCommands[1]);
        if (subCommands.length != 2) {
            for (int i = 2; i < subCommands.length; i++)
                second_part.append(subCommands[i]);
        }

        if (first_part.equalsIgnoreCase("show")) {
            if (second_part.toString().equalsIgnoreCase("tables")) {
                ResultSet resultSet = database_connection.send_query("SHOW TABLES");
                printResultSet(resultSet);
                System.out.println("Showing Tables Done!");
            } else {
                System.err.println("Wrong command!");
                printUsage();
            }
        } else if (first_part.equalsIgnoreCase("load")) {
            if (second_part.toString().equalsIgnoreCase("data")) {
                readData();
                System.out.println("Data Load Done!");
            } else {
                System.err.println("Wrong command!");
                printUsage();
            }
        } else if (first_part.equalsIgnoreCase("query")) {
            int query_number = Integer.parseInt(second_part.toString());
            String query = null;
            switch (query_number) {
                case 1:
                    query = "SELECT P.fname, P.flastname, P.pname\n" +
                            "FROM (SELECT pname, MAX(amount) AS maxAmount\n" +
                            "FROM produces GROUP BY pname) AS TMP, produces P\n" +
                            "WHERE P.pname = TMP.pname AND P.amount = TMP.maxAmount";
                    break;
                case 2:
                    query = "SELECT B.pname, B.fname, B.flastname\n" +
                            "FROM (SELECT pname, MAX(amount) AS maxAmount\n" +
                            "FROM buys GROUP BY pname) AS TMP, buys B\n" +
                            "WHERE B.pname = TMP.pname AND B.amount = TMP.maxAmount";
                    break;
                case 3:
                    query = "SELECT FF.fname, FF.flastname\n" +
                            "FROM (SELECT R.fname, R.flastname, R.pname, (TMP.amount*R.price) as cash\n" +
                            "FROM (SELECT fname, flastname, pname, MAX(amount) as amount FROM \n" +
                            "buys GROUP BY fname,flastname) AS TMP, registers R\n" +
                            "WHERE R.fname = TMP.fname AND R.flastname = TMP.flastname AND \n" +
                            "R.pname = TMP.pname\n" +
                            "ORDER BY cash DESC\n" +
                            "LIMIT 1) AS FF;";
                    break;
                case 4:
                    query = "SELECT M.city, M.name, M.addr\n" +
                            "FROM (SELECT city, name, MAX(budget) AS maxBudget\n" +
                            "FROM market GROUP BY city) AS TMP, market M\n" +
                            "WHERE M.city = TMP.city AND M.budget = TMP.maxBudget";
                    break;
                case 5:
                    query = "SELECT F.numFarmer + M.numMarket as Total_Users\n" +
                            "FROM (SELECT COUNT(*) AS numFarmer\n" +
                            "FROM farmer) AS F\n" +
                            "CROSS JOIN(SELECT COUNT(*) AS numMarket FROM market) as M";
                    break;
                default:
                    query = "show tables";
            }
            ResultSet resultSet = database_connection.send_query(query);
            printResultSet(resultSet);
            System.out.println("Query Done!");
        } else if (first_part.equalsIgnoreCase("add")) {
            if (second_part.toString().toLowerCase().startsWith("farmers")) {
                doCommand(second_part.toString(), "farmers");
            } else if (second_part.toString().toLowerCase().startsWith("farmer")) {
                doCommand(second_part.toString(), "farmer");
            } else if (second_part.toString().toLowerCase().startsWith("products")) {
                doCommand(second_part.toString(), "products");
            } else if (second_part.toString().toLowerCase().startsWith("product")) {
                doCommand(second_part.toString(), "product");
            } else if (second_part.toString().toLowerCase().startsWith("markets")) {
                doCommand(second_part.toString(), "markets");
            } else if (second_part.toString().toLowerCase().startsWith("market")) {
                doCommand(second_part.toString(), "market");
            } else {
                System.err.println("Wrong command!");
                printUsage();
            }
        } else if (first_part.equalsIgnoreCase("register")) {
            if (second_part.toString().toLowerCase().startsWith("products")) {
                doCommand(second_part.toString(), "registers");
            } else if (second_part.toString().toLowerCase().startsWith("product")) {
                doCommand(second_part.toString(), "register");
            } else {
                System.err.println("Wrong command!");
                printUsage();
            }
        } else if (first_part.equalsIgnoreCase("delete")) {
            if (second_part.toString().equalsIgnoreCase("tables")) {
                database_connection.dropTables();
                System.out.println("Deleting Tables Done!");
            }
        } else if (first_part.equalsIgnoreCase("create")) {
            if (second_part.toString().equalsIgnoreCase("tables")) {
                database_connection.createTables();
                System.out.println("Creating Tables Done!");
            }
        } else {
            System.err.println("Wrong command!");
            printUsage();
        }
    }

    private static void printResultSet(ResultSet resultSet) {
        try {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnsNumber = resultSetMetaData.getColumnCount();
            while (resultSet.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) System.out.print(",  ");
                    String columnValue = resultSet.getString(i);
                    System.out.print(columnValue + " ");
                }
                System.out.println("");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error Occured while writing results");
        }
    }

    private static void doCommand(String bulkCommand, String table) {
        ArrayList<String> data = new ArrayList<>();
        String[] commands = bulkCommand.split(":");
        for (String command : commands) {
            data.add(get_data_from_commands(command));
        }
        database_connection.readData(data, table);
    }

    private static String get_data_from_commands(String second_command) {
        if (second_command == null) {
            System.err.println("Wrong command!");
            printUsage();
            return null;
        }
        int start = second_command.lastIndexOf('(');
        int end = second_command.lastIndexOf(')');

        if (start == -1 || end == -1) {
            System.err.println("Wrong command!");
            printUsage();
            return null;
        }

        return second_command.substring(start + 1, end);
    }

    private static void printUsage() {
        System.out.println("Supported Commands: SHOW TABLES | LOAD DATA | QUERY # | ADD FARMER(...) |"
                + " ADD PRODUCT(...) | ADD MARKET() | REGISTER PRODUCT(...) |\n\t ADD FARMERs(...) |"
                + " ADD PRODUCTs(...) | ADD MARKETs() | REGISTER PRODUCTs(...) | DELETE TABLES | CREATE TABLES | EXIT | QUIT");
    }
}
