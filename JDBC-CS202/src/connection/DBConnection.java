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

package connection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DBConnection {
    private Connection conn;

    public DBConnection(String url) throws SQLException {
        conn = DriverManager.getConnection(url);
        conn.setAutoCommit(false);
    }

    public void close() {
        if (conn == null) return;
        try {
            conn.close();
        } catch (SQLException ignored) {
        }
    }

    /**
     * A method to send select statement to the underlying DBMS (e.g., "select * from Table1")
     *
     * @param query_statement A query to run on the underlying DBMS
     * @return Resultset the query result.
     */
    public ResultSet send_query(String query_statement) {
        // Feel free to make them Class attributes
        Statement stmt = null;
        ResultSet rs = null;

        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(query_statement);
        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        } finally {
            // it is a good idea to release
            // resources in a finally{} block
            // in reverse-order of their creation
            // if they are no-longer needed
            /*
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ignored) {
                } // ignore

                rs = null;
            }*//*
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ignored) {
                } // ignore
                stmt = null;
            }*/
        }
        return rs;
    }

    public void createTables() {
        try {
            ArrayList<String> tables = new ArrayList<>();
            tables.add("CREATE TABLE IF NOT EXISTS farmer (\n" +
                    "  name varchar(32) NOT NULL,\n" +
                    "  lastname varchar(32) NOT NULL,\n" +
                    "  addr varchar(100) NOT NULL,\n" +
                    "  zip int(5) NOT NULL,\n" +
                    "  city varchar(32) NOT NULL,\n" +
                    "  PRIMARY KEY (name,lastname,addr)\n" +
                    ") DEFAULT CHARSET=utf8mb4");
            tables.add("CREATE TABLE IF NOT EXISTS farmerPhone (\n" +
                    "  name varchar(32) NOT NULL,\n" +
                    "  lastname varchar(32) NOT NULL,\n" +
                    "  addr varchar(100) NOT NULL,\n" +
                    "  phoneNumber varchar(32) NOT NULL,\n" +
                    "  PRIMARY KEY (phoneNumber)\n" +
                    ") DEFAULT CHARSET=utf8mb4");
            tables.add("CREATE TABLE IF NOT EXISTS farmerEmail (\n" +
                    "  name varchar(32) NOT NULL,\n" +
                    "  lastname varchar(32) NOT NULL,\n" +
                    "  addr varchar(100) NOT NULL,\n" +
                    "  email varchar(100) NOT NULL,\n" +
                    "  PRIMARY KEY (email)\n" +
                    ") DEFAULT CHARSET=utf8mb4");
            tables.add("CREATE TABLE IF NOT EXISTS market (\n" +
                    "  name varchar(32) NOT NULL,\n" +
                    "  addr varchar(32) NOT NULL,\n" +
                    "  zip int(5) NOT NULL,\n" +
                    "  city varchar(32) NOT NULL,\n" +
                    "  phoneNumber varchar(32) NOT NULL,\n" +
                    "  budget int(128) NOT NULL,\n" +
                    "  PRIMARY KEY (phoneNumber)\n" +
                    ") DEFAULT CHARSET=utf8mb4");
            tables.add("CREATE TABLE IF NOT EXISTS product (\n" +
                    "  name varchar(32) NOT NULL,\n" +
                    "  plant_date varchar(32) NOT NULL,\n" +
                    "  harvest_date varchar(32) NOT NULL,\n" +
                    "  altitude int(100) NOT NULL,\n" +
                    "  min_temp int(32) NOT NULL,\n" +
                    "  hardness TINYINT NOT NULL,\n" +
                    "  CHECK (hardness>=1 AND hardness<=20),\n" +
                    "  PRIMARY KEY (name)\n" +
                    ") DEFAULT CHARSET=utf8mb4");
            tables.add("CREATE TABLE IF NOT EXISTS buys (\n" +
                    "  transactionNum int NOT NULL AUTO_INCREMENT, \n" +
                    "  fname varchar(32) NOT NULL,\n" +
                    "  flastname varchar(32) NOT NULL,\n" +
                    "  pname varchar(32) NOT NULL,\n" +
                    "  mname varchar(32) NOT NULL,\n" +
                    "  maddr varchar(32) NOT NULL,\n" +
                    "  amount int(100) NOT NULL,\n" +
                    "  creditCard varchar(32) NOT NULL,\n" +
                    "  PRIMARY KEY (transactionNum)\n" +
                    ") DEFAULT CHARSET=utf8mb4");
            tables.add("CREATE TABLE IF NOT EXISTS produces (\n" +
                    "  produceNum int NOT NULL AUTO_INCREMENT, \n" +
                    "  fname varchar(32) NOT NULL,\n" +
                    "  flastname varchar(32) NOT NULL,\n" +
                    "  pname varchar(32) NOT NULL,\n" +
                    "  amount int(100) NOT NULL,\n" +
                    "  year int(32) NOT NULL,\n" +
                    "  PRIMARY KEY (produceNum)\n" +
                    ") DEFAULT CHARSET=utf8mb4");
            tables.add("CREATE TABLE IF NOT EXISTS registers (\n" +
                    "  regNum int NOT NULL AUTO_INCREMENT, \n" +
                    "  fname varchar(32) NOT NULL,\n" +
                    "  flastname varchar(32) NOT NULL,\n" +
                    "  pname varchar(32) NOT NULL,\n" +
                    "  amount int(100) NOT NULL,\n" +
                    "  price float(32) NOT NULL,\n" +
                    "  IBAN varchar(100) NOT NULL,\n" +
                    "  PRIMARY KEY (regNum)\n" +
                    ") DEFAULT CHARSET=utf8mb4");
            for (String table : tables) {
                PreparedStatement statement = conn.prepareStatement(table);
                statement.executeUpdate();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void dropTables() {
        try {
            ArrayList<String> tables = new ArrayList<>();
            tables.add("DROP TABLE IF EXISTS registers");
            tables.add("DROP TABLE IF EXISTS buys");
            tables.add("DROP TABLE IF EXISTS produces");
            tables.add("DROP TABLE IF EXISTS farmer");
            tables.add("DROP TABLE IF EXISTS farmerPhone");
            tables.add("DROP TABLE IF EXISTS farmerEmail");
            tables.add("DROP TABLE IF EXISTS market");
            tables.add("DROP TABLE IF EXISTS product");
            for (String table : tables) {
                PreparedStatement statement = conn.prepareStatement(table);
                statement.executeUpdate();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void readFiles(String file) {
        String line;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            reader.readLine();
            while ((line = reader.readLine()) != null) {
            	if(file.equals("sources/registers.csv")){
            		line = line.replace(",", ".");
            	}
            	line = line.replace(";", ",");
                String[] strings = line.split(",");
                List<String> list = new ArrayList<String>(Arrays.asList(strings));
                for(int i=0; i<strings.length;i++){
                	list.remove("");
                }
                if (file.contains("farmer")) addFarmer(strings);
                else if (file.contains("market")) addMarket(strings);
                else if (file.contains("product")) addProduct(strings);
                else if (file.contains("registers")) addRegister(strings);
                else if (file.contains("produces")) addProduce(strings);
                else if (file.contains("buys")) addBuy(strings);
            }
            conn.commit();
            reader.close();
        } catch (Exception e) {
            try {
                conn.rollback();
            } catch (SQLException ee) {
                System.out.println("Cannot Rollback!");
            }
            System.out.println("Error occured while reading file: " + file);
        }
    }

    public void readData(ArrayList<String> commands, String table) {
        try {
            for (String command : commands) {
                command += ",false";
                String[] strings = command.split(",");
                if (table.contains("farmer")) addFarmer(strings);
                else if (table.contains("market")) addMarket(strings);
                else if (table.contains("product")) addProduct(strings);
                else if (table.contains("register")) addRegister(strings);
                else if (table.contains("produce")) addProduce(strings);
                else if (table.contains("buy")) addBuy(strings);
            }
            conn.commit();
            System.out.println("Adding " + table + " Done!");
        } catch (Exception e) {
            try {
                conn.rollback();
            } catch (SQLException ee) {
                System.out.println("Cannot Rollback!");
            }
            System.out.println("Error occured while adding new " + table);
        }
    }

    private void addFarmer(String[] strings) throws Exception {
        String[] phones = strings[5].split("\\|");
        String[] emails = strings[6].split("\\|");

        if (strings[strings.length - 1].equals("false")) {
            if (checkAddrZipCity("farmer", strings[2], strings[3], strings[4]))
                throw new Exception();
            for (String phone : phones)
                if (checkPhoneNameLastname(strings[0], strings[1], phone))
                    throw new Exception();
        }

        for (String phone : phones) {
            String command = String.format("INSERT INTO farmerphone () VALUES ('%s', '%s', '%s', '%s')",
                    strings[0], strings[1], strings[2], phone);
            PreparedStatement insertPhone = conn.prepareStatement(command);
            insertPhone.executeUpdate();
        }

        for (String email : emails) {
            String command = String.format("INSERT INTO farmeremail () VALUES ('%s', '%s', '%s', '%s')",
                    strings[0], strings[1], strings[2], email);
            PreparedStatement insertEmail = conn.prepareStatement(command);
            insertEmail.executeUpdate();
        }

        String insert = String.format("INSERT INTO farmer () VALUES ('%s', '%s','%s', %d, '%s')",
                strings[0], strings[1], strings[2], Integer.parseInt(strings[3]), strings[4]);
        PreparedStatement statement = conn.prepareStatement(insert);
        statement.executeUpdate();
    }

    private void addMarket(String[] strings) throws Exception {
        if (strings[strings.length - 1].equals("false")) {
            if (checkAddrZipCity("market", strings[1], strings[2], strings[3]))
                throw new Exception();
        }
        String insert = String.format("INSERT INTO market () VALUES ('%s', '%s', '%s', '%s', '%s', '%s')",
                strings[0], strings[1], strings[2], strings[3], strings[4], strings[5]);
        PreparedStatement statement = conn.prepareStatement(insert);
        statement.executeUpdate();
    }

    private void addProduct(String[] strings) throws Exception {
        if (strings[strings.length - 1].equals("false")) {
            if (checkProductFD(strings[1], strings[2], strings[3], strings[4]))
                throw new Exception();
        }
        String insert = String.format("INSERT INTO product () VALUES ('%s', '%s', '%s', '%s', '%s', '%s')",
                strings[0], strings[1], strings[2], strings[3], strings[4], strings[5]);
        PreparedStatement statement = conn.prepareStatement(insert);
        statement.executeUpdate();
    }

    private void addRegister(String[] strings) throws Exception {
        if (strings[strings.length - 1].equals("false")) {
            if (checkRegister(strings[0], strings[1], strings[2])) {
                System.out.println("Cannot Register that Product\n" +
                        "Farmer or Product does not exist");
                throw new Exception();
            }
        }
        String insert = String.format("INSERT INTO registers (fname,flastname,pname,amount,price,IBAN) VALUES ('%s', '%s', '%s', '%s', '%s', '%s')",
                strings[0], strings[1], strings[2], strings[3], strings[4], strings[5]);
        PreparedStatement statement = conn.prepareStatement(insert);
        statement.executeUpdate();
    }

    private void addProduce(String[] strings) throws Exception {
        String insert = String.format("INSERT INTO produces (fname,flastname,pname,amount,year) VALUES ('%s', '%s', '%s', '%s', '%s')",
                strings[0], strings[1], strings[2], strings[3], strings[4]);
        PreparedStatement statement = conn.prepareStatement(insert);
        statement.executeUpdate();
    }

    private void addBuy(String[] strings) throws Exception {
        if (strings[strings.length - 1].equals("false")) {
            if (checkBuyFD(strings[3], strings[6]))
                throw new Exception();
        }
        String insert = String.format("INSERT INTO buys (fname,flastname,pname,mname,maddr,amount,creditCard) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s')",
                strings[0], strings[1], strings[2], strings[3], strings[4], strings[5], strings[6]);
        PreparedStatement statement = conn.prepareStatement(insert);
        statement.executeUpdate();
    }

    private boolean checkPhoneNameLastname(String name, String lastname, String phone) {
        String insert = "SELECT fp.name, fp.lastname\n" +
                "FROM farmerphone fp\n" +
                "WHERE fp.phoneNumber='" + phone + "'";
        try {
            ResultSet resultSet = send_query(insert);
            if (!resultSet.isBeforeFirst()) return false;
            resultSet.next();
            String dbName = resultSet.getString(1);
            String dbLastname = resultSet.getString(2);
            if (dbName.equalsIgnoreCase(name) && dbLastname.equalsIgnoreCase(lastname))
                return false;
            System.out.println("Cannot add due to FD Constraints!");
        } catch (SQLException e) {
            System.out.println("Error Occured while checking FD");
        }
        return true;
    }

    private boolean checkAddrZipCity(String table, String addr, String zip, String city) {
        String insert = "SELECT m.zip, m.city\n" +
                "FROM " + table + " m\n" +
                "WHERE m.addr='" + addr + "'";
        try {
            ResultSet resultSet = send_query(insert);
            if (!resultSet.isBeforeFirst()) return false;
            resultSet.next();
            String dbZip = resultSet.getString(1);
            String dbCity = resultSet.getString(2);
            if (dbZip.equalsIgnoreCase(zip) && dbCity.equalsIgnoreCase(city))
                return false;
            System.out.println("Cannot add due to FD Constraints!");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error Occured while checking FD");
        }
        return true;
    }

    private boolean checkProductFD(String plant, String harvest, String alt, String temp) {
        String insert1 = "SELECT m.harvest_date\n" +
                "FROM product m\n" +
                "WHERE m.plant_date='" + plant + "'";
        String insert2 = "SELECT m.min_temp\n" +
                "FROM product m\n" +
                "WHERE m.altitude='" + alt + "'";
        try {
            ResultSet resultSet = send_query(insert1);
            ResultSetMetaData rsMetaData = resultSet.getMetaData();
            int columnCount = rsMetaData.getColumnCount();
            if (columnCount < 1 || !resultSet.isBeforeFirst()) return false;
            resultSet.next();
            String dbHarvest = resultSet.getString(1);
            resultSet = send_query(insert2);
            rsMetaData = resultSet.getMetaData();
            columnCount = rsMetaData.getColumnCount();
            if (columnCount < 1 || !resultSet.isBeforeFirst()) return false;
            resultSet.next();
            String dbTemp = resultSet.getString(1);
            if (dbHarvest.equalsIgnoreCase(harvest) && dbTemp.equalsIgnoreCase(temp))
                return false;
            System.out.println("Cannot add due to FD Constraints!");
        } catch (SQLException e) {
            System.out.println("Error Occured while checking FD");
        }
        return true;
    }

    private boolean checkBuyFD(String name, String creditCard) {
        String insert = "SELECT m.mname\n" +
                "FROM buys m\n" +
                "WHERE m.creditCard='" + creditCard + "'";
        try {
            ResultSet resultSet = send_query(insert);
            if (!resultSet.isBeforeFirst()) return false;
            resultSet.next();
            String dbName = resultSet.getString(1);
            if (dbName.equalsIgnoreCase(name))
                return false;
            System.out.println("Cannot add due to FD Constraints!");
        } catch (SQLException e) {
            System.out.println("Error Occured while checking FD");
        }
        return true;
    }

    private boolean checkRegister(String fname, String flastname, String pname) {
        String farmer = "SELECT f.lastname\n" +
                "FROM farmer f\n" +
                "WHERE f.name='" + fname + "'";
        String product = "SELECT f.name\n" +
                "FROM product f\n" +
                "WHERE f.name='" + pname + "'";
        boolean isNotFarmer = true;
        boolean isNotProduct = true;
        try {
            ResultSet resultSet = send_query(farmer);
            if (!resultSet.isBeforeFirst()) return true;
            while (resultSet.next()) {
                String dbLastname = resultSet.getString(1);
                if (dbLastname.equalsIgnoreCase(flastname))
                    isNotFarmer = false;
            }
            resultSet = send_query(product);
            if (!resultSet.isBeforeFirst()) return true;
            while (resultSet.next()) {
                String dbProduct = resultSet.getString(1);
                if (dbProduct.equalsIgnoreCase(pname))
                    isNotProduct = false;
            }
            return isNotFarmer && isNotProduct;
        } catch (SQLException e) {
            System.out.println("Error Occured while checking for Product Register");
        }
        return true;
    }
}
