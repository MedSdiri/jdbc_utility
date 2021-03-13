import java.sql.*;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class dbUtility {
    static Connection myConn;
    static ResultSet rs;
    static Statement stmnt;


    public static void createConnection(){

        String connectionStr = "jdbc:oracle:thin:@52.23.203.15:1521:XE";
        String username = "hr";
        String password = "hr";

        try{
             myConn = DriverManager.getConnection(connectionStr, username, password);
            System.out.println("CONNECTION SUCCESSFUL");
        }catch (SQLException e){
            System.err.println("!!! ERROR !!! Connection to database has failed "+ e.getMessage());
        }


    }

    public static ResultSet runQuery(String query){
        try {
            stmnt = myConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            rs = stmnt.executeQuery(query);
        }catch (SQLException e){
            System.err.println("!!! ERROR !!! failed to get RESULSET "+ e.getMessage());
        }

        return rs ;
    }

    public static void destroy(){
        try {

            if (rs != null) rs.close();

            if (stmnt != null) stmnt.close();

            if (myConn != null) myConn.close();

        }catch (SQLException e) {
            System.err.println("!! ERROR !!! Error occured while closing the resources " +e.getMessage());
        }

    }

    public static int getRowCount(){
        int rowCount =0;
        try {
            rs.last();
            rowCount = rs.getRow();
            rs.beforeFirst();

        }catch (SQLException e){
            System.err.println("!!! ERROR !!! Error occured while getting row count "+e.getMessage() );
        }
        return rowCount;
    }

    public static int getColumnCount(){
        int colCount =0;
        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            colCount = rsmd.getColumnCount();
        }catch (SQLException e){
            System.err.println("!!! ERROR !!! Error while getting column count "+e.getMessage());
        }
        return colCount;
    }

    public static List<String> getColumnNames(){
        List<String> colnamesList = new ArrayList<>();

        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            for (int colIndex = 1; colIndex <= rsmd.getColumnCount() ; colIndex++){
                String colName= rsmd.getColumnName(colIndex);
                colnamesList.add(colName);
            }

        } catch (SQLException e) {
            System.err.println("!!! ERROR !!! Error while getting column names "+e.getMessage());
        }
        return colnamesList;
    }

    public static List<String> getRowDataAsList(int rowNum){
        List<String> rowDataList = new ArrayList<>();

        try {
            rs.absolute(rowNum);
            for (int colNum = 1; colNum <= getColumnCount() ; colNum ++){
                String cellValue = rs.getString(colNum);
                rowDataList.add(cellValue);
            }

        } catch (SQLException e) {
            System.err.println("!!! ERROR !!! Error occured while getting row data as list "+e.getMessage());
        }
        return rowDataList;
    }

    public static String getColumnDataAtRow (int rowNum, int columnIndex){
        String result ="";

        try {
            rs.absolute(rowNum);
            result = rs.getString(columnIndex);
            rs.beforeFirst();
        } catch (SQLException e) {
            System.err.println("!!! ERROR !!! Error while getting column "+columnIndex+" data at row "+rowNum
                    +" "+e.getMessage());
        }
        return result;
    }

    public static String getColumnDataAtRow(int rowNum, String columnName){
        String result ="";

        try {
            rs.absolute(rowNum);
            result = rs.getString(columnName);
            rs.beforeFirst();
        } catch (SQLException e) {
            System.err.println("!!! ERROR !!! Error while getting column "+columnName+" data at row "+rowNum
                    +" "+e.getMessage());
        }
        return result;
    }



    public static void main(String[] args) throws SQLException {
        createConnection();
        ResultSet myresult = runQuery("SELECT * FROM REGIONS");
        myresult.next();
        System.out.println(myresult.getString(1));
        System.out.println("3rd row second column is " +getColumnDataAtRow(3, 2));
        System.out.println("3rd row second column is "+getColumnDataAtRow(2, "region_name"));

      destroy();
    }

}
