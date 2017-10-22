package dik.tudarmstadt.services;


import java.sql.*;
import java.util.logging.*;

public class JobMaster {

    private final static Logger LOG = Logger.getLogger(JobMaster.class.getName());

    //JDBC driver details and the database URL
    static final String DRIVER_JDBC = "com.mysql.jdbc.Driver";
    static final String URL_DB = "jdbc:mysql://localhost/vrpdb?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";

    //Credentials for the database
    static final String USERNAME = "root";
    static final String PASS = "Kumar**25";

    public static void main(String[] args){
        Connection con = null;
        Statement stmt = null;

        try{
            //Class.forName(DRIVER_JDBC);
            con = DriverManager.getConnection(URL_DB, USERNAME, PASS);
            stmt = con.createStatement();

            String sqlQuery;
            sqlQuery = "select distinct service_type, count(tech_id) from vrpdb.service_desc_matrix group by " +
                                "service_type order by count(tech_id) desc;";
            ResultSet resultSet = stmt.executeQuery(sqlQuery);

            while (resultSet.next()){
                System.out.println(resultSet.getString("service_type") + " " +
                        resultSet.getString(2));
            }
            resultSet.close();
            stmt.close();
            con.close();

        }catch(Exception e){e.printStackTrace();}
    }

}
