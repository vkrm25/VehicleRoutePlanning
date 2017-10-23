package dik.tudarmstadt.services;


import java.sql.*;
import java.util.*;
import java.util.logging.*;

public class JobMaster {

    private final static Logger LOG = Logger.getLogger(JobMaster.class.getName());

    //database URL
    static final String URL_DB = "jdbc:mysql://localhost/vrpdb?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";

    //Credentials for the database
    static final String USERNAME = "root";
    static final String PASS = "Kumar**25";

    //Contains Service type and associated technician count
    HashMap<String, Integer> serviceType = new HashMap<String, Integer>();

    //Contains Customer Service job and its associated type
    HashMap<String, String> serviceJob = new HashMap<String, String>();

    //Contains Techinian id with associated service type skills
    HashMap<String, ArrayList<String>> technicianCompetency = new HashMap<String, ArrayList<String>>();

    //Sorted Service jobs based on complexity
    LinkedHashMap<String, String> sortedServiceJob = new LinkedHashMap<>();

    //Sorted technician id based on lowest competency first
    LinkedHashMap<String, ArrayList<String>> sortedTechCompetency = new LinkedHashMap<>();

    public static void main(String[] args){
        JobMaster jobMaster = new JobMaster();
        //get service types from database
        jobMaster.getServiceType();
        jobMaster.serviceType.forEach((servtype, count)-> System.out.println(servtype + " " + count));
        //get all service jobs from database
        jobMaster.getServiceJob();
        jobMaster.serviceJob.forEach((jobid, servtype)-> System.out.println(jobid + " " + servtype));
        //get all technicians with their competency from database
        jobMaster.getTechnicianCompetency();
        jobMaster.technicianCompetency.forEach((techid, servtypelist) ->
            System.out.println(techid + " " + servtypelist.toString()));
        //Sorted all the service jobs
        jobMaster.sortServiceJobs();
        jobMaster.sortedServiceJob.forEach((jobid, servtype)-> System.out.println(jobid + " " + servtype));

        //Sorted the technicians based on lowest competency first
        jobMaster.sortTechCompetency();
        jobMaster.sortedTechCompetency.forEach((techid, servtypelist) ->
                System.out.println(techid + " " + servtypelist.toString()));
    }

    //Get service type and associated number of technician from database
    public void getServiceType() {

        Connection con = null;
        Statement stmt = null;

        try{
            con = DriverManager.getConnection(URL_DB, USERNAME, PASS);
            stmt = con.createStatement();

            String sqlQuery;
            sqlQuery = "select distinct service_type, count(tech_id) from vrpdb.service_desc_matrix group by " +
                    "service_type order by count(tech_id) desc;";
            ResultSet resultSet = stmt.executeQuery(sqlQuery);

            while (resultSet.next()){

                serviceType.put(resultSet.getString("service_type"),resultSet.getInt(2) );
            }
            resultSet.close();
            stmt.close();
            con.close();

        }catch(Exception e){e.printStackTrace();}

    }

    //Get customer service job and its associated type from cust_service_desc table
    public void getServiceJob(){
        Connection con = null;
        Statement stmt = null;

        try{
            con = DriverManager.getConnection(URL_DB, USERNAME, PASS);
            stmt = con.createStatement();

            String sqlQuery;
            sqlQuery = "select job_id, service_type from vrpdb.cust_service_req;";
            ResultSet resultSet = stmt.executeQuery(sqlQuery);

            while (resultSet.next()){

                serviceJob.put(resultSet.getString("job_id"),resultSet.getString("service_type") );
            }
            resultSet.close();
            stmt.close();
            con.close();

        }catch(Exception e){e.printStackTrace();}

    }

    //Get technician competency
    public void getTechnicianCompetency(){
        Connection con = null;
        Statement stmt = null;

        try{
            con = DriverManager.getConnection(URL_DB, USERNAME, PASS);
            stmt = con.createStatement();

            String sqlQuery;
            sqlQuery = "select tech_id, service_type from vrpdb.service_desc_matrix;";
            ResultSet resultSet = stmt.executeQuery(sqlQuery);

            while (resultSet.next()){
                String techId = resultSet.getString("tech_id");
                String servType = resultSet.getString("service_type");

                if(technicianCompetency.containsKey(techId)){
                    technicianCompetency.get(techId).add(servType);
                }
                else{
                    ArrayList<String> serviceTypeList = new ArrayList<>();
                    serviceTypeList.add(servType);
                    technicianCompetency.put(techId, serviceTypeList);
                }
            }
            resultSet.close();
            stmt.close();
            con.close();

        }catch(Exception e){e.printStackTrace();}

    }

    private void sortServiceJobs(){
        HashMap<String, Integer> comparatorMap = new HashMap<>();
        serviceJob.forEach((jobid, servtype) -> comparatorMap.put(jobid, serviceType.get(servtype)));
        Map<String, Integer> sortedMap = SortMapUtility.sortByValueDesc(comparatorMap);
        sortedMap.forEach((jobid, count) -> sortedServiceJob.put(jobid, serviceJob.get(jobid)));

    }

    private void sortTechCompetency(){
        HashMap<String, Integer> tempMap =  new HashMap<>();
        technicianCompetency.forEach((techid, complist) -> tempMap.put(techid, complist.size()));
        Map<String, Integer> sortedMap = SortMapUtility.sortByValueAsc(tempMap);
        sortedMap.forEach((techid, comp) -> sortedTechCompetency.put(techid, technicianCompetency.get(techid)));
    }

}
