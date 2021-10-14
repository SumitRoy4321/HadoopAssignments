package Assignment3;

import Assignment4.Assignment4MapDriver;
import com.google.protobuf.util.JsonFormat;
import org.apache.hadoop.mapreduce.Job;
import proto.building.Building;
import proto.building.BuildingList;
import proto.employee.Employee;
import proto.employee.EmployeeList;
import proto.employee.Floor;

import java.io.*;
import java.util.List;

public class CSVToProto {

    public static void main(String[] args) throws IOException {
        CSVToProto csvToProto = new CSVToProto();
        EmployeeList.Builder protoEmployeeList = EmployeeList.newBuilder();
        csvToProto.employeeCsvToProto(protoEmployeeList);
        EmployeeList employeeList = protoEmployeeList.build();
        System.out.println(" Printing Employee Proto Object ");
        System.out.println(employeeList);
        csvToProto.WriteToFile(null, employeeList, new File("EmployeeSerializedFile"));
        System.out.println("Printing Building Proto Object ");
        BuildingList.Builder protoBuildingList = BuildingList.newBuilder();
        csvToProto.buildingCsvToProto(protoBuildingList);
        BuildingList buildingList = protoBuildingList.build();
        System.out.println(buildingList);
        csvToProto.WriteToFile(buildingList, null, new File("BuildingSerializedFile"));
        Assignment4MapDriver assignment4 = new Assignment4MapDriver();
//        assignment4.mapDriver();
    }

    private void WriteToFile(BuildingList buildingList, EmployeeList employeeList, File fileName) throws IOException {
        FileOutputStream empOutPutFile = new FileOutputStream(fileName);
        JsonFormat.Printer printer = JsonFormat.printer()
                .includingDefaultValueFields()
                .preservingProtoFieldNames();
        String jsonString=null;
        if(buildingList != null) {
            jsonString = printer.print(buildingList);
        }else{
            jsonString = printer.print(employeeList);
        }

        empOutPutFile.write(jsonString.getBytes());
        empOutPutFile.close();
    }

    private void buildingCsvToProto(BuildingList.Builder protoBuildingList) {

        try (BufferedReader br = new BufferedReader(new FileReader("BuildingData.csv"))) {
            String line;
            br.readLine();
            //building_code;total_floor;companies_in_the_building;cafeteria_code
            while ((line = br.readLine()) != null) {
                String[] values = line.split(";");
//                System.out.println(Arrays.toString(values));
                Building.Builder building = Building.newBuilder();
                building.setBuildingCode(Integer.parseInt(values[0]))
                        .setTotalFloor(Integer.parseInt(values[1]))
                        .setCompaniesInTheBuilding(Integer.parseInt(values[2]))
                        .setCafeteriaCode(Integer.parseInt(values[3]));

                protoBuildingList.addBuildings(building.build());
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void employeeCsvToProto(EmployeeList.Builder protoEmployeeList) {
        try (BufferedReader br = new BufferedReader(new FileReader("EmployeeData.csv"))) {
            String line;
            br.readLine();
            //employee_id;name;building_code;floor_number;salary;department
            while ((line = br.readLine()) != null) {
                String[] values = line.split(";");
                Employee.Builder emp = Employee.newBuilder();
                emp.setEmployeeId(Integer.parseInt(values[0]))
                        .setName(values[1])
                        .setBuildingCode(Integer.parseInt(values[2]))
                        .setFloorNumber(Floor.forNumber(Integer.parseInt(values[3])))
                        .setSalary(Integer.parseInt(values[4]))
                        .setDepartment(values[5]);
                protoEmployeeList.addEmployees(emp.build());
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static void dumpingProtoBufToHbase(List<Employee> protoEmployeeList, String tableName) throws IOException {
        Job job = Job.getInstance();
        job.setJarByClass(CSVToProto.class);
        job.setJobName("ProtoBufToHbase");
//        job.setInputFormatClass();
    }
}
