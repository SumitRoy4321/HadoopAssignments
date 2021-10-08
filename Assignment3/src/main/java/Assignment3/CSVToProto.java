package Assignment3;

import proto.building.BuildingOuterClass.*;
import proto.employee.EmployeeOuterClass.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CSVToProto {

    public static void main(String[] args) {
        System.out.println(" Printing Employee Proto Object ");
        List<Employee> protoEmployeeList = employeeCsvToProto();
        for (Employee emp : protoEmployeeList) {
            System.out.println(emp.toString());
        }
        System.out.println(" Printing Building Proto Object ");
        List<Building> protoBuildingList = buildingCsvToProto();
        for (Building building : protoBuildingList) {
            System.out.println(building.toString());
        }
    }

    private static List<Building> buildingCsvToProto() {
        List<Building> protoBuildingList = new ArrayList();
        try (BufferedReader br = new BufferedReader(new FileReader("BuildingData.csv"))) {
            String line;
            br.readLine();
            //building_code;total_floor;companies_in_the_building;cafeteria_code
            while ((line = br.readLine()) != null) {
                String[] values = line.split(";");
                System.out.println(Arrays.toString(values));
                Building.Builder building = Building.newBuilder();
                building.setBuildingCode(Integer.parseInt(values[0]))
                        .setTotalFloor(Integer.parseInt(values[1]))
                        .setCompaniesInTheBuilding(Integer.parseInt(values[2]))
                        .setCafeteriaCode(Integer.parseInt(values[3]));

                protoBuildingList.add(building.build());
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return protoBuildingList;
    }

    private static List<Employee> employeeCsvToProto() {
        List<Employee> protoEmployeeList = new ArrayList();
        try (BufferedReader br = new BufferedReader(new FileReader("EmployeeData.csv"))) {
            String line;
            br.readLine();
            //employee_id;name;building_code;floor_number;salary;department
            while ((line = br.readLine()) != null) {
                String[] values = line.split(";");
                System.out.println(Arrays.toString(values));
                Employee.Builder emp = Employee.newBuilder();
                emp.setEmployeeId(Integer.parseInt(values[0]))
                        .setName(values[1])
                        .setBuildingCode(Integer.parseInt(values[2]))
                        .setFloorNumber(Floor.forNumber(Integer.parseInt(values[3])))
                        .setSalary(Integer.parseInt(values[4]))
                        .setDepartment(values[5]);

                protoEmployeeList.add(emp.build());
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return protoEmployeeList;
    }
}
