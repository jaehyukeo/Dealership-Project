package com.dealership;

import java.net.InetSocketAddress;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Scanner;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

public class CarDealership {

    // Database Connection
    public static CqlSession connectToDatabase() {
        try {
            return CqlSession.builder()
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .withLocalDatacenter("datacenter1")
                .withKeyspace("dealership_system")
                .build();
        } catch (Exception e) {
            System.out.println("❌ Failed to connect: " + e.getMessage());
            return null;
        }
    }

    public static boolean carExists(CqlSession session, String vin) {
        String query = String.format("SELECT * FROM car WHERE vin = '%s';", vin);
        ResultSet rs = session.execute(query);
        return rs.one() != null; 
    }

    // CRUD
    public static void addCar(CqlSession session, String vin, String brand, String model, int year, String type, BigDecimal price) {
        // Status is now strictly written as 'Available' for every new entry
        String query = String.format(
            "INSERT INTO car (vin, brand, model, year, type, price, status) VALUES ('%s', '%s', '%s', %d, '%s', %s, 'Available');",
            vin, brand, model, year, type, price.toString()
        );
        session.execute(query);
        System.out.println("ADDED: " + year + " " + brand + " " + model + " (Status: Available)");
    }

    public static void viewAllCars(CqlSession session) {
        System.out.println("\n--- Current Dealership Inventory ---");
        String query = "SELECT * FROM car;";
        ResultSet rs = session.execute(query);
        
        for (Row row : rs) {
            System.out.println(
                "VIN: " + row.getString("vin") + 
                " | Car: " + row.getString("brand") + " " + row.getString("model") + 
                " | Type: " + row.getString("type") +
                " | Price: ₱" + row.getBigDecimal("price") +
                " | Status: " + row.getString("status")
            );
        }
        System.out.println("------------------------------------\n");
    }

    public static void updateCarStatus(CqlSession session, String vin, String newStatus) {
        if (!carExists(session, vin)) {
            System.out.println("Error: Car with VIN '" + vin + "' not found. Status update aborted.");
            return;
        }
        String query = String.format("UPDATE car SET status = '%s' WHERE vin = '%s';", newStatus, vin);
        session.execute(query);
        System.out.println("UPDATED: VIN " + vin + " is now '" + newStatus + "'");
    }

    public static void deleteCar(CqlSession session, String vin) {
        String query = String.format("DELETE FROM car WHERE vin = '%s';", vin);
        session.execute(query);
        System.out.println("DELETED: VIN " + vin + " removed from system.");
    }

    public static void createInvoice(CqlSession session, int invNumber, int spId, String spName, String spPos, int branchId, String branchName, int custId, String custName, String custPhone, String carModel, BigDecimal amount, String payMethod, String vinForUpdate) {

        if (!carExists(session, vinForUpdate)) {
            System.out.println("Error: VIN " + vinForUpdate + " not found.");
            return;
        }

        // ID Ownership Constraints
        // Branch Check
        Row branchRow = session.execute(String.format("SELECT branch_name FROM branch WHERE branch_id = %d;", branchId)).one();
        if (branchRow != null && !branchRow.getString("branch_name").equalsIgnoreCase(branchName)) {
            System.out.println("Error: Branch ID " + branchId + " is already '" + branchRow.getString("branch_name") + "'.");
            return;
        }

        // Salesperson Check
        Row spRow = session.execute(String.format("SELECT sp_name FROM salesperson WHERE salesperson_id = %d;", spId)).one();
        if (spRow != null && !spRow.getString("sp_name").equalsIgnoreCase(spName)) {
            System.out.println("Error: Salesperson ID " + spId + " belongs to '" + spRow.getString("sp_name") + "'.");
            return;
        }

        // Customer Check
        Row custRow = session.execute(String.format("SELECT customer_name FROM customer WHERE customer_id = %d;", custId)).one();
        if (custRow != null && !custRow.getString("customer_name").equalsIgnoreCase(custName)) {
            System.out.println("Error: Customer ID " + custId + " belongs to '" + custRow.getString("customer_name") + "'.");
            return;
        }

        session.execute(String.format("INSERT INTO branch (branch_id, branch_name) VALUES (%d, '%s');", branchId, branchName));
        session.execute(String.format("INSERT INTO salesperson (salesperson_id, sp_name, position, branch_id) VALUES (%d, '%s', '%s', %d);", spId, spName, spPos, branchId));
        session.execute(String.format("INSERT INTO customer (customer_id, customer_name, phone) VALUES (%d, '%s', '%s');", custId, custName, custPhone));

        // Transaction Record
        long timestamp = Instant.now().toEpochMilli();
        String invQuery = String.format(
            "INSERT INTO invoice (invoice_number, invoice_date, total_amount, salesperson_id, sp_name, customer_id, customer_name, car_model, payment_method) " +
            "VALUES (%d, %d, %s, %d, '%s', %d, '%s', '%s', '%s');", 
            invNumber, timestamp, amount.toString(), spId, spName, custId, custName, carModel, payMethod
        );
        session.execute(invQuery);
        
        updateCarStatus(session, vinForUpdate, "Sold");
        System.out.println("Invoice #" + invNumber + " created. Branch, Dealer, and Customer records synchronized.");
    }

    public static void createServiceTicket(CqlSession session, String ticketId, String vin, String custName, int mechId, String mechName, String mechSpec, BigDecimal laborCost, String partsList, BigDecimal partsCost) {
    	
        if (!carExists(session, vin)) {
            System.out.println("Error: VIN " + vin + " not found.");
            return;
        }

        // Sold Constraint (Preventing service on sold cars)
        String currentStatus = getCarStatus(session, vin);
        if (currentStatus.equalsIgnoreCase("Sold")) {
            System.out.println("Error: VIN " + vin + " is already SOLD. Cannot create a service ticket.");
            return;
        }

        // Mechanic ID Ownership Constraint
        Row mechRow = session.execute(String.format("SELECT mechanic_name FROM mechanic WHERE mechanic_id = %d;", mechId)).one();
        if (mechRow != null && !mechRow.getString("mechanic_name").equalsIgnoreCase(mechName)) {
            System.out.println("Error: Mechanic ID " + mechId + " belongs to '" + mechRow.getString("mechanic_name") + "'.");
            return;
        }

        session.execute(String.format("INSERT INTO mechanic (mechanic_id, mechanic_name, specialization) VALUES (%d, '%s', '%s');", mechId, mechName, mechSpec));

        // Calculations 
        BigDecimal totalCost = laborCost.add(partsCost);
        long timestamp = Instant.now().toEpochMilli();

        // Format Parts List for Cassandra
        String formattedParts = "[]"; 
        if (!partsList.trim().isEmpty()) {
            String[] parts = partsList.split(",");
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < parts.length; i++) {
                sb.append("'").append(parts[i].trim()).append("'");
                if (i < parts.length - 1) sb.append(", ");
            }
            sb.append("]");
            formattedParts = sb.toString();
        }

        String query = String.format(
            "INSERT INTO service_ticket (ticket_id, vin, customer_name, mechanic_name, service_date, labor_cost, parts_list, parts_cost, total_cost) " +
            "VALUES ('%s', '%s', '%s', '%s', %d, %s, %s, %s, %s);", 
            ticketId, vin, custName, mechName, timestamp, laborCost.toString(), formattedParts, partsCost.toString(), totalCost.toString()
        );
        session.execute(query);

        updateCarStatus(session, vin, "In Service");
        System.out.println("✅ Service Ticket " + ticketId + " generated successfully.");
    }
    
    // Status checker
    public static String getCarStatus(CqlSession session, String vin) {
        Row row = session.execute(String.format("SELECT status FROM car WHERE vin = '%s';", vin)).one();
        return (row != null) ? row.getString("status") : "";
    }
    
    public static String getValidUpdateStatus(Scanner scanner) {
        while (true) {
            System.out.print("Enter New Status (In Service / Completed): ");
            String input = scanner.nextLine().trim(); 

            if (input.equalsIgnoreCase("In Service")) return "In Service";
            if (input.equalsIgnoreCase("Completed")) return "Completed"; 
            
            System.out.print("Invalid! Manual updates only allow 'In Service' or 'Completed': ");
        }
    }
    
    public static void showCustomerHistory(CqlSession session, String customerName) {
        System.out.println("\n============================================");
        System.out.println("      BILLING SUMMARY: " + customerName.toUpperCase());
        System.out.println("============================================\n");

        // Sales Invoice
        System.out.println(">> SALES INVOICES");
        String invQuery = String.format("SELECT * FROM invoice WHERE customer_name = '%s' ALLOW FILTERING;", customerName);
        ResultSet invRs = session.execute(invQuery);
        
        boolean hasSales = false;
        for (Row row : invRs) {
            hasSales = true;
            System.out.println("--------------------------------------------");
            System.out.println("Invoice #: " + row.getInt("invoice_number"));
            System.out.println("Date:      " + row.getInstant("invoice_date"));
            System.out.println("Car Model: " + row.getString("car_model"));
            System.out.println("Sold By:   " + row.getString("sp_name"));
            System.out.println("Payment:   " + row.getString("payment_method"));
            System.out.println("TOTAL DUE: ₱" + row.getBigDecimal("total_amount"));
        }
        if (!hasSales) System.out.println("(No purchase records found)");

        // Service Tickets
        System.out.println("\n>> SERVICE & REPAIR HISTORY");
        String srvQuery = String.format("SELECT * FROM service_ticket WHERE customer_name = '%s' ALLOW FILTERING;", customerName);
        ResultSet srvRs = session.execute(srvQuery);
        
        boolean hasService = false;
        for (Row row : srvRs) {
            hasService = true;
            System.out.println("--------------------------------------------");
            System.out.println("Ticket #:    " + row.getInt("ticket_id"));
            System.out.println("Date:        " + row.getInstant("service_date"));
            System.out.println("Car VIN:     " + row.getString("vin"));
            System.out.println("Mechanic:    " + row.getString("mechanic_name"));
            System.out.println("Parts Used:  " + row.getList("parts_list", String.class));
            System.out.println("BREAKDOWN:");
            System.out.println("   Labor:    ₱" + row.getBigDecimal("labor_cost"));
            System.out.println("   Parts:    ₱" + row.getBigDecimal("parts_cost"));
            System.out.println("TOTAL COST:  ₱" + row.getBigDecimal("total_cost"));
        }
        if (!hasService) System.out.println("(No service records found)");
        
        System.out.println("\n============================================\n");
    }

    // Menu for visual rep  
    public static void main(String[] args) {
        System.out.println("Connecting to Cassandra Database...");
        CqlSession session = connectToDatabase();

        if (session == null) {
            System.out.println("Shutting down due to connection error.");
            return;
        }

        System.out.println("Connected Successfully!\n");
        Scanner scanner = new Scanner(System.in);
        boolean running = true;

        while (running) {
            System.out.println("====================================");
            System.out.println("            Car Dealership          ");
            System.out.println("====================================");
            System.out.println("1. View Inventory");
            System.out.println("2. Add a Car");
            System.out.println("3. Update Car Status");
            System.out.println("4. Delete a Car");
            System.out.println("5. Create Sales Invoice");
            System.out.println("6. Create Service Ticket");
            System.out.println("7. View Customer Billing/History");
            System.out.println("8. Exit System");
            System.out.print("Select an option (1-8): ");
            
            int choice = scanner.nextInt();
            scanner.nextLine();

            switch (choice) {
                case 1:
                    viewAllCars(session);
                    break;
                case 2:
                    System.out.print("Enter VIN: "); String vin = scanner.nextLine();
                    System.out.print("Enter Brand: "); String brand = scanner.nextLine();
                    System.out.print("Enter Model: "); String model = scanner.nextLine();
                    System.out.print("Enter Year: "); int year = scanner.nextInt(); scanner.nextLine(); 
                    System.out.print("Enter Type: "); String type = scanner.nextLine();
                    System.out.print("Enter Price: "); BigDecimal price = scanner.nextBigDecimal(); scanner.nextLine();
                    addCar(session, vin, brand, model, year, type, price);
                    break;

                case 3:
                    System.out.print("Enter the VIN of the car to update: "); 
                    String upVin = scanner.nextLine();
                    String newStatus = getValidUpdateStatus(scanner); 
                    updateCarStatus(session, upVin, newStatus);
                    break;
                case 4:
                    System.out.print("Enter the VIN to delete: "); String deleteVin = scanner.nextLine();
                    deleteCar(session, deleteVin);
                    break;
                    
                case 5:
                    System.out.println("\n--- NEW SALES INVOICE ---");
                    System.out.print("Invoice Number: "); int invId = scanner.nextInt(); scanner.nextLine();
  
                    System.out.print("Branch ID: "); int bId = scanner.nextInt(); scanner.nextLine();
                    System.out.print("Branch Name (e.g. Taguig): "); String bName = scanner.nextLine();
 
                    System.out.print("Salesperson ID: "); int spId = scanner.nextInt(); scanner.nextLine();
                    System.out.print("Salesperson Name: "); String spName = scanner.nextLine();
                    System.out.print("Position: "); String spPos = scanner.nextLine();

                    System.out.print("Customer ID: "); int cId = scanner.nextInt(); scanner.nextLine();
                    System.out.print("Customer Name: "); String cName = scanner.nextLine();
                    System.out.print("Phone: "); String cPhone = scanner.nextLine();
                    
                    System.out.print("Car Model Sold: "); String cModel = scanner.nextLine();
                    System.out.print("Total Amount: "); BigDecimal amt = scanner.nextBigDecimal(); scanner.nextLine();
                    System.out.print("Payment Method: "); String payMethod = scanner.nextLine();
                    System.out.print("Confirm VIN: "); String sellVin = scanner.nextLine();
                    
                    createInvoice(session, invId, spId, spName, spPos, bId, bName, cId, cName, cPhone, cModel, amt, payMethod, sellVin);
                    break;

                case 6:
                    System.out.println("\n--- NEW SERVICE TICKET ---");
                    System.out.print("Ticket Number: "); String tId = scanner.nextLine();
                    System.out.print("Car VIN: "); String srvVin = scanner.nextLine();
                    System.out.print("Customer Name: "); String sCustName = scanner.nextLine();
                    
                    System.out.print("Mechanic ID: "); int mId = scanner.nextInt(); scanner.nextLine();
                    System.out.print("Mechanic Name: "); String mName = scanner.nextLine();
                    System.out.print("Specialization: "); String mSpec = scanner.nextLine();
                    
                    System.out.print("Labor Cost: "); BigDecimal lCost = scanner.nextBigDecimal(); scanner.nextLine();
                    System.out.print("Parts Used (comma-separated): "); String parts = scanner.nextLine();
                    
                    BigDecimal pTotal = BigDecimal.ZERO;
                    if (!parts.trim().isEmpty()) {
                        System.out.print("Enter total Parts Cost: ");
                        pTotal = scanner.nextBigDecimal(); scanner.nextLine();
                    }        
                    createServiceTicket(session, tId, srvVin, sCustName, mId, mName, mSpec, lCost, parts, pTotal);
                    break;
                    
                case 7:
                    System.out.print("Enter Customer Name to search: ");
                    String searchName = scanner.nextLine();
                    showCustomerHistory(session, searchName);
                    break;
                case 8:
                    running = false;
                    break;
            }
        }
        scanner.close();
        session.close();
        System.out.println("\n Database connection securely closed. Goodbye!");
    }
}