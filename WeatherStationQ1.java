// Assignment 2 :   Tools & Techniques for Large Scale Data Analytics
// Name         :   Rahul Kumar
// Student ID   :   20230113

// Q1. Find the  maximum temperature measured by the weather station between startTime and endTime(asked from user).

// Input        :   1. Given a choice among the weather stations. Select the station to find out the details.
//                  2. After selecting station, enter the startTime and EndTime to calculate the max Temperature in the given range.
// Constraints  :   choice, StartTime and EndTime cannot take value 0 (Rest all integers are accepted)
// Output       :   Details of the time and temperature measured at the station along with the max temperature and time of occurance within the range.


package javaassignment2;

// Importing the "java.util" package for taking input from Scanner class
// Importing the next 2 package for using stream operations and comparing objects. 

import java.util.*;
import static java.util.Comparator.comparing;
import java.util.stream.*;


// Create a class MeasurementQ1. Objects of class MeasurementQ1 should have attributes: 
// 1. Time (an integer, representing the time of the measurement)
// 2. Temperature (a double number).
class MeasurementQ1 {

    //Setting up class attributes
    private int time;
    private double temperature;

    //Setting up constructor for the MeasurementQ1 object
    public MeasurementQ1(int time, double temperature){
        this.time = time;
        this.temperature=temperature;
    }

    //setters and getters for the attributes of the object MeasurementQ1
    public void setTime(int time){
        this.time=time;
    }

    public int getTime(){
        return time;        
    }

    public void setTemperature(double temperature){
        this.temperature = temperature;
    }

    public double getTemperature(){
        return temperature;
    }
}

// Create a class WeatherStationQ1 with three attributes (fields): 
// 1. The city where the station is located, 
// 2. The station’s measurements (a list of objects of class MeasurementQ1), and
// 3. A static field stations (a list of all existing weather stations). --- used WeatherStationQ2.java file

// Add a method maxTemperature(startTime, endTime) to this class which returns
// the maximum temperature measured by the weather station between startTime 
// and endTime.

public class WeatherStationQ1 {

    //Setting up class attributes
    private String city;                            
    private List<MeasurementQ1> measurements;       
    public static List<WeatherStationQ1> stations = new ArrayList<>();  
    
    //Setting up constructor for the WeatherStationQ1 object
    public WeatherStationQ1(String city, List<MeasurementQ1> measurements){
        this.city = city;
        this.measurements = measurements;
    }

    //Setters and getters for the attributes of the object WeatherStationQ1
    public void setCity(String city){
        this.city = city;
    }

    public String getCity(){
        return city;        
    }

    public void setMeasurements(List<MeasurementQ1> measurements){
        this.measurements = measurements;
    }

    public List<MeasurementQ1> getMeasurements(){
        return measurements;
    }

    //maxTemperature() to return the object of MeasurementQ1 class having highest temperature within a given time range
    public MeasurementQ1 maxTemperature(int startTime, int endTime){                // getMeasurements(a list of measurements object of the calling 
        MeasurementQ1 tempValue =   getMeasurements().stream()                      // weather station) and convert it into a stream.
                                    .filter(t   ->  t.getTime() >= startTime &&     // filter out the temperature based on conditions
                                                    t.getTime() <= endTime)         // find the max temperature by using comparing function
                                    .max(comparing(MeasurementQ1::getTemperature)).get();
        return tempValue;       
    }

    // Driver function defination
    public static void main(String Args[]){
        
        // Creating Scanner object        
        Scanner ip = new Scanner(System.in);
        // variable to use in switch statement below
        int choice = 0;
        // variables to capture the start and end Times
        int stTime = 0, eTime = 0;
        
        //Create a series of MeasurementQ1 object
        MeasurementQ1 m1 = new MeasurementQ1(01, 7.5);
        MeasurementQ1 m2 = new MeasurementQ1(12, 8.8);
        MeasurementQ1 m3 = new MeasurementQ1(30, 11.5);
        MeasurementQ1 m4 = new MeasurementQ1(14, 14.6);
        MeasurementQ1 m5 = new MeasurementQ1(05, 7.0);
        MeasurementQ1 m6 = new MeasurementQ1(28, 14.2);
        MeasurementQ1 m7 = new MeasurementQ1(17, 4.8);
        MeasurementQ1 m8 = new MeasurementQ1(07, 5.9);
        MeasurementQ1 m9 = new MeasurementQ1(15, 8.5);
        MeasurementQ1 m10 = new MeasurementQ1(01, 12.4);
        MeasurementQ1 m11 = new MeasurementQ1(13, 12.9);
        MeasurementQ1 m12 = new MeasurementQ1(03, 12.5);
        MeasurementQ1 m13 = new MeasurementQ1(19, 9.0);
        MeasurementQ1 m14 = new MeasurementQ1(07, 10.0);
        MeasurementQ1 m15 = new MeasurementQ1(02, 6.6);
        MeasurementQ1 m16 = new MeasurementQ1(29, 13.2);
        MeasurementQ1 m17 = new MeasurementQ1(31, 11.1);
        MeasurementQ1 m18 = new MeasurementQ1(18, 12.0);
        
        // Create a list and populate the list - each list is being used for different stations as measurements
        List<MeasurementQ1> listMeasure1 = new ArrayList<>();
        listMeasure1.add(m1);
        listMeasure1.add(m2);
        listMeasure1.add(m3);
        
        List<MeasurementQ1> listMeasure2 = new ArrayList<>();
        listMeasure2.add(m4);
        listMeasure2.add(m5);
        listMeasure2.add(m6);
        
        List<MeasurementQ1> listMeasure3 = new ArrayList<>();
        listMeasure3.add(m7);
        listMeasure3.add(m8);
        listMeasure3.add(m9);
        
        List<MeasurementQ1> listMeasure4 = new ArrayList<>();
        listMeasure4.add(m10);
        listMeasure4.add(m11);
        listMeasure4.add(m12);
        
        List<MeasurementQ1> listMeasure5 = new ArrayList<>();
        listMeasure5.add(m13);
        listMeasure5.add(m14);
        listMeasure5.add(m15);
        
        List<MeasurementQ1> listMeasure6 = new ArrayList<>();
        listMeasure6.add(m16);
        listMeasure6.add(m17);
        listMeasure6.add(m18);

        //Create the WeatherStationQ1 object - assign city and measurements(List made above)
        WeatherStationQ1 WS1 = new WeatherStationQ1("Cork", listMeasure1);
        WeatherStationQ1 WS2 = new WeatherStationQ1("Limerick", listMeasure2);
        WeatherStationQ1 WS3 = new WeatherStationQ1("Dublin", listMeasure3);
        WeatherStationQ1 WS4 = new WeatherStationQ1("Galway", listMeasure4);
        WeatherStationQ1 WS5 = new WeatherStationQ1("Donegal", listMeasure5);
        WeatherStationQ1 WS6 = new WeatherStationQ1("Mayo", listMeasure6);
        
        System.out.println("\t\t\t\tAssignment-2: Q1: Find the maximum temperature");
        System.out.println("1. Cork\n2. Limerick\n3. Dublin\n4. Galway\n5. Donegal\n6. Mayo");
        
        do{
            // try - catch to assure correct choice input is given
            do{
                try{
                    System.out.print("\nSelect the corresponding city(1-6): ");
                    choice = ip.nextInt();
                }
                catch (InputMismatchException e) {
                    System.out.print("\nInvalid input !!");
                }
                ip.nextLine(); // clears the buffer
            }while(choice == 0);    
        
            //Switch statement to choose between the weatherstations to find the details
            switch(choice){
                case 1: System.out.println("\nThe Temperature recorded in "+WS1.getCity()+ " are : \n"); 
                        // using for loop to print the list of measurements recorded at the station 
                        for(int i=0;i<(WS1.getMeasurements().size());i++){    //WS1.getMeasurements().size() returns the size of list of measurements  
                            System.out.print("Temperature: "+WS1.getMeasurements().get(i).getTemperature()); // print out each object's temperature 
                            System.out.print("\t\tTime: "+WS1.getMeasurements().get(i).getTime());              // and time.
                            System.out.println("");
                        }
                        // try - catch to assure correct starttime is given
                        do{
                            try{
                                System.out.print("\nEnter the start time: ");
                                stTime = ip.nextInt();
                            }
                            catch (InputMismatchException e) {
                                System.out.print("\nInvalid input !!");
                            }
                            ip.nextLine(); // clears the buffer
                        }while(stTime == 0);    
                        // try - catch to assure correct endtime is given
                        do{
                            try{
                                System.out.print("\nEnter the End time: ");
                                eTime = ip.nextInt();
                            }
                            catch (InputMismatchException e) {
                                System.out.print("\nInvalid input !!");
                            }
                            ip.nextLine(); // clears the buffer
                            if(eTime<stTime){   //  the endTime should always be > Start Time
                                System.out.print("\nEnd Time should be greater than Start time !!");
                            }
                        }while(eTime == 0 || eTime < stTime);
                        
                        System.out.println("\nStart Time: "+stTime+"\t\t End Time: "+eTime);
                        
                        // try-catch to capture exception if there are no records within the given range
                        try{
                            WS1.maxTemperature(stTime,eTime).getTemperature();
                        }
                        catch (NoSuchElementException e) {
                            System.out.print("\nNo record found in specified Range!\n");
                            break;
                        }
                        
                        // using weatherStationQ1 object to call maxTemperature() with entered start and end time.
                        // the maxTemperature() returns an object of MeasurementQ1 class which is then used to call getTemperature() 
                        // and getTime() to get the results.
                        
                        System.out.println("\nThe maximum temperature recorded in "+WS1.getCity()+"  was: "+WS1.maxTemperature(stTime,eTime).getTemperature()+" and it happen at time: "+WS1.maxTemperature(stTime,eTime).getTime());
                        System.out.println("");
                        break;
                        
            // All the rest of the switch cases are same as case 1; only the weatherStationQ1 object are different(depending on the case)            
            
                case 2: System.out.println("\nThe Temperature recorded in "+WS2.getCity()+ " are : \n");
                        for(int i=0;i<(WS2.getMeasurements().size());i++){
                            System.out.print("Temperature: "+WS2.getMeasurements().get(i).getTemperature());
                            System.out.print("\t\tTime: "+WS2.getMeasurements().get(i).getTime());
                            System.out.println("");
                        }
                        
                        do{
                            try{
                                System.out.print("\nEnter the start time: ");
                                stTime = ip.nextInt();
                            }
                            catch (InputMismatchException e) {
                                System.out.print("\nInvalid input !!");
                            }
                            ip.nextLine(); // clears the buffer
                        }while(stTime == 0);    
        
                        do{
                            try{
                                System.out.print("\nEnter the End time: ");
                                eTime = ip.nextInt();
                            }
                            catch (InputMismatchException e) {
                                System.out.print("\nInvalid input !!");
                            }
                            ip.nextLine(); // clears the buffer
                            if(eTime<stTime){
                                System.out.print("\nEnd Time should be greater than Start time !!");
                            }
                        }while(eTime == 0 || eTime < stTime);
                        
                        System.out.println("\nStart Time: "+stTime+"\t\t End Time: "+eTime);
                        try{
                            WS2.maxTemperature(stTime,eTime).getTemperature();
                        }
                        catch (NoSuchElementException e) {
                            System.out.print("\nNo record found in specified Range!\n");
                            break;
                        }
                        System.out.println("\nThe maximum temperature recorded in "+WS2.getCity()+"  was: "+WS2.maxTemperature(stTime,eTime).getTemperature()+" and it happen at time: "+WS2.maxTemperature(stTime,eTime).getTime());
                        System.out.println("");
                        break;
                    
                case 3: System.out.println("\nThe Temperature recorded in "+WS3.getCity()+ " are : \n");
                        for(int i=0;i<(WS3.getMeasurements().size());i++){
                            System.out.print("Temperature: "+WS3.getMeasurements().get(i).getTemperature());
                            System.out.print("\t\tTime: "+WS3.getMeasurements().get(i).getTime());
                            System.out.println("");
                        }
                        
                        do{
                            try{
                                System.out.print("\nEnter the start time: ");
                                stTime = ip.nextInt();
                            }
                            catch (InputMismatchException e) {
                                System.out.print("\nInvalid input !!");
                            }
                            ip.nextLine(); // clears the buffer
                        }while(stTime == 0);    
        
                        do{
                            try{
                                System.out.print("\nEnter the End time: ");
                                eTime = ip.nextInt();
                            }
                            catch (InputMismatchException e) {
                                System.out.print("\nInvalid input !!");
                            }
                            ip.nextLine(); // clears the buffer
                            if(eTime<stTime){
                                System.out.print("\nEnd Time should be greater than Start time !!");
                            }
                        }while(eTime == 0 || eTime < stTime);
                        
                        System.out.println("\nStart Time: "+stTime+"\t\t End Time: "+eTime);
                        try{
                            WS3.maxTemperature(stTime,eTime).getTemperature();
                        }
                        catch (NoSuchElementException e) {
                            System.out.print("\nNo record found in specified Range!\n");
                            break;
                        }
                        System.out.println("\nThe maximum temperature recorded in "+WS3.getCity()+"  was: "+WS3.maxTemperature(stTime,eTime).getTemperature()+" and it happen at time: "+WS3.maxTemperature(stTime,eTime).getTime());
                        System.out.println("");
                        break;
                    
                case 4: System.out.println("\nThe Temperature recorded in "+WS4.getCity()+ " are : \n");
                        for(int i=0;i<(WS4.getMeasurements().size());i++){
                            System.out.print("Temperature: "+WS4.getMeasurements().get(i).getTemperature());
                            System.out.print("\t\tTime: "+WS4.getMeasurements().get(i).getTime());
                            System.out.println("");
                        }   
                        
                        do{
                            try{
                                System.out.print("\nEnter the start time: ");
                                stTime = ip.nextInt();
                            }
                            catch (InputMismatchException e) {
                                System.out.print("\nInvalid input !!");
                            }
                            ip.nextLine(); // clears the buffer
                        }while(stTime == 0);    
        
                        do{
                            try{
                                System.out.print("\nEnter the End time: ");
                                eTime = ip.nextInt();
                            }
                            catch (InputMismatchException e) {
                                System.out.print("\nInvalid input !!");
                            }
                            ip.nextLine(); // clears the buffer
                            if(eTime<stTime){
                                System.out.print("\nEnd Time should be greater than Start time !!");
                            }
                        }while(eTime == 0 || eTime < stTime);
                        
                        System.out.println("\nStart Time: "+stTime+"\t\t End Time: "+eTime);
                        try{
                            WS4.maxTemperature(stTime,eTime).getTemperature();
                        }
                        catch (NoSuchElementException e) {
                            System.out.print("\nNo record found in specified Range!\n");
                            break;
                        }
                        System.out.println("\nThe maximum temperature recorded in "+WS4.getCity()+"  was: "+WS4.maxTemperature(stTime,eTime).getTemperature()+" and it happen at time: "+WS4.maxTemperature(stTime,eTime).getTime());
                        System.out.println("");
                        break;
                    
                case 5: System.out.println("\nThe Temperature recorded in "+WS5.getCity()+ " are : \n");
                        for(int i=0;i<(WS5.getMeasurements().size());i++){
                            System.out.print("Temperature: "+WS5.getMeasurements().get(i).getTemperature());
                            System.out.print("\t\tTime: "+WS5.getMeasurements().get(i).getTime());
                            System.out.println("");
                        }
                        
                        do{
                            try{
                                System.out.print("\nEnter the start time: ");
                                stTime = ip.nextInt();
                            }
                            catch (InputMismatchException e) {
                                System.out.print("\nInvalid input !!");
                            }
                            ip.nextLine(); // clears the buffer
                        }while(stTime == 0);    
        
                        do{
                            try{
                                System.out.print("\nEnter the End time: ");
                                eTime = ip.nextInt();
                            }
                            catch (InputMismatchException e) {
                                System.out.print("\nInvalid input !!");
                            }
                            ip.nextLine(); // clears the buffer
                            if(eTime<stTime){
                                System.out.print("\nEnd Time should be greater than Start time !!");
                            }
                        }while(eTime == 0 || eTime < stTime);
                        
                        System.out.println("\nStart Time: "+stTime+"\t\t End Time: "+eTime);
                        try{
                            WS5.maxTemperature(stTime,eTime).getTemperature();
                        }
                        catch (NoSuchElementException e) {
                            System.out.print("\nNo record found in specified Range!\n");
                            break;
                        }
                        System.out.println("\nThe maximum temperature recorded in "+WS5.getCity()+"  was: "+WS5.maxTemperature(stTime,eTime).getTemperature()+" and it happen at time: "+WS5.maxTemperature(stTime,eTime).getTime());
                        System.out.println("");
                        break;
                    
                case 6: System.out.println("\nThe Temperature recorded in "+WS6.getCity()+ " are : \n");
                        for(int i=0;i<(WS6.getMeasurements().size());i++){
                            System.out.print("Temperature: "+WS6.getMeasurements().get(i).getTemperature());
                            System.out.print("\t\tTime: "+WS6.getMeasurements().get(i).getTime());
                            System.out.println("");
                        }
                        
                        do{
                            try{
                                System.out.print("\nEnter the start time: ");
                                stTime = ip.nextInt();
                            }
                            catch (InputMismatchException e) {
                                System.out.print("\nInvalid input !!");
                            }
                            ip.nextLine(); // clears the buffer
                        }while(stTime == 0);    
        
                        do{
                            try{
                                System.out.print("\nEnter the End time: ");
                                eTime = ip.nextInt();
                            }
                            catch (InputMismatchException e) {
                                System.out.print("\nInvalid input !!");
                            }
                            ip.nextLine(); // clears the buffer
                            if(eTime<stTime){
                                System.out.print("\nEnd Time should be greater than Start time !!");
                            }
                        }while(eTime == 0 || eTime < stTime);
                        
                        System.out.println("\nStart Time: "+stTime+"\t\t End Time: "+eTime);
                        try{
                            WS6.maxTemperature(stTime,eTime).getTemperature();
                        }
                        catch (NoSuchElementException e) {
                            System.out.print("\nNo record found in specified Range!\n");
                            break;
                        }
                        System.out.println("\nThe maximum temperature recorded in "+WS6.getCity()+"  was: "+WS6.maxTemperature(stTime,eTime).getTemperature()+" and it happen at time: "+WS6.maxTemperature(stTime,eTime).getTime());
                        System.out.println("");
                        break;
                    
                default: System.out.println("\nEnter correct option(1-6) !!!");
            }
        }while(choice<1 || choice>6);
 
    }
}