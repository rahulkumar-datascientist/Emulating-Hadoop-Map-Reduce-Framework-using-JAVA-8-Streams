// Assignment 2 :   Tools & Techniques for Large Scale Data Analytics
// Name         :   Rahul Kumar
// Student ID   :   20230113

// Q2. Find the  temperature T1 paired with the number of times a temperature in the interval [t1-r..t1+r] has been measured so far by any of the
//     weather stations in stations, similarly for T2. ***emulating MAP-REDUCE***

// Input        :   1. Enter value of T1, T2 and R.
// Constraints  :   T1, T2 and R cannot take value 0 (Rest all double are accepted)
// Output       :   Pair of (T1,count within range) and (T2,count within range)

package javaassignment2;


// Importing the "java.util" package for taking input from Scanner class
// Importing the next 2 package for using stream operations and comparing objects. 

import java.util.*;
import static java.util.Comparator.comparing;
import java.util.stream.*;

//  Class data structure for representing (key,value)pairs. Objects will have attributes:
// 1. Key (a Double value, representing the key which will be used for mapping)
// 2. count (an integer, representing the value corresponding to the key)
class keyValue{
    
    // setting up class attributes
    private double key ;
    private int count;
    
    // Setting up constructor for the keyValue object
    public keyValue(double key,int count){
        this.key = key;
        this.count = count;
    }
    
    // getter for the attributes of the keyValue object
    public double getKey(){
        return key;
    }
    
    public int getCount(){
        return count;
    }
}

// Create a class MeasurementQ2. Objects of class MeasurementQ2 should have attributes: 
// 1. Time (an integer, representing the time of the measurement)
// 2. Temperature (a double number).
class MeasurementQ2 {

    //Setting up class attributes
    private int time;
    private double temperature;

    //Setting up constructor for the MeasurementQ2 object
    public MeasurementQ2(int time, double temperature){
        this.time = time;
        this.temperature=temperature;
    }

    //setters and getters for the attributes of the object MeasurementQ2
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

// Create a class WeatherStationQ2 with three attributes (fields): 
// 1. The city where the station is located, 
// 2. The station’s measurements (a list of objects of class Measurement), and
// 3. A static field stations (a list of all existing weather stations.

// Add a method countTemperatures(t1,t2,r) which returns a list of two pairs: 
// 1. temperature t1 paired with the count of temperature measured within [t1-r..t1+r] by any of the weatherStations in stations
// 2. temperature t2 paired with the count of temperature measured within [t2-r..t2+r] by any of the weatherStations in stations

public class WeatherStationQ2 {

    //Setting up class attributes
    private String city;                            
    private List<MeasurementQ2> measurements;       
    public static List<WeatherStationQ2> stations = new ArrayList<>();  
    
    //Setting up constructor for the WeatherStationQ2 object
    public WeatherStationQ2(String city, List<MeasurementQ2> measurements){
        this.city = city;
        this.measurements = measurements;
    }

    //Setters and getters for the attributes of the object WeatherStationQ2
    public void setCity(String city){
        this.city = city;
    }

    public String getCity(){
        return city;        
    }

    public void setMeasurements(List<MeasurementQ2> measurements){
        this.measurements = measurements;
    }

    public List<MeasurementQ2> getMeasurements(){
        return measurements;
    }

    //maxTemperature() to return the object of MeasurementQ2 class having highest temperature within a given time range
    public MeasurementQ2 maxTemperature(int startTime, int endTime){                // getMeasurements(a list of measurements object of the calling 
        MeasurementQ2 tempValue =   getMeasurements().stream()                      // weather station) and convert it into a stream.
                                    .filter(t   ->  t.getTime() >= startTime &&     // filter out the temperature based on conditions
                                                    t.getTime() <= endTime)         // find the max temperature by using comparing function
                                    .max(comparing(MeasurementQ2::getTemperature)).get();
        return tempValue;       
    }

    //countTemperature() to return a list of 2 pairs ***emulating MapReduce***
    public List<Map<Object,Long>> countTemperature(double t1, double t2, double r){
     
        List<Double> param = new ArrayList<>();     // creating an arrayList 'parm' and adding t1, t2 and r
        param.add(t1);                              // using this list to retrive the temperature(t1 or t2) when calculating the range
        param.add(t2);                              
        param.add(r);                               
        
        // creating a list of all the temperatures measured within all the stations
        List<Double> temp = stations.parallelStream()                           // converts stations(list of objects of WeatherStationQ2) into stream
                            .flatMap(stlist -> stlist.getMeasurements().parallelStream()) // flattening class object stream into stream of measurements
                            .map(mslist -> mslist.getTemperature())                 // using map to get all the temperatures from measurement stream
                            .sorted()                                                       // sorting it in ascending order (for reading purpose)
                            .collect(Collectors.toList());                                  // and collecting it in a list.
                
        System.out.println("\nList of all the temperature measured (sorted): \n"+temp);
        
        List<Map<Object,Long>> res = new ArrayList<>();         // creating a list of Map object
        
        for (int i = 0;i<2;i++){                // as we need 2 pairs
            int finalI = i;                     // use index 'i' as finalI(used to retrieve the temperature from 'parm' list made above)

            // sequence of steps performed ***emulating MapReduce***
            // creating a Map object containing the temperature mapped with the count within the range
            // 1. using the temperature list and converting it into stream
            // 2. filtering out temperature satisfying the condition **similar to spliting**
            // 3. mapping the temperature to object of keyValue as(t,1) **similar to mapping**
            // 4. grouping by the key(t) **similar to shuffling** and counting the number of values **similar to reduce**
            
            Map<Object,Long> occur =    temp.parallelStream()   
                                        .filter(aDouble ->  (aDouble >= (param.get(finalI) - param.get(2)) &&    
                                                             aDouble <= (param.get(finalI) + param.get(2))))
                                        .map(c -> new keyValue(param.get(finalI),1))
                                        .collect(Collectors.groupingBy(m -> m.getKey(),Collectors.counting()));
            
            // adding the (temperature,count) to the list
            res.add(occur);
        }
        return res;
    }
         
    // Driver function defination
    public static void main(String Args[]){
        // Creating Scanner object        
        Scanner ip = new Scanner(System.in);
        // store the result of countTemperature()
        List<Map<Object,Long>> res;
        // initializing the temperature value
        double t1 =0.0, t2=0.0, r=0.0;
        
        //Creates a series of MeasurementQ2 object
        MeasurementQ2 m1 = new MeasurementQ2(01, 7.5);
        MeasurementQ2 m2 = new MeasurementQ2(12, 8.8);
        MeasurementQ2 m3 = new MeasurementQ2(30, 11.5);
        MeasurementQ2 m4 = new MeasurementQ2(14, 14.6);
        MeasurementQ2 m5 = new MeasurementQ2(05, 7.0);
        MeasurementQ2 m6 = new MeasurementQ2(28, 14.2);
        MeasurementQ2 m7 = new MeasurementQ2(17, 4.8);
        MeasurementQ2 m8 = new MeasurementQ2(07, 5.9);
        MeasurementQ2 m9 = new MeasurementQ2(15, 8.5);
        MeasurementQ2 m10 = new MeasurementQ2(01, 12.4);
        MeasurementQ2 m11 = new MeasurementQ2(13, 12.9);
        MeasurementQ2 m12 = new MeasurementQ2(03, 12.5);
        MeasurementQ2 m13 = new MeasurementQ2(19, 9.0);
        MeasurementQ2 m14 = new MeasurementQ2(07, 10.0);
        MeasurementQ2 m15 = new MeasurementQ2(02, 6.6);
        MeasurementQ2 m16 = new MeasurementQ2(29, 13.2);
        MeasurementQ2 m17 = new MeasurementQ2(31, 11.1);
        MeasurementQ2 m18 = new MeasurementQ2(18, 12.0);
        
        // Create a list and populate the list - each list is being used for different stations as measurements
        List<MeasurementQ2> listMeasure1 = new ArrayList<>();
        listMeasure1.add(m1);
        listMeasure1.add(m2);
        listMeasure1.add(m3);
        
        List<MeasurementQ2> listMeasure2 = new ArrayList<>();
        listMeasure2.add(m4);
        listMeasure2.add(m5);
        listMeasure2.add(m6);
        
        List<MeasurementQ2> listMeasure3 = new ArrayList<>();
        listMeasure3.add(m7);
        listMeasure3.add(m8);
        listMeasure3.add(m9);
        
        List<MeasurementQ2> listMeasure4 = new ArrayList<>();
        listMeasure4.add(m10);
        listMeasure4.add(m11);
        listMeasure4.add(m12);
        
        List<MeasurementQ2> listMeasure5 = new ArrayList<>();
        listMeasure5.add(m13);
        listMeasure5.add(m14);
        listMeasure5.add(m15);
        
        List<MeasurementQ2> listMeasure6 = new ArrayList<>();
        listMeasure6.add(m16);
        listMeasure6.add(m17);
        listMeasure6.add(m18);

        //Create the WeatherStationQ1 object - assign city and measurements(List made above)
        WeatherStationQ2 WS1 = new WeatherStationQ2("Cork", listMeasure1);
        WeatherStationQ2 WS2 = new WeatherStationQ2("Limerick", listMeasure2);
        WeatherStationQ2 WS3 = new WeatherStationQ2("Dublin", listMeasure3);
        WeatherStationQ2 WS4 = new WeatherStationQ2("Galway", listMeasure4);
        WeatherStationQ2 WS5 = new WeatherStationQ2("Donegal", listMeasure5);
        WeatherStationQ2 WS6 = new WeatherStationQ2("Mayo", listMeasure6);
        
        //adding weather station object to a list of stations
        stations.add(WS1);
        stations.add(WS2);
        stations.add(WS3);
        stations.add(WS4);
        stations.add(WS5);
        stations.add(WS6);
        
        System.out.println("\t\t\t\tAssignment-2: Q2: Find the count of temperature between the provided range");
        
        do{
            // try - catch to assure correct input T1 is given
            try{
                System.out.print("\nEnter the value for Temperature T1: ");
                t1 = ip.nextDouble();
            }
            catch (InputMismatchException e) {
                    System.out.print("\nInvalid input !!");
            }
            ip.nextLine();  // clears the buffer
        }while(t1 == 0.0);
        
        do{
            // try - catch to assure correct input T2 is given
            try{
                System.out.print("\nEnter the value for Temperature T2: ");
                t2 = ip.nextDouble();
            }
            catch (InputMismatchException e) {
                    System.out.print("\nInvalid input !!");
            }
            ip.nextLine();
        }while(t2 == 0.0);
        
        
        do{
            // try - catch to assure correct input R is given
            try{
                System.out.print("\nEnter the value for range R: ");
                r = ip.nextDouble();
            }
            catch (InputMismatchException e) {
                    System.out.print("\nInvalid input !!");
            }
            ip.nextLine();
        }while(r == 0.0);
        
        res = WS1.countTemperature(t1,t2,r);
        
        System.out.println("\nfor T1: "+t1+" and R: "+r);
        System.out.println("Lower bound(T1-R): "+(t1-r)+"\t\tUpper bound(T1+R): "+(t1+r));
        System.out.println("\n\nfor T2: "+t2+" and R: "+r);
        System.out.println("Lower bound(T2-R): "+(t2-r)+"\t\tUpper bound(T2+R): "+(t2+r));
        System.out.println("\nThe count is:");
        System.out.println("\n"+res+"\n");
        
    }
}
