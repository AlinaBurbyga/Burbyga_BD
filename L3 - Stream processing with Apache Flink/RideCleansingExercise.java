package com.ververica.flinktraining.exercises.datastream_java.basics;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.exercises.datastream_java.utils.MissingSolutionException;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RideCleansingExercise extends ExerciseBase 
{
    public static void main(String[] args) throws Exception 
    {

        // Получение параметров из командной строки
        ParameterTool params = ParameterTool.fromArgs(args);
        
        final String input = params.get("input", ExerciseBase.pathToRideData);
        System.out.println("point 0");
        
        final int maxEventDelay = 60;  // Максимальная задержка события    
        final int servingSpeedFactor = 600; // Фактор скорости обработки событий 
        System.out.println("point 1");
        // Настраиваем окружение для потоковой обработки данных
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT);
        System.out.println("point 2");
        // Создание источника данных о поездках 
        DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)));
        System.out.println("point 3");
        // Фильтр поездок
        DataStream<TaxiRide> filteredRides = rides
                .filter(new NYCFilter());
        System.out.println("point 4");
        // Вывод отфильтрованного потока данных
        printOrTest(filteredRides);
        env.execute("Taxi Ride Cleansing");
    }

    // Класс фильтра для отбора поездок, начинающихся и заканчивающихся в Нью-Йорке
    public static class NYCFilter implements FilterFunction<TaxiRide> 
    {
        @Override
        public boolean filter(TaxiRide taxiRide) throws Exception 
        {
            return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) && GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
        }
    }
}