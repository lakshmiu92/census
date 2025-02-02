import java.io.Closeable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * Implement the two methods below. We expect this class to be stateless and thread safe.
 */
public class Census {
    /**
     * Number of cores in the current machine.
     */
    private static final int CORES = Runtime.getRuntime().availableProcessors();

    /**
     * Output format expected by our tests.
     */
    public static final String OUTPUT_FORMAT = "%d:%d:%d"; // Position:Age=Total

    /**
     * Factory for iterators.
     */
    private final Function<String, Census.AgeInputIterator> iteratorFactory;

    /**
     * Creates a new Census calculator.
     *
     * @param iteratorFactory factory for the iterators.
     */
    public Census(Function<String, Census.AgeInputIterator> iteratorFactory) {
        this.iteratorFactory = iteratorFactory;
    }

    /**
     * Given one region name, call {@link #iteratorFactory} to get an iterator for this region and return
     * the 3 most common ages in the format specified by {@link #OUTPUT_FORMAT}.
     */
    public String[] top3Ages(String region) {

//        In the example below, the top three are ages 10, 15 and 12
//        return new String[]{
//                String.format(OUTPUT_FORMAT, 1, 10, 38),
//                String.format(OUTPUT_FORMAT, 2, 15, 35),
//                String.format(OUTPUT_FORMAT, 3, 12, 30)
//        };

        try (Census.AgeInputIterator ageInputIterator = iteratorFactory.apply(region)) {
            if(!ageInputIterator.hasNext()){
                return new String[0];
            }
            else{
                return getTop3AgeCountsFromIterator(ageInputIterator);
            }
        }
        catch(Exception e){
            throw new RuntimeException("Error occurred for region: " + region + ": " + e.getMessage(), e);
        }

    }

    /*
    * Method getTop3Ages accepts a Map of a particular age and the number of instances
    * of the age in the dataset(ie. count).
    *
    * */
    // change to private
    public String[] getTop3Ages(Map<Integer, Integer> ageCounts){

        List <Map.Entry<Integer,Integer>> agesList = ageCounts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(3)
                .collect(Collectors.toList());

        String[] result  = new String[agesList.size()];
        for(int i = 0; i < agesList.size(); i++){
            Map.Entry<Integer,Integer> entry = agesList.get(i);
            //Index is stored as 1,2,3...
            result[i] = String.format(OUTPUT_FORMAT, i+1, entry.getKey(), entry.getValue());
        }
        return result;
    }

    /*
    * Method getAgeCountsForIterator accepts an Iterator and returns a HashMap of counts of ages within that iterator.
    * */

    private Map<Integer,Integer> getAgeCountsForIterator(Census.AgeInputIterator ageInputIterator){
        Map<Integer,Integer> iteratorMap = new HashMap<>();
        while(ageInputIterator.hasNext()){
            int age = ageInputIterator.next();
            //Handling for invalid ages
            if (age >= 0){
                iteratorMap.put(age, iteratorMap.getOrDefault(age, 0) + 1);
            }

        }
        return iteratorMap;
    }

    /*
    *  Method getTop3AgeCountsFromIterator gets the top 3 ages within an iterator. It uses the getAgeCountsForIterator
    * method to create a Map of the counts.
    * */

    private String[] getTop3AgeCountsFromIterator(Census.AgeInputIterator ageInputIterator){
        Map<Integer,Integer> ageCounts = getAgeCountsForIterator(ageInputIterator);
        return getTop3Ages(ageCounts);
    }

    /**
     * Given a list of region names, call {@link #iteratorFactory} to get an iterator for each region and return
     * the 3 most common ages across all regions in the format specified by {@link #OUTPUT_FORMAT}.
     * We expect you to make use of all cores in the machine, specified by {@link #CORES).
     */
    public String[] top3Ages(List<String> regionNames) {

//        In the example below, the top three are ages 10, 15 and 12
//        return new String[]{
//                String.format(OUTPUT_FORMAT, 1, 10, 38),
//                String.format(OUTPUT_FORMAT, 2, 15, 35),
//                String.format(OUTPUT_FORMAT, 3, 12, 30)
//        };

        ExecutorService executor = Executors.newFixedThreadPool(CORES);
        List <CompletableFuture<Map<Integer,Integer>>> futures = new ArrayList<>();

        for(String region: regionNames){
            CompletableFuture<Map<Integer,Integer>> future = CompletableFuture.supplyAsync(() ->{
                try(Census.AgeInputIterator ageInputIterator = iteratorFactory.apply(region)) {
                    Map<Integer, Integer> ageCounts = new HashMap<>();
                    while(ageInputIterator.hasNext()){
                        int age = ageInputIterator.next();
                        ageCounts.put(age, ageCounts.getOrDefault(age, 0) + 1);
                    }
                    return ageCounts;
                }
                catch(Exception e) {
                    throw new RuntimeException("Error occurred : " + e.getMessage(), e);
                }
            }, executor);
            futures.add(future);
        }

        Map<Integer, Integer> mergedAgeCounts = new HashMap<>();
        for ( CompletableFuture<Map<Integer,Integer>> future: futures){
            try{
                Map<Integer,Integer> ageCounts = future.get();
                ageCounts.forEach((age, count) -> mergedAgeCounts.put(age, mergedAgeCounts.getOrDefault(age,0) + count));
            } catch (Exception e) {
                throw new RuntimeException("Error while merging different regions", e);
            }

        }
        executor.shutdown();
        return getTop3Ages(mergedAgeCounts);

        //throw new UnsupportedOperationException();
    }


    /**
     * Implementations of this interface will return ages on call to {@link Iterator#next()}. They may open resources
     * when being instantiated created.
     */
    public interface AgeInputIterator extends Iterator<Integer>, Closeable {
    }

}
