import java.util.*;
import java.util.function.Function;


public class IntermediateTester {
    private static Function<String, Census.AgeInputIterator> factory = IntermediateTester::iteratorForRegion;
    private static Census census = new Census(factory);
    private static Map<String, Census.AgeInputIterator> createdIterators = new HashMap<>();

    private static Census.AgeInputIterator iteratorForRegion(String region) {
        return Optional.ofNullable(createdIterators.get(region))
                .orElseThrow(() -> new RuntimeException("Couldn't find region " + region));
    }

    public static void main(String[] args){

        Map<Integer, Integer> testMap = new HashMap<Integer, Integer>() {
            {
                put(20, 100);
                put(25, 75);
                put(30, 200);
                put(35, 50);
            }
        };
        System.out.println("TEST");
        System.out.println(Arrays.toString(census.getTop3Ages(testMap)));
    }
}
