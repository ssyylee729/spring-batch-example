package spring.batch.part6;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import spring.batch.part4.UserRepository;

import java.util.HashMap;
import java.util.Map;

public class UserLevelUpPartitioner implements Partitioner
{
    private final UserRepository userRepository;

    public UserLevelUpPartitioner(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        //gridSize는 slave step의 사이즈가 됨.
        long minId = userRepository.findMinId(); //1
        Long maxId = userRepository.findMaxId(); //40000

        long targetSize = (maxId - minId ) / gridSize + 1; //5000
        /*
        * partition0: 1, 5000
        * partition1: 5001, 10000
        * ...
        * partition7: 35001, 40000
        * */
        Map <String, ExecutionContext> result = new HashMap<>();

        long stepNumber = 0;

        long start = minId;
        long end = start + targetSize -1;

        while(start<= maxId){
           ExecutionContext value =  new ExecutionContext();

           result.put("partition"+ stepNumber, value);
           if(end >= maxId) {
               end = maxId;
           }

           value.putLong("minId", start);
           value.putLong("maxId", end);

           start += targetSize;
           end += targetSize;
           stepNumber++;
        }
        return result;
    }
}