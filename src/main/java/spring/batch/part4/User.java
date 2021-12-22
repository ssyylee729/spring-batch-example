package spring.batch.part4;

import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import spring.batch.part5.Orders;

import javax.persistence.*;
import java.time.LocalDate;
import java.util.List;
import java.util.Objects;

@Getter
@Entity
@NoArgsConstructor
public class User {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String username;

    @Enumerated(EnumType.STRING)
    private Level level = Level.NORMAL;

    //user 저장 시 order도 저장되도록 cascade 설정
    //중간 테이블 없애기 위하여 @JoinColumn 추가
    @OneToMany(cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @JoinColumn(name = "user_id")
    private List<Orders> orders;


    private LocalDate updatedDate;

    @Builder
    private User(String username, List<Orders> orders){
        this.username = username;
        this.orders = orders;
    }

    public boolean availableLevelUp() {
        return Level.availableLevelUp(this.getLevel(), this.getTotalAmount());
    }


    private int getTotalAmount(){
        return this.orders.stream()
                .mapToInt(Orders::getAmount)
                .sum();
    }

    public Level levelUp(){
        Level nextLevel = Level.getNextLevel(this.getTotalAmount());
        this.level = nextLevel;
        this.updatedDate = LocalDate.now();

        return nextLevel;

    }
    public enum Level{
        VIP(500_000, null),
        GOLD(500_000, VIP),
        SILVER(300_000, GOLD),
        NORMAL(200_000, SILVER);

        private final int nextAmount;
        private final Level nextLevel;

        Level(int nextAmount, Level nextLevel) {

            this.nextAmount = nextAmount;
            this.nextLevel = nextLevel;

        }

        private static boolean availableLevelUp(Level level, int totalAmount) {
            if(Objects.isNull(level)){
                return false;
            }
            if(Objects.isNull(level.nextLevel)){ //VIP는 nextLevel이 없기 때문에 등급 상향 조건에 맞지 않음.
                return false;
            }
            return totalAmount >= level.nextAmount;
        }

        private static Level getNextLevel(int totalAmount) {
            if(totalAmount >= Level.VIP.nextAmount)
                return VIP;
            if(totalAmount >= Level.GOLD.nextAmount)
                return GOLD.nextLevel;
            if(totalAmount >= Level.SILVER.nextAmount)
                return SILVER.nextLevel;
            if(totalAmount >= Level.NORMAL.nextAmount)
                return NORMAL.nextLevel;

            return NORMAL;
        }
    }



}