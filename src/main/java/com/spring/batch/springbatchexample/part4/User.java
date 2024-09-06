package com.spring.batch.springbatchexample.part4;

import com.spring.batch.springbatchexample.part5.Orders;
import jakarta.persistence.*;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

@Entity(name = "users")
@Table(name = "users")
@Getter
@NoArgsConstructor
public class User {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false)
    private Long id;

    private String username;

    @Enumerated(EnumType.STRING)
    private Grade grade = Grade.NORMAL;

    @OneToMany(cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
    @JoinColumn(name="user_id")
    List<Orders> orders;

    private LocalDateTime updatedDate;

    @Builder
    private User(String username, List<Orders> orders) {
        this.username = username;
        this.orders = orders;
    }

    public boolean availableLevelUp() {
        return Grade.availableLevelUp(this.getGrade(), this.getTotalAmount());
    }

    public Grade levelUp() {
        Grade nextLevel = Grade.getNextLevel(this.getTotalAmount());

        this.grade = nextLevel;
        this.updatedDate = LocalDateTime.now();
        return nextLevel;
    }

    private int getTotalAmount() {
        return this.orders.stream().mapToInt(Orders::getAmount).sum();
    }

    public enum Grade {
        VIP(500_000, null),
        GOLD(500_000, VIP),
        SILVER(300_000, GOLD),
        NORMAL(200_000, SILVER);

        private final int nextAmount;
        private final Grade nextLevel;

        Grade(int nextAmount, Grade nextLevel) {
            this.nextAmount = nextAmount;
            this.nextLevel = nextLevel;
        }

        public static boolean availableLevelUp(Grade grade, int totalAmount) {
            if(Objects.isNull(grade)){
                return false;
            }

            if(Objects.isNull(grade.nextAmount)){
                return false;
            }

            return totalAmount >= grade.nextAmount;
        }

        private static Grade getNextLevel(int totalAmount) {
            if(totalAmount >= Grade.VIP.nextAmount)
                return Grade.VIP;
            if(totalAmount >= Grade.GOLD.nextAmount)
                return GOLD.nextLevel;
            if(totalAmount >= Grade.SILVER.nextAmount)
                return SILVER.nextLevel;
            if(totalAmount >= Grade.NORMAL.nextAmount)
                return NORMAL.nextLevel;

            return NORMAL;
        }
    }
}
