package com.spring.batch.springbatchexample.part4;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.Collection;

@Repository
public interface UserRepository extends JpaRepository<User, Long> {
    Collection<User> findAllByUpdatedDate(LocalDate date);

    @Query(value = "select min(u.id) from users u")
    long findMinId();

    @Query(value = "select max(u.id) from users u")
    long findMaxId();
}
