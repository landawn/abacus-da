package com.landawn.abacus.da.entity;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.landawn.abacus.annotation.Column;
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.annotation.Table;

import lombok.Data;

@Data
@Table("simplex.users")
public class Users {
    @Id
    private UUID id;
    @Column
    private Name name;
    private Map<String, Address> addresses;
    @Column("someDate")
    private LocalDate someDate;
    private LocalTime lastUpdateTime;
    private Timestamp createdTime;
    private List<Byte> bytes;
    private Set<Short> shorts;

    @Data
    public static class Name {
        private String firstName;
        private String lastName;
    }

    @Data
    public static class Address {
        private String street;
        private String city;
        private int zipCode;
    }
}