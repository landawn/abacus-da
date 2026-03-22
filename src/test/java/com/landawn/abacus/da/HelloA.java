package com.landawn.abacus.da;

import com.landawn.abacus.da.cassandra.CqlBuilder.NSC;
import com.landawn.abacus.da.entity.Users;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.In;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.NamingPolicy;

import lombok.Builder;
import lombok.Data;

public class HelloA {

    private NamingPolicy namingPolicy = NamingPolicy.CAMEL_CASE;

    /**
     * 
     *
     * @param args 
     */
    public static void main(String[] args) {
        In cond = Filters.in("id", N.asList("a", "b"));
        N.println(NSC.deleteFrom(Users.class).append(cond).build());

        N.println(NSC.delete("name").from(Users.class).append(cond).build().query());
        N.println(NSC.delete(Users.class).from(Users.class).append(cond).build().query());
        N.println(NSC.delete(Users.class, N.asSet("name")).from(Users.class).append(cond).build().query());
    }

    @Builder
    @Data
    public static class User {

        private int id;
        private String name;

    }

}
