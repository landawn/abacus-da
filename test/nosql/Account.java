/*
 * Generated by Abacus.
 */
package nosql;

import java.sql.Timestamp;

import com.landawn.abacus.util.N;

/**
 * Generated by Abacus. DO NOT edit it!
 * @version ${version}
 */
public class Account {
    private String id;
    private String gui;
    private String firstName;
    private String lastName;
    private int status;
    private Timestamp lastUpdateTime;
    private Timestamp createTime;

    public Account() {
    }

    public Account(String id) {
        this();

        setId(id);
    }

    public String getId() {
        return id;
    }

    public Account setId(String id) {
        this.id = id;

        return this;
    }

    public String getGUI() {
        return gui;
    }

    public Account setGUI(String gui) {
        this.gui = gui;

        return this;
    }

    public String getFirstName() {
        return firstName;
    }

    public Account setFirstName(String firstName) {
        this.firstName = firstName;

        return this;
    }

    public String getLastName() {
        return lastName;
    }

    public Account setLastName(String lastName) {
        this.lastName = lastName;

        return this;
    }

    public int getStatus() {
        return status;
    }

    public Account setStatus(int status) {
        this.status = status;

        return this;
    }

    public Timestamp getLastUpdateTime() {
        return lastUpdateTime;
    }

    public Account setLastUpdateTime(Timestamp lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;

        return this;
    }

    public Timestamp getCreateTime() {
        return createTime;
    }

    public Account setCreateTime(Timestamp createTime) {
        this.createTime = createTime;

        return this;
    }

    public Account copy() {
        Account copy = new Account();

        copy.id = this.id;
        copy.gui = this.gui;
        copy.firstName = this.firstName;
        copy.lastName = this.lastName;
        copy.status = this.status;
        copy.lastUpdateTime = this.lastUpdateTime;
        copy.createTime = this.createTime;

        return copy;
    }

    @Override
    public int hashCode() {
        int h = 17;
        h = 31 * h + N.hashCode(id);
        h = 31 * h + N.hashCode(gui);
        h = 31 * h + N.hashCode(firstName);
        h = 31 * h + N.hashCode(lastName);
        h = 31 * h + N.hashCode(status);
        h = 31 * h + N.hashCode(lastUpdateTime);
        h = 31 * h + N.hashCode(createTime);

        return h;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof Account) {
            Account other = (Account) obj;

            if (N.equals(id, other.id) && N.equals(gui, other.gui) && N.equals(firstName, other.firstName) && N.equals(lastName, other.lastName)
                    && N.equals(status, other.status) && N.equals(lastUpdateTime, other.lastUpdateTime) && N.equals(createTime, other.createTime)) {

                return true;
            }
        }

        return false;
    }

    @Override
    public String toString() {
        return "{" + "id=" + N.toString(id) + ", " + "gui=" + N.toString(gui) + ", " + "firstName=" + N.toString(firstName) + ", " + "lastName="
                + N.toString(lastName) + ", " + "status=" + N.toString(status) + ", " + "lastUpdateTime=" + N.toString(lastUpdateTime) + ", " + "createTime="
                + N.toString(createTime) + "}";
    }
}