package bigdata.com.bean;

import java.util.ArrayList;

public class UserPermission {
    private int id;
    private String identity;
    private String description;
    private String permission;

    public UserPermission(String identity, String description, String permission) {
        this.identity = identity;
        this.description = description;
        this.permission = permission;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getIdentity() {
        return identity;
    }

    public void setIdentity(String identity) {
        this.identity = identity;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getPermission() {
        return permission;
    }

    public void setPermission(String permission) {
        this.permission = permission;
    }
}
