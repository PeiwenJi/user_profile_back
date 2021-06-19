package bigdata.com.bean;

import java.util.ArrayList;

public class UserPermission {
    private String identity;
    private String permission;
    private String data_1;
    private String data_2;
    private String userSearch;
    private String userManagement_1;
    private String userManagement_2;
    private String userManagement_3;
    private String tagManagement_1;
    private String tagManagement_2;
    private String tagManagement_3;
    private String rolePermission;

    public UserPermission(String identity, String permission, String data_1, String data_2, String userSearch, String userManagement_1, String userManagement_2, String userManagement_3, String tagManagement_1, String tagManagement_2, String tagManagement_3, String rolePermission) {
        this.identity = identity;
        this.permission = permission;
        this.data_1 = data_1;
        this.data_2 = data_2;
        this.userSearch = userSearch;
        this.userManagement_1 = userManagement_1;
        this.userManagement_2 = userManagement_2;
        this.userManagement_3 = userManagement_3;
        this.tagManagement_1 = tagManagement_1;
        this.tagManagement_2 = tagManagement_2;
        this.tagManagement_3 = tagManagement_3;
        this.rolePermission = rolePermission;
    }

    public String getIdentity() {
        return identity;
    }

    public void setIdentity(String identity) {
        this.identity = identity;
    }

    public String getPermission() {
        return permission;
    }

    public void setPermission(String permission) {
        this.permission = permission;
    }

    public String getData_1() {
        return data_1;
    }

    public void setData_1(String data_1) {
        this.data_1 = data_1;
    }

    public String getData_2() {
        return data_2;
    }

    public void setData_2(String data_2) {
        this.data_2 = data_2;
    }

    public String getUserSearch() {
        return userSearch;
    }

    public void setUserSearch(String userSearch) {
        this.userSearch = userSearch;
    }

    public String getUserManagement_1() {
        return userManagement_1;
    }

    public void setUserManagement_1(String userManagement_1) {
        this.userManagement_1 = userManagement_1;
    }

    public String getUserManagement_2() {
        return userManagement_2;
    }

    public void setUserManagement_2(String userManagement_2) {
        this.userManagement_2 = userManagement_2;
    }

    public String getUserManagement_3() {
        return userManagement_3;
    }

    public void setUserManagement_3(String userManagement_3) {
        this.userManagement_3 = userManagement_3;
    }

    public String getTagManagement_1() {
        return tagManagement_1;
    }

    public void setTagManagement_1(String tagManagement_1) {
        this.tagManagement_1 = tagManagement_1;
    }

    public String getTagManagement_2() {
        return tagManagement_2;
    }

    public void setTagManagement_2(String tagManagement_2) {
        this.tagManagement_2 = tagManagement_2;
    }

    public String getTagManagement_3() {
        return tagManagement_3;
    }

    public void setTagManagement_3(String tagManagement_3) {
        this.tagManagement_3 = tagManagement_3;
    }

    public String getRolePermission() {
        return rolePermission;
    }

    public void setRolePermission(String rolePermission) {
        this.rolePermission = rolePermission;
    }
}
