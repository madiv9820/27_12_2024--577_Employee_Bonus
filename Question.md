# [577. Employee Bonus](https://leetcode.com/problems/employee-bonus?envType=study-plan-v2&envId=top-sql-50)

**Type:** Easy <br>
**Topics:** Databases <br>
**Companies:** Google, Meta, Amazon, Microsoft, Netsuite, Uber, Bloomberg, Yahoo, Infosys, tcs
<hr>

Table: `Employee`
```
+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| empId       | int     |
| name        | varchar |
| supervisor  | int     |
| salary      | int     |
+-------------+---------+
empId is the column with unique values for this table.
Each row of this table indicates the name and the ID of an employee in addition to their salary and the id of their manager.
``` 

Table: `Bonus`
```
+-------------+------+
| Column Name | Type |
+-------------+------+
| empId       | int  |
| bonus       | int  |
+-------------+------+
empId is the column of unique values for this table.
empId is a foreign key (reference column) to empId from the Employee table.
Each row of this table contains the id of an employee and their respective bonus.
```

Write a solution to report the name and bonus amount of each employee with a bonus **less than** `1000`.

Return the result table in **any order**.

The result format is in the following example.
<hr>

### Examples
- **Example 1:**
    - **Input:** 
        ```
        Employee table:
        +-------+--------+------------+--------+
        | empId | name   | supervisor | salary |
        +-------+--------+------------+--------+
        | 3     | Brad   | null       | 4000   |
        | 1     | John   | 3          | 1000   |
        | 2     | Dan    | 3          | 2000   |
        | 4     | Thomas | 3          | 4000   |
        +-------+--------+------------+--------+
        Bonus table:
        +-------+-------+
        | empId | bonus |
        +-------+-------+
        | 2     | 500   |
        | 4     | 2000  |
        +-------+-------+
        ```
    - **Output:** 
        ```
        +------+-------+
        | name | bonus |
        +------+-------+
        | Brad | null  |
        | John | null  |
        | Dan  | 500   |
        +------+-------+
        ```
<hr>

### Hints
- If the EmpId in table Employee has no match in table Bonus, we consider that the corresponding bonus is null and null is smaller than 1000.
- Inner join is the default join, we can solve the mismatching problem by using outer join.