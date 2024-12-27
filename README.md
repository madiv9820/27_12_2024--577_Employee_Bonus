# 577. Employee Bonus

- ### Intuition and Approach
    - #### Problem Description:
    We are given two tables (or DataFrames):
    1. **Employee Data**: Contains information about employees, including their unique `empId` and their `name`.
    2. **Bonus Data**: Contains information about employees' bonuses, identified by `empId`.

    The task is to retrieve the names of employees who:
    - Have a bonus **less than 1000**, or
    - Do not have a bonus (i.e., their bonus value is **NULL** or missing).

    The goal is to return a list of employees with their corresponding bonuses under these conditions.

- ### Approach:
    - #### 1. **Joining the Tables (or DataFrames)**:
        - **Join Operation**: The first step is to combine the two data sources (`Employee` and `Bonus`) into one unified dataset. This is typically done using a **join** operation on the common column `empId`, which identifies employees across both datasets.
            - A **LEFT JOIN** is used to ensure that all employees are included, even if they do not have a corresponding bonus entry.
            - The result of the join will contain employee names along with their bonus values. If an employee does not have a bonus, the bonus column will contain a **NULL** value.

    - #### 2. **Filtering the Data**:
        - After the tables are joined, the next step is to filter the data based on the following criteria:
            - **Employees with bonuses less than 1000**: We want to include employees who have a bonus value below 1000.
            - **Employees with no bonus (NULL values)**: If the `bonus` column is `NULL` (indicating the employee does not have a bonus), those employees should also be included.
        - The filter should select rows where either of these conditions hold true.

    - #### 3. **Selecting Relevant Columns**:
        - Once the filtering is done, we only need to return the employee's **name** and their **bonus**. 
        - Thus, we select the `name` and `bonus` columns from the filtered dataset for the final result.

    - #### 4. **Returning the Result**:
        - The final step is to return the resulting dataset, which contains the list of employee names and their corresponding bonus values (if they meet the filter criteria).

    #### Why this approach works:
    - **LEFT JOIN**: Ensures that we include all employees, even if they don't have a bonus. If an employee doesn't have a bonus, the `bonus` column will be `NULL`, and the filtering step will handle this case.
    - **Filtering** ensures that we only select employees with a bonus below a threshold (1000) or those without any bonus at all.
    - **Selecting columns** ensures that the final output only contains relevant information: employee names and their bonuses.

    This approach is flexible and works across various tools (SQL, PySpark, Pandas) as the core operations (join, filter, select) are common to all of them.

- ### Code Implementation
    - **SQL:**
        ```sql []
        SELECT 
            e.name, b.bonus  -- Select employee's name and the corresponding bonus
        FROM Employee e  -- From the 'Employee' table (aliased as e)
        LEFT JOIN        -- Perform a LEFT JOIN to include all employees, even those without a bonus
        Bonus b          -- Join with the 'Bonus' table (aliased as b)
        ON e.empId = b.empId  -- The join condition: match employees with their bonus using empId
        WHERE b.bonus < 1000 or b.bonus IS NULL  -- Only select employees with bonus less than 1000 or no bonus
        ```
    - **PySpark:**
        ```python3 []
        def employee_bonus(employee: pyspark.sql.dataframe.DataFrame,
                        bonus: pyspark.sql.dataframe.DataFrame ) -> pyspark.sql.dataframe.DataFrame:
            # Perform a LEFT JOIN between the 'employee' DataFrame and 'bonus' DataFrame based on 'empId'.
            # A LEFT JOIN ensures all employees will be included, even those without a bonus.
            names_With_Less_Or_No_Bonus = employee.join(
                bonus,                    # Joining the 'bonus' DataFrame
                on = 'empId',             # The condition for the join: matching 'empId' from both DataFrames
                how = 'left'              # Perform a LEFT JOIN to include employees even if they don't have a bonus
            )\
            .filter(
                # Filter employees to select only those with a 
                # bonus less than 1000 or no bonus at all (NULL).
                (bonus.bonus < 1000) | (bonus.bonus.isNull())
            )\
            .select(
                # Select only the 'name' and 'bonus' 
                # columns from the resulting DataFrame.
                ['name', 'bonus'] 
            )    
            
            # Return the resulting DataFrame with employees' names and their bonus (if they qualify).
            return names_With_Less_Or_No_Bonus
        ```
    - **Pandas:**
        ```python3 []
        def employee_bonus(employee: pd.DataFrame, bonus: pd.DataFrame) -> pd.DataFrame:
            # Perform a LEFT JOIN between the 'employee' DataFrame and the 'bonus' DataFrame based on 'empId'.
            # This ensures that all employees are included, even if they don't have a corresponding bonus.
            tables_Join = employee.merge(bonus, on = ['empId'], how = 'left')
            
            # Filter the rows where the bonus is either:
            # 1. Less than 1000 (bonus < 1000), or
            # 2. NULL (i.e., no bonus) using `isna()` method.
            # This will create a DataFrame of employees with low or no bonuses.
            data_With_Less_Or_No_Bonus = tables_Join[(tables_Join['bonus'] < 1000) | (tables_Join['bonus'].isna())]
            
            # Select only the 'name' and 'bonus' columns from the filtered DataFrame.
            # This will create a final DataFrame with employee names and their corresponding bonus (if available).
            names_With_Less_Or_No_Bonus = data_With_Less_Or_No_Bonus[['name', 'bonus']]
            
            # Return the DataFrame containing employees with less or no bonus.
            return names_With_Less_Or_No_Bonus
        ```