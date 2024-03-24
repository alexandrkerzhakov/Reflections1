package ru.gb;

import ru.gb.Annotation.Column;
import ru.gb.Annotation.Id;
import ru.gb.Annotation.Table;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;

/**
 * 0. Разобрать код с семниара
 * 1. Повторить код с семниара без подглядываний на таблице Student с полями:
 * 1.1 id - int
 * 1.2 firstName - string
 * 1.3 secondName - string
 * 1.4 age - int
 * 2.* Попробовать подключиться к другой БД
 * 3.** Придумать, как подружить запросы и reflection:
 * 3.1 Создать аннотации Table, Id, Column
 * 3.2 Создать класс, у которого есть методы:
 * 3.2.1 save(Object obj) сохраняет объект в БД
 * 3.2.2 update(Object obj) обновляет объект в БД
 * 3.2.3 Попробовать объединить save и update (сначала select, потом update или insert)
 */


public class Main {

    public static void main(String[] args) throws SQLException {
        workWithTableStudent();
        getTablePerson();
    }

    public static void getTablePerson() {
        boolean isPrimaryKey = false;
        Map<String, String> tableMap = new HashMap<>();
        Class<Person> personClass = Person.class;

        if (personClass.getAnnotation(Table.class) != null) {
            tableMap.put("Table", personClass.getSimpleName());
        }

        Field[] fields = personClass.getDeclaredFields();
        int count_column = 1;
        for (Field field : fields) {
            if (field.getAnnotation(Id.class) != null) {
                tableMap.put("Id", field.getName());
            }
            if (field.getAnnotation(Column.class) != null) {
                tableMap.put("Column" + count_column, field.getName());
                count_column++;
            }
        }

        Optional<String> any = tableMap.keySet().stream().filter(key -> key.contains("Column") && tableMap.get(key).equals(tableMap.get("Id"))).findAny();
        if (any.isPresent()) {
            isPrimaryKey = true;
            tableMap.remove(any.get());
        }

        String tableName = tableMap.get("Table").toLowerCase(Locale.ROOT); // person
        String id = tableMap.get("Id"); // id

        String idType = Arrays.stream(fields)
                .filter(field -> field.getName().equals(id))
                .map(field -> field.getType().getSimpleName())
                .findAny().get(); // int

        String column = tableMap
                .keySet()
                .stream()
                .filter(key-> key.contains("Column"))
                .findAny()
                .get(); // name

        String columnType = Arrays.stream(fields)
                .filter(field -> field.getName().equals(column))
                .map(field -> field.getType().getSimpleName())
                .findAny().get(); // String

        if (columnType.equals("String")) {
            columnType = "varchar(256)";
        }

        StringBuilder sql = new StringBuilder("create table ");
        sql.append(tableName).append(" (");
        sql.append(id).append(" ").append(idType).append(", ");
        sql.append(column).append(" ").append(columnType).append(", ");
        if (isPrimaryKey) sql.append("primary key").append(" (").append(id).append(")");
        sql.append(")");

        System.out.println(sql); // create table person (id int, name varchar(256), primary key (id))
    }

    @Table(name = "person")
    static class Person {
        @Id
        @Column(name = "id")
        private int id;
        @Column(name = "name")
        private String name;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
        }

    public static void workWithTableStudent() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:h2:mem:student");
        createTable(connection);
        insertToTable(connection);
        printResultSet(connection);
        updateRowInTable(connection, 1);
        deleteRowInTable(connection, 2);
        printResultSet(connection);
        //        Insert rows: 5
        //        Student with id=1: Tom Ivanov 15
        //        Student with id=2: John Petrov 25
        //        Student with id=3: Victor Diveeev 35
        //        Student with id=4: Mike Jonson 30
        //        Student with id=5: David Bloom 20
        //        Update row with id = 1
        //        Delete row with id = 2
        //        Student with id=1: Sam Wick 50
        //        Student with id=3: Victor Diveeev 35
        //        Student with id=4: Mike Jonson 30
        //        Student with id=5: David Bloom 20
    }

    public static void createTable(Connection connection) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(
                    """
                            create table student(
                            id int,
                            firstName varchar(256),
                            secondName varchar(256),
                            age int)            
                            """
            );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void insertToTable(Connection connection) {
        try (Statement statement = connection.createStatement()) {
            int countRows = statement.executeUpdate("""
                            insert into student (id, firstName, secondName, age) values (1, 'Tom', 'Ivanov', 15),
                                                                                        (2, 'John', 'Petrov', 25), 
                                                                                        (3, 'Victor', 'Diveeev', 35),
                                                                                        (4, 'Mike', 'Jonson', 30),
                                                                                        (5, 'David', 'Bloom', 20)
                    """);
            System.out.println("Insert rows: " + countRows);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void printResultSet(Connection connection) {
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("""
                            select * from student
                    """);

            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String firstName = resultSet.getString("firstName");
                String secondName = resultSet.getString("secondName");
                int age = resultSet.getInt("age");
                System.out.println("Student with id=" + id + ": " + firstName + " " + secondName + " " + age);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void updateRowInTable(Connection connection, int id) {
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                            update student set firstName = $1, secondName = $2, age = $3 where id = $4
                """)) {
            preparedStatement.setString(1, "Sam");
            preparedStatement.setString(2, "Wick");
            preparedStatement.setInt(3, 50);
            preparedStatement.setInt(4, id);
            preparedStatement.execute();
            System.out.println("Update row with id = " + id);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void deleteRowInTable(Connection connection, int id) {
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                            delete from student where id = $1
                """)) {
            preparedStatement.setInt(1, id);
            preparedStatement.execute();
            System.out.println("Delete row with id = " + id);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}



