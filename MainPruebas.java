package ClaseJDBCGenérica;

import java.sql.SQLException;

public class MainPruebas {
    public static void main(String[] args) {
        String urlConnection = "jdbc:mysql://127.0.0.1:3306/", bbdd = "bd_prueba", user = "root", passwd = "root";
        OperationsJDBC jdbc = new OperationsJDBC(urlConnection, bbdd, user, passwd);

        String[] campos = {"id", "nombre"};
        String[][] datos = {{"1", "genérico"}, {"2", "elite"}};
        String innerJoin = "SELECT conductores.nombre FROM conductores INNER JOIN camiones ON conductores.tipoCamion = camiones.id where camiones.id=1;";
        try {

       jdbc.createTable("productos","id VARCHAR(30),producto VARCHAR(13),PRIMARY KEY (id)");
//        jdbc.createTable("create table conductores(dni  varchar(13) not null primary key, nombre varchar(30) null,  tipoCamion varchar(30) null,  foreign key fk_conductores_camiones (tipoCamion) references camiones (id));");

           // jdbc.insertBatch("camiones", campos, datos);
           // campos = new String[]{"dni", "nombre", "tipoCamion "};
//            jdbc.insertBatch("conductores", campos, datos);
            jdbc.select(innerJoin);

            //jdbc.select("SELECT ID FROM camiones");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
