package ClaseJDBCGenérica;


import java.sql.*;


/**
 * <h1>Clase para realizar operaciones DDL y DML con JDBC</h1>
 *
 * @author Fernando Granados Juárez
 * @version 1.0
 */
public class OperationsJDBC {
    private Connection miConexion;
    private Statement sentencia = null;
    private PreparedStatement sentenciaPreparada = null;

    /**
     * Constructor para la conexión
     *
     * @param url      Es la unión del conector y la dirección de la máquina.
     * @param bbdd     El nombre de la base de datos
     * @param user     Usuario para la conexión
     * @param password La contraseña correspondiente al usuario.
     */
    public OperationsJDBC(String url, String bbdd, String user, String password) {
        try {
            miConexion = DriverManager.getConnection(url + bbdd, user, password);
            sentencia = this.miConexion.createStatement();

        } catch (SQLException e) {
            System.out.println("No se ha podido realizar la conexión");
        }
    }

    /**
     * Método para crear la base de datos en el caso de que no existe en base al nombre indicado por parámetro.
     *
     * @param databaseName Nombre de la base de datos que se quiere crear.
     */
    public void createDatabase(String databaseName) throws SQLException {
        String sql = "CREATE DATABASE IF NOT EXISTS " + databaseName + ";";
        sentencia.execute(sql);

        sql = "USE " + databaseName;
        sentencia.execute(sql);
    }

    //CREATE TABLE
    public void createTable(String tabla, String campos) throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS " + tabla + " (" + campos + ")";
        sentencia.execute(sql);

    }

    // INSERT
    //INSERT WITH STATEMENT
    public void insert(String sql) throws SQLException {
        if (sql.toUpperCase().contains("INSERT")) {
            int filasAfectadas = sentencia.executeUpdate(sql);
            System.out.println("Filas insertadas: " + filasAfectadas);
        } else {
            System.out.println("Operación SQL para introducir registros incorrecta");
        }
    }

    //INSERT WITH PREPARED STATEMENT WITH BATCH
    public void insertBatch(String tabla, String[] campos, String[][] datosClientes) throws SQLException {
        if (miConexion.getMetaData().supportsBatchUpdates()) {
            //Completar la sentencia SQL con los campos y las interrogaciones correspondientes.
            String campo = "";
            String campoValues = "";
            for (int i = 0; i < campos.length; i++) {
                if (i != campos.length - 1) {
                    campoValues += "?,";
                    campo += campos[i] + ", ";
                } else {
                    campo += campos[i];
                    campoValues += "?";
                }

            }
            String sql = "INSERT INTO " + tabla + "(" + campo + ") VALUES (" + campoValues + ")";
            sentenciaPreparada = miConexion.prepareStatement(sql);

            //Comienzo de las operaciones
            miConexion.setAutoCommit(false);
            for (int i = 0; i < datosClientes.length; i++) {
                for (int j = 0; j < datosClientes[i].length; j++) {
                    sentenciaPreparada.setString(j + 1, datosClientes[i][j]);
                }

                sentenciaPreparada.addBatch();
            }
            sentenciaPreparada.executeBatch();
            miConexion.commit();
            selectAll(tabla);
        } else {
            System.out.println("Esta BBDD no soporta insert por lotes");
        }
    }

    //UPDATE
    //UPDATE WITH STATEMENT
    public void update(String sql) throws SQLException {
        if (sql.toUpperCase().contains("UPDATE")) {
            int filasAfectadas = sentencia.executeUpdate(sql);
            System.out.println("Filas actualizadas: " + filasAfectadas);
        } else {
            System.out.println("Operación SQL para introducir registros incorrecta");
        }
    }

    //UPDATE WITH PREPARED STATEMENT
    public void updatePreparedStatement(String sql, String[] datos) throws SQLException {
        if (sql.toUpperCase().contains("UPDATE")) {
            sentenciaPreparada = miConexion.prepareStatement(sql);
            for (int i = 0; i < datos.length; i++) {
                sentenciaPreparada.setString(i + 1, datos[i]);
            }
            sentenciaPreparada.executeUpdate();
        } else {
            System.out.println("Operación SQL para introducir registros incorrecta");
        }
    }


    // DELETE
    //DELETE WITH STATEMENT
    public void delete(String sql) throws SQLException {
        if (sql.toUpperCase().contains("DELETE")) {
            int filasAfectadas = sentencia.executeUpdate(sql);
            System.out.println("Filas removidas: " + filasAfectadas);
        } else {
            System.out.println("Operación SQL para introducir registros incorrecta");
        }
    }

    //DELETE WITH PREPARED STATEMENT
    public void deletePreparedStatement(String sql, String[] datos) throws SQLException {
        if (sql.toUpperCase().contains("DELETE")) {
            sentenciaPreparada = miConexion.prepareStatement(sql);
            for (int i = 0; i < datos.length; i++) {
                sentenciaPreparada.setString(i + 1, datos[i]);
            }
            sentenciaPreparada.executeUpdate();
        } else {
            System.out.println("Operación SQL para introducir registros incorrecta");
        }
    }

    //SELECT
    //SELECT WITH STATEMENT

    //Método para formatear el resultado de la consulta sin la cabecera
    private String getStringConsulta(String[] campos, String lineas, ResultSet rs) throws SQLException {
        while (rs.next()) {
            for (int i = 0; i < campos.length; i++) {
                lineas += rs.getString(campos[i]) + " ";
            }
            lineas += "\n";
        }
        return lineas;
    }

    /**
     * Método para realizar la consulta de todos los campos
     *
     * @param tabla Nombre de la tabla en la que se quiere realizar la consulta
     */
    public void selectAll(String tabla) throws SQLException {
        String sql = "SELECT * FROM " + tabla, campos = "", lineas = "";
        ResultSet rs = sentencia.executeQuery(sql);
        String[] metaData = new String[rs.getMetaData().getColumnCount()];

        for (int i = 0; i < metaData.length; i++) {
            metaData[i] = rs.getMetaData().getColumnName(i + 1);
            campos += metaData[i] + " ";
        }

        lineas = getStringConsulta(metaData, lineas, rs);
        System.out.println(campos + "\n" + lineas);
        rs.close();
    }

    //Método para realizar la consulta de unos campos concretos
    public void select(String tabla, String[] campos) throws SQLException {
        //Completar la sentencia SQL con los campos y las interrogaciones correspondientes.
        String campo = "", lineas = "";

        for (int i = 0; i < campos.length; i++) {
            if (i != campos.length - 1) {
                campo += campos[i] + ", ";
            } else {
                campo += campos[i];
            }

        }
        String sql = "SELECT" + campo + "FROM" + tabla + ")";
        ResultSet rs = sentencia.executeQuery(sql);
        for (int i = 0; i < campos.length; i++) {
            campo += campos[i] + " ";
        }

        lineas = getStringConsulta(campos, lineas, rs);
        System.out.println(campo + "\n" + lineas);
        rs.close();

    }

    //En el caso de ser una consulta un poco más complicada con cláusula where  o inner join.
    public void select(String sql) throws SQLException {
        if (sql.toUpperCase().contains("SELECT")) {
            String lineas = "", campos = "";
            ResultSet rs = sentencia.executeQuery(sql);
            String[] metaData = new String[rs.getMetaData().getColumnCount()];

            for (int i = 0; i < metaData.length; i++) {
                metaData[i] = rs.getMetaData().getColumnName(i + 1);
                campos += metaData[i] + " ";
            }

            lineas = getStringConsulta(metaData, lineas, rs);
            System.out.println(campos + "\n" + lineas);
            rs.close();

        } else {
            System.out.println("Operación SQL para consulta incorrecta");
        }
    }

    //Select para hacer consultas llamando a funciones PS/SQL
    public void selectFromCallableStatement(String sqlCall) throws SQLException {
        //sqlCall = "{ ? = call libro_ad.apellidos_cliente (?)}";
        CallableStatement cs = miConexion.prepareCall(sqlCall);
        cs.registerOutParameter(1, Types.VARCHAR);
        cs.setString(2, "78901234X");
        cs.execute();
        System.out.println("Apellidos: " + cs.getString(1));
    }


}
