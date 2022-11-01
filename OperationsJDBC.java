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
     * <h2> CREATE </h2>
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

    /**
     * Método para crear la tabla en el caso de que no existe.
     *
     * @param tabla Nombre de la tabla que se quiere crear.
     * @param campos Se manda un string que contemple los campos, los tipos, el número y si se incluyen keys.
     *
     */
    public void createTable(String tabla, String campos) throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS " + tabla + " (" + campos + ")";
        sentencia.execute(sql);

    }

    /**
     *  <h2> INSERT </h2>
     * Método para Insertar simple en el que solo se ejecuta la sentencia SQL.
     *
     * @param sql La sentencia SQL de inserción.
     *
     */
    public void insert(String sql) throws SQLException {
        if (sql.toUpperCase().contains("INSERT")) {
            int filasAfectadas = sentencia.executeUpdate(sql);
            System.out.println("Filas insertadas: " + filasAfectadas);
        } else {
            System.out.println("Operación SQL para introducir registros incorrecta");
        }
    }

    /**
     * Método para Insertar por lotes.
     *
     * @param tabla Nombre de la tabla con la que se quiere trabajar
     * @param campos Array Campos que se quieren insertar datos.
     * @param datosClientes Array bidimensional con los datos de las filas.
     *
     */
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
            selectAllColumns(tabla);
        } else {
            System.out.println("Esta BBDD no soporta insert por lotes");
        }
    }

    /**
     *  <h2> UPDATE </h2>
     * Método para Actualizar simple
     *
     * @param sql La sentencia SQL que actualiza.
     *
     */
    public void update(String sql) throws SQLException {
        if (sql.toUpperCase().contains("UPDATE")) {
            int filasAfectadas = sentencia.executeUpdate(sql);
            System.out.println("Filas actualizadas: " + filasAfectadas);
        } else {
            System.out.println("Operación SQL para introducir registros incorrecta");
        }
    }


    /**
     * Método para Actualizar en el que en la sentencia SQL ya vienen las marcas para el PreparedStatement
     * y se pasa un Array con los datos que corresponden.
     *
     * @param sql La sentencia SQL que actualiza.
     * @param datos Array con los datos que van en orden en los PreparedStatement
     *
     */
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


    /**
     *  <h2> DELETE </h2>
     * Método para Eliminar datos de la tabla con un simple SQL dado, que además informa de las filas afectadas
     *
     * @param sql La sentencia SQL que elimina datos de la tabla
     *
     */
    public void delete(String sql) throws SQLException {
        if (sql.toUpperCase().contains("DELETE")) {
            int filasAfectadas = sentencia.executeUpdate(sql);
            System.out.println("Filas removidas: " + filasAfectadas);
        } else {
            System.out.println("Operación SQL para introducir registros incorrecta");
        }
    }

    /**
     * Método para Eliminar datos de la tabla con una sentencia SQL ya vienen las marcas para el PreparedStatement
     * y se pasa un Array con los datos que corresponden.
     *
     * @param sql La sentencia SQL que elimina datos de la tabla
     * @param datos Array con los datos a eliminar
     *
     */
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


    /**
     * <h2> SELECT </h2>
     * Método para formatear la salida de los metadatos de los campos obtenido s
     *
     * @param campos Todos los campos para mostrarlos en forma de cabecera.
     * @param lineas Se va almacenando cada línea para luego mostrarlas.
     * @param rs ResultSet cargado con el select realizado.
     *
     */
    private String getStringQuery(String[] campos, String lineas, ResultSet rs) throws SQLException {
        while (rs.next()) {
            for (int i = 0; i < campos.length; i++) {
                lineas += rs.getString(campos[i]) + " ";
            }
            lineas += "\n";
        }
        return lineas;
    }

    /**
     * Método para realizar la consulta genérica.
     *
     * @param sql Sentencia SQL para realizar la consulta
     */
    public void select(String sql) throws SQLException {
        if (sql.toUpperCase().contains("SELECT")) {
            String lineas = "", campo = "";
            ResultSet rs = sentencia.executeQuery(sql);
            String[] campos = new String[rs.getMetaData().getColumnCount()];

            for (int i = 0; i < campos.length; i++) {
                campos[i] = rs.getMetaData().getColumnName(i + 1);
                campo += campos[i] + " ";
            }

            lineas = getStringQuery(campos, lineas, rs);
            System.out.println(campos+ "\n" + lineas);
            rs.close();

        } else {
            System.out.println("Operación SQL para consulta incorrecta");
        }
    }

    /**
     * Método para realizar la consulta de todos los campos con solo el nombre de la tabla
     *
     * @param tabla Nombre de la tabla en la que se quiere realizar la consulta
     */
    public void selectAllColumns(String tabla) throws SQLException {
        String sql = "SELECT * FROM " + tabla, campo = "", lineas = "";
        ResultSet rs = sentencia.executeQuery(sql);
        String[] campos = new String[rs.getMetaData().getColumnCount()];

        //Se sacan los campos
        for (int i = 0; i < campos.length; i++) {
            campos[i] = rs.getMetaData().getColumnName(i + 1);
            campo += campos[i] + " ";
        }

        lineas = getStringQuery(campos, lineas, rs);
        System.out.println(campo + "\n" + lineas);
        rs.close();
    }
    /**
     * Método para realizar la consulta de una tabla y unos campos específicos
     *
     * @param tabla Nombre de la tabla en la que se quiere realizar la consulta.
     * @param campos Array con los campos que se quieren consultar.
     */
    public void select(String tabla, String[] campos) throws SQLException {
        //Completar la sentencia SQL con los campos y las interrogaciones correspondientes.
        String campo = "", lineas = "";

        //Se formatea el apartado de los campos para la sentencia SQL
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

        lineas = getStringQuery(campos, lineas, rs);
        System.out.println(campo + "\n" + lineas);
        rs.close();

    }


    /**
     * Método para realizar consultas llamando a funciones PS/SQL
     *
     * @param sqlCall Nombre método PS/SQL
     */
    //TODO genérica
    public void selectFromCallableStatement(String sqlCall) throws SQLException {
        //sqlCall = "{ ? = call libro_ad.apellidos_cliente (?)}";
        CallableStatement cs = miConexion.prepareCall(sqlCall);
        cs.registerOutParameter(1, Types.VARCHAR);
        cs.setString(2, "78901234X");
        cs.execute();
        System.out.println("Apellidos: " + cs.getString(1));
    }


}
