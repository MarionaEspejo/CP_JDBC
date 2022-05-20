package org.milaifontanals.jdbc;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.mf.persistence.GestioProjectesException;
import org.mf.persistence.IGestioProjectes;

public class GestioProjectesJDBC implements IGestioProjectes {

    private Connection con;

    /**
     * Constructor que estableix connexió amb el servidor a partir de les dades
     * informades en fitxer de propietats de nom GestioProjectesJDBC.properties.
     *
     * @throws GestioProjectesException si hi ha algun problema en el fitxer de
     * propietats o en establir la connexió
     */
    public GestioProjectesJDBC() throws GestioProjectesException {
        this("GestioProjectesJDBC.properties");
    }

    /**
     * Constructor que estableix connexió amb el servidor a partir de les dades
     * informades en fitxer de propietats, i en cas de ser null cercarà el
     * fitxer de nom GestioProjectesJDBC.properties.
     *
     */
    public GestioProjectesJDBC(String nomFitxerPropietats) throws GestioProjectesException {
        if (nomFitxerPropietats == null) {
            nomFitxerPropietats = "GestioProjectesJDBC.properties";
        }
        Properties p = new Properties();
        try {
            p.load(new FileInputStream(nomFitxerPropietats));
        } catch (IOException ex) {
            throw new GestioProjectesException("Error en llegir de fitxer de propietats", ex);
        }
        String url = p.getProperty("url");
        if (url == null || url.length() == 0) {
            throw new GestioProjectesException("Fitxer de propietats " + nomFitxerPropietats + " no inclou propietat \"url\"");
        }
        String user = p.getProperty("user");
        String password = p.getProperty("password");
        String driver = p.getProperty("driver");    // optatiu
        if (driver != null && driver.length() > 0) {
            try {
                Class.forName(driver).newInstance();
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
                throw new GestioProjectesException("Problemes en carregar el driver ", ex);
            }
        }
        try {
            if (user != null && user.length() > 0) {
                con = DriverManager.getConnection(url, user, password);
            } else {
                con = DriverManager.getConnection(url);
            }
        } catch (SQLException ex) {
            throw new GestioProjectesException("Problemes en establir la connexió ", ex);
        }
        try {
            con.setAutoCommit(false);
        } catch (SQLException ex) {
            throw new GestioProjectesException("Problemes desactivar el auto commit ", ex);
        }
    }

    @Override
    public List<Object> Login(String user, String password) throws GestioProjectesException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

   /* @Override
    public List<Projecte> GetProjectes(String session_id) throws GestioProjectesException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<Tasca> GetTasquesAssignades(String session_id) throws GestioProjectesException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<Tasca> GetDetallTasca(String session_id, int tasca_id) throws GestioProjectesException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<Tasca> GetNotificacionsPendents(String session_id) throws GestioProjectesException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<Usuari> LlistaUsuaris(String session_id) throws GestioProjectesException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int NovaEntrada(String session_id, String novaEntrada, Estat estat, Usuari nouResponsable) throws GestioProjectesException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int NovaEntrada(String session_id, String novaEntrada, Usuari nouResponsable) throws GestioProjectesException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int NovaEntrada(String session_id, String novaEntrada, Estat estat) throws GestioProjectesException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int NovaEntrada(String session_id, String novaEntrada) throws GestioProjectesException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }*/

    @Override
    public void close() throws GestioProjectesException {
        if (con != null) {
            try {
                con.rollback();
                con.close();
            } catch (SQLException ex) {
                throw new GestioProjectesException("Problemes en tancar la connexió ", ex);
            }
            con = null;
        }
    }

    @Override
    public void closeTransaction(char typeClose) throws GestioProjectesException {
        typeClose = Character.toUpperCase(typeClose);
        if (typeClose != 'C' && typeClose != 'R') {
            throw new GestioProjectesException("Paràmetre " + typeClose + " erroni en closeTransaction");
        }
        if (typeClose == 'C') {
            try {
                con.commit();
            } catch (SQLException ex) {
                throw new GestioProjectesException("Error en fer commit", ex);
            }
        } else {
            try {
                con.rollback();
            } catch (SQLException ex) {
                throw new GestioProjectesException("Error en fer rollback", ex);
            }
        }
    }

    @Override
    public void closeCapa() throws GestioProjectesException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Tanca la transacció activa validant els canvis a la BD.
     *
     * @throws GestioProjectesException si hi ha algun problema
     */
    @Override
    public void commit() throws GestioProjectesException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Tanca la transacció activa sense validar els canvis a la BD.
     *
     * @throws GestioProjectesException si hi ha algun problema
     */
    @Override
    public void rollback() throws GestioProjectesException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
