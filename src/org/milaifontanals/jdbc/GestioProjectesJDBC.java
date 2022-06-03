package org.milaifontanals.jdbc;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.mf.persistence.GestioProjectesException;
import org.mf.persistence.IGestioProjectes;
import org.milaifontanals.model.Entrada;
import org.milaifontanals.model.Estat;
import org.milaifontanals.model.Projecte;
import org.milaifontanals.model.Rol;
import org.milaifontanals.model.Tasca;
import org.milaifontanals.model.Usuari;
import org.milaifontanals.model.UsuariToken;

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
    public UsuariToken Login(String user, String password) throws GestioProjectesException {
        try {
            ResultSet set;
            String consulta = "select * from usuari where usr_login=? and usr_psswd_hash=?";
            PreparedStatement psLin = con.prepareStatement(consulta);
            psLin.setString(1, user);
            psLin.setString(2, password);
            set = psLin.executeQuery();
            if (set.next() == false) {
                return null;
            } else {
                int usr_id = set.getInt("usr_id");
                String usr_nom = set.getString("usr_nom");
                String usr_cognom1 = set.getString("usr_cognom1");
                Date usr_data_naix = set.getDate("usr_data_naixament");
                UsuariToken usuTok = new UsuariToken(usr_id, user, password);

                return usuTok;
            }
        } catch (SQLException ex) {
            Logger.getLogger(GestioProjectesJDBC.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    @Override
    public List<Projecte> getLlistaProjectes(Usuari usu) throws GestioProjectesException {
        List<Projecte> projectes = new ArrayList<>();
        try {
            ResultSet set;
            String consulta = "select * from projecte join projecte_usuari on proj_id = proj_usu_id_projecte where proj_usu_id_usuari = ?";
            PreparedStatement psLin = con.prepareStatement(consulta);
            psLin.setInt(1, usu.getID());
            set = psLin.executeQuery();
            while (set.next()) {
                int proj_id = set.getInt("proj_id");
                String proj_nom = set.getString("proj_nom");
                String proj_desc = set.getString("proj_descripcio");
                int id_usu_cap = set.getInt("proj_cap_projecte");

                Usuari cap = this.getUsuari(id_usu_cap);
                Projecte proj = new Projecte(proj_id, proj_nom, proj_desc, cap);
                projectes.add(proj);
            }
            return projectes;
        } catch (SQLException ex) {
            Logger.getLogger(GestioProjectesJDBC.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    
    @Override
    public List<Projecte> getLlistaProjectesNoAssignats(Usuari usuari) throws GestioProjectesException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Rol getRolUsu(Projecte idProj, Usuari idUsu) throws GestioProjectesException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Rol getRol(int id) throws GestioProjectesException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int deleteUsuari(int id) throws GestioProjectesException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int insertUsuari(Usuari usu) throws GestioProjectesException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int updateUsuari(Usuari usu) throws GestioProjectesException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void desassignarProjecte(Usuari usu, Projecte proj, Rol rol) throws GestioProjectesException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void assignarProjecte(Usuari usu, Projecte proj, Rol rol) throws GestioProjectesException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Projecte getProjecte(int id) throws GestioProjectesException {
        try {
            ResultSet set;
            String consulta = "select * from projecte where proj_id=?";
            PreparedStatement psLin = con.prepareStatement(consulta);
            psLin.setInt(1, id);
            set = psLin.executeQuery();
            if (set.next() == false) {
                return null;
            } else {
                String proj_nom = set.getString("proj_nom");
                String proj_desc = set.getString("proj_descripcio");
                int usu = set.getInt("proj_cap_projecte");

                Usuari cap = this.getUsuari(usu);

                Projecte proj = new Projecte(id, proj_nom, proj_desc, cap);

                return proj;
            }
        } catch (SQLException ex) {
            Logger.getLogger(GestioProjectesJDBC.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    @Override
    public int getProjIDTasca(int id) throws GestioProjectesException {
        try {
            ResultSet set;
            String consulta = "select tasc_projecte from tasca where tasc_id=?";
            PreparedStatement psLin = con.prepareStatement(consulta);
            psLin.setInt(1, id);
            set = psLin.executeQuery();
            if (set.next() == false) {
                return 0;
            } else {
                int proj_id = set.getInt("tasc_projecte");
                return proj_id;
            }
        } catch (SQLException ex) {
            Logger.getLogger(GestioProjectesJDBC.class.getName()).log(Level.SEVERE, null, ex);
        }
        return 0;
    }

    @Override
    public List<Projecte> getProjectes() throws GestioProjectesException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<Tasca> GetTasquesAssignades(Usuari usu) throws GestioProjectesException {
        PreparedStatement psLin = null;
        List<Tasca> tasques = new ArrayList();
        try {
            ResultSet set;
            String consulta = "select * from tasca where tasc_responsable = ?";
            psLin = con.prepareStatement(consulta);
            psLin.setInt(1, usu.getID());
            set = psLin.executeQuery();
            while (set.next()) {
                int tasc_id = set.getInt("tasc_id");
                String tasc_nom = set.getString("tasc_nom");
                String tasc_desc = set.getString("tasc_descripcio");
                int id_responsable = set.getInt("tasc_propietari");
                Date tasc_data_creacio = set.getDate("tasc_data_creacio");

                Tasca task = new Tasca(tasc_id, tasc_data_creacio, tasc_nom, tasc_desc, usu);
                tasques.add(task);
            }
        } catch (SQLException ex) {
            Logger.getLogger(GestioProjectesJDBC.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                psLin.close();
            } catch (SQLException ex) {
                Logger.getLogger(GestioProjectesJDBC.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return tasques;
    }

    @Override
    public List<Tasca> getTasquesIDProj(int idProj) throws GestioProjectesException {
        List<Tasca> tasques = new ArrayList<>();
        try {
            ResultSet set;
            String consulta = "select * from tasca where tasc_projecte=? and tasc_estat = 5";
            PreparedStatement psLin = con.prepareStatement(consulta);
            psLin.setInt(1, idProj);
            set = psLin.executeQuery();
            while (set.next()) {
                int tasc_id = set.getInt("tasc_id");
                String tasc_nom = set.getString("tasc_nom");
                String tasc_desc = set.getString("tasc_descripcio");
                int id_responsable = set.getInt("tasc_propietari");
                Date tasc_data_creacio = set.getDate("tasc_data_creacio");

                Usuari usu = this.getUsuari(id_responsable);

                Tasca task = new Tasca(tasc_id, tasc_data_creacio, tasc_nom, tasc_desc, usu);
                tasques.add(task);
            }
        } catch (SQLException ex) {
            Logger.getLogger(GestioProjectesJDBC.class.getName()).log(Level.SEVERE, null, ex);
        }
        return tasques;
    }

     @Override
    public List<Tasca> getTasquesIDProjTots(int idProj) throws GestioProjectesException {
        List<Tasca> tasques = new ArrayList<>();
        try {
            ResultSet set;
            String consulta = "select * from tasca where tasc_projecte=?";
            PreparedStatement psLin = con.prepareStatement(consulta);
            psLin.setInt(1, idProj);
            set = psLin.executeQuery();
            while (set.next()) {
                int tasc_id = set.getInt("tasc_id");
                String tasc_nom = set.getString("tasc_nom");
                String tasc_desc = set.getString("tasc_descripcio");
                int id_responsable = set.getInt("tasc_propietari");
                Date tasc_data_creacio = set.getDate("tasc_data_creacio");

                Usuari usu = this.getUsuari(id_responsable);

                Tasca task = new Tasca(tasc_id, tasc_data_creacio, tasc_nom, tasc_desc, usu);
                tasques.add(task);
            }
        } catch (SQLException ex) {
            Logger.getLogger(GestioProjectesJDBC.class.getName()).log(Level.SEVERE, null, ex);
        }
        return tasques;
    }

    
    @Override
    public List<Entrada> getEntradaIDTasca(int idTasca) throws GestioProjectesException {
        List<Entrada> entrades = new ArrayList<>();
        try {
            ResultSet set;
            String consulta = "select * from entrada where entry_id_tasca=?";
            PreparedStatement psLin = con.prepareStatement(consulta);
            psLin.setInt(1, idTasca);
            set = psLin.executeQuery();
            while (set.next()) {
                int entry_numero = set.getInt("entry_numero");
                String entrada = set.getString("entrada");
                int id_entry_escriptor = set.getInt("entry_escriptor");
                Date entry_date = set.getDate("entry_data");

                Usuari escriptor = this.getUsuari(id_entry_escriptor);

                Entrada entry = new Entrada(entry_numero, entry_date, entrada, escriptor);
                entrades.add(entry);
            }
        } catch (SQLException ex) {
            Logger.getLogger(GestioProjectesJDBC.class.getName()).log(Level.SEVERE, null, ex);
        }
        return entrades;
    }

    @Override
    public List<Projecte> getProjecteFiltreNom(String nom) throws GestioProjectesException {
        List<Projecte> projectes = new ArrayList<>();
        try {
            ResultSet set;
            String consulta = "select * from projecte where proj_nom like ?";
            PreparedStatement psLin = con.prepareStatement(consulta);
            psLin.setString(1, nom);
            set = psLin.executeQuery();
            while (set.next()) {
                int proj_id = set.getInt("proj_id");
                String proj_nom = set.getString("proj_nom");
                String proj_desc = set.getString("proj_descripcio");
                int id_usu_cap = set.getInt("proj_cap_projecte");

                Usuari cap = this.getUsuari(id_usu_cap);
                Projecte proj = new Projecte(proj_id, proj_nom, proj_desc, cap);
                projectes.add(proj);
            }
        } catch (SQLException ex) {
            Logger.getLogger(GestioProjectesJDBC.class.getName()).log(Level.SEVERE, null, ex);
        }
        return projectes;
    }

    @Override
    public List<Estat> getEstats() throws GestioProjectesException {
        List<Estat> estats = new ArrayList<>();
        try {
            ResultSet set;
            String consulta = "select * from estat";
            PreparedStatement psLin = con.prepareStatement(consulta);
            set = psLin.executeQuery();
            while (set.next()) {
                int estat_id = set.getInt("estat_id");
                String estat_nom = set.getString("estat_nom");

                Estat es = new Estat(estat_id, estat_nom);
                estats.add(es);
            }
        } catch (SQLException ex) {
            Logger.getLogger(GestioProjectesJDBC.class.getName()).log(Level.SEVERE, null, ex);
        }
        return estats;
    }

    @Override
    public List<Projecte> getLlistaProjectesTascaEstat(String nomEstat) throws GestioProjectesException {
        List<Projecte> projectes = new ArrayList<>();
        try {
            ResultSet set;
            String consulta = "select proj_id, proj_nom, proj_descripcio, proj_cap_projecte "
                    + "from projecte join tasca on tasc_projecte = proj_id "
                    + "join estat on estat_id = tasc_estat "
                    + "where estat_nom like ?";
            PreparedStatement psLin = con.prepareStatement(consulta);
            psLin.setString(1, nomEstat);
            set = psLin.executeQuery();
            while (set.next()) {
                int proj_id = set.getInt("proj_id");
                String proj_nom = set.getString("proj_nom");
                String proj_desc = set.getString("proj_descripcio");
                int id_usu_cap = set.getInt("proj_cap_projecte");

                Usuari cap = this.getUsuari(id_usu_cap);
                Projecte proj = new Projecte(proj_id, proj_nom, proj_desc, cap);
                projectes.add(proj);
            }
        } catch (SQLException ex) {
            Logger.getLogger(GestioProjectesJDBC.class.getName()).log(Level.SEVERE, null, ex);
        }
        return projectes;
    }

    @Override
    public int getID(String login, String pwd) throws GestioProjectesException {
        try {
            ResultSet set;
            String consulta = "select usr_id from usuari where usr_login=? and usr_psswd_hash=?";
            PreparedStatement psLin = con.prepareStatement(consulta);
            psLin.setString(1, login);
            psLin.setString(2, pwd);
            set = psLin.executeQuery();
            if (set.next() == false) {
                return 0;
            } else {
                int usr_id = set.getInt("usr_id");
                return usr_id;
            }
        } catch (SQLException ex) {
            Logger.getLogger(GestioProjectesJDBC.class.getName()).log(Level.SEVERE, null, ex);
        }
        return 0;
    }

    @Override
    public int NovaEntrada(Entrada newEntrada, int idTask) throws GestioProjectesException {
        try {
            Date sqlDate = new java.sql.Date(Calendar.getInstance().getTimeInMillis());
            ResultSet set;
            String consulta = "insert into entrada (entry_id_tasca, entry_numero, entry_data, entrada, entry_escriptor, entry_nova_assignacio, entry_estat_id)"
                    + " values(?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement psLin = con.prepareStatement(consulta);
            psLin.setInt(1, idTask);
            psLin.setInt(2, newEntrada.getNumero());
            psLin.setDate(3, sqlDate);
            psLin.setString(4, newEntrada.getEntrada());
            psLin.setInt(5, newEntrada.getEscriptor().getID());
            if (newEntrada.getNovaAssignacio() == null) {
                psLin.setObject(6, null);
            } else {
                psLin.setInt(6, newEntrada.getNovaAssignacio().getID());
            }
            if (newEntrada.getNouEstat() == null) {
                psLin.setObject(7, null);
            } else {
                psLin.setInt(7, newEntrada.getNouEstat().getId());
            }

            int res = psLin.executeUpdate();
            if (res != 0) {
                return 0;
            } else {
                return 1;
            }
        } catch (SQLException ex) {
            Logger.getLogger(GestioProjectesJDBC.class.getName()).log(Level.SEVERE, null, ex);
        }
        return 0;
    }

    @Override
    public int getNumeroEntrada(int idTaca) throws GestioProjectesException {
        try {
            ResultSet set;
            String consulta = "select max(entry_numero) as entry_numero from entrada where entry_id_tasca = ?";
            PreparedStatement psLin = con.prepareStatement(consulta);
            psLin.setInt(1, idTaca);
            set = psLin.executeQuery();
            if (set.next() == false) {
                return 0;
            } else {
                int entry_numero = set.getInt("entry_numero");
                return entry_numero;
            }
        } catch (SQLException ex) {
            Logger.getLogger(GestioProjectesJDBC.class.getName()).log(Level.SEVERE, null, ex);
        }
        return 0;
    }

    @Override
    public Estat getEstat(String nomEstat) throws GestioProjectesException {
        try {
            ResultSet set;
            String consulta = "select estat_id, estat_nom from estat where estat_nom like ?";
            PreparedStatement psLin = con.prepareStatement(consulta);
            psLin.setString(1, nomEstat);
            set = psLin.executeQuery();
            if (set.next() == false) {
                return null;
            } else {
                int estat_id = set.getInt("estat_id");
                Estat estat = new Estat(estat_id, nomEstat);
                return estat;
            }
        } catch (SQLException ex) {
            Logger.getLogger(GestioProjectesJDBC.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

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

    @Override
    public void commit() throws GestioProjectesException {
        try {
            con.commit();
        } catch (SQLException ex) {
            throw new GestioProjectesException("Error en fer commit", ex);
        }
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

    @Override
    public List<Usuari> getLlistaUsuaris() throws GestioProjectesException {
        PreparedStatement psLin = null;
        List<Usuari> usu = new ArrayList<>();
        try {
            ResultSet set;
            String consulta = "select * from usuari";
            psLin = con.prepareStatement(consulta);
            set = psLin.executeQuery();
            while (set.next()) {
                int usr_id = set.getInt("usr_id");
                String usr_nom = set.getString("usr_nom");
                String usr_cognom = set.getString("usr_cognom1");
                Date usr_data_naixament = set.getDate("usr_data_naixament");
                String usr_login = set.getString("usr_login");
                String usr_psswd_hash = set.getString("usr_psswd_hash");

                Usuari u = new Usuari(usr_id, usr_nom, usr_cognom, "", usr_data_naixament, usr_login, usr_psswd_hash);
                usu.add(u);
            }
        } catch (SQLException ex) {
            Logger.getLogger(GestioProjectesJDBC.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                psLin.close();
            } catch (SQLException ex) {
                Logger.getLogger(GestioProjectesJDBC.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return usu;
    }

    @Override
    public Usuari getUsuari(int id) throws GestioProjectesException {
        PreparedStatement psLin = null;
        Usuari usu = null;
        try {
            ResultSet set;
            String consulta = "select * from usuari where usr_id = ?";
            psLin = con.prepareStatement(consulta);
            psLin.setInt(1, id);
            set = psLin.executeQuery();
            if (set.next() == false) {
                return null;
            } else {
                int usr_id = set.getInt("usr_id");
                String usr_nom = set.getString("usr_nom");
                String usr_cognom = set.getString("usr_cognom1");
                Date usr_data_naixament = set.getDate("usr_data_naixament");
                String usr_login = set.getString("usr_login");
                String usr_psswd_hash = set.getString("usr_psswd_hash");

                usu = new Usuari(usr_id, usr_nom, usr_cognom, "", usr_data_naixament, usr_login, usr_psswd_hash);
            }
        } catch (SQLException ex) {
            Logger.getLogger(GestioProjectesJDBC.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                psLin.close();
            } catch (SQLException ex) {
                Logger.getLogger(GestioProjectesJDBC.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return usu;
    }

    @Override
    public int ultimID() throws GestioProjectesException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<Projecte> getProjecteFiltreTextTasca(String testTask) throws GestioProjectesException {
        List<Projecte> projectes = new ArrayList<>();
        try {
            ResultSet set;
            String consulta = "select * "
                    + "from projecte join tasca on proj_id = tasc_projecte "
                    + "where tasc_descripcio like '%"+testTask+"%' or tasc_nom like '%"+testTask+"%'";
            PreparedStatement psLin = con.prepareStatement(consulta);
            set = psLin.executeQuery();
            while (set.next()) {
                int proj_id = set.getInt("proj_id");
                String proj_nom = set.getString("proj_nom");
                String proj_desc = set.getString("proj_descripcio");
                int id_usu_cap = set.getInt("proj_cap_projecte");

                Usuari cap = this.getUsuari(id_usu_cap);
                Projecte proj = new Projecte(proj_id, proj_nom, proj_desc, cap);
                projectes.add(proj);
            }
        } catch (SQLException ex) {
            Logger.getLogger(GestioProjectesJDBC.class.getName()).log(Level.SEVERE, null, ex);
        }
        return projectes;
    }
    
    

}
