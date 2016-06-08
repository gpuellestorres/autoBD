/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package autobd;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import javax.sql.rowset.CachedRowSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider; 

/**
 *
 * @author Julio 
 */
public class DBConector { 
    public static int FREE_STATEMENT = 1;
    public static int STORED_PROCEDURE = 2;
    public static int FUNCTION = 3;
    
    private String driver; 
    private String host;
    private String puerto;
    private String database;
    private String usuario;
    private String contraseña;
    private String conexionString;
     
    private HashMap<String,String> params;
    private String procedureFunctionName;
    private String queryString;
    private int tipo;
    private boolean autoCommit;
    private Connection con;
    private String[] warnings;
    private List<ActionListener> warningListeners;
     
    /*
        
    */
    public DBConector
        (String host, 
        String puerto,  
        String database, 
        String usuario, 
        String contraseña)
    {
        this.driver = "org.postgresql.Driver";
        this.host = host;
        this.puerto = puerto;
        this.database = database;
        this.conexionString = "jdbc:postgresql://" + host + ":" + puerto + "/" + database;
        this.usuario = usuario;
        this.contraseña = contraseña; 
        this.params = null;
        this.procedureFunctionName = null;
        this.tipo = DBConector.FREE_STATEMENT;
        this.queryString = null;
        this.autoCommit = true;
        this.warnings = null;
    } 
    
    /*
        Crea una instancia de Conector con los datos por defecto
        @driver: "postgres"
        @database: "jdbc:postgresql://localhost:5432/fenixdb"
        @usuario: "postgres"
        @contraseña: "aurussu" 
    */
    public DBConector(){
        this("localhost"
            , "1680"
            , "autoBD"
            , "postgres"
            , "pgmasterkey*.temporal"); 
    }
    
    public void setAutoCommit(boolean autoCommit){
        this.autoCommit = autoCommit;
    }
    
    public boolean getAutoCommit(){
        return this.autoCommit;
    }
    
    public void commit() {
        try{ 
            this.autoCommit = true;
            this.con.commit();
            this.con.close();
            this.con = null;
        }catch(Exception ex){
            //throw new Exception("Ha ocurrido un error. "+ex.getMessage());
        } 
    }
    
    public void rollback() {
        try{ 
            this.autoCommit = true;
            this.con.rollback();
            this.con.close();
            this.con = null;
        }catch(SQLException ex){
            //throw new Exception("Ha ocurrido un error. "+ex.getMessage());
        } 
    }
    /*
        Ejecuta una query y retorna registros como resultado
        @query: consulta en sql
    */
    public CachedRowSet executeFunction(String query) throws Exception { 
        try{
            Class.forName(driver);
            if(this.con==null || this.con.isClosed())
                this.con = DriverManager.getConnection(conexionString, usuario, contraseña);
            con.setAutoCommit(this.autoCommit); 
            this.queryString = query;   
            try{
                this.warnings = null;
                Statement st = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                ResultSet rs = st.executeQuery(query/*.toUpperCase()*/);
                RowSetFactory rsfactory = RowSetProvider.newFactory();
                CachedRowSet cachedrs = rsfactory.createCachedRowSet();
                cachedrs.populate(rs);
                
                /* WARNINGS */
                try{
                    SQLWarning warnings = st.getWarnings();
                    List<String> lsw = new LinkedList();

                    while(warnings!=null){
                        if(warnings.getSQLState().compareToIgnoreCase("77777")==0) 
                            lsw.add(warnings.getMessage()); 
                        warnings = warnings.getNextWarning();
                    }
                    this.warnings = (String[]) lsw.toArray();
                    if(this.hasWarnings())
                        this.FireWarning(new ActionEvent(this, ActionEvent.ACTION_PERFORMED, "WARNINGS"));
                }catch(Exception ex){
                    System.out.println(ex.getMessage());
                }
                
                if(this.autoCommit){
                    rs.close();
                    st.close();
                    con.close();
                }
                this.tipo = DBConector.FREE_STATEMENT;
                
                return cachedrs;
            }catch(SQLException ex){
                throw new Exception("Ha ocurrido un error al ejecutar la consulta: " + queryString+": "+ex.getMessage());
            } 
        }catch(ClassNotFoundException ex){
            throw new Exception("Conector de base de datos no encontrado");  
        }
    }
    /*
        Ejecuta una consulta que no retorna registros resultado
        @sql: script sql
    */
    public void executeStoreProcedure(String query) throws Exception
    {
        try{
            this.warnings = null;
            this.queryString = query;
            Class.forName(driver);
            if(this.con==null || this.con.isClosed()) 
                con = DriverManager.getConnection(conexionString, usuario, contraseña);
            con.setAutoCommit(this.autoCommit); 
            Statement st = con.createStatement();  
            st.execute(query/*.toUpperCase()*/);
            this.tipo = DBConector.FREE_STATEMENT;
            
            /* WARNINGS */
            try{
                SQLWarning warnings = st.getWarnings();
                List<String> lsw = new LinkedList();

                while(warnings!=null){
                    if(warnings.getSQLState().compareToIgnoreCase("77777")==0) 
                        lsw.add(warnings.getMessage());  
                    warnings = warnings.getNextWarning();
                }
                this.warnings = (String[]) lsw.toArray();
                if(this.hasWarnings())
                    this.FireWarning(new ActionEvent(this, ActionEvent.ACTION_PERFORMED, "WARNINGS"));
            }catch(Exception ex){} 
            if(this.autoCommit){
                st.close();
                con.close();
            } 
        }catch(ClassNotFoundException ex){
            throw new Exception("Conector de base de datos no encontrado"); 
        }catch(SQLException ex){
            throw new Exception("Ha ocurrido un error al ejecutar la consulta: " + queryString);
        }  
    }
    
    /*
    
    */
    public void prepareStoreProcedure(String procedureFunctionName){
        if(this.params == null) 
            this.params = new HashMap<String, String>();
        this.params.clear(); 
        this.procedureFunctionName = procedureFunctionName;
        this.tipo = DBConector.STORED_PROCEDURE; 
    }
     
    public void prepareFunction(String procedureFunctionName){
        if(this.params == null) 
            this.params = new HashMap<String, String>(); 
        this.params.clear(); 
        this.procedureFunctionName = procedureFunctionName;
        this.tipo = DBConector.FUNCTION; 
    }
    
    public String addParammeter(String paramName, Object paramValue) throws Exception{ 
        if (this.params == null || this.tipo == DBConector.FREE_STATEMENT) 
            throw new Exception("Primero debe preparar la funcion o el metodo antes de agregar los parametros");
        if(paramName == null || paramName.trim().length()<1) 
            return null;
        if(paramValue == null)
            return this.params.put(paramName, "NULL");
        if(paramValue.getClass() == Date.class) 
            return this.params.put(paramName, "'" + new SimpleDateFormat("MM-dd-yyyy HH:mm:ss").format(paramValue) +"'");
        if(paramValue.toString().trim().isEmpty())
            return this.params.put(paramName, "''");
        return this.params.put(paramName, paramValue.toString());
    }
    
    public void executeStoreProcedure() throws Exception{
        if (this.params == null || this.tipo != DBConector.STORED_PROCEDURE) 
            throw new Exception("Primero debe preparar el procedimiento antes de ejecutarlo");
        
        String query = "SELECT " + this.procedureFunctionName + "(";  
        
        if(params.size()>1)
        {
            for (HashMap.Entry<String, String> p : params.entrySet()) {
                    query += p.getKey() + " := " + p.getValue() + ",";
            }
            query = query.substring(0, query.lastIndexOf(',')) ;
        }
        else{
            for (HashMap.Entry<String, String> p : params.entrySet()) {
                    query += p.getKey() + " := " + p.getValue();
            }
        }
        
        query += ");";
               
        this.executeStoreProcedure(query);
    }
    
    public CachedRowSet executeFunction() throws Exception{
        if (this.params == null || this.tipo != DBConector.FUNCTION) 
            throw new Exception("Primero debe preparar la funcion antes de ejecutarla");
        
        String query = "SELECT * FROM " + this.procedureFunctionName + "(";  
        
        if(params.size()>1)
        {
            for (HashMap.Entry<String, String> p : params.entrySet()) {
                    query += p.getKey() + " := " + p.getValue() + ",";
            }
            query = query.substring(0, query.lastIndexOf(',')) ;
        }
        else{
            for (HashMap.Entry<String, String> p : params.entrySet()) {
                    query += p.getKey() + " := " + p.getValue();
            }
        }
        
        query += ");";
        return this.executeFunction(query); 
    }
    
    public String getconexionString(){
        return this.conexionString;
    } 
    
    public String getUsuario(){
        return this.usuario;
    }
    
    public String getContraseña(){
        return this.contraseña;
    }
    
    public Connection getConnection(){
        try{ 
            Class.forName(driver);
            return DriverManager.getConnection(conexionString, usuario, contraseña);
        }catch(Exception ex){}
        return null;
    }
     
    public String[] getWarnings() {
        return warnings;
    } 
    
    public boolean hasWarnings(){
        return (this.warnings==null? false: this.warnings.length>0);
    }
    
    public void addWarningListener(ActionListener listener){
        if(this.warningListeners==null) this.warningListeners = new LinkedList();
        if(!this.warningListeners.contains(listener))
            this.warningListeners.add(listener);
    }
    
    public void removeAllWarningListeners(){
        this.warningListeners.clear();
    }
    
    private void FireWarning(ActionEvent evt){
        if(this.warningListeners!=null)
            for(int i=0; i<this.warningListeners.size(); i++)
                this.warningListeners.get(i).actionPerformed(evt);
    }
}
