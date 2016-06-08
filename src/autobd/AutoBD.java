/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package autobd;

import java.lang.reflect.Field;

/**
 *
 * @author Guillermo
 */
public class AutoBD {

    public static void main(String[] args) {
        bdHelper.crearTabla(Cuadrado.class, true);
        bdHelper.crearTabla(Dvd.class, true);

        Cuadrado cuadrado = new Cuadrado();
        
        cuadrado.ancho=10;
        cuadrado.nombre = "primero";
        
        bdHelper.add(cuadrado);
        
        Dvd dvd = new Dvd();
        dvd.hd=false;
        dvd.minutos=135;
        dvd.titulo="The Big Lebowski";
        
        bdHelper.add(dvd);
    }
}

class Dvd {

    public Boolean hd;
    String titulo;
    int minutos;
}

class Cuadrado {

    public int ancho;
    public String nombre;

    public int getArea() {
        return ancho * ancho;
    }
}

class bdHelper {

    public static void crearTabla(Class dato, Boolean eliminarSiExiste) {
        DBConector conexion = new DBConector();
        try {

            String nombreClase = dato.getName().replace("autobd.", "");

            String cadena = "create table \n" + nombreClase
                    + " ( ID SERIAL NOT NULL,";

            if (eliminarSiExiste) {
                cadena = "drop table " + nombreClase + ";" + cadena;
            }

            Field[] fields = dato.getFields();

            for (Field field : fields) {

                System.out.println(field.getType());

                if (field.getType().equals(int.class)) {
                    cadena += " " + field.getName() + " integer NOT NULL,";
                } else if (field.getType().equals(String.class)) {
                    cadena += " " + field.getName() + " text NOT NULL,";
                } else if (field.getType().equals(float.class) || field.getType().equals(double.class)) {
                    cadena += " " + field.getName() + " double precision NOT NULL,";
                } else if (field.getType().equals(Boolean.class)) {
                    cadena += " " + field.getName() + " bit NOT NULL,";
                }
            }

            cadena = cadena + " CONSTRAINT id_key" + nombreClase + " PRIMARY KEY(ID));";

            System.out.println(cadena);

            conexion.executeStoreProcedure(cadena);
        } catch (Exception e) {
            System.out.println("Error en consulta");
            e.printStackTrace();
        }
    }

    public static void insercionPrueba() {
        DBConector conexion = new DBConector();
        try {
            String cadena = "INSERT INTO prueba VALUES('1')";

            System.out.println(cadena);

            conexion.executeStoreProcedure(cadena);
        } catch (Exception e) {
            System.out.println("Error en consulta");

            e.printStackTrace();
        }
    }

    public static void add(Object instancia) {
        DBConector conexion = new DBConector();

        Class clase = instancia.getClass();
        try {

            String nombreClase = clase.getName().replace("autobd.", "");

            String cadena = "INSERT INTO \n" + nombreClase
                    + "( ";

            Field[] fields = clase.getFields();
            
            int i = 0;
            for (Field field : fields) {
                if(i==0){
                    cadena += field.getName();
                    i++;
                }
                else
                {
                    cadena += ", " + field.getName();
                }
            }

            cadena = cadena + ") VALUES('";
            
            i=0;
            for (Field field : fields) {
                if (i == 0) {
                    cadena += field.get(instancia);
                    i++;
                } else {
                    cadena += "', '" + field.get(instancia);
                }
            }

            cadena = cadena + "')";

            System.out.println(cadena);

            conexion.executeStoreProcedure(cadena);
        } catch (Exception e) {
            System.out.println("Error en consulta");

            e.printStackTrace();
        }
    }
}
