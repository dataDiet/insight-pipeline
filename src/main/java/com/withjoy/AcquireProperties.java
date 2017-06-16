/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.withjoy;

import java.util.Properties;
import java.io.FileInputStream;
import java.io.IOException;
/**
 *
 * @author akiramadono
 */
public class AcquireProperties {
    private Properties default_properties;
    public AcquireProperties(String property_file){
        this.default_properties = new Properties();
        
        try{
            FileInputStream in = new FileInputStream(property_file);
            default_properties.load(in);
            in.close();
        }
        catch(IOException io){
            System.out.println("IO Exception in acquisition of properties: "+io.getMessage());
        }
    }

    public Properties getProperties(){
        return this.default_properties;
    }   
}

