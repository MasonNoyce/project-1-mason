package sparkproj;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.junit.Before;
import org.junit.Test;

public class io {

    
    @Test
    public void CSVfileExists() throws Exception
    {
        File file = new File("hurricane.csv");
        BufferedReader br = new BufferedReader(new FileReader(file));
        String[] line = br.readLine().split(",");
        assertEquals("Testing File Exists", "ID", line[0]);
    }



}