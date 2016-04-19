import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.File;
import java.io.OutputStreamWriter;

public class testMapper implements MapperInterface {

    public String map(String ip, String op) {
        System.out.println("Oh look, you loaded a class dynamically!!");
        BufferedReader fileReader = null;
        String line;
        try {
            fileReader = new BufferedReader(new FileReader(ip));
        } catch (Exception e) {
            System.out.println("Bad input file?? " + e.getMessage());
            e.printStackTrace();
        }
        File fout = new File(op);
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(fout);
        } catch (Exception e) {
            e.printStackTrace();
        }

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
        while(true) {
            try {
                if((line = fileReader.readLine()) == null)
                    break;
            } catch (Exception e) {
                System.out.println("File read problems?? " + e.getMessage());
                e.printStackTrace();
                break;
            }
            if(line.contains("amogh")) {
                try {
                    bw.write(line);
                    bw.newLine();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }
        try {
            bw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }
}
