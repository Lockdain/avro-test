import com.example.avroSample.model.Automobile;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        // Serialization
        Automobile auto = Automobile.newBuilder().setMake("Speedy Car Company")
                .setModelName("Speedyspeedster").setModelYear(2013).
        setPassengerCapacity(2).build();

        DatumWriter<Automobile> datumWriter =
                new SpecificDatumWriter<Automobile>(Automobile.class);
        DataFileWriter<Automobile> fileWriter =
                new DataFileWriter<Automobile>(datumWriter);
        File outputFile = new File("auto.avro");
        try {
            fileWriter.create(auto.getSchema(), outputFile);
            fileWriter.append(auto);
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Deserialization
        DatumReader<Automobile> datumReader =
                new SpecificDatumReader<Automobile>(Automobile.class);
        try {
            DataFileReader<Automobile> fileReader =
                    new DataFileReader<Automobile>(outputFile, datumReader);

            Automobile desAuto = null;

            if (fileReader.hasNext()) {
                auto = fileReader.next(auto);
            }

            System.out.println("Deserialized auto: " + auto);

        } catch (IOException e) {
           e.printStackTrace();
        }
    }
}
