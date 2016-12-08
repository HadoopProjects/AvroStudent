import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;
import java.io.IOException;

/**
 * Created by hadoop on 7/12/16.
 */
public class AvroStudentDeserializer {

    public static void main(String[] args) throws IOException {

        DatumReader<Student> datumReader = new SpecificDatumReader<Student>(Student.class);
        DataFileReader<Student> dataFileReader = new DataFileReader<Student>(new File("student.avro"),datumReader);

        while(dataFileReader.hasNext()) {

            Student student = dataFileReader.next();
            System.out.println(student);

        }

    }

}
