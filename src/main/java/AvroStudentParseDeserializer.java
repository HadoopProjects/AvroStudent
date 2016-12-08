import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.File;
import java.io.IOException;

/**
 * Created by hadoop on 7/12/16.
 */
public class AvroStudentParseDeserializer {

    public static void main(String[] args) throws IOException {

        Schema newSchema = new Schema.Parser().parse(new File("student_new.avsc"));
        Schema oldSchema = new Schema.Parser().parse(new File("student_old.avsc"));

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(oldSchema,newSchema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File("studentparse.avro"),datumReader);

        while(dataFileReader.hasNext()) {

            GenericRecord record = dataFileReader.next();
            System.out.println(record);
        }

        dataFileReader.close();

    }

}
