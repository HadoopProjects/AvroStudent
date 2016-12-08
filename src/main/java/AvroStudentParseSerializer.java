import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hadoop on 7/12/16.
 */
public class AvroStudentParseSerializer {

    public static void main(String[] args) throws IOException {

        Schema schema = new Schema.Parser().parse(new File("student_new.avsc"));

        GenericRecord record = new GenericData.Record(schema);
        record.put("name","Rahul");
        record.put("age",10);
        List<CharSequence> subjects = new ArrayList<CharSequence>();
        subjects.add("Maths");
        subjects.add("Science");
        subjects.add("English");
        record.put("subjects", subjects);

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);

        dataFileWriter.create(schema,new File("studentparse.avro"));
        dataFileWriter.append(record);

        dataFileWriter.close();

    }

}
