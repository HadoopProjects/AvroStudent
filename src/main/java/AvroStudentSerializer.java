import avro.shaded.com.google.common.collect.Lists;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hadoop on 7/12/16.
 */
public class AvroStudentSerializer {

    public static void main(String[] args) throws IOException {

        Student student = new Student();
        student.setName("Rahul");
        student.setAge(10);
        List<CharSequence> subjects = new ArrayList<CharSequence>();
        subjects.add("Maths");
        subjects.add("Science");
        subjects.add("English");
        student.setSubjects(subjects);

        DatumWriter<Student> datumWriter = new SpecificDatumWriter<Student>(Student.class);
        DataFileWriter<Student> dataFileWriter = new DataFileWriter<Student>(datumWriter);

        dataFileWriter.create(student.getSchema(), new File("student.avro"));

        dataFileWriter.append(student);

        dataFileWriter.close();

    }

}
