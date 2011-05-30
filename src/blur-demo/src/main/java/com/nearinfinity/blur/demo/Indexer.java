package com.nearinfinity.blur.demo;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.lucene.search.FairSimilarity;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.ColumnFamily;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.utils.RowIndexWriter;

public class Indexer {

    public static void main(String[] args) throws CorruptIndexException, LockObtainFailedException, IOException, URISyntaxException {
        //"hdfs://localhost:9000/user/hive/warehouse/employee_super_mart/000000_0"
        URI uri = new URI(args[0]);
        BlurAnalyzer analyzer = new BlurAnalyzer(new StandardAnalyzer(Version.LUCENE_30));
        Directory dir = FSDirectory.open(new File("./index"));
        IndexWriter indexWriter = new IndexWriter(dir, analyzer, MaxFieldLength.UNLIMITED);
        indexWriter.setUseCompoundFile(false);
        indexWriter.setSimilarity(new FairSimilarity());
        RowIndexWriter writer  = new RowIndexWriter(indexWriter, analyzer);
        
        FileSystem fileSystem = FileSystem.get(uri, new Configuration());
        FSDataInputStream inputStream = fileSystem.open(new Path(uri.getPath()));
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        Row row = new Row();
        String[] values = new String[25];
        int max = 1000;
        int total = 0;
        int count = 0;
        while ((line = reader.readLine()) != null) {
            if (count >= max) {
                System.out.println("Total [" + total + "]");
                count = 0;
            }
            long hash = parse(line,values);
            String rowId = values[0];
            if (!rowId.equals(row.id)) {
                addRow(row,writer);
                row = new Row().setId(rowId);
            }
            addRecord(row,values,hash);
            total++;
            count++;
        }
        addRow(row,writer);
        indexWriter.optimize();
        indexWriter.close();
    }

    private static void addRow(Row row, RowIndexWriter writer) throws IOException {
        if (row.id != null) {
            fixStuff(row);
            writer.replace(row);
        }
    }

    private static void fixStuff(Row row) {
        String managerSalary = null;
        String employeeSalary = null;
        Set<ColumnFamily> columnFamilies = row.columnFamilies;
        ColumnFamily empCf = null;
        for (ColumnFamily cf : columnFamilies) {
            if (cf.family.equals("manager")) {
                Collection<Set<Column>> values = cf.records.values();
                for (Set<Column> cols : values) {
                    for (Column column : cols) {
                        if (column.name.equals("salary")) {
                            managerSalary = column.values.get(0);
                        }
                    }
                }
            }
            if (cf.family.equals("salary")) {
                empCf = cf;
                Collection<Set<Column>> values = cf.records.values();
                for (Set<Column> cols : values) {
                    for (Column column : cols) {
                        if (column.name.equals("salary")) {
                            employeeSalary = column.values.get(0);
                        }
                    }
                }
            }
        }
        long man = Long.parseLong(managerSalary);
        long emp = Long.parseLong(employeeSalary);
        if (emp > man) {
            Collection<Set<Column>> values = empCf.records.values();
            Set<Column> cols = values.iterator().next();
            Column column = new Column().setName("makesMoreThanManager");
            column.addToValues("T");
            cols.add(column);
        }
    }

    private static void addRecord(Row row, String[] values, long hash) {
        String cf = values[11];
        if ("employee".equals(cf)) {
            addEmployee(row,values,hash);
        } else if ("title".equals(cf)) {
            addTitle(row,values,hash);
        } else if ("title_history".equals(cf)) {
            addTitleHistory(row,values,hash);
        } else if ("salary".equals(cf)) {
            addSalary(row,values,hash);
        } else if ("salary_history".equals(cf)) {
            addSalaryHistory(row,values,hash);
        } else if ("manager".equals(cf)) {
            addManager(row,values,hash);
        } else if ("department".equals(cf)) {
            addDepartment(row,values,hash);
        } else if ("department_history".equals(cf)) {
            addDepartmentHistory(row,values,hash);
//        } else if ("title_salary".equals(cf)) {
//            addTitleSalary(row,values,hash);
        } else {
            for (int i = 0; i < values.length; i++) {
                System.out.println("i=" + i + " value=" + values[i]);
            }
            throw new RuntimeException("Column Family [" + cf + "] Not Found");
        }
    }

    private static void addTitleHistory(Row row, String[] values, long hash) {
        ColumnFamily columnFamily = new ColumnFamily();
        columnFamily.family = "titleHistory";
        Set<Column> cols = new HashSet<Column>();
        
        //from_date, to_date, title
        Column fromDate = new Column().setName("fromDate");
        fromDate.addToValues(values[2]);
        Column toDate = new Column().setName("toDate");
        toDate.addToValues(values[3]);
        Column title = new Column().setName("title");
        title.addToValues(values[1]);
        
        cols.add(fromDate);
        cols.add(toDate);
        cols.add(title);
        columnFamily.putToRecords(Long.toString(Math.abs(hash)), cols);
        row.addToColumnFamilies(columnFamily);
    }
    
    private static void addManager(Row row, String[] values, long hash) {
        ColumnFamily columnFamily = new ColumnFamily();
        columnFamily.family = "manager";
        Set<Column> cols = new HashSet<Column>();
        
//        de.from_date, 
//        de.to_date, 
//        d.dept_name as col1, 
//        dm.from_date as col2, 
//        dm.to_date as col3, 
//        e2.emp_no as col4, 
//        e2.first_name as col5, 
//        e2.last_name as col6,
        
        //from_date, to_date, title
        Column manEmpNo = new Column().setName("managerEmpNo");
        manEmpNo.addToValues(values[1]);
        Column birthDate = new Column().setName("birthDate");
        birthDate.addToValues(values[2]);
        Column firstName = new Column().setName("firstName");
        firstName.addToValues(values[3]);
        Column lastName = new Column().setName("lastName");
        lastName.addToValues(values[4]);
        Column gender = new Column().setName("gender");
        gender.addToValues(values[5]);
        Column hireDate = new Column().setName("hireDate");
        hireDate.addToValues(values[6]);
        Column salary = new Column().setName("salary");
        salary.addToValues(bufferSalary(values[7]));
        Column fromDate = new Column().setName("fromDate");
        fromDate.addToValues(values[8]);
        Column toDate = new Column().setName("toDate");
        toDate.addToValues(values[9]);
        
        cols.add(manEmpNo);
        cols.add(birthDate);
        cols.add(firstName);
        cols.add(lastName);
        cols.add(gender);
        cols.add(hireDate);
        cols.add(salary);
        cols.add(fromDate);
        cols.add(toDate);
        columnFamily.putToRecords(Long.toString(Math.abs(hash)), cols);
        row.addToColumnFamilies(columnFamily);
    }

    private static void addDepartment(Row row, String[] values, long hash) {
        ColumnFamily columnFamily = new ColumnFamily();
        columnFamily.family = "department";
        Set<Column> cols = new HashSet<Column>();
        
        Column deptNo = new Column().setName("deptNo");
        deptNo.addToValues(values[1]);
        Column name = new Column().setName("name");
        name.addToValues(values[2]);
        Column moreThanOneDepartment = new Column().setName("moreThanOneDepartment");
        moreThanOneDepartment.addToValues(values[3]);
        
        cols.add(moreThanOneDepartment);
        cols.add(name);
        cols.add(deptNo);
        
        columnFamily.putToRecords(Long.toString(Math.abs(hash)), cols);
        row.addToColumnFamilies(columnFamily);
    }
    
    private static void addDepartmentHistory(Row row, String[] values, long hash) {
        ColumnFamily columnFamily = new ColumnFamily();
        columnFamily.family = "departmentHistory";
        Set<Column> cols = new HashSet<Column>();
        
        Column deptNo = new Column().setName("deptNo");
        deptNo.addToValues(values[1]);
        Column name = new Column().setName("name");
        name.addToValues(values[2]);
        
        cols.add(name);
        cols.add(deptNo);
        
        columnFamily.putToRecords(Long.toString(Math.abs(hash)), cols);
        row.addToColumnFamilies(columnFamily);
    }

//    private static void addTitleSalary(Row row, String[] values, long hash) {
//        
//    }

    private static void addTitle(Row row, String[] values, long hash) {
        ColumnFamily columnFamily = new ColumnFamily();
        columnFamily.family = "title";
        Set<Column> cols = new HashSet<Column>();
        
        //from_date, to_date, title
        Column fromDate = new Column().setName("fromDate");
        fromDate.addToValues(values[2]);
        Column toDate = new Column().setName("toDate");
        toDate.addToValues(values[3]);
        Column title = new Column().setName("title");
        title.addToValues(values[1]);
        
        cols.add(fromDate);
        cols.add(toDate);
        cols.add(title);
        columnFamily.putToRecords(Long.toString(Math.abs(hash)), cols);
        row.addToColumnFamilies(columnFamily);
    }

    private static void addSalary(Row row, String[] values, long hash) {
        ColumnFamily columnFamily = new ColumnFamily();
        columnFamily.family = "salary";
        Set<Column> cols = new HashSet<Column>();
        
        //from_date, to_date, salary
        Column fromDate = new Column().setName("fromDate");
        fromDate.addToValues(values[2]);
        Column toDate = new Column().setName("toDate");
        toDate.addToValues(values[3]);
        Column salary = new Column().setName("salary");
        salary.addToValues(bufferSalary(values[1]));
        
        cols.add(fromDate);
        cols.add(toDate);
        cols.add(salary);
        columnFamily.putToRecords(Long.toString(Math.abs(hash)), cols);
        row.addToColumnFamilies(columnFamily);
    }
    
    private static void addSalaryHistory(Row row, String[] values, long hash) {
        ColumnFamily columnFamily = new ColumnFamily();
        columnFamily.family = "salaryHistory";
        Set<Column> cols = new HashSet<Column>();
        
        //from_date, to_date, salary
        Column fromDate = new Column().setName("fromDate");
        fromDate.addToValues(values[2]);
        Column toDate = new Column().setName("toDate");
        toDate.addToValues(values[3]);
        Column salary = new Column().setName("salary");
        salary.addToValues(bufferSalary(values[1]));
        
        cols.add(fromDate);
        cols.add(toDate);
        cols.add(salary);
        columnFamily.putToRecords(Long.toString(Math.abs(hash)), cols);
        row.addToColumnFamilies(columnFamily);
    }

    private static String bufferSalary(String s) {
        while (s.length() < 14) {
            s = "0" + s;
        }
        return s;
    }

    private static void addEmployee(Row row, String[] values, long hash) {
        ColumnFamily columnFamily = new ColumnFamily();
        columnFamily.family = "employee";
        Set<Column> cols = new HashSet<Column>();
        
        //from_date, to_date, birth_date, first_name, last_name, gender
//        Column fromDate = new Column().setName("fromDate");
//        fromDate.addToValues(values[2]);
//        Column toDate = new Column().setName("toDate");
//        toDate.addToValues(values[3]);
        Column birthDate = new Column().setName("birthDate");
        birthDate.addToValues(values[1]);
        Column firstName = new Column().setName("firstName");
        firstName.addToValues(values[2]);
        Column lastName = new Column().setName("lastName");
        lastName.addToValues(values[3]);
        Column gender = new Column().setName("gender");
        gender.addToValues(values[4]);
        Column hireDate = new Column().setName("hireDate");
        hireDate.addToValues(values[5]);
        
        Column name = new Column().setName("name");
        name.addToValues(values[2] + " " + values[3]);
        
//        cols.add(fromDate);
//        cols.add(toDate);
        cols.add(birthDate);
        cols.add(firstName);
        cols.add(lastName);
        cols.add(gender);
        cols.add(name);
        cols.add(hireDate);
        
        columnFamily.putToRecords(Long.toString(Math.abs(hash)), cols);
        row.addToColumnFamilies(columnFamily);
    }

    private static long parse(String line, String[] values) {
        int start = 0;
        int index = line.indexOf('\t',start);
        int i = 0;
        long hash = line.length();
        while (index >= 0) {
            String token = line.substring(start, index);
            values[i++] = token;
            start = index + 1;
            index = line.indexOf('\t',start);
            hash += 31L * token.hashCode();
        }
        String token = line.substring(start);
        values[i++] = token;
        
//        StringTokenizer tokenizer = new StringTokenizer(line,"\t");
//        int i = 0;
//        long hash = line.length();
//        while (tokenizer.hasMoreTokens()) {
//            String token = tokenizer.nextToken();
//            values[i++] = token;
//            hash += 31L * token.hashCode();
//        }
//        for (; i < values.length; i++) {
//            values[i] = null;
//        }
        return hash;
    }

}
