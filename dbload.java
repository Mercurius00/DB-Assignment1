import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.time.DayOfWeek;
import java.time.Month;
import java.util.ArrayList;
import java.util.List;

/**
 * load data into db heap file
 */
public class dbload {

    public static void main(String[] args) {

        // check the input page size
        Integer pageSize = Integer.valueOf(args[1]);
        if (pageSize < 0) {
            System.err.println("invalid pagesize");
            return;
        }

        /* check input file */
        String srcPath = args[2];
        if (!srcPath.endsWith(".xml")) {
            System.err.println("only accept xml source file");
        }
        FileInputStream fis;
        try {
            fis = new FileInputStream(srcPath);
        } catch (FileNotFoundException e) {
            System.err.println("cannot find source xml file at input path");
            e.printStackTrace();
            return;
        }

        XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
        XMLStreamReader xmlStreamReader = null;

        try {
            // initialize xml reader
            xmlStreamReader = xmlInputFactory.createXMLStreamReader(fis);

            // read source file
            streamRead(xmlStreamReader, pageSize);
        } catch (Exception e) {
            System.err.println("read source file exception");
            e.printStackTrace();
        } finally {
            try {
                fis.close();
                xmlStreamReader.close();
            } catch (Exception e) {
                System.err.println("file reader close failed");
                e.printStackTrace();
            }
        }

    }

    /**
     * read xml source file in stream and write data to heap file
     * @param xmlStreamReader
     * @param pageSize
     * @throws XMLStreamException
     */
    private static void streamRead(XMLStreamReader xmlStreamReader, Integer pageSize) throws XMLStreamException {

        OutputStream outputStream = null;
        try {
            outputStream = new FileOutputStream("heap." + pageSize);
        } catch (FileNotFoundException e) {
            System.err.println("open target heap file failed");
            e.printStackTrace();
        }

        long startTime = System.currentTimeMillis();

        // the former tag
        String tag = null;
        // total page number
        int pageNumber = 0;
        // total record count
        int totalCount = 0;

        // current page data length, when it reaches page size, write and flush
        int length = 4;
        List<RowData> pageData = new ArrayList<>();

        RowData rowData = null;

        try {

            // get input file stream and deal with xml event
            while (xmlStreamReader.hasNext()) {
                int eventId = xmlStreamReader.next();
                switch (eventId) {
                    case XMLStreamConstants.START_ELEMENT:
                        // new xml row tag
                        tag = xmlStreamReader.getName().getLocalPart();
                        if (tag.equals("row")) {
                            rowData = new RowData();
                        }
                        break;
                    case XMLStreamConstants.CHARACTERS:
                        // get xml tag content
                        String content = xmlStreamReader.getText();
                        // set entity attribute
                        fillRowAttribute(rowData, tag, content);
                        break;
                    case XMLStreamConstants.END_ELEMENT:
                        tag = xmlStreamReader.getName().getLocalPart();
                        if (tag.equals("row")) {
                            // no space for new record, write current page and reset page buffer
                            if (length + rowData.length() >= pageSize) {
                                savePage(outputStream, pageData, pageSize);
                                pageData.clear();
                                length = 4;
                                pageNumber ++;
                            }

                            /* add row to page buffer */
                            pageData.add(rowData);
                            totalCount ++;
                            length += rowData.length();
                        }
                        break;
                }
            }

            // write the last page
            if (pageData.size() > 0) {
                savePage(outputStream, pageData, pageSize);
                pageNumber ++;
            }

            long endTime = System.currentTimeMillis();
            System.out.println(totalCount + " records, " + pageNumber + " pages saved in " + (endTime - startTime) + "ms");
        } catch (IOException e) {
            System.err.println("save heap file failed");
            e.printStackTrace();
        } finally {
            try {
                outputStream.close();
            } catch (IOException e) {
                System.err.println("heap output stream close failed");
                e.printStackTrace();
            }
        }
    }

    /**
     * write a page to heap file
     * @param outputStream output stream
     * @param pageData page data list
     * @param pageSize page size
     * @throws IOException
     */
    private static void savePage(OutputStream outputStream, List<RowData> pageData, Integer pageSize) throws IOException {
        byte[] byteBuffer = new byte[pageSize];

        // part1: record count in current page
        setArrayData(byteBuffer, toBytes(pageData.size()), 0);

        // the length of fixed length part of a record
        int recordFixedPartLength = 34;
        // the offset of record variable length fields to page start position
        int pageVariableOffset = recordFixedPartLength * pageData.size() + 4;

        /* part2+part3: page data */
        for (int i = 0; i < pageData.size(); i++) {
            RowData rowData = pageData.get(i);
            /* fixed-length part */
            setArrayData(byteBuffer, toBytes(rowData.getId()), recordFixedPartLength * i + 4);
            setArrayData(byteBuffer, rowData.getDateTimeStr().getBytes(), recordFixedPartLength * i + 8);
            byteBuffer[recordFixedPartLength * i + 18] = (byte) rowData.getDay();
            setArrayData(byteBuffer, toBytes(rowData.getYear()), recordFixedPartLength * i + 19);
            byteBuffer[recordFixedPartLength * i + 23] = (byte) rowData.getMonth();
            byteBuffer[recordFixedPartLength * i + 24] = (byte) rowData.getMdate();
            byteBuffer[recordFixedPartLength * i + 25] = (byte) rowData.getTime();
            setArrayData(byteBuffer, toBytes(rowData.getSensorId()), recordFixedPartLength * i + 26);
            setArrayData(byteBuffer, toBytes(rowData.getHourlyCounts()), recordFixedPartLength * i + 30);
            setArrayData(byteBuffer, toBytes(pageVariableOffset), recordFixedPartLength * i + 34);

            /* variable length part */
            byte[] sdtNameBytes = rowData.sdtName().getBytes();
            setArrayData(byteBuffer, toBytes(sdtNameBytes.length), pageVariableOffset);
            pageVariableOffset += 4;
            setArrayData(byteBuffer, sdtNameBytes, pageVariableOffset);
            pageVariableOffset += sdtNameBytes.length;
            byte[] sensorNameBytes = rowData.getSensorName().getBytes();
            setArrayData(byteBuffer, toBytes(sensorNameBytes.length), pageVariableOffset);
            pageVariableOffset += 4;
            setArrayData(byteBuffer, sensorNameBytes, pageVariableOffset);
            pageVariableOffset += sensorNameBytes.length;
        }

        outputStream.write(byteBuffer);
        outputStream.flush();
        System.out.println("write page");
    }

    /**
     * put subArray data at the position of mainArray
     * @param mainArray
     * @param subArray
     * @param position
     */
    private static void setArrayData(byte[] mainArray, byte[] subArray, int position) {
        for (int i = 0; i < subArray.length; i++) {
            mainArray[position + i] = subArray[i];
        }
    }

    /**
     * convert int number to byte array
     * @param number source int value
     * @return
     */
    public static byte[] toBytes(int number) {
        // use 4 bytes to save int
        ByteBuffer buff = ByteBuffer.allocate(4);
        buff.putInt(number);
        return buff.array();
    }

    /**
     * fetch attribute value from xml tag and set row data
     * @param rowData row data entity
     * @param tag current xml tag
     * @param content current xml tag content
     */
    private static void fillRowAttribute(RowData rowData, String tag, String content) {
        if (tag.equals("id")) {
            rowData.setId(Integer.parseInt(content));
        } else if (tag.equals("sensor_id")) {
            rowData.setSensorId(Integer.parseInt(content));
        } else if (tag.equals("date_time")) {
            rowData.setDateTime(content);
        } else if (tag.equals("year")) {
            rowData.setYear(Integer.parseInt(content));
        } else if (tag.equals("month")) {
            rowData.setMonth(Month.valueOf(content.toUpperCase()).getValue());
        } else if (tag.equals("mdate")) {
            rowData.setMdate(Integer.parseInt(content));
        } else if (tag.equals("time")) {
            rowData.setTime(Integer.parseInt(content));
        } else if (tag.equals("day")) {
            rowData.setDay(DayOfWeek.valueOf(content.toUpperCase()).getValue());
        } else if (tag.equals("hourly_counts")) {
            rowData.setHourlyCounts(Integer.parseInt(content));
        } else if (tag.equals("sensor_name")) {
            rowData.setSensorName(content);
        }
    }
}
