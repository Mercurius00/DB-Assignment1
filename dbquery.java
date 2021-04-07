import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.text.MessageFormat;

/**
 * query data from heap file
 */
public class dbquery {

    public static void main(String[] args) {
        if (args == null || args.length != 2) {
            System.err.println("invalid argument");
            return;
        }

        String queryText = args[0].trim();
        Integer pageSize = Integer.parseInt(args[1].trim());
        String heapFilePath = "heap." + pageSize;

        InputStream is = null;
        try {

            long start = System.currentTimeMillis();
            // heap file input stream
            is = new FileInputStream(heapFilePath);
            // read page data into buffer
            byte[] buffer = new byte[pageSize];
            // result record count
            int recordCount = 0;

            // query in each page
            while (true) {
                int read = is.read(buffer);
                if (read < 0) {
                    break;
                }

                // search current page data
                int count = searchCurrentPage(buffer, queryText);
                recordCount += count;
            }

            long cost = System.currentTimeMillis() - start;
            System.out.println(MessageFormat.format("{0} records queried in {1} ms.", recordCount, cost));

        } catch (Exception e) {
            System.err.println("read heap file exception");
            e.printStackTrace();
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                System.err.println("close file stream exception");
                e.printStackTrace();
            }
        }
    }

    /**
     * query text in page byte data
     * @param buffer
     * @param queryText
     * @return
     */
    private static int searchCurrentPage(byte[] buffer, String queryText) {
        int result = 0;

        // page record count
        int recordCount = getIntValue(buffer, 0);

        // the length of fixed length part of a record
        int recordFixedPartLength = 34;

        for (int i = 0; i < recordCount; i++) {

            // offset position of variable length fields
            int recordOffset = getIntValue(buffer, recordFixedPartLength * i + 34);
            // byte length of SDT_NAME field
            int sdtNameLength = getIntValue(buffer, recordOffset);
            // SDT_NAME value
            String sdtName = getStringValue(buffer, recordOffset + 4, sdtNameLength);

            if (sdtName.equals(queryText.trim())) {
                int hourlyCounts = getIntValue(buffer, recordFixedPartLength * i + 30);
                // byte length of sensor_name field
                int sensorNameLength = getIntValue(buffer, recordOffset + 4 + sdtNameLength);
                // sensor_name value
                String sensorName = getStringValue(buffer, recordOffset + 8 + sdtNameLength, sensorNameLength);

                RowData rowData = new RowData();
                int id = getIntValue(buffer, recordFixedPartLength * i + 4);
                String dateTime = getStringValue(buffer, recordFixedPartLength * i + 8, 10);
                int sensorId = getIntValue(buffer, recordFixedPartLength * i + 26);
                rowData.setSensorId(sensorId);
                rowData.setDateTime(dateTime);
                rowData.setId(id);
                rowData.setHourlyCounts(hourlyCounts);
                rowData.setSensorName(sensorName);
                System.out.println(rowData);
                result ++;
            }
        }
        return result;
    }

    /**
     * convert bytes to int value
     * @param bytes
     * @return
     */
    public static int getIntValue(byte[] bytes, int position){
        byte[] subArray = new byte[4];
        for(int i = 0; i < subArray.length ; i++){
            subArray[i] = bytes[position + i];
        }
        return ByteBuffer.wrap(subArray).getInt();
    }

    /**
     * convert bytes to string value
     * @param bytes
     * @return
     */
    public static String getStringValue(byte[] bytes, int position, int length){
        byte[] stringBytes = new byte[length];
        for(int i = 0; i < length ; i++){
            stringBytes[i] = bytes[position + i];
        }
        return new String(stringBytes);
    }
}
