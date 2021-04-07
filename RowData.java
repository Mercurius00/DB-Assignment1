import java.time.DayOfWeek;

/**
 * entity
 */
public class RowData {

    /**
     * id
     */
    private int id;
    /**
     * sensor_id
     */
    private int sensorId;
    /**
     * date_time
     */
    private String dateTime;
    /**
     * year
     */
    private int year;
    /**
     * month
     */
    private int month;
    /**
     * mdate
     */
    private int mdate;
    /**
     * day
     */
    private int day;
    /**
     * time
     */
    private int time;
    /**
     * hourly_counts
     */
    private int hourlyCounts;
    /**
     * sensor_name
     */
    private String sensorName;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getSensorId() {
        return sensorId;
    }

    public void setSensorId(int sensorId) {
        this.sensorId = sensorId;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getMdate() {
        return mdate;
    }

    public void setMdate(int mdate) {
        this.mdate = mdate;
    }

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getHourlyCounts() {
        return hourlyCounts;
    }

    public void setHourlyCounts(int hourlyCounts) {
        this.hourlyCounts = hourlyCounts;
    }

    public String getSensorName() {
        return sensorName;
    }

    public void setSensorName(String sensorName) {
        this.sensorName = sensorName;
    }

    /**
     * return formatted date time, yyyyMMddhh
     * @return
     */
    public String getDateTimeStr() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(year);
        if (month < 10) {
            stringBuilder.append("0").append(month);
        } else {
            stringBuilder.append(month);
        }
        if (mdate < 10) {
            stringBuilder.append("0").append(mdate);
        } else {
            stringBuilder.append(mdate);
        }
        if (time < 10) {
            stringBuilder.append("0").append(time);
        } else {
            stringBuilder.append(time);
        }
        return stringBuilder.toString();
    }

    /**
     * calculate the byte length of entity
     * id: 4 bytes
     * dateTimeStr: 10 bytes
     * day: 1 byte
     * year: 4 bytes
     * month: 1 byte
     * mdate: 1 byte
     * time: 1 byte
     * sensorId: 4 bytes
     * hourlyCounts: 4 bytes
     * offset: 4 bytes
     * dateTimeOffset: 4 bytes
     * sensorNameOffset: 4 bytes
     * sdtNameLength: variable length
     * sensorNameLength: variable length
     * @return
     */
    public int length() {
        return 42 + sdtName().getBytes().length + sensorName.getBytes().length;
    }

    /**
     * SDT_NAME index
     * @return
     */
    public String sdtName() {
        return sensorId + "_" + dateTime;
    }

    @Override
    public String toString() {
        return "RowData{" +
                "id=" + id +
                ", sensorId=" + sensorId +
                ", dateTime='" + dateTime + '\'' +
                ", hourlyCounts=" + hourlyCounts +
                ", sensorName='" + sensorName + '\'' +
                '}';
    }
}
