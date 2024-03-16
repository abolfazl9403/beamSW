package abolfazl.younesi.bolts;

import lombok.Getter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TaxiData {
    @Getter
    private final String medallion;
    private final String hack_license, vendor_id, payment_type;
    private final Date pickup_datetime;
    @Getter
    private final double surcharge;
    private final double fare_amount, mta_tax, tip_amount, tolls_amount, total_amount;

    public TaxiData(String[] data) throws ParseException {
        try {
            this.medallion = data[0];
            this.hack_license = data[1];
            this.vendor_id = data[2];
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            this.pickup_datetime = sdf.parse(data[3]);
            this.payment_type = data[4];
            this.fare_amount = Double.parseDouble(data[5]);
            this.surcharge = Double.parseDouble(data[6]);
            this.mta_tax = Double.parseDouble(data[7]);
            this.tip_amount = Double.parseDouble(data[8]);
            this.tolls_amount = Double.parseDouble(data[9]);
            this.total_amount = Double.parseDouble(data[10]);
        } catch (ParseException | NumberFormatException | ArrayIndexOutOfBoundsException e) {
            throw new ParseException("Error parsing taxi data: " + e.getMessage(), 0);
        }
    }

    public String getHackLicense() {
        return hack_license;
    }

    public String getVendorId() {
        return vendor_id;
    }

    public Date getPickupDatetime() {
        return pickup_datetime;
    }

    public String getPaymentType() {
        return payment_type;
    }

    public double getFareAmount() {
        return fare_amount;
    }

    public double getMtaTax() {
        return mta_tax;
    }

    public double getTipAmount() {
        return tip_amount;
    }

    public double getTollsAmount() {
        return tolls_amount;
    }

    public double getTotalAmount() {
        return total_amount;
    }
}
