import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.security.MessageDigest;

import java.util.*;

public class GetCSVRec extends DoFn<TableRow, String> {
    public String hasing(String input) {
        try {

            // invoking the static getInstance() method of the MessageDigest class
            // Notice it has MD5 in its parameter.
            MessageDigest msgDst = MessageDigest.getInstance("MD5");

// the digest() method is invoked to compute the message digest
// from an input digest() and it returns an array of byte
            byte[] msgArr = msgDst.digest(input.getBytes());

// getting signum representation from byte array msgArr
            BigInteger bi = new BigInteger(1, msgArr);

// Converting into hex value
            String hshtxt = bi.toString(16);

            while (hshtxt.length() < 32) {
                hshtxt = "0" + hshtxt;
            }
            return hshtxt;
        }
// for handling the exception
        catch (NoSuchAlgorithmException abc) {
            throw new RuntimeException(abc);
        }
    }
    @ProcessElement
    public void processElement(ProcessContext context) {
        String bf = "";
        TableRow r = context.element();

        Set<String> keys = r.keySet();
        Boolean flag = false;
        for (String key : keys) {
            if (flag) {
                bf += ",";
            }
            bf += r.get(key);
            flag = true;

        }
        String[] strColumns;
        strColumns = bf.split(",");
        String number = strColumns[2];

        String resultInput=hasing(strColumns[2]);

        strColumns[2]=resultInput;

        StringJoiner stringJoiner = new StringJoiner(",");
        for (String str : strColumns) {
            stringJoiner.add(str);
        }
        context.output(stringJoiner.toString());
    }
}
