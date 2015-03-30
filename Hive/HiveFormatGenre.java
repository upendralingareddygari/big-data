import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class HiveFormatGenre extends UDF {
    public Text evaluate(Text input) {
    	String netId = "uxr130130";
    	StringBuilder builder = new StringBuilder();
        Text to_value = new Text("");
        if (input != null) {
            try {
            	String inputString = input.toString();
                String[] tokens = inputString.split("\\|");
                if(tokens.length == 1) {
                	return new Text(tokens[0]);
                }
                inputString = inputString.replaceAll("\\|", ", ");
                int lastIndexOfComma = inputString.lastIndexOf(", ");
                return new Text(inputString.substring(0,  lastIndexOfComma + 1) + " & " + inputString.substring(lastIndexOfComma + 2) + " - " + netId);
            } catch (Exception e) { // Should never happen
                to_value = new Text(input);
            }
        }
        return to_value;
    }
}
