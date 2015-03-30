import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class Question4UDF extends EvalFunc<String>{

	@Override
	public String exec(Tuple input){
		String netId = "uxr130130";		
		try {
            if (input == null || input.size() == 0) {
                return null;
            }

            String inputString = (String) input.get(0);
            String[] tokens = inputString.split("\\|");
            
            if(tokens.length == 1) {
            	return new StringBuilder(tokens[0] + " " + netId).toString();
            }
            
            if(tokens.length == 2) {
            	return new StringBuilder(tokens[1] + " & " + tokens[0] + " " + netId).toString();
            }
            
            inputString = inputString.replaceAll("\\|", ",");
            int lastIndexOfComma = inputString.lastIndexOf(",");
            return new StringBuilder(inputString.substring(0,  lastIndexOfComma) + " & " + inputString.substring(lastIndexOfComma + 1) + " " + netId).toString();
            
           
        } catch (ExecException ex) {
            System.out.println("Error: " + ex.toString());
        }
		return null;
	}
	
}
