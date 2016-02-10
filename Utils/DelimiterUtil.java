package Utils;

import java.util.regex.Pattern;

public class DelimiterUtil {
	public static final String POSW = "w";
	public static final String POST = "T";
	public static final String MARK = "M";
	public static final String flag = ";;";
	public static final String DELIMITER = " ";
	public static final Pattern DELIMITER_PATTERN = Pattern.compile("\\" + DELIMITER);	
	
}
