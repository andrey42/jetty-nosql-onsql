package org.eclipse.jetty.nosql.onsql;

import java.util.Calendar;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;

  
public class Base62Converter {
 
	public static final String ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
 
	public static final int BASE = ALPHABET.length();
 
	private Base62Converter() {}
 
	public static String fromBase10(long i) {
                if (i == 0) return "0";
		StringBuilder sb = new StringBuilder("");
	        boolean negative = (i < 0);
                if (negative) i= -i;
		while (i > 0) {
			i = fromBase10(i, sb);
		}
                //if (negative) sb.insert(0, "_");
                sb = sb.reverse();
                if (negative) sb.insert(0,'_');
		return sb.toString();
	}
 
	private static long fromBase10(long i, final StringBuilder sb) {                
		long rem = i % BASE;
		sb.append(ALPHABET.charAt(Integer.parseInt(new Long(rem).toString())));
		return i / BASE;
	}
 
	public static long toBase10(String str) {
                StringBuilder sb = new StringBuilder(str);
                boolean negative = false;
                if (sb.indexOf("_")==0) {negative = true; sb.deleteCharAt(0); }
                long retval = toBase10(sb.reverse().toString().toCharArray());
                if (negative) retval = -retval;
		return retval;
	}
 
	private static long toBase10(char[] chars) {
		long n = 0;
		for (int i = chars.length - 1; i >= 0; i--) {
			n += toBase10(ALPHABET.indexOf(chars[i]), i);
		}
		return n;
	}
 
	private static long toBase10(long n, int pow) {
		return n * (long) Math.pow(BASE, pow);
	}
    
	public static String longToLexiSortableBase62(long input) {
    	return StringUtils.leftPad(Base62Converter.fromBase10(input), 11, "0");
    }
    
    public static void main(String[] args) {
        long long_val = Long.MAX_VALUE;  //AzL8n0Y58m7
        log("max long value="+long_val);
        //l8000014D1F9F26C1
        //AzL8n0Y58m7
        String base62str = Base62Converter.fromBase10(long_val);
        log("base62encoded=   "+base62str);
        log("base62decoded=   "+Base62Converter.toBase10(base62str));
        log("---");
        long int_val = Integer.MAX_VALUE;  //AzL8n0Y58m7
        log("max int value="+int_val);
        base62str = Base62Converter.fromBase10(int_val);
        log("base62encoded=   "+base62str);
        log("base62decoded=   "+Base62Converter.toBase10(base62str));
    }
    
    public static String getDayBase62KeyPart(long p_timestamp) {
    	Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    	c.setTimeInMillis(p_timestamp);
    	c.set(Calendar.HOUR_OF_DAY, 0);
    	c.set(Calendar.MINUTE, 0);
    	c.set(Calendar.SECOND, 0);
    	c.set(Calendar.MILLISECOND, 0);
    	//log("dec day time="+c.getTimeInMillis()/100000);
    	return StringUtils.leftPad(Base62Converter.fromBase10(c.getTimeInMillis()/100000), 5, "0");
    }
    
    public static String getTimeOfDayBase62KeyPart(long p_timestamp) {
    	Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    	c.setTimeInMillis(p_timestamp);
    	long ltime = c.get(Calendar.HOUR_OF_DAY) * 60 *60 * 1000	    		   
    		   + c.get(Calendar.MINUTE) * 60 * 1000
    		   + c.get(Calendar.SECOND) * 1000
    		   + c.get(Calendar.MILLISECOND);
    	//log("dec hours time="+l);
    	return StringUtils.leftPad(Base62Converter.fromBase10(ltime), 5, "0");
    }
    
    public static String reconstructUnixtimestampFromTwoParts(String p_day_keypart,String p_time_keypart) {
    	//log("reconstructed day time="+ Base62Converter.toBase10(p_day_keypart));
    	//log("reconstructed dof time="+Base62Converter.toBase10(p_time_keypart));
    	return new Long(
    			Base62Converter.toBase10(p_day_keypart)*100000
    	+ Base62Converter.toBase10(p_time_keypart)).toString();
    }
    private static void log(String info) {
    	System.out.println(info);
    }
}